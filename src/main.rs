//! Standalone TCP forwarder.
//!
//! Listens on a local address, accepts TCP connections, and relays all traffic
//! verbatim to a configured target. No packet interception, no DPI handling.
//!
//! Each accepted connection is handled on its own tokio task; the relay is a
//! bidirectional copy between the inbound and outbound sockets.
//!
//! # Upstream IP rotation
//!
//! When `<TARGET>` is omitted, the upstream address for every connection is
//! taken from `ip.txt` next to the executable: one upstream IP per line. Each
//! accepted connection gets the next unused IP and no IP is ever reused for the
//! lifetime of the process. Once the list is exhausted, new connections are
//! refused.
//!
//! # SOCKS5 proxy
//!
//! Upstream connections are dialled through a SOCKS5 proxy, defaulting to
//! `127.0.0.1:9050`. Repeat `--proxy` to chain several hops, or pass
//! `--no-proxy` to connect directly.

use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use clap::Parser;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpSocket, TcpStream};
use tracing::{debug, info, warn};

/// The upstream port to connect to by default.
pub const CONNECT_PORT: u16 = 443;

/// Per-direction relay copy buffer. Larger buffers reduce syscall overhead
/// and inter-task wakeups under high concurrency.
const COPY_BUF_SIZE: usize = 128 * 1024;

/// Listener accept backlog: the kernel queues this many pending connections
/// while the accept task is busy, absorbing bursts like 100 parallel connects.
const LISTEN_BACKLOG: u32 = 1024;

/// Default name of the upstream-IP list, resolved next to the executable.
const DEFAULT_IP_FILE: &str = "ip.txt";

/// Default SOCKS5 proxy (e.g. a local Tor daemon on 9050).
const DEFAULT_PROXY: &str = "127.0.0.1:9050";

/// SOCKS5 protocol constants (RFC 1928 / RFC 1929).
const SOCKS5_VERSION: u8 = 0x05;
const SOCKS5_AUTH_NONE: u8 = 0x00;
const SOCKS5_AUTH_USERPASS: u8 = 0x02;
const SOCKS5_CMD_CONNECT: u8 = 0x01;
const SOCKS5_ATYP_IPV4: u8 = 0x01;
const SOCKS5_ATYP_DOMAIN: u8 = 0x03;
const SOCKS5_ATYP_IPV6: u8 = 0x04;

#[derive(Debug, Parser)]
#[command(name = "forwarder", about = "Accept TCP on a local listen address and relay it to an upstream IP")]
struct Args {
    /// Fixed upstream IPv4/IPv6 address (or hostname) to forward to. When
    /// omitted, upstream IPs are read from --ip-file and rotated per
    /// connection (each IP used once).
    target: Option<String>,
    /// Upstream port to connect to.
    #[arg(long, default_value_t = CONNECT_PORT)]
    port: u16,
    /// Local address:port to listen on.
    #[arg(long, default_value = "127.0.0.1:40443")]
    listen: String,
    /// File with one upstream IP per line, used when <TARGET> is omitted.
    /// Resolved next to the executable when relative.
    #[arg(long, default_value = DEFAULT_IP_FILE)]
    ip_file: PathBuf,
    /// SOCKS5 proxy to reach the upstream through. Repeatable; hops are chained
    /// in order. Format: [user:pass@]host:port
    #[arg(long = "proxy", value_name = "SPEC", default_value = DEFAULT_PROXY)]
    proxy: Vec<String>,
    /// Dial the upstream directly, ignoring the default proxy.
    #[arg(long)]
    no_proxy: bool,
    /// Force-close each relay after this many seconds (0 disables).
    #[arg(long, default_value_t = 0)]
    max_lifetime: u64,
    /// Number of tokio worker threads.
    #[arg(long, default_value_t = 8)]
    workers: usize,
}

fn configured_relay_max_lifetime(secs: u64) -> Option<Duration> {
    (secs > 0).then(|| Duration::from_secs(secs))
}

async fn resolve_target(raw: &str, port: u16) -> anyhow::Result<SocketAddr> {
    if let Ok(ip) = raw.parse::<IpAddr>() {
        return Ok(SocketAddr::new(ip, port));
    }
    tokio::net::lookup_host((raw, port))
        .await
        .with_context(|| format!("resolving target '{raw}'"))?
        .next()
        .with_context(|| format!("no addresses found for '{raw}'"))
}

/// A fixed list of upstream addresses, handed out one at a time.
///
/// `next` only ever moves forward, so no index is handed out twice and every
/// address is used for at most one connection per process run.
struct IpPool {
    addrs: Vec<SocketAddr>,
    next: AtomicUsize,
}

impl IpPool {
    /// Take the next unused upstream address, or `None` when exhausted.
    fn take(&self) -> Option<SocketAddr> {
        let idx = self.next.fetch_add(1, Ordering::Relaxed);
        self.addrs.get(idx).copied()
    }
}

/// Where each connection's upstream address comes from.
enum TargetSource {
    /// A single fixed address (`<TARGET>` was given).
    Fixed(SocketAddr),
    /// A one-shot pool from `ip.txt` (`<TARGET>` was omitted).
    Pool(Arc<IpPool>),
}

impl TargetSource {
    /// Pick the upstream address for one connection.
    fn next_address(&self) -> Option<SocketAddr> {
        match self {
            TargetSource::Fixed(addr) => Some(*addr),
            TargetSource::Pool(pool) => pool.take(),
        }
    }
}

/// Resolve a (possibly relative) IP-list path next to the executable.
///
/// Relative paths prefer the executable's directory; if that file does not
/// exist we fall back to the current working directory, which keeps
/// `cargo run` usable during development.
fn resolve_ip_file(path: &Path) -> PathBuf {
    if path.is_absolute() {
        return path.to_path_buf();
    }
    if let Some(dir) = std::env::current_exe()
        .ok()
        .and_then(|exe| exe.parent().map(Path::to_path_buf))
    {
        let candidate = dir.join(path);
        if candidate.exists() {
            return candidate;
        }
    }
    path.to_path_buf()
}

/// Parse the upstream-IP list: one address per line, blank lines and `#`
/// comments ignored. Leading/trailing whitespace is trimmed.
fn load_ip_pool(path: &Path, port: u16) -> anyhow::Result<IpPool> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("reading upstream IP list {}", path.display()))?;

    let mut addrs = Vec::new();
    for (i, raw) in text.lines().enumerate() {
        let line = raw.split('#').next().unwrap_or("").trim();
        if line.is_empty() {
            continue;
        }
        let ip: IpAddr = line
            .parse()
            .with_context(|| format!("{}:{}: invalid IP '{line}'", path.display(), i + 1))?;
        addrs.push(SocketAddr::new(ip, port));
    }

    if addrs.is_empty() {
        anyhow::bail!("upstream IP list {} contains no addresses", path.display());
    }

    Ok(IpPool {
        addrs,
        next: AtomicUsize::new(0),
    })
}

/// One hop in the SOCKS5 chain: where to connect and how to authenticate.
struct ProxyHop {
    addr: SocketAddr,
    user: Option<String>,
    pass: Option<String>,
}

/// Split `host:port`, accepting bracketed IPv6 (`[::1]:1080`).
fn split_host_port(spec: &str) -> anyhow::Result<(&str, u16)> {
    let spec = spec.trim();
    if let Some(rest) = spec.strip_prefix('[') {
        let (host, after) = rest
            .split_once(']')
            .context("missing ']' in IPv6 proxy address")?;
        let port = after
            .strip_prefix(':')
            .context("missing ':port' after ']'")?
            .parse()
            .context("invalid proxy port")?;
        return Ok((host, port));
    }
    let (host, port) = spec
        .rsplit_once(':')
        .context("expected host:port")?;
    Ok((host, port.parse().context("invalid proxy port")?))
}

/// Resolve one `[user:pass@]host:port` proxy spec into a concrete hop.
async fn resolve_proxy(spec: &str) -> anyhow::Result<ProxyHop> {
    let (creds, hostport) = match spec.rsplit_once('@') {
        Some((creds, hostport)) => (Some(creds), hostport),
        None => (None, spec),
    };
    let (user, pass) = match creds {
        Some(creds) => {
            let (user, pass) = creds
                .split_once(':')
                .with_context(|| format!("proxy '{spec}': credentials must be user:pass"))?;
            (Some(user.to_string()), Some(pass.to_string()))
        }
        None => (None, None),
    };

    let (host, port) =
        split_host_port(hostport).with_context(|| format!("proxy '{spec}'"))?;
    let addr = match host.parse::<IpAddr>() {
        Ok(ip) => SocketAddr::new(ip, port),
        Err(_) => tokio::net::lookup_host((host, port))
            .await
            .with_context(|| format!("resolving proxy '{host}'"))?
            .next()
            .with_context(|| format!("no addresses found for proxy '{host}'"))?,
    };

    Ok(ProxyHop { addr, user, pass })
}

async fn resolve_proxies(specs: &[String]) -> anyhow::Result<Vec<ProxyHop>> {
    let mut hops = Vec::with_capacity(specs.len());
    for spec in specs {
        hops.push(resolve_proxy(spec).await?);
    }
    Ok(hops)
}

/// Perform a SOCKS5 CONNECT handshake on an already-connected `stream`,
/// authenticating to the proxy at the far end with `hop`'s credentials, and
/// return the same `stream` now tunnelled to `target`.
async fn socks5_connect(
    mut stream: TcpStream,
    target: SocketAddr,
    hop: &ProxyHop,
) -> anyhow::Result<TcpStream> {
    // Greeting: offer no-auth, plus user/pass when credentials are configured.
    let mut methods = vec![SOCKS5_AUTH_NONE];
    if hop.user.is_some() {
        methods.push(SOCKS5_AUTH_USERPASS);
    }
    let mut greeting = Vec::with_capacity(2 + methods.len());
    greeting.push(SOCKS5_VERSION);
    greeting.push(methods.len() as u8);
    greeting.extend_from_slice(&methods);
    stream
        .write_all(&greeting)
        .await
        .context("sending SOCKS5 greeting")?;

    let mut chosen = [0u8; 2];
    stream
        .read_exact(&mut chosen)
        .await
        .context("reading SOCKS5 greeting reply")?;
    if chosen[0] != SOCKS5_VERSION {
        anyhow::bail!("SOCKS5: unexpected version {:#04x}", chosen[0]);
    }
    match chosen[1] {
        SOCKS5_AUTH_NONE => {}
        SOCKS5_AUTH_USERPASS => {
            let user = hop.user.as_deref().unwrap_or("");
            let pass = hop.pass.as_deref().unwrap_or("");
            let mut auth = Vec::with_capacity(3 + user.len() + pass.len());
            auth.push(0x01); // username/password auth subnegotiation version
            auth.push(user.len() as u8);
            auth.extend_from_slice(user.as_bytes());
            auth.push(pass.len() as u8);
            auth.extend_from_slice(pass.as_bytes());
            stream
                .write_all(&auth)
                .await
                .context("sending SOCKS5 credentials")?;

            let mut auth_reply = [0u8; 2];
            stream
                .read_exact(&mut auth_reply)
                .await
                .context("reading SOCKS5 auth reply")?;
            if auth_reply[1] != 0x00 {
                anyhow::bail!("SOCKS5: authentication failed (status {:#04x})", auth_reply[1]);
            }
        }
        method => anyhow::bail!("SOCKS5: no acceptable auth method (server chose {method:#04x})"),
    }

    // CONNECT request.
    let mut request = Vec::with_capacity(4 + 16 + 2);
    request.push(SOCKS5_VERSION);
    request.push(SOCKS5_CMD_CONNECT);
    request.push(0x00); // reserved
    match target {
        SocketAddr::V4(v4) => {
            request.push(SOCKS5_ATYP_IPV4);
            request.extend_from_slice(&v4.ip().octets());
        }
        SocketAddr::V6(v6) => {
            request.push(SOCKS5_ATYP_IPV6);
            request.extend_from_slice(&v6.ip().octets());
        }
    }
    request.extend_from_slice(&target.port().to_be_bytes());
    stream
        .write_all(&request)
        .await
        .context("sending SOCKS5 CONNECT")?;

    // Reply: VER REP RSV ATYP BND.ADDR BND.PORT
    let mut head = [0u8; 4];
    stream
        .read_exact(&mut head)
        .await
        .context("reading SOCKS5 CONNECT reply")?;
    if head[0] != SOCKS5_VERSION {
        anyhow::bail!("SOCKS5: unexpected reply version {:#04x}", head[0]);
    }
    if head[1] != 0x00 {
        anyhow::bail!("SOCKS5: connect failed (reply {:#04x})", head[1]);
    }
    match head[3] {
        SOCKS5_ATYP_IPV4 => {
            let mut bound = [0u8; 4];
            stream.read_exact(&mut bound).await.context("SOCKS5 bound address")?;
        }
        SOCKS5_ATYP_IPV6 => {
            let mut bound = [0u8; 16];
            stream.read_exact(&mut bound).await.context("SOCKS5 bound address")?;
        }
        SOCKS5_ATYP_DOMAIN => {
            let mut len = [0u8; 1];
            stream.read_exact(&mut len).await.context("SOCKS5 bound address")?;
            let mut bound = vec![0u8; len[0] as usize];
            stream.read_exact(&mut bound).await.context("SOCKS5 bound address")?;
        }
        atyp => anyhow::bail!("SOCKS5: bad bound address type {atyp:#04x}"),
    }
    let mut bound_port = [0u8; 2];
    stream
        .read_exact(&mut bound_port)
        .await
        .context("SOCKS5 bound port")?;

    Ok(stream)
}

/// Connect to `target`, optionally through a chain of SOCKS5 proxies.
async fn connect_upstream(
    proxies: &[ProxyHop],
    target: SocketAddr,
) -> anyhow::Result<TcpStream> {
    let Some((first, rest)) = proxies.split_first() else {
        return TcpStream::connect(target)
            .await
            .with_context(|| format!("connect {target}"));
    };

    let mut stream = TcpStream::connect(first.addr)
        .await
        .with_context(|| format!("connect proxy {}", first.addr))?;
    let _ = stream.set_nodelay(true);

    // Each hop is reached by CONNECTing through the previous proxy.
    let mut peer = first;
    for hop in rest {
        stream = socks5_connect(stream, hop.addr, peer)
            .await
            .with_context(|| format!("SOCKS5 hop to proxy {}", hop.addr))?;
        peer = hop;
    }
    socks5_connect(stream, target, peer)
        .await
        .with_context(|| format!("SOCKS5 to target {target}"))
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    let workers = args.workers;

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_all()
        .build()
        .context("building tokio runtime")?;

    let result = runtime.block_on(run(args));

    // When launched by double-clicking (no arguments) Windows would close the
    // console as soon as we return, hiding the error. Keep it open for a moment
    // so the message can be read.
    if result.is_err() && std::env::args_os().count() <= 1 {
        if let Err(e) = &result {
            eprintln!("\nforwarder stopped:\n  {e:#}\n");
        }
        eprintln!("Press Enter to close this window...");
        let mut line = String::new();
        let _ = std::io::stdin().read_line(&mut line);
    }

    result
}

async fn run(args: Args) -> anyhow::Result<()> {
    let relay_max_lifetime = configured_relay_max_lifetime(args.max_lifetime);
    let proxies = if args.no_proxy {
        Vec::new()
    } else {
        resolve_proxies(&args.proxy).await?
    };
    let proxies = Arc::new(proxies);
    if proxies.is_empty() {
        info!("connecting upstream directly (no proxy)");
    } else {
        let hops: Vec<SocketAddr> = proxies.iter().map(|h| h.addr).collect();
        info!(hops = hops.len(), ?hops, "routing upstream through SOCKS5 proxy chain");
    }

    let target_source = match args.target.as_deref() {
        Some(target) => {
            let addr = resolve_target(target, args.port).await?;
            info!(target = %addr, listen = %args.listen, max_lifetime = ?relay_max_lifetime, "forwarder starting");
            TargetSource::Fixed(addr)
        }
        None => {
            let path = resolve_ip_file(&args.ip_file);
            let pool = Arc::new(load_ip_pool(&path, args.port)?);
            info!(
                ip_file = %path.display(),
                upstream_ips = pool.addrs.len(),
                port = args.port,
                listen = %args.listen,
                max_lifetime = ?relay_max_lifetime,
                "forwarder starting (rotating upstream IPs, one per connection)"
            );
            TargetSource::Pool(pool)
        }
    };

    run_forwarder(args.listen, target_source, proxies, relay_max_lifetime).await
}

/// Listen for inbound connections and relay each one to its upstream address.
///
/// The upstream address is chosen per connection; the accept loop stays hot by
/// spawning every connection as its own tokio task.
async fn run_forwarder(
    listen: String,
    target_source: TargetSource,
    proxies: Arc<Vec<ProxyHop>>,
    relay_max_lifetime: Option<Duration>,
) -> anyhow::Result<()> {
    let listen_addr: SocketAddr = listen.parse().context("invalid listen address")?;
    let socket = match listen_addr {
        SocketAddr::V4(_) => TcpSocket::new_v4()?,
        SocketAddr::V6(_) => TcpSocket::new_v6()?,
    };
    // SO_REUSEADDR lets us rebind immediately after a restart instead of
    // waiting out TIME_WAIT; the large backlog queues bursts of connects.
    socket.set_reuseaddr(true)?;
    socket.bind(listen_addr)?;
    let listener = socket
        .listen(LISTEN_BACKLOG)
        .with_context(|| format!("listen {listen_addr}"))?;
    match &target_source {
        TargetSource::Fixed(target) => {
            info!(%listen_addr, %target, backlog = LISTEN_BACKLOG, "listening");
        }
        TargetSource::Pool(pool) => {
            info!(%listen_addr, upstream_ips = pool.addrs.len(), backlog = LISTEN_BACKLOG, "listening");
        }
    }

    loop {
        let (incoming, peer) = match listener.accept().await {
            Ok(x) => x,
            Err(e) => {
                warn!(error = %e, "accept failed");
                continue;
            }
        };
        debug!(%peer, "accepted");

        // Each connection gets its own upstream IP; a pool address is never
        // handed out twice, so once empty we refuse further connections.
        let target = match target_source.next_address() {
            Some(addr) => addr,
            None => {
                warn!(%peer, "upstream IP pool exhausted, refusing connection");
                continue;
            }
        };

        let proxies = Arc::clone(&proxies);
        tokio::spawn(async move {
            if let Err(e) =
                handle_connection(target, incoming, peer, proxies, relay_max_lifetime).await
            {
                warn!(%peer, error = %e, "connection failed");
            }
        });
    }
}

/// Relay one accepted connection to the target.
async fn handle_connection(
    connect_addr: SocketAddr,
    incoming: TcpStream,
    peer: SocketAddr,
    proxies: Arc<Vec<ProxyHop>>,
    relay_max_lifetime: Option<Duration>,
) -> anyhow::Result<()> {
    let via_proxy = !proxies.is_empty();
    info!(%peer, upstream = %connect_addr, via_proxy, "forwarding connection");

    // TCP_NODELAY disables Nagle so small request/response exchanges don't
    // wait on delayed ACKs.
    let _ = incoming.set_nodelay(true);
    let outgoing = connect_upstream(&proxies, connect_addr)
        .await
        .with_context(|| format!("connect upstream {connect_addr}"))?;
    let _ = outgoing.set_nodelay(true);

    let relay = counting_relay(incoming, outgoing, relay_max_lifetime).await;
    info!(
        %peer,
        upstream = %connect_addr,
        via_proxy,
        c2s_bytes = relay.c2s_bytes,
        s2c_bytes = relay.s2c_bytes,
        reason = ?relay.reason,
        "relay finished"
    );

    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelayEndReason {
    Completed,
    MaxLifetime,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RelayResult {
    c2s_bytes: u64,
    s2c_bytes: u64,
    reason: RelayEndReason,
}

/// Run a bidirectional relay between `incoming` and `outgoing`.
async fn counting_relay(
    incoming: TcpStream,
    outgoing: TcpStream,
    max_lifetime: Option<Duration>,
) -> RelayResult {
    let (inc_rd, inc_wr) = incoming.into_split();
    let (out_rd, out_wr) = outgoing.into_split();

    let c2s_atomic = Arc::new(AtomicU64::new(0));
    let s2c_atomic = Arc::new(AtomicU64::new(0));

    let mut c2s_task = tokio::spawn(copy_counting(inc_rd, out_wr, c2s_atomic.clone()));
    let mut s2c_task = tokio::spawn(copy_counting(out_rd, inc_wr, s2c_atomic.clone()));

    let result = if let Some(max_lifetime) = max_lifetime {
        let mut c2s_done: Option<u64> = None;
        let mut s2c_done: Option<u64> = None;
        let deadline = tokio::time::sleep(max_lifetime);
        tokio::pin!(deadline);

        loop {
            tokio::select! {
                _ = &mut deadline => {
                    if c2s_done.is_none() {
                        c2s_task.abort();
                    }
                    if s2c_done.is_none() {
                        s2c_task.abort();
                    }
                    break RelayResult {
                        c2s_bytes: c2s_done.unwrap_or_else(|| c2s_atomic.load(Ordering::Relaxed)),
                        s2c_bytes: s2c_done.unwrap_or_else(|| s2c_atomic.load(Ordering::Relaxed)),
                        reason: RelayEndReason::MaxLifetime,
                    };
                }
                c2s_result = &mut c2s_task, if c2s_done.is_none() => {
                    c2s_done = Some(c2s_result.unwrap_or(0));
                    if let (Some(c2s_bytes), Some(s2c_bytes)) = (c2s_done, s2c_done) {
                        break RelayResult {
                            c2s_bytes,
                            s2c_bytes,
                            reason: RelayEndReason::Completed,
                        };
                    }
                }
                s2c_result = &mut s2c_task, if s2c_done.is_none() => {
                    s2c_done = Some(s2c_result.unwrap_or(0));
                    if let (Some(c2s_bytes), Some(s2c_bytes)) = (c2s_done, s2c_done) {
                        break RelayResult {
                            c2s_bytes,
                            s2c_bytes,
                            reason: RelayEndReason::Completed,
                        };
                    }
                }
            }
        }
    } else {
        let (c2s_result, s2c_result) = tokio::join!(c2s_task, s2c_task);
        RelayResult {
            c2s_bytes: c2s_result.unwrap_or(0),
            s2c_bytes: s2c_result.unwrap_or(0),
            reason: RelayEndReason::Completed,
        }
    };

    result
}

/// Copy all bytes from `reader` to `writer`, updating `counter` after each
/// chunk. Shuts down `writer` gracefully on EOF or error, then returns the
/// total bytes copied.
async fn copy_counting(
    mut reader: tokio::net::tcp::OwnedReadHalf,
    mut writer: tokio::net::tcp::OwnedWriteHalf,
    counter: Arc<AtomicU64>,
) -> u64 {
    let mut buf = vec![0u8; COPY_BUF_SIZE];
    let mut total = 0u64;
    loop {
        let n = match reader.read(&mut buf).await {
            Ok(0) | Err(_) => break,
            Ok(n) => n,
        };
        if writer.write_all(&buf[..n]).await.is_err() {
            break;
        }
        total += n as u64;
        counter.store(total, Ordering::Relaxed);
    }
    let _ = writer.shutdown().await;
    total
}
