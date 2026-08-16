//! Standalone TCP forwarder.
//!
//! Listens on a local address, accepts TCP connections, and relays all traffic
//! verbatim to a configured target. No packet interception, no DPI handling.
//!
//! Each accepted connection is handled on its own tokio task; the relay is a
//! bidirectional copy between the inbound and outbound sockets.

use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicU64, Ordering};
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

#[derive(Debug, Parser)]
#[command(name = "forwarder", about = "Accept TCP on a local listen address and relay it to a target IP")]
struct Args {
    /// Target IPv4/IPv6 address (or hostname) to forward traffic to.
    target: String,
    /// Upstream port to connect to.
    #[arg(long, default_value_t = CONNECT_PORT)]
    port: u16,
    /// Local address:port to listen on.
    #[arg(long, default_value = "127.0.0.1:44444")]
    listen: String,
    /// Force-close each relay after this many seconds (0 disables).
    #[arg(long, default_value_t = 0)]
    max_lifetime: u64,
    /// Number of tokio worker threads. Defaults to the CPU count.
    #[arg(long)]
    workers: Option<usize>,
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

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    let workers = args.workers.unwrap_or_else(|| {
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
    });

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_all()
        .build()
        .context("building tokio runtime")?;
    runtime.block_on(run(args))
}

async fn run(args: Args) -> anyhow::Result<()> {
    let target = resolve_target(&args.target, args.port).await?;
    let relay_max_lifetime = configured_relay_max_lifetime(args.max_lifetime);
    info!(target = %target, listen = %args.listen, max_lifetime = ?relay_max_lifetime, "forwarder starting");

    run_forwarder(args.listen, target, relay_max_lifetime).await
}

/// Listen for inbound connections and relay each one to `target`.
///
/// The target address is fixed for the lifetime of the process; the accept
/// loop stays hot by spawning every connection as its own tokio task.
async fn run_forwarder(
    listen: String,
    target: SocketAddr,
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
    info!(%listen_addr, %target, backlog = LISTEN_BACKLOG, "listening");

    loop {
        let (incoming, peer) = match listener.accept().await {
            Ok(x) => x,
            Err(e) => {
                warn!(error = %e, "accept failed");
                continue;
            }
        };
        debug!(%peer, "accepted");

        tokio::spawn(async move {
            if let Err(e) = handle_connection(target, incoming, peer, relay_max_lifetime).await {
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
    relay_max_lifetime: Option<Duration>,
) -> anyhow::Result<()> {
    debug!(%peer, %connect_addr, "forwarding connection");

    // TCP_NODELAY disables Nagle so small request/response exchanges don't
    // wait on delayed ACKs.
    let _ = incoming.set_nodelay(true);
    let outgoing = match TcpStream::connect(connect_addr).await {
        Ok(s) => s,
        Err(e) => {
            return Err(e).context("connect upstream");
        }
    };
    let _ = outgoing.set_nodelay(true);

    let relay = counting_relay(incoming, outgoing, relay_max_lifetime).await;
    debug!(
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
