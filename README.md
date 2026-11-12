# forwarder

A minimal standalone TCP forwarder. It listens on a local address, accepts TCP
connections, and relays all traffic verbatim to a fixed upstream IP or, when no
target is given, to a different IP from `ip.txt` per connection — no DPI
manipulation, no packet interception.

## Usage

```
forwarder.exe [target] [options]
```

| Option | Default | Description |
| ------ | ------- | ----------- |
| `[target]` | — | Fixed upstream IPv4/IPv6 address (or hostname). Omit it to rotate through the IPs in `ip.txt`, one per connection |
| `--port` | `443` | Upstream port to connect to |
| `--listen` | `127.0.0.1:40443` | Local address:port to listen on |
| `--ip-file` | `ip.txt` | Upstream IP list, resolved next to the executable when relative |
| `--proxy` | `127.0.0.1:9050` | SOCKS5 proxy to reach the upstream through. Repeatable to chain hops. Format: `[user:pass@]host:port` |
| `--no-proxy` | — | Dial the upstream directly, ignoring the default proxy |
| `--max-lifetime` | `0` | Force-close each relay after N seconds (`0` disables) |
| `--workers` | `8` | Number of tokio worker threads |

Examples:

```
forwarder.exe 104.17.225.49
forwarder.exe 1.2.3.4 --port 443 --listen 127.0.0.1:40443 --workers 8
forwarder.exe --port 443 --listen 127.0.0.1:40443 --workers 8
```

Running `forwarder.exe` with no arguments (or double-clicking it) is the same
as:

```
forwarder.exe --port 443 --listen 127.0.0.1:40443 --workers 8
```

So a double-click just needs an `ip.txt` next to the executable. If it fails to
start, the console stays open showing the error.

## Upstream IP rotation (`ip.txt`)

If you omit `<target>`, the forwarder reads `ip.txt` (next to the executable by
default) and uses a different upstream IP for every connection. One IP per line;
blank lines and `#` comments are ignored:

```
23.227.39.200
23.227.38.74
23.227.38.65
```

Each accepted connection connects to the next unused IP, and **no IP is ever
reused for the lifetime of the process**. A list of `N` IPs therefore serves
exactly `N` connections per launch; once the list is exhausted, further
connections are accepted and immediately closed. Restart the process (with a
fresh list) to continue.

Relative paths are resolved against the executable's directory first, falling
back to the current working directory (handy for `cargo run`). Override the
location with `--ip-file`. A template is provided as `ip.txt.example`.

## SOCKS5 proxy chain

Pass one or more SOCKS5 proxies with `--proxy`; each upstream connection is then
dialled through them, in the order given. **The default is `127.0.0.1:9050`**
(the usual Tor SOCKS port), so with no arguments every upstream connection goes
through a local Tor instance. With a single proxy it is a plain
`forwarder → proxy → upstream` chain; repeat `--proxy` to tunnel through several
hops, and the last proxy opens the connection to the upstream IP:

```
forwarder.exe --proxy 127.0.0.1:9050
forwarder.exe --proxy 10.0.0.1:1080 --proxy 10.0.0.2:1080
forwarder.exe --proxy alice:s3cret@127.0.0.1:1080
forwarder.exe --no-proxy        # connect straight to the upstream
```

Format is `[user:pass@]host:port`. Credentials use SOCKS5 username/password
auth (RFC 1929) and are offered only when set; otherwise no-auth is used. IPv6
proxies use brackets, e.g. `[::1]:1080`. Hostnames are resolved at startup.

Use `--no-proxy` to skip the default and dial the upstream directly.

## Logging

Every accepted connection is logged at `info` level together with the upstream
IP it was relayed to, and again when the relay finishes:

```
INFO forwarding connection peer=127.0.0.1:52344 upstream=23.227.38.74
INFO relay finished peer=127.0.0.1:52344 upstream=23.227.38.74 c2s_bytes=812 s2c_bytes=4096 reason=Completed
```

Set `RUST_LOG=debug` for extra detail (accept events), or `RUST_LOG=off` to
silence everything.

## Build

```
cargo +stable-x86_64-pc-windows-gnu build --release
```

The binary is written to `target/release/forwarder.exe`. The GNU toolchain
(mingw-w64, e.g. via `choco install mingw`) must be on `PATH`; MSVC builds
require Visual Studio C++ build tools.

## Architecture

- accept loop — binds the listener (`SO_REUSEADDR`, backlog 1024) and spawns a
  tokio task per connection
- upstream selection — a fixed `<target>`, or the next unused IP from `ip.txt`
  (never reused)
- dialer — direct TCP, or a chained SOCKS5 CONNECT handshake (`--proxy`)
- per-connection handler — connects upstream, sets `TCP_NODELAY`, relays
- bidirectional relay — two copy tasks, one per direction, with graceful
  shutdown and optional max lifetime
- copy loop — 128 KiB buffers with byte counting

## Performance

Verified against a concurrent in-process upstream server on loopback:

| Concurrent requests | Result | Time | Rate |
| ------------------- | ------ | ---- | ---- |
| 200                 | 200/200 OK | 0.18s | ~1100 req/s |
| 1000                | 1000/1000 OK | 0.98s | ~1020 req/s |

Tuning: `--workers`, `SO_REUSEADDR`, listener backlog 1024, `TCP_NODELAY` on
both sockets, 128 KiB copy buffers.
