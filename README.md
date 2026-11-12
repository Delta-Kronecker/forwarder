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
