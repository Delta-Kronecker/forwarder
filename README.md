# forwarder

A minimal standalone TCP forwarder. It listens on a local address, accepts TCP
connections, and relays all traffic verbatim to a given target IP — no DPI
manipulation, no packet interception.

## Usage

```
forwarder.exe <target> [options]
```

| Option | Default | Description |
| ------ | ------- | ----------- |
| `<target>` | — | Target IPv4/IPv6 address (or hostname) |
| `--port` | `443` | Upstream port to connect to |
| `--listen` | `127.0.0.1:44444` | Local address:port to listen on |
| `--max-lifetime` | `0` | Force-close each relay after N seconds (`0` disables) |
| `--workers` | CPU count | Number of tokio worker threads |

Examples:

```
forwarder.exe 104.17.225.49
forwarder.exe 1.2.3.4 --port 443 --listen 127.0.0.1:40443 --workers 8
```

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
