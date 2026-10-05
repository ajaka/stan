# stan

[![CI](https://github.com/ajaka/stan/actions/workflows/ci.yml/badge.svg)](https://github.com/ajaka/stan/actions/workflows/ci.yml)
[![license](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![edition](https://img.shields.io/badge/rust-2024-orange.svg)](https://doc.rust-lang.org/edition-guide/rust-2024/)
[![unsafe](https://img.shields.io/badge/unsafe-none-brightgreen.svg)](#at-a-glance)

A publish/subscribe server in Rust: a hand-rolled binary wire protocol, radix-tree
topic routing that needs no locks, queue-group load balancing, constant-time token
authentication, and coordinated graceful shutdown.

**0 unsafe** · 99 tests over real loopback TCP · CI: fmt, clippy `-D warnings`,
Linux/macOS/Windows, MSRV 1.85 · MIT

```bash
cargo run      # 127.0.0.1:4222
cargo test
```

## At a glance

|              |                                                    |
| ------------ | -------------------------------------------------- |
| Language     | Rust 2024, MSRV 1.85                               |
| Unsafe code  | none, `#![forbid(unsafe_code)]`                    |
| Concurrency  | single-writer actor, no locks on the routing path  |
| Protocol     | length-prefixed binary frames, big-endian          |
| Delivery     | at-most-once, fire-and-forget                      |
| Dependencies | `tokio`, `serde`, `serde_yaml`, `subtle`, `anyhow` |

## What it does

- **Topic wildcards** — `foo.*` matches exactly one token, `foo.>` matches one or
  more, `>` matches everything. `>` is rejected in any non-final position, so it can
  never mask a subtree.
- **Queue groups** — messages are distributed round-robin across the members of a
  group, so work is load-balanced rather than broadcast.
- **Fan-out** — one publish reaches every matching subscription across every group.
- **Bounded everything** — payload, topic, group and token lengths are validated
  before a single byte is allocated, so a client claiming a 4 GB length is rejected
  rather than obeyed.
- **Token auth** — constant-time comparison, length-capped before the body is read,
  and a 1-second deadline so a stalled handshake cannot hold a socket open.
- **Clean disconnect** — a client can hang up explicitly; the server drops every
  subscription that connection held and reclaims the trie nodes they were the last
  users of, so a churning client set does not grow the routing tree.
- **Graceful shutdown** — a one-way latch closes every connection and joins every
  task before the process exits.

## Usage

```bash
cargo run                                # 127.0.0.1:4222
STAN_AUTH_TOKEN=s3cret cargo run         # with auth_required: true
cargo test
cargo clippy --all-targets -- -D warnings
```

Configuration lives in `config/stan.yml`; see [Configuration](docs/DESIGN.md#configuration).
When `auth_required` is set, the shared token is read from the `STAN_AUTH_TOKEN`
environment variable rather than the config file.

The `runtime` field is a build stamp of whatever toolchain wrote the file, not a
resolved dependency; `rust-version` in `Cargo.toml` is the authoritative minimum.

## Testing

99 tests: 65 unit, 34 integration. The integration suite runs against a real
listener on an OS-assigned port over real loopback TCP — no mocked sockets.

Its client is a second, independent implementation of the wire format, written
against the spec rather than reusing the server's encoder. A client that shared the
encoder would agree with it by construction and could only catch asymmetric bugs,
never a length field written the same wrong way on both sides.

Coverage includes wildcard matching, queue-group balancing, malformed and truncated
frames, oversized topics and payloads, token auth at the exact limit boundary,
disconnect cleanup and node reclamation, and four shutdown scenarios.

CI runs `cargo fmt`, `cargo clippy -D warnings`, and build + test across Linux,
macOS and Windows, with a dedicated MSRV job on 1.85.

## Layout

```
src/
  common/     shutdown latch, topic validation
  config/     YAML config, env token
  core/       actor, radix trie, queue groups
  network/    TCP accept loop, frame parser, auth
tests/        integration suite + independent protocol client
```

## Further reading

[Design notes](docs/DESIGN.md) — the actor model and why the routing path is
lock-free, how the trie handles wildcards and reuses nodes, and the complete wire
protocol reference.

## Status

Work in progress. Working today: the protocol parser and its bounds checks, the
routing trie and wildcard semantics, queue groups, authentication with a handshake
deadline, and graceful shutdown. Not yet done: persistence and replay,
request/reply, connection keepalives, slow-consumer accounting, per-subject
authorization, rate limiting, structured logging, metrics, and TLS.

Performance claims in the design notes are qualitative until a benchmark suite
exists; no throughput figure is quoted here because none has been measured.

## License

[MIT](LICENSE)
