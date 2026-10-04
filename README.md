# stan

A NATS-inspired publish/subscribe server written from scratch in Rust.

`stan` is a work in progress — an ongoing exercise in building a correct, memory-safe
messaging server: a hand-rolled binary wire protocol, a radix tree for topic routing
that needs no locks at all, queue-group load balancing, constant-time authentication,
and coordinated graceful shutdown.

```
cargo run          # start the server on 127.0.0.1:4222
cargo test         # 88 tests
```

---

## Contents

- [What works](#what-works)
- [Architecture](#architecture)
  - [Why a single actor](#why-a-single-actor)
  - [The handshake runs before the writer exists](#the-handshake-runs-before-the-writer-exists)
  - [Shutdown](#shutdown)
- [The routing trie](#the-routing-trie)
- [Wire protocol](#wire-protocol)
- [Configuration](#configuration)
- [Design decisions](#design-decisions)
- [Testing](#testing)
- [Project layout](#project-layout)
- [Status](#status)

---

## What works

| Capability           | Notes                                                                     |
| -------------------- | ------------------------------------------------------------------------- |
| Binary wire protocol | Length-prefixed frames, big-endian, no string parsing on the hot path     |
| Topic wildcards      | `foo.*` (exactly one token), `foo.>` (one or more), `>` (everything)      |
| Queue groups         | Round-robin delivery across the members of a group                        |
| Fan-out              | One publish reaches every matching subscription across every group        |
| Token auth           | Constant-time comparison via `subtle`, length-capped before allocation    |
| Bounded resources    | Payload, topic, group and token sizes validated _before_ allocating       |
| Graceful shutdown    | One-way latch closes every connection, drains the actor, exits cleanly    |
| Wildcard safety      | `>` is rejected in any non-final position, so it can never hide a subtree |

---

## Architecture

A single-writer actor owns all routing state. Everything else is a concurrent task
that talks to it over channels, which means the trie is never locked.

```
   TCP :4222
      │
      ▼
┌────────────────────────────────────────────────────────────────┐
│  accept loop — select! { shutdown, accept() }                  │
│  connections: JoinSet                                          │
└─────────────────────────────┬──────────────────────────────────┘
                              │ one task per connection
                              ▼
┌────────────────────────────────────────────────────────────────┐
│  per-connection task                                           │
│                                                                │
│    1. auth handshake      (raw socket, before the writer       │
│       runs first            task exists — writes its own       │
│                             2-byte ERR and returns)            │
│                          │                                     │
│    2. writer task  ◀── WriterMessage (bounded 100)             │
│       owns write half        ▲                                 │
│                          │                                     │
│    3. read loop       ── AppEvent                              │
│       BufReader + read_frame                                   │
└─────────────────────────────┬──────────────────────────────────┘
                              │ Event (bounded 100)
                              ▼
┌────────────────────────────────────────────────────────────────┐
│  dispatch()  — fans a parsed frame out to the right consumer   │
└─────────────────────────────┬──────────────────────────────────┘
                              ▼
┌────────────────────────────────────────────────────────────────┐
│  actor — a single task, the only writer of routing state       │
│                                                                │
│    owns  Trie    arena-allocated radix tree + free list        │
│          Group   queue groups, round-robin per group           │
│          id      monotonic message counter                     │
└─────────────────────────────┬──────────────────────────────────┘
                              │ WriterMessage::Msg
                              ▼
                       matching subscribers
```

### Why a single actor

The routing tree is the only genuinely shared mutable state, and it is the hottest
path in the server. Rather than guard it with a `RwLock` and accept the lock
contention that implies, ownership is confined to one task:

- **No locks on the hot path.** The trie is a plain `&mut self`, so the compiler
  proves there are no data races rather than a reviewer having to.
- **No lock-ordering hazards.** There is exactly one lock-free ordering to reason
  about — the actor's event loop.
- **Backpressure for free.** The actor channel is bounded. When routing falls
  behind, publishers await on `send` and are throttled by the runtime instead of
  the server unboundedly buffering.

The trade-off is that the actor is a serialization point: routing throughput is
bounded by one core. That is the right trade at this scale and the wrong one at
NATS scale, where it is sharded across nodes.

### The handshake runs before the writer exists

The token handshake is the one place that writes to the socket directly instead of
going through the writer task, and it does so because at that point **there is no
writer task** — it is spawned only after auth succeeds. A rejected connection never
allocates a channel, never spawns a task, and never enters the read loop; it writes
its two-byte ERR and returns.

```rust
let (mut reader, mut writer) = socket.into_split();
if cloned_app.config.auth_required {
    match handshake(&mut reader, &mut writer, token.as_deref()).await {
        Ok(true) => {}
        Ok(false) => return,          // no writer task was ever created
        ...
    }
}
// only now: the writer task and its channel come into existence
let (conn_sender, mut conn_receiver) = mpsc::channel::<WriterMessage>(100);
```

So the handshake hand-rolls its ERR rather than calling `AppError::to_bytes`. That
is a deliberate trade: a rejected connection is the cheapest possible path, and
routing a two-byte failure through a channel and a task would be pure overhead. It
does mean auth error codes bypass the shared encoder, which is safe today only
because both of them — `AuthError` and `MaxTokenLengthError` — are bare codes that
carry no context.

### Shutdown

`Shutdown` is a one-way latch backed by a `watch` channel, and the choice of
`watch` over `Notify` is deliberate. `watch` stores the _current value_ rather
than signalling an edge, so a task that calls `wait()` after the signal already
fired returns immediately. With an edge-triggered primitive there is a real race
where a task parks just after the signal and waits forever.

```rust
let _ = rx.wait_for(|state| *state == State::ShuttingDown).await;
```

Every `select!` in the server is `biased` with the shutdown arm first, so
shutdown is observed ahead of any pending I/O rather than racing it. On signal the
accept loop stops accepting, each connection task breaks out of its read loop, each
writer task stops writing, and `serve` joins every task before returning.

Worth being precise about: this is a **hard stop, not a drain**. Queued messages in
a writer's channel are dropped rather than flushed, so a graceful drain — letting
in-flight writes finish, with a deadline — is still to come.

---

## The routing trie

Topics are stored in a **radix tree keyed on dot-separated tokens**, not on whole
strings. `foo.bar.baz` and `foo.bar.qux` share the `foo` and `bar` nodes instead of
storing `foo.bar.` twice.

```
                     root
                       │
                      foo            ← stored once, shared
                   ┌────┴─────┐
                  bar        other
                   │
             ┌─────┴─────┐
            baz         qux
```

Nodes live in a flat `Vec<Node>` and reference each other **by index**:

```rust
pub struct Trie {
    nodes: Vec<Node>,
    free:  Vec<usize>,   // recycled slots
}

struct Node {
    children: HashMap<String, usize>,   // token -> child node index
    groups:   HashMap<String, Group>,   // group name -> queue group
}
```

This is the same arena technique `slotmap` and `petgraph` use, and it is chosen for
a specific reason: a trie built from nested structs needs recursive `&mut self` to
insert and delete, and the borrow checker fights you the whole way. In an arena,
`&mut self.nodes[i]` and `&mut self.nodes[j]` are provably disjoint, so insert and
prune are both plain iteration.

### Wildcards live in the traversal, not the node

`>` needs to match a variable number of trailing tokens, which no string comparison
can express. The mechanism:

1. `*` and `>` are stored as ordinary `children` keys.
2. `*` needs no special handling — it is looked up as a literal token and descended
   into, consuming exactly one segment.
3. `>` is **taken and not descended into**: its groups are collected and the walk
   stops there.
4. `validate_topic` rejects `>` in any non-final position, so a node can never exist
   _beneath_ a `>` node.

Step 4 is what makes step 3 sound. Without it, `foo.>.bar` could create a `bar` node
under `>`, and the take-and-stop rule would silently hide it forever. Because the
structure is unrepresentable, the traversal needs no guard.

| Subscription | Matches                  | Does not match          |
| ------------ | ------------------------ | ----------------------- |
| `foo.bar`    | `foo.bar`                | `foo.bar.baz`, `foo`    |
| `foo.*`      | `foo.bar`, `foo.baz`     | `foo.bar.baz`, `foo`    |
| `foo.>`      | `foo.bar`, `foo.bar.baz` | `foo` (needs ≥ 1 token) |
| `>`          | any non-empty topic      | `""`                    |

### Reuse, not growth

`Vec::remove(i)` shifts every later element and would invalidate every index
pointing at it, so nodes are never deleted. Pruned nodes are pushed onto a free
list; new inserts pop from it first. There is a test that churns a subscription
50 times and asserts the arena does not grow past its high-water mark.

### Queue groups

Each `(topic, group)` pair owns a `Group`, which holds its member connections and a
round-robin cursor. A publish fans out to every matching node, and within each node
to every group, and within each group to **exactly one** member:

```rust
let index = self.next % len;
self.next = (index + 1) % len;   // reassigned, so the cursor stays in range
```

Delivery uses `try_send` rather than `send().await`, so a slow subscriber can never
stall the actor or the publisher. The cost is that a full queue drops the message —
at-most-once, which is core NATS's own delivery guarantee.

---

## Wire protocol

Every integer is **big-endian**. Every frame begins with a one-byte command.

### Client → server

| Cmd | Name  | Frame                                                            |
| --: | ----- | ---------------------------------------------------------------- |
| `1` | INFO  | `[cmd]`                                                          |
| `2` | PING  | `[cmd]`                                                          |
| `3` | SUB   | `[cmd][sub_id:1][topic_len:4][group_len:4][topic][group]`        |
| `4` | PUB   | `[cmd][topic_len:4][payload_len:4][topic][payload][timestamp:8]` |
| `5` | UNSUB | `[cmd][sub_id:1][topic_len:4][group_len:4][topic][group]`        |

`SUB` and `UNSUB` share one layout and one parser, since they are the same
operation in opposite directions.

### Server → client

|   Code | Name | Frame                                                        |
| -----: | ---- | ------------------------------------------------------------ |
| `0x02` | ERR  | `[err][code:1]` + optional `[ctx_len:4][ctx]`                |
| `0x03` | MSG  | `[msg][sub_id:1][id:8][timestamp:8][payload_len:4][payload]` |
| `0x04` | INFO | `[info]` + server config                                     |
| `0x05` | PONG | `[pong]`                                                     |

### Error codes

|   Code | Name                  | Context             |
| -----: | --------------------- | ------------------- |
| `0x01` | `AuthError`           | —                   |
| `0x02` | `MaxPayloadErr`       | —                   |
| `0x03` | `MaxArtifactsErr`     | `topic` or `group`  |
| `0x04` | `InvalidTopic`        | the offending topic |
| `0x05` | `WildcardInPublish`   | the offending topic |
| `0x06` | `MaxTokenLengthError` | —                   |

The intended client design is to hold these codes as constants and branch on the
code byte to decide whether a context follows:

```rust
/// Mirrors `AppError::context`: only these codes are followed by a context
/// string. `AuthError`, `MaxPayloadErr` and `MaxTokenLengthError` are bare.
fn err_carries_context(code: u8) -> bool {
    matches!(code,
        x if x == ErrorCode::InvalidTopic as u8
            || x == ErrorCode::WildcardInPublish as u8
            || x == ErrorCode::MaxArtifactsErr as u8)
}
```

A context, when present, is length-prefixed as `[ctx_len:4][ctx]`. The frame carries
no flag saying whether one is coming, so the code byte is the only signal — getting
the branch wrong desynchronises the stream rather than failing cleanly.

### The MSG envelope

`sub_id` echoes the ID from the `SUB` that matched, so a client multiplexing several
subscriptions on one connection can tell them apart. `id` is a server-assigned
monotonically increasing counter, which lets a client detect gaps.

### Authentication

When `auth_required` is set, the **first** thing a connection must send is the token
frame — before any command:

```
[token_len:4][token]
```

The length is validated against the 1024-byte cap _before_ the body is read, so a
client claiming a 4 GB token is rejected without the server allocating anything.
Comparison is constant-time, so a wrong token cannot be recovered by timing.

---

## Configuration

`config/stan.yml`, read at startup:

```yaml
server_id: "01a0fc82-0a4d-72af-9619-a4559b7da88e"
version: "0.1.0"
runtime: "1.98.0"
host: "127.0.0.1"
port: 4222
max_payload: 1048576 # 1 MiB
max_control_line: 1024 # max topic or group length
tls_required: false
auth_required: false
```

When `auth_required: true`, the shared token is read from the `STAN_AUTH_TOKEN`
environment variable rather than the config file, so it never lands in version
control. Startup fails loudly if it is missing or over-length.

```bash
STAN_AUTH_TOKEN=s3cret cargo run
```

---

## Design decisions

**Validate length before allocating.** A malicious client can claim any length it
likes in a frame header. Every read path checks the declared length against a limit
_first_, and only then allocates. The integration suite proves this by claiming a
4 GB topic and a 4 GB payload while sending no body at all — a server that
allocated before validating would OOM.

**`Vec<u8>` + explicit lengths instead of line-delimited text.** NATS uses
space-delimited ASCII lines, which means the server must scan for delimiters and
copy. Length-prefixed binary frames make parsing a fixed-size read plus one
allocation, and make over-long input a bounds check rather than a scan.

**Owned at the boundary, borrowed internally.** Public functions take `String` by
value because the actor already owns those values — they came off an `mpsc` — so
moving them costs nothing. Inside, everything borrows. The rule is that the tree
owns what it keeps and borrows what it only reads.

**One `println!` today, `tracing` later.** Instrumentation is currently raw
`eprintln!`, which is fine at this stage but is the first thing to replace before
this is production-shaped.

---

## Testing

```
cargo test
```

**88 tests, all passing** — 61 unit, 27 integration.

The integration suite in `tests/` runs against a real listener on an
OS-assigned port over real loopback TCP.

Its client is a deliberate **second, independent implementation of the wire
format**. It does not call the server's `to_bytes`; it encodes and decodes from
the protocol spec, the way a real client would:

```rust
// The encoders here are written independently of `to_bytes` rather than
// reusing it, so a change to the server's serialisation can't quietly
// make a test agree with a bug.
```

That duplication is the point, not an oversight. A test client that shared the
server's encoder would agree with the server by construction and could never catch
a serialisation bug — it would only confirm the bug is symmetric. Written
independently, it fails in the same way a real client written against the spec would
fail.

The second consequence is that **changing the wire format breaks these tests**, and
that is the assertion doing its job: it proves the change is a contract change rather
than a silent internal edit. A green suite after a framing change means either the
change was genuinely compatible or the tests are no longer testing anything.

Coverage includes wildcard matching and non-matching, queue-group balancing,
unsubscribe, malformed frames, truncated frames, oversized topics and payloads,
token auth including the exact limit boundary, and four shutdown scenarios.

Unit tests cover the trie's matching table, node reuse and pruning, round-robin
distribution, topic validation, and the shutdown latch's race conditions.

CI runs `cargo fmt`, `cargo clippy -D warnings`, and build + test across Linux,
macOS and Windows, with an MSRV check on 1.85.

---

## Project layout

```
src/
  main.rs              entry point, Ctrl-C wiring
  lib.rs               start() / start_with(shutdown)
  common/
    shutdown.rs        one-way watch-channel latch
    utils.rs           topic splitting and validation
  config/
    config.rs          YAML load, env token, INFO serialisation
  core/
    actor.rs           single-writer event loop
    tree.rs            radix trie: arena, wildcards, pruning
    group.rs           queue groups, round-robin
    types.rs           Message, WriterMessage, AppError
  network/
    net.rs             accept loop, per-connection tasks, frame reader
    types.rs           AppEvent, ResponseType, ErrorCode, FrameError
    auth.rs            token handshake
tests/
  common/mod.rs        independent protocol client + server harness
  integration.rs       end-to-end tests over real sockets
```

About 1,850 lines of source and 710 of tests.

---

## Status

Work in progress, built as a portfolio piece. Solid today: the protocol parser and
its bounds checks, the routing trie and wildcard semantics, queue groups,
authentication, and graceful shutdown.

MIT licensed.
