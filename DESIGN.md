# minikafka — Design Document

Implements the requirements in `PRD.md` (same folder). For the exact,
task-by-task build sequence (including full source for every file), see
`docs/superpowers/plans/2026-09-23-minikafka-implementation.md`. This
document is the durable reference for *how the system is shaped and why* —
the plan is the disposable checklist for *how it got built*.

## Scope recap

Single JVM process, single broker, no replication/ZooKeeper/rebalancing/
compaction. See `PRD.md` → Non-goals for the full exclusion list and why.

## Architecture

```
                 TCP, framed binary
 CLI / Client ───────────────────────▶  Network layer (Server, ConnectionHandler)
                                              │  decodes apiKey + correlationId,
                                              │  routes to a handler
                                              ▼
                                           Broker
                              (topic/partition registry, partitioning,
                               produce/fetch/offset-commit dispatch)
                                              │
                              ┌───────────────┼────────────────┐
                              ▼               ▼                ▼
                          Log (t-0)       Log (t-1)      OffsetStore
                       (segments+index) (segments+index)  (offsets.log)
```

Four layers, each independently testable:

1. **Network layer** (`minikafka.server`) — thread-per-connection TCP server.
   Each connection reads length-prefixed frames and processes them
   sequentially, in arrival order (matches real Kafka's per-connection
   ordering guarantee). One daemon thread per accepted connection; the
   accept loop itself also runs on a daemon thread so nothing it spawns can
   keep the JVM alive after `Server.stop()`.
2. **Wire protocol** (`minikafka.proto`) — pure encode/decode: frame headers,
   API keys, error codes, and every request/response body. Has no knowledge
   of brokers, sockets, or disk — it only knows how to turn typed Kotlin
   values into bytes and back.
3. **Broker** (`minikafka.broker`) — in-memory registry of
   `topic name → partition count → Log instances`, rebuilt from the data
   directory on startup. Owns partition assignment (hash-by-key or
   round-robin) and is the only layer that talks to both the wire protocol's
   error codes and the log layer's `Record` type — it's the seam between
   them.
4. **Log layer** (`minikafka.log`) — one append-only, segmented log per
   topic-partition, each segment backed by a `.log` file and a sparse
   `.index` file. Knows nothing about topics, partitions, or the network —
   just "append a record, get an offset back" / "read from an offset."

A shared `minikafka.io` package holds the primitive nullable-string and
nullable-bytes encoding helpers used by *both* the wire protocol (Task 7)
and the on-disk formats (`Record`, `OffsetStore`) — this is what keeps the
log layer from needing to depend on the protocol layer, and vice versa.

**Dependency direction (strict):** `server` → `broker` → `log`, and
`server`/`broker` → `proto`. `proto` and `log` both depend only on `io`, and
never on each other. `client` depends only on `proto` (for the wire format) —
the client never touches `log` or `broker` directly, exactly as a real Kafka
client wouldn't link against broker-internal code. `cli` depends on both
`server` (to run the `server` subcommand) and `client` (for the other
subcommands).

## Wire protocol

Custom binary protocol over TCP, deliberately shaped like real Kafka's own
protocol (see references below) rather than invented from scratch:

```
Request:  size:int32 | apiKey:int16 | correlationId:int32 | body
Response: size:int32 | correlationId:int32 | body
```

- `size` counts only the bytes that follow it (so a reader always knows
  exactly how many bytes make up the next message before parsing any of it).
- All integers are big-endian (the JVM's `DataOutput`/`DataInput` default).
- Strings are `length:int16 | utf8 bytes`, with `length = -1` meaning null.
- Byte arrays (keys/values) are `length:int32 | bytes`, with `length = -1`
  meaning null.
- `correlationId` lets a client match responses to requests on a connection
  that could, in principle, pipeline multiple in-flight requests (minikafka's
  own client doesn't pipeline — it sends one request and blocks for the
  matching response — but the field exists because it's part of what makes
  this a *protocol* rather than a bespoke one-off call/reply pair).

| apiKey | Name | Request body | Response body |
|---|---|---|---|
| 1 | CREATE_TOPIC | topic:string, numPartitions:int32 | errorCode:int16 |
| 2 | METADATA | *(empty)* | topicCount:int32, then per topic: name:string, numPartitions:int32 |
| 3 | PRODUCE | topic:string, key:nullable-bytes, value:bytes | errorCode:int16, partition:int32, offset:int64 |
| 4 | FETCH | topic:string, partition:int32, offset:int64, maxBytes:int32 | errorCode:int16, recordCount:int32, then per record: offset:int64, timestamp:int64, key:nullable-bytes, value:bytes |
| 5 | OFFSET_COMMIT | group:string, topic:string, partition:int32, offset:int64 | errorCode:int16 |
| 6 | OFFSET_FETCH | group:string, topic:string, partition:int32 | errorCode:int16, offset:int64 (`-1` if none committed) |

`errorCode` `0` means success. Nonzero codes (`UNKNOWN_TOPIC`,
`UNKNOWN_PARTITION`, `OFFSET_OUT_OF_RANGE`, `TOPIC_ALREADY_EXISTS`) are
returned **in-band** in the response body, not as connection-level failures
— a client always gets a well-formed response and decides what to do with
the error code, exactly like real Kafka.

## Storage format

Per topic-partition, a directory `<dataDir>/<topic>-<partition>/`:

- **`<baseOffset>.log`** — the segment's records, back to back:
  `offset:int64 | timestamp:int64 | keyLen:int32 (-1=null) | key bytes | valueLen:int32 | value bytes`.
  `baseOffset` is the offset of the first record in the file, zero-padded to
  20 digits in the filename (`00000000000000000000.log`), matching real
  Kafka's segment naming convention. The active segment rolls over to a new
  file — named after the next offset to be written — once its size exceeds
  `segmentMaxBytes` (default 10MB).
- **`<baseOffset>.index`** — a *sparse* index into the matching `.log` file:
  every `indexIntervalBytes` (default 4KB) of log data written, one entry
  `relativeOffset:int32 | filePosition:int32` is appended (`relativeOffset`
  is `offset - baseOffset`, keeping entries small since a segment's own
  offset range is bounded). A read binary-searches this index for the
  closest entry at or before the target offset, seeks there, then scans
  forward record-by-record until it reaches the target — avoiding a full
  linear scan of the segment for every read, while keeping the index itself
  small (indexing every Nth byte, not every record — same trade-off real
  Kafka's `log.index.interval.bytes` makes).

Consumer offsets live in a single append-only `<dataDir>/offsets.log` file:
`group:string | topic:string | partition:int32 | offset:int64` records. The
broker keeps the *latest* offset per `(group, topic, partition)` in memory,
rebuilt on startup by replaying this file forward from the start (last
write for a given key wins — no compaction needed since this file is small
and rewritten in full only by replay, never truncated in place).

### Crash recovery

A segment reopening after an unclean shutdown replays its own `.log` file
from the start. If it hits an `EOFException` partway through decoding a
record (meaning the last write was cut off mid-record), it truncates the
file back to the last successfully-decoded record's end and treats that as
the segment's true end — silently discarding the partial write, the same
way real Kafka's log recovery does. `nextOffset` and `sizeInBytes` are
derived from this replay, never trusted from stale in-memory state.

## Concurrency model

- Each `LogSegment` synchronizes its own `append`/`read` methods — a single
  lock per segment, held only for the duration of one append or one read.
- Each `Log` synchronizes `append` (segment-roll decision + delegation) —
  one lock per partition.
- Since every topic-partition has its own `Log`, there is no contention
  *across* partitions — this is the same scalability property that makes
  partitions Kafka's unit of parallelism in the first place.
- `Broker`'s topic registry is a `ConcurrentHashMap`, since topic creation
  can race with concurrent produce/fetch lookups from other connections'
  handler threads.
- `OffsetStore` synchronizes `commit` (read-modify-append is not otherwise
  atomic); `fetch` reads a `ConcurrentHashMap` and needs no lock.
- The TCP accept loop and every per-connection handler run on **daemon**
  threads, so neither the server process nor a test JVM is kept alive by
  them after `Server.stop()` — a connection's blocking socket read simply
  unblocks with an exception once its socket is closed, and the thread
  exits.

## CLI

A single `minikafka` entry point (via Gradle's `application` plugin),
subcommands mirroring real Kafka's separate `bin/kafka-*.sh` scripts:

```
minikafka server --port 9092 [--data-dir ./data]
minikafka topics create --topic t --partitions 3
minikafka topics list
minikafka produce --topic t [--key k] <value>
minikafka consume --topic t --partition 0 [--from-beginning] [--group g]
```

`consume` without `--group` always starts from offset 0 (or `--from-beginning`
is implied — there's no "latest" mode in v1, since there's no notion of
"now" without a committed offset or a timestamp index). With `--group`, it
resumes from the group's last committed offset (or 0 if none), and commits
its new position back after each fetch batch.

## Testing strategy

- **Unit tests**, one per layer, isolated from the network:
  `Record`, `OffsetIndex`, `LogSegment` (including the crash-recovery
  scenario), `Log` (including segment rolling and restart persistence),
  the wire protocol's encode/decode round trips, `OffsetStore`, and
  `Broker` (partitioning behavior, error codes, offset commit/fetch) —
  all exercised directly, without a socket in the loop.
- **Integration test**: starts a real `Server` on an ephemeral loopback
  port and drives it with the real `MiniKafkaClient` over an actual TCP
  connection — create topic, produce keyed messages, fetch them back,
  commit and fetch a consumer offset, and confirm an unknown topic returns
  a nonzero error code. This is what proves the layers actually compose,
  not just that each one works in isolation.

## Error handling

- A malformed frame (truncated length prefix or body) surfaces as an
  `EOFException`/`IOException` on that connection, which the connection
  handler catches by closing the socket — it never brings down the server
  or other connections.
- Application-level errors (unknown topic, unknown partition) are returned
  as a nonzero `errorCode` in an otherwise well-formed response — never by
  throwing across the wire.
- Disk-level partial writes are handled by segment recovery (see above),
  not treated as a fatal startup error.

## References consulted while designing this

- [Kafka Internals: Segments, Segment Size & Indexes (Conduktor)](https://www.conduktor.io/kafka/kafka-topics-internals-segments-and-indexes)
- [Deep dive into Apache Kafka storage internals (Strimzi)](https://strimzi.io/blog/2021/12/17/kafka-segment-retention/)
- [Deep Dive Into Apache Kafka | Storage Internals](https://rohithsankepally.github.io/Kafka-Storage-Internals/)
- [Apache Kafka protocol guide](https://kafka.apache.org/24/protocol.html)
- [Kafka Protocol Message Format (AxonOps)](https://axonops.com/docs/data-platforms/kafka/architecture/client-connections/protocol-messages/)
