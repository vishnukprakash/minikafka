# minikafka design

A miniature, single-broker Apache Kafka clone in Kotlin, built to be simple, clean, and working end-to-end.

## Goals / non-goals

- Goal: reproduce the core Kafka mental model — topics, partitions, an append-only
  disk log per partition, offset-based consumption, key-based partitioning,
  consumer offset tracking — in a small, readable codebase.
- Non-goal: replication, leader election, ZooKeeper/KRaft, consumer group
  rebalancing, transactions, compaction of the data log. (Note: a much more
  elaborate clustered implementation with these already exists in the sibling
  `ds-patterns-workshop/kafkalite` project — minikafka intentionally does not
  duplicate that scope.)

## Architecture

Single JVM process (`minikafka server`) with four layers:

1. **Network layer** — thread-per-connection TCP server. Each connection reads
   length-prefixed frames and dispatches requests sequentially (matches Kafka's
   real per-connection ordering guarantee).
2. **API layer** — decodes `apiKey`/`correlationId` header, routes to a handler,
   encodes the response.
3. **Broker** — in-memory map of topic name → partition count → `Log` instances.
   Rebuilt on startup by scanning the data directory. Owns topic creation and
   partition assignment (key hash or round-robin).
4. **Log layer** — one append-only, segmented log per topic-partition on disk,
   plus a sparse offset index per segment.

```
client (CLI) --TCP(framed binary)--> Network layer --> API layer --> Broker --> Log (per partition, on disk)
```

## Wire protocol

Custom binary protocol over TCP, framed like real Kafka:

```
Request:  size:int32 | apiKey:int16 | correlationId:int32 | body
Response: size:int32 | correlationId:int32 | body
```

`size` counts only the bytes that follow it. All integers big-endian. Strings
are `length:int16 | utf8 bytes` (length -1 means null).

APIs:

| apiKey | Name | Request body | Response body |
|---|---|---|---|
| 1 | CREATE_TOPIC | topic:string, numPartitions:int32 | errorCode:int16 |
| 2 | METADATA | (empty) | topicCount:int32, then per topic: name:string, numPartitions:int32 |
| 3 | PRODUCE | topic:string, key:nullable-string, value:string | errorCode:int16, partition:int32, offset:int64 |
| 4 | FETCH | topic:string, partition:int32, offset:int64, maxBytes:int32 | errorCode:int16, recordCount:int32, then records: offset:int64, timestamp:int64, key:nullable-string, value:string |
| 5 | OFFSET_COMMIT | group:string, topic:string, partition:int32, offset:int64 | errorCode:int16 |
| 6 | OFFSET_FETCH | group:string, topic:string, partition:int32 | errorCode:int16, offset:int64 (-1 if none committed) |

`errorCode` 0 = success; nonzero values cover unknown topic/partition, offset
out of range, etc.

## Storage format

Per topic-partition, a directory `data/<topic>-<partition>/`:

- `<baseOffset>.log` — sequential records:
  `offset:int64 | timestamp:int64 | keyLen:int32 (-1=null) | key bytes | valueLen:int32 | value bytes`.
  The active segment rolls over to a new file (named after the next offset)
  once it exceeds `segment.max.bytes` (default 10MB).
- `<baseOffset>.index` — sparse index, appended every `index.interval.bytes`
  (default 4KB) written to the log: `relativeOffset:int32 | filePosition:int32`.
  Reads binary-search the index for the closest offset ≤ target, then scan
  forward in the `.log` file from that position.

Consumer offsets: a single append-only `offsets.log` file at the broker's data
root, storing `group:string | topic:string | partition:int32 | offset:int64`
records; the broker keeps the latest offset per (group, topic, partition) in
memory, rebuilt on startup by replaying the file forward.

## Concurrency

Each `Log` instance guards appends and segment rolls with its own lock; reads
take a consistent snapshot of the active segment list. One log per partition
means no cross-partition contention. The broker's topic/partition map uses a
concurrent map since topic creation can race with lookups.

## CLI

A single `minikafka` entry point with subcommands:

- `minikafka server --port 9092 --data-dir ./data`
- `minikafka topics create --topic t --partitions 3`
- `minikafka topics list`
- `minikafka produce --topic t [--key k] <value>`
- `minikafka consume --topic t --partition 0 [--from-beginning] [--group g]`

## Testing

- Unit tests (`Log`): append, read-back, segment rolling at the size
  threshold, index-assisted lookup for an arbitrary offset.
- Unit tests (protocol encode/decode): round-trip each request/response body.
- Integration test: start a real broker on an ephemeral loopback port, drive
  it through the TCP client — create topic, produce several keyed messages,
  fetch from an arbitrary offset, commit and fetch a consumer offset — and
  assert on the wire responses.

## Error handling

- Malformed frames (bad length, truncated body) close the connection.
- Unknown topic/partition, offset out of range, and out-of-range fetch return
  a nonzero `errorCode` in-band rather than throwing across the connection.
- Corrupted/partial trailing writes in a log segment (e.g. after a crash) are
  truncated at the last valid record on startup, matching real Kafka's
  recovery behavior.
