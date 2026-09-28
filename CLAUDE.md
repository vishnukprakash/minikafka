# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

minikafka is a miniature, single-broker Apache Kafka clone in Kotlin: topics/partitions, an
append-only segmented log with a sparse offset index, a custom TCP binary wire protocol, and a
small CLI. It is a deliberately-scoped-down educational project, not a production broker — see
`PRD.md` for goals/non-goals (no replication, no ZooKeeper/KRaft, no consumer-group rebalancing,
no transactions, no log compaction) and `DESIGN.md` for the full technical design (architecture,
wire protocol table, storage format, concurrency model). The task-by-task build history lives
under `docs/superpowers/plans/`, and the original spec under `docs/superpowers/specs/`.

## Commands

```bash
./gradlew build                                          # build + run all tests
./gradlew test                                            # run all tests
./gradlew test --tests "minikafka.log.LogSegmentTest"      # run one test class
./gradlew test --tests "minikafka.log.LogSegmentTest.recovers by truncating*"  # run one test method
./gradlew test --rerun-tasks                               # force a fresh run (bypass Gradle's cache)
./gradlew run --args="server --port 9092 --data-dir ./data"
./gradlew run --args="topics create --topic demo --partitions 2"
./gradlew run --args="produce --topic demo --key user-1 hello"
./gradlew run --args="consume --topic demo --partition 0 --from-beginning"
```

`consume` fetches until it catches up to the log's current end offset and then exits — it does
not "tail" the log waiting for new messages, unlike a real `kafka-console-consumer`.

Requires Gradle 9.7.1 (wrapper-pinned) and Kotlin 2.4.10 — this combination was chosen after an
older Gradle version proved incompatible with this environment's JDK; see the ruling note at the
top of `docs/superpowers/plans/2026-09-23-minikafka-implementation.md` before changing either
version.

## Architecture

Four layers with a strict, one-directional dependency graph — never introduce a dependency that
points the other way:

```
cli → server → broker → log
cli → client → proto
server, broker → proto
proto, log → io   (proto and log never depend on each other)
```

- **`minikafka.io`** — shared binary-encoding primitives (nullable string/bytes read+write on
  `DataOutput`/`DataInput`). Exists specifically so `log` (on-disk format) and `proto` (wire
  format) can both use the same primitives without depending on each other.
- **`minikafka.log`** — `Record` (a single message's binary layout), `OffsetIndex` (sparse
  offset→byte-position index), `LogSegment` (one segment file + its index, with crash recovery
  that truncates a partial trailing write left by an unclean shutdown), `Log` (rolls segments,
  rebuilds itself from disk on construction). Knows nothing about topics, the network, or the
  wire protocol.
- **`minikafka.proto`** — the wire protocol: `Framing` (length-prefixed request/response frames),
  `Protocol` (ApiKeys/ErrorCodes constants), `Requests`/`Responses` (every request/response body,
  each with `encode(DataOutput)` + companion `decode(DataInput)`). Pure encode/decode; no sockets,
  no broker knowledge.
- **`minikafka.broker`** — `Broker` is the seam between `log` and `proto`: owns the topic/partition
  registry (rebuilt from the data directory on startup), key-hash/round-robin partitioning, and
  produce/fetch/offset-commit/offset-fetch dispatch. `OffsetStore` is a disk-backed consumer-offset
  tracker, replayed forward on startup.
- **`minikafka.server`** / **`minikafka.client`** — `Server`/`ConnectionHandler` (thread-per-connection
  TCP server; accept-loop and every handler thread are daemon threads) and `MiniKafkaClient` (the
  matching TCP client used by both the CLI and tests).
- **`minikafka.cli`** — the `minikafka` subcommands (`server`, `topics create/list`, `produce`,
  `consume`), the only package that depends on both `server` and `client`.

Concurrency: each `Log`/`LogSegment` synchronizes its own append/read (one lock per partition, so
partitions never contend with each other); `Broker`'s topic registry is a `ConcurrentHashMap` with
`computeIfAbsent`-style atomic creation (not check-then-act, which previously caused a race that
could silently change a topic's partition count across a restart).

All wire-protocol and on-disk integers are big-endian (the JVM's `DataOutput`/`DataInput` default)
— never introduce little-endian encoding anywhere.

Every request/response body type lives in `minikafka.proto` and must expose both `encode` and a
companion `decode` — there is no ad-hoc serialization anywhere else in the codebase except `log`'s
own `Record`, which is an intentional, separate format from the wire protocol.
