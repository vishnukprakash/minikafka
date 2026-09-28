# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

minikafka is a miniature Apache Kafka clone in Kotlin: topics/partitions, an append-only segmented
log with a sparse offset index, a custom TCP binary wire protocol, and a small CLI — plus, as of
the 2026-09-28 ADR, a replicated multi-broker cluster coordinated by ZooKeeper (controller
election, leader/ISR replication, failover). It is a deliberately-scoped-down educational project,
not a production broker — see `PRD.md` for goals/non-goals and `DESIGN.md` for the full technical
design (architecture, ZooKeeper znode layout, wire protocol table, on-disk format, core algorithms,
guarantees table, known limitations, concurrency model). Original single-broker build history is
under `docs/superpowers/plans/2026-09-23-minikafka-implementation.md`; the multi-broker ADR and
task-by-task plan is `docs/superpowers/plans/2026-09-28-multi-broker-zookeeper.md`.

## Commands

```bash
./gradlew build                                             # build + run all tests + e2eTest (check depends on e2eTest)
./gradlew test                                              # unit/integration tests only (excludes @Tag("e2e","slow"))
./gradlew test -PexcludeTags=zk                              # also exclude @Tag("zk") tests (e.g. no local ZK available)
./gradlew test --tests "minikafka.log.LogSegmentTest"        # run one test class
./gradlew test --tests "minikafka.log.LogSegmentTest.recovers by truncating*"  # run one test method
./gradlew test --rerun-tasks                                 # force a fresh run (bypass Gradle's cache)
./gradlew e2eTest                                            # end-to-end + slow tests against an installed dist (installDist first)
./gradlew e2eTest -Dnemesis.seed=<seed>                       # replay a specific NemesisE2eTest run (seed is printed on every run)

./gradlew run --args="zk --port 2181 --data-dir ./zk-data"                        # single-node embedded ZooKeeper
./gradlew run --args="server --broker-id 1 --zk localhost:2181 --port 9092 --data-dir ./data1"
./gradlew run --args="topics create --topic demo --partitions 2 --replication-factor 3"
./gradlew run --args="topics describe --topic demo"
./gradlew run --args="produce --topic demo --key user-1 --acks all hello"
./gradlew run --args="consume --topic demo --partition 0 --from-beginning"
```

`server` requires `--broker-id` and `--zk host:port[/chroot]`; `--advertised-host` and
`--min-insync` (min.insync.replicas, must match across the cluster) are optional.
`scripts/local-cluster.sh` scripts standing up `zk` + 3 brokers locally (see `README.md`).

`consume` fetches up to the partition's **high watermark** (the offset up to which every in-sync
replica has replicated), not the log's true end — it does not "tail" the log waiting for new
messages either, unlike a real `kafka-console-consumer`.

Requires Gradle 9.7.1 (wrapper-pinned) and Kotlin 2.4.10 — this combination was chosen after an
older Gradle version proved incompatible with this environment's JDK; see the ruling note at the
top of `docs/superpowers/plans/2026-09-23-minikafka-implementation.md` before changing either
version. Do not change Gradle/Kotlin versions for the multi-broker work either (carried-forward
ruling in the 2026-09-28 plan's shared context).

## Architecture

Dependency graph — never introduce an edge pointing the other way:

```
cli     → server, client, proto (ErrorCodes), org.apache.zookeeper (the `zk` subcommand)
server  → broker, cluster, zk, proto, model
cluster → zk, net, proto, model
broker  → log, net, proto, model
client  → net, proto
zk      → model, org.apache.curator / org.apache.zookeeper
net     → proto
proto, log → io          model → nothing   (proto and log never depend on each other)
```

- **`minikafka.model`** — shared plain data types (`TopicPartition`, `PartitionState`,
  `BrokerInfo`, `Versioned`), no behaviour.
- **`minikafka.io`** — shared binary-encoding primitives (nullable string/bytes, record CRC body),
  used by both `log` (on-disk format) and `proto` (wire format) without either depending on the
  other.
- **`minikafka.log`** — `Record` (offset, crc, leaderEpoch, timestamp, key, value),
  `LeaderEpochCache`, `OffsetIndex`, `LogSegment` (crash recovery: truncates on a partial write or a
  bad CRC), `Log` (`appendAsLeader`/`appendAsFollower`, `truncateTo`, rebuilds from disk). Knows
  nothing about topics, replication, or the network.
- **`minikafka.proto`** — the wire protocol: `Framing`, `Protocol` (`ApiKeys`/`ErrorCodes`, 8 keys,
  17 error codes), `Requests`/`Responses` (`encode`/`decode` pairs, including `LeaderAndIsr*` and
  `OffsetsForLeaderEpoch*`). Pure encode/decode; no sockets, no cluster knowledge.
- **`minikafka.net`** — `Connection`: framed request/response over a socket, shared by the client
  and by broker-internal senders (`ReplicaFetcher`, `ControllerChannel`). Must be closed and
  recreated after any `IOException` (the stream is left desynchronized) — never reused.
- **`minikafka.zk`** — `ZkStore` (Curator-backed reads/writes/watches, election `multi`, fenced CAS,
  retry-after-replay recognition), `ZkPaths`, `KvCodec` (tiny sorted `k=v\n` payload format).
- **`minikafka.cluster`** — `Controller` (single event-thread state machine: election,
  reconciliation, fenced writes), `ControllerEvent`, `ControllerChannel` (one FIFO sender thread per
  broker), `PartitionLeaderElector`, `ReplicaAssigner`.
- **`minikafka.broker`** — `Partition` (per-partition leader/ISR/HW state, wraps a `Log`),
  `ReplicaManager` (this broker's partition registry, applies `LeaderAndIsr`, produce/fetch
  dispatch, acks=all waiting), `ReplicaFetcher`/`ReplicaFetcherManager` (follower replication:
  epoch handshake + fetch loop), `IsrUpdater` (per-broker ISR CAS proposer thread), `IsrWriter`
  (interface implemented over `ZkStore` in `server`, so `broker` itself has no `zk` dependency),
  `FollowerTruncation`, `BrokerSnapshot`, `Clock` (injectable). (The old single-broker `Broker` and
  `OffsetStore` classes are gone.)
- **`minikafka.server`** — composition root: `Server`/`ServerConfig` (bind, ZK connect, register
  `/brokers/ids/<id>`, wire `ReplicaManager` + `Controller`), `ConnectionHandler`
  (thread-per-connection, routes all 8 API keys), `DataDirLock` (`.lock` + `broker.id` mismatch
  check), `ZkBrokerResolver` (METADATA reads ZK directly, no cache), `ZkIsrStore`.
- **`minikafka.client`** — `MiniKafkaClient` (bootstrap list, metadata cache, one `Connection` per
  broker, retry-with-metadata-refresh on NOT_LEADER/LEADER_NOT_AVAILABLE/FENCED/IO),
  `Partitioner` (client-side hash-by-key/round-robin — the client must know the partition before it
  can find that partition's leader).
- **`minikafka.cli`** — `zk` (embedded `ZooKeeperServerMain`), `server`, `topics
  create/list/describe`, `produce`, `consume`. The only package depending on both `server` and
  `client`, and (along with `zk`) the only one touching `org.apache.zookeeper` directly.

## Concurrency

- Every thread is a **daemon**, named `b<id>-<role>[-topic-p]` (e.g. `b2-fetcher-demo-0`,
  `b1-controller`, `b3-isr-updater`).
- **Lock order: `Partition` → `Log`.** A `Partition` lock is **never held across network or ZK
  I/O** — ISR CAS, LeaderAndIsr delivery, and the follower epoch handshake all prepare intent under
  the lock, release it, do the I/O, then re-acquire to apply a rechecked result.
- `ReplicaManager.applyLeaderAndIsr` holds a per-broker `leaderAndIsrLock` while updating every
  partition's role, releasing each partition lock before rewiring fetchers.
- `IsrUpdater` (one thread/broker) and `ReplicaFetcher` (one thread/follower-partition) never hold
  a partition lock across I/O, per the rule above.
- `Controller` runs its whole state machine on **one event thread** draining a queue — no locks
  needed on controller state; only the elected controller watches ZK (no herd across brokers).
- `Log`/`LogSegment` still synchronize their own append/read — one lock per partition's log, no
  cross-partition contention.
- Every ZK write is retry-safe: on `NodeExists`/`BadVersion`, re-read and check whether the current
  state is recognizably this writer's own already-committed write before treating it as a real
  conflict (Curator can replay a write after `ConnectionLoss`).

All wire-protocol and on-disk integers are big-endian (the JVM's `DataOutput`/`DataInput` default)
— never introduce little-endian encoding anywhere.

Every request/response body type lives in `minikafka.proto` and must expose both `encode` and a
companion `decode` — there is no ad-hoc serialization anywhere else in the codebase except `log`'s
own `Record`, which is an intentional, separate format (it now also carries `crc` and
`leaderEpoch`, sharing the CRC computation with `proto.FetchedRecord` via `minikafka.io`).
