# minikafka — Product Requirements Document

## Summary

minikafka is a miniature Apache Kafka clone in Kotlin: topics and partitions, an append-only
segmented log with a sparse offset index, a custom TCP binary wire protocol, a small CLI — and,
as of the 2026-09-28 ADR (`docs/superpowers/plans/2026-09-28-multi-broker-zookeeper.md`), a
replicated, ZooKeeper-coordinated multi-broker cluster with controller election and leader
failover. It remains a learning project: the goal is to demonstrate the core mental model of a
Kafka-style distributed log — topics, partitions, replication, leader election, in-sync replicas,
offset-based consumption — in a small enough codebase to read end-to-end, not to build a
production broker.

It is not a production message queue. It is a reference implementation: something a developer can
build, run, break, and read to understand *why* Kafka (and pre-KRaft Kafka's ZooKeeper-based
design in particular) is built the way it is, without wading through the actual Kafka codebase's
operational complexity (KRaft, tiered storage, exactly-once semantics, rack-aware placement,
etc.).

## Background / motivation

This project lives in `distributed-systems/`, alongside a sibling workshop repo
(`ds-patterns-workshop`) that already contains a much more elaborate `kafkalite` — a clustered,
ZooKeeper-backed Kafka clone with controller election and replica management, built for a
distributed-systems patterns course. minikafka started as deliberately *not* that: the
single-broker subset of Kafka's design, the part that explains how a message log actually works on
disk, independent of clustering.

The 2026-09-28 ADR extends minikafka into the clustering space that PRD v1 originally excluded,
following a hand-rolled, pre-KRaft Kafka design (ZooKeeper as the consistent core, controller
election with epoch fencing, leader/ISR replication) rather than reusing `kafkalite`'s mechanism
wholesale — see the ADR (`docs/superpowers/plans/2026-09-28-multi-broker-zookeeper.md`, ADR-001)
for the full context, decision table (D1–D22), and consequences. The two projects remain
complementary: minikafka's version of clustering is built fresh, kept as simple and readable as a
correct implementation allows, and documented in `DESIGN.md`.

## Goals

1. Demonstrate topics and partitions as the unit of parallelism and ordering.
2. Demonstrate the append-only, segmented, disk-backed log as Kafka's core storage primitive,
   including a sparse index for fast offset lookup and crash recovery (truncating a partial
   trailing write).
3. Demonstrate offset-based consumption: a consumer reads forward from an offset it controls, and
   can optionally have the cluster remember that offset on its behalf (consumer offset
   commit/fetch, backed by ZooKeeper).
4. Demonstrate a real, custom binary wire protocol over TCP — not a REST/JSON shortcut — so the
   networking layer teaches the same lesson Kafka's own protocol does (length-prefixed framing, a
   versioned-style request header, correlation IDs).
5. **Demonstrate replicated, multi-broker clustering coordinated by ZooKeeper as the consistent
   core**: controller election with epoch-based fencing (KAFKA-6082), broker-epoch fencing of
   stale commands (KIP-380), leader/follower replication with a maximal-ISR high-watermark
   protocol (KIP-497), leader-epoch-based log truncation on failover instead of naive
   high-watermark truncation (KIP-101/279), and zero acked-write loss under `acks=all` with
   `min.insync.replicas` ≥ 2 across any single broker failure. See the ADR
   (`docs/superpowers/plans/2026-09-28-multi-broker-zookeeper.md`) for the full mechanism.
6. Ship a working CLI so the whole cluster can be driven and observed by hand, the way
   `kafka-console-producer.sh`/`kafka-console-consumer.sh`/`kafka-topics.sh --describe` are used to
   poke at a real Kafka cluster — including standing up ZooKeeper itself (`minikafka zk`) and
   describing cluster/partition state (`topics describe`).

## Non-goals

Explicitly out of scope, and intentionally so — adding any of these would turn this from a small,
readable reference implementation into a second `kafkalite` (or into real Kafka):

- **Unclean leader election** — always disabled, not configurable; an offline partition (all ISR
  members dead) stays offline rather than electing a leader that might be missing acked data.
- **KRaft** — this project models the older, ZooKeeper-based architecture on purpose (that's the
  part worth understanding as a distinct historical design); no Raft-based metadata quorum.
- **`acks=0`** — only `acks=1` (leader-only) and `acks=all` (ISR) are supported.
- Consumer group rebalancing / partition assignment protocols. (Basic consumer *offset*
  commit/fetch is in scope; the rebalance protocol that decides which consumer owns which
  partition is not.)
- Transactions / exactly-once semantics; idempotent producer (delivery is at-least-once — retries
  after a timeout or ambiguous error may duplicate).
- Log compaction (only time/size-based segment rolling), retention/deletion, per-topic config.
- `UpdateMetadata` RPC / metadata caching (every broker answers `METADATA` by reading ZooKeeper
  directly — see ADR D6), controlled-shutdown RPC, preferred-leader rebalance, partition
  reassignment, adding partitions to an existing topic, topic deletion.
- Fetch batching/long-poll, fetch-from-follower.
- Consumer-group rebalancing, rack awareness.
- Authentication, authorization, encryption in transit; ZooKeeper ACLs/TLS.
- fsync-based durability (durability comes from replication across the ISR, not from flushing to
  disk — see the guarantees table in `DESIGN.md`).

## Users / use cases

The primary "user" is a developer (including future-me) who wants to:

- Stand up a small ZooKeeper-coordinated cluster locally (`scripts/local-cluster.sh`, or `minikafka
  zk` + several `minikafka server` processes by hand) and manually produce/consume messages via the
  CLI to build intuition for topics, partitions, replication, and offsets.
- Kill a broker (leader or controller) mid-workload and watch the cluster fail over — a concrete,
  observable demonstration of the mechanism a real Kafka deployment relies on.
- Read the source to understand a concrete, minimal implementation of a segmented log with an
  offset index, and of ZooKeeper-coordinated controller election / replica management — the same
  ideas behind Kafka, most embedded databases' WALs, and event-sourced systems generally.
- Use it as a small, controlled target for further learning experiments (e.g. adding a feature,
  changing partitioning strategy, benchmarking, injecting faults).

## Functional requirements

- **Topics:** create a topic with a fixed number of partitions and a replication factor (≤ live
  broker count); list topics; describe cluster and per-partition state (controller, brokers,
  leader, leader epoch, replicas, ISR, OFFLINE/UNDER-REPLICATED markers). No delete/alter/add
  partitions in v1.
- **Produce:** write a message (optional key, required value) to a topic, with `acks=1` (leader
  only) or `acks=all` (waits for the in-sync replica set, subject to `min.insync.replicas`).
  Messages with a key are routed deterministically to a partition by hashing the key; messages
  without a key are spread round-robin across partitions — done client-side, since the client must
  know the partition before it can find that partition's leader.
- **Fetch:** read messages from a given topic-partition starting at a given offset, up to an
  approximate byte cap; only the current leader serves consumer fetches; a consumer never sees
  past the high watermark (the offset up to which every in-sync replica has replicated).
- **Consumer offsets:** commit and fetch a single named consumer group's offset for a
  topic-partition, so a consumer can resume where it left off — stored in ZooKeeper (cluster-wide,
  last-writer-wins), not on a single broker's disk.
- **Replication and failover:** each partition has a replication factor RF and a leader elected by
  the controller; followers replicate from the leader and are tracked in an in-sync replica (ISR)
  set; if the leader dies, the controller elects a new leader from the surviving ISR with zero
  acknowledged-write loss (subject to the guarantees table in `DESIGN.md`); if the controller dies,
  another broker is elected controller.
- **Persistence:** all of the above survive a broker restart — topics, messages, and committed
  offsets are all read back from disk/ZooKeeper on startup. Topic *metadata* (partition
  count/assignment/leader/ISR) lives only in ZooKeeper; a broker's data directory holds only the
  partitions it's currently assigned.
- **CLI:** `zk`, `server`, `topics create/list/describe`, `produce`, `consume` subcommands,
  sufficient to exercise every API above by hand, plus `scripts/local-cluster.sh` to script a local
  multi-broker demo.

## Non-functional requirements

- **Simplicity first.** Prefer an obviously-correct, readable implementation over a performant or
  defensive one. No premature abstraction, no configuration surface beyond what's described here
  and in the ADR.
- **Minimal third-party dependencies.** Apache Curator + an embedded ZooKeeper server (for
  coordination) and slf4j (logging) are the direct dependencies; ZooKeeper itself pulls in
  `metrics-core` and `snappy-java` at runtime (`build.gradle.kts`'s `runtimeOnly` deps — ZooKeeper
  3.9's snapshot writer loads `SnappyOutputStream` even when snapshots aren't compressed). The wire
  protocol, log, and network layers remain hand-rolled JDK standard library (`java.io`,
  `java.net`) code, so the point of seeing how little you need to build the storage/protocol layers
  still stands — the dependency surface is ZooKeeper's, not minikafka's own.
- **Multi-broker, multi-JVM cluster**, coordinated through ZooKeeper as the single consistent core;
  no peer-to-peer gossip or distributed consensus implemented by minikafka itself. Multiple
  concurrent client connections per broker must be safely supported.
- **Crash safety within a partition.** A partial trailing write from an unclean shutdown must not
  corrupt subsequent reads — it's truncated away on the next startup. A corrupted record (bad CRC)
  is likewise truncated away rather than propagated, matching real Kafka's segment-recovery
  behavior.
- **No fsync.** Durability comes from replication (an acked `acks=all` write survives any single
  broker failure at RF=3/minISR=2), not from flushing to disk — see `DESIGN.md`'s known
  limitations for what this does and doesn't protect against.

## Success criteria

- A fresh checkout builds and tests pass with a single command (`./gradlew build`).
- Standing up ZooKeeper and a 3-broker cluster, creating a replicated topic, producing with
  `acks=all`, killing the leader, and observing the controller elect a new leader with no
  acknowledged data lost, then consuming the full log back — all via the CLI — works end-to-end.
  (`scripts/local-cluster.sh` scripts this demo; see `README.md`.)
- Integration and end-to-end tests exercise the full stack (real TCP sockets, real disk, a real
  embedded ZooKeeper ensemble, and a chaos/nemesis test that injects broker/ZK faults under load)
  and pass, so "working" is verified by more than a manual smoke test.

## Open questions / future ideas (explicitly not committed to)

- Timestamp-based index/seek (Kafka's `.timeindex`) — not needed for v1's offset-only consumption
  model.
- A pluggable partitioner interface — v1 hardcodes hash-by-key / round-robin, which is sufficient
  to demonstrate the concept.
- Retention/deletion of old segments — v1 keeps everything forever.
- Preferred-leader rebalance, partition reassignment, adding partitions, topic deletion, rack
  awareness, KRaft migration — all explicitly deferred by the 2026-09-28 ADR; see its Non-goals.

See `DESIGN.md` in this same folder for the technical design that implements this PRD,
`docs/superpowers/plans/2026-09-23-minikafka-implementation.md` for the original single-broker
task-by-task implementation plan, and `docs/superpowers/plans/2026-09-28-multi-broker-zookeeper.md`
(ADR-001) for the multi-broker/ZooKeeper design this PRD now incorporates.
