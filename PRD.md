# minikafka — Product Requirements Document

## Summary

minikafka is a miniature, single-broker clone of Apache Kafka, built as a
learning project to demonstrate the core mental model of a Kafka-style
log-structured message broker: topics, partitions, an append-only disk log,
offset-based consumption, and a real TCP wire protocol — in a small enough
codebase to read end-to-end in one sitting.

It is not a production message queue. It is a reference implementation:
something a developer can build, run, break, and read to understand *why*
Kafka is built the way it is, without wading through the actual Kafka
codebase's operational complexity (replication, ZooKeeper/KRaft, rebalancing
protocols, etc.).

## Background / motivation

This project lives in `distributed-systems/`, alongside a sibling workshop
repo (`ds-patterns-workshop`) that already contains a much more elaborate
`kafkalite` — a clustered, ZooKeeper-backed Kafka clone with controller
election and replica management, built for a distributed-systems patterns
course. minikafka is deliberately **not** that. It targets the single-broker
subset of Kafka's design: the part that explains *how a message log actually
works on disk*, independent of the (substantial) extra complexity clustering
adds. The two projects are complementary, not duplicates.

## Goals

1. Demonstrate topics and partitions as the unit of parallelism and ordering.
2. Demonstrate the append-only, segmented, disk-backed log as Kafka's core
   storage primitive, including a sparse index for fast offset lookup and
   crash recovery (truncating a partial trailing write).
3. Demonstrate offset-based consumption: a consumer reads forward from an
   offset it controls, and can optionally have the broker remember that
   offset on its behalf (consumer offset commit/fetch).
4. Demonstrate a real, custom binary wire protocol over TCP — not a REST/JSON
   shortcut — so the networking layer teaches the same lesson Kafka's own
   protocol does (length-prefixed framing, a versioned-style request header,
   correlation IDs).
5. Ship a working CLI so the whole system can be driven and observed by hand,
   the way `kafka-console-producer.sh`/`kafka-console-consumer.sh` are used to
   poke at a real Kafka cluster.

## Non-goals

Explicitly out of scope, and intentionally so — adding any of these would
turn this from a small, readable reference implementation into a second
`kafkalite`:

- Replication, leader/follower partitions, ISR, leader election.
- ZooKeeper or KRaft-style consensus/metadata quorum.
- Consumer group rebalancing / partition assignment protocols. (Basic
  consumer *offset* commit/fetch is in scope; the rebalance protocol that
  decides which consumer owns which partition is not.)
- Transactions / exactly-once semantics.
- Log compaction (only time/size-based segment rolling).
- Authentication, authorization, encryption in transit.
- Multi-broker deployment of any kind.

## Users / use cases

The primary "user" is a developer (including future-me) who wants to:

- Stand up a broker locally and manually produce/consume messages via the
  CLI to build intuition for topics, partitions, and offsets.
- Read the source to understand a concrete, minimal implementation of a
  segmented log with an offset index — the same idea behind Kafka, most
  embedded databases' WALs, and event-sourced systems generally.
- Use it as a small, controlled target for further learning experiments
  (e.g. adding a feature, changing partitioning strategy, benchmarking).

## Functional requirements

- **Topics:** create a topic with a fixed number of partitions; list topics
  and their partition counts. No delete/alter in v1.
- **Produce:** write a message (optional key, required value) to a topic.
  Messages with a key are routed deterministically to a partition by hashing
  the key; messages without a key are spread round-robin across partitions.
  The broker returns the partition and offset the message landed at.
- **Fetch:** read messages from a given topic-partition starting at a given
  offset, up to an approximate byte cap.
- **Consumer offsets:** commit and fetch a single named consumer group's
  offset for a topic-partition, so a consumer can resume where it left off.
- **Persistence:** all of the above survive a broker restart — topics,
  messages, and committed offsets are all read back from disk on startup.
- **CLI:** `server`, `topics create`, `topics list`, `produce`, `consume`
  subcommands, sufficient to exercise every API above by hand.

## Non-functional requirements

- **Simplicity first.** Prefer an obviously-correct, readable implementation
  over a performant or defensive one. No premature abstraction, no
  configuration surface beyond what's described here.
- **No third-party runtime dependencies.** JDK standard library only
  (`java.io`, `java.net`) — the point is to see how little you actually need
  to build this.
- **Single broker, single JVM process.** No distributed coordination.
  Multiple concurrent client connections must be safely supported, since
  that's a real property of even a single Kafka broker.
- **Crash safety within a partition.** A partial trailing write from an
  unclean shutdown must not corrupt subsequent reads — it's truncated away
  on the next startup, matching real Kafka's segment-recovery behavior.

## Success criteria

- A fresh checkout builds and tests pass with a single command
  (`./gradlew build`).
- Starting the server, creating a topic, producing several messages (some
  keyed, some not), and consuming them back — including after restarting the
  broker — works end-to-end via the CLI.
- An integration test exercises the full stack (real TCP socket, real disk)
  and passes, so "working" is verified by more than a manual smoke test.

## Open questions / future ideas (explicitly not committed to)

- Timestamp-based index/seek (Kafka's `.timeindex`) — not needed for v1's
  offset-only consumption model.
- A pluggable partitioner interface — v1 hardcodes hash-by-key / round-robin,
  which is sufficient to demonstrate the concept.
- Retention/deletion of old segments — v1 keeps everything forever.

See `DESIGN.md` in this same folder for the technical design that implements
this PRD, and `docs/superpowers/plans/2026-09-23-minikafka-implementation.md`
for the task-by-task implementation plan.
