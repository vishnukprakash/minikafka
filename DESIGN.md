# minikafka — Design Document

Implements the requirements in `PRD.md` (same folder). For the exact, task-by-task build sequence
of the original single-broker system, see `docs/superpowers/plans/2026-09-23-minikafka-implementation.md`;
for the multi-broker/ZooKeeper cluster (this document's main subject), see the ADR and plan at
`docs/superpowers/plans/2026-09-28-multi-broker-zookeeper.md` (ADR-001) — that document is the
disposable, task-by-task checklist and the record of *why* each decision was made; this document is
the durable reference for *how the system is shaped*, describing the code as built (including a
handful of places where implementation rulings refined the ADR's original algorithm — noted inline).

## Scope recap

A cluster of one or more single-JVM-per-broker processes, coordinated by a single ZooKeeper
ensemble (also single-JVM here, via `minikafka zk`). No KRaft, no consumer-group rebalancing, no
transactions, no log compaction, no unclean leader election. See `PRD.md` → Non-goals for the full
exclusion list.

## Architecture

```
                 TCP, framed binary
 CLI / Client ───────────────────────▶  Network layer (Server, ConnectionHandler)
                                              │  decodes apiKey + correlationId, routes to a handler
                                              ▼
                                        ReplicaManager (per broker)
                              (partition registry: opened only for
                               partitions this broker is assigned, on
                               LeaderAndIsr; produce/fetch dispatch)
                                              │
                              ┌───────────────┼────────────────┐
                              ▼               ▼                ▼
                        Partition (t-0)  Partition (t-1)   ReplicaFetcher (per follower partition)
                         (leader/ISR/HW,       ...          pulls from that partition's leader
                          wraps a Log)
                                              │
                                              ▼
                                             Log
                                       (segments + index)

                                        ZooKeeper (consistent core)
                                  /controller, /controller_epoch,
                                  /brokers/ids/<id>, /brokers/topics/<t>,
                                  /brokers/topics/<t>/partitions/<p>/state,
                                  /consumers/<g>/offsets/<t>/<p>
                                              ▲
                              only the elected controller watches this;
                              every broker registers itself and reads
                              metadata from it directly (no cache)
```

Package dependency graph (strict, one-directional — never introduce an edge pointing the other
way):

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

(This exact edge list is re-derived from `grep -h '^import minikafka\.' src/main/kotlin/minikafka/<pkg>/*.kt`
per package, and kept identical in `CLAUDE.md`.) `server`'s composition root wires `ReplicaManager`,
`Controller`, and `ZkStore`/`ZkIsrStore` together; `cluster` is the controller (election,
reconciliation); `broker` has no dependency on `zk` — it talks to ZK only through the `IsrWriter`/
`BrokerResolver` interfaces `server` implements over `ZkStore`. `proto` and `log` still never depend
on each other — both depend only on `io` for shared nullable-string/bytes encoding primitives.

- **`minikafka.model`** — plain data types shared across layers with no behaviour of their own:
  `TopicPartition`, `PartitionState`, `BrokerInfo`, `Versioned` (a value plus a ZK `zkVersion`, used
  for CAS).
- **`minikafka.io`** — shared binary-encoding primitives (nullable string/bytes, plus the record CRC
  body/computation shared by `log.Record` and `proto.FetchedRecord`) — the reason `log` and `proto`
  can both use the same primitives without depending on each other.
- **`minikafka.log`** — `Record` (offset, CRC, leader epoch, timestamp, key, value),
  `LeaderEpochCache` (epoch → start-offset, rebuilt from records on recovery, no checkpoint file),
  `OffsetIndex` (sparse offset→byte-position index, now with `truncateTo`), `LogSegment` (segment +
  index, crash recovery, `truncateTo`), `Log` (rolls segments, `appendAsLeader`/`appendAsFollower`,
  `truncateTo`, rebuilds itself from disk on construction). Still knows nothing about topics,
  replication, or the network.
- **`minikafka.proto`** — the wire protocol: `Framing`, `Protocol` (`ApiKeys`/`ErrorCodes`),
  `Requests`/`Responses` (every request/response body, `encode`/`decode`). Pure encode/decode; no
  sockets, no broker/cluster knowledge.
- **`minikafka.net`** — `Connection`, extracted from the old `MiniKafkaClient.request` so both the
  client and the broker-internal `ReplicaFetcher`/`ControllerChannel` can send framed requests and
  get framed responses over a socket, with the same timeout/reconnect rules. A `Connection` must be
  closed and recreated after any `IOException` (the stream is left desynchronized).
- **`minikafka.zk`** — `ZkStore` (Curator-backed reads/writes/watches: election multi, fenced CAS
  writes, retry-safe-write recognition per D16), `ZkPaths` (every znode path), `KvCodec` (a tiny
  sorted `k=v\n` payload codec — deliberately not `java.util.Properties`, whose date comment makes
  byte output unstable).
- **`minikafka.cluster`** — the controller: `Controller` (single event-thread state machine —
  election, `BrokersChanged`/`TopicsChanged`/`Elect` reconciliation, fenced ZK writes),
  `ControllerEvent` (event union), `ControllerChannel` (one FIFO sender thread per broker, retries
  `LEADER_AND_ISR` until delivered or the broker leaves/bounces), `PartitionLeaderElector`,
  `ReplicaAssigner` (round-robin replica placement for new topics).
- **`minikafka.broker`** — no longer a single `Broker` god-object (that class, and `OffsetStore`,
  were deleted when this became a cluster): `BrokerConfig`, `Partition` (per-partition leader/ISR/
  HW state machine, wraps a `Log`), `ReplicaManager` (registry of this broker's partitions, applies
  `LeaderAndIsr`, produce/fetch dispatch, acks=all waiting), `ReplicaFetcher` +
  `ReplicaFetcherManager` (follower replication: epoch handshake, truncation, fetch loop),
  `IsrUpdater` (per-broker thread that proposes ISR shrink/expand CAS writes), `IsrWriter`
  (interface `Partition`/`IsrUpdater` use to CAS the ISR znode — implemented over `ZkStore` in
  `server`, keeping `broker` free of a direct `zk` dependency), `FollowerTruncation` (pure,
  independently-testable truncation-target computation), `BrokerSnapshot`, `Clock` (injectable, for
  deterministic lag/backoff tests).
- **`minikafka.server`** — the composition root: `Server`/`ServerConfig` (binds the TCP port,
  acquires the data-dir lock, connects to ZooKeeper, builds `ReplicaManager` + `Controller`,
  registers `/brokers/ids/<id>`, starts the controller event thread), `ConnectionHandler`
  (thread-per-connection request routing over all 8 API keys), `DataDirLock` (`FileChannel.tryLock`
  on `<dataDir>/.lock` + a `<dataDir>/broker.id` mismatch check), `ZkBrokerResolver` (reads
  `/brokers/*` for `METADATA` responses), `ZkIsrStore` (the `IsrWriter` implementation over
  `ZkStore`).
- **`minikafka.client`** — `MiniKafkaClient` (bootstrap list, metadata cache, one `Connection` per
  broker, retry-on-`NOT_LEADER`/`LEADER_NOT_AVAILABLE`/`FENCED_LEADER_EPOCH`/IO with metadata
  refresh), `Partitioner` (client-side hash-by-key/round-robin — the client must know the partition
  before it can look up that partition's leader, so partitioning moved out of the broker).
- **`minikafka.cli`** — `zk` (runs an embedded `ZooKeeperServerMain`), `server`, `topics
  create/list/describe`, `produce`, `consume` subcommands — the only package that depends on both
  `server` and `client` (and, per the graph above, is also the only place outside `zk` that touches
  `org.apache.zookeeper` directly, and reaches into `proto.ErrorCodes` to interpret responses).

## ZooKeeper layout

All paths below are relative to an optional chroot/namespace (`--zk host:port/chroot`, mapped to a
Curator `namespace`, used to isolate test runs on a shared server):

```
/brokers/ids/<id>                          EPHEMERAL  host=..,port=..        czxid = broker epoch
/brokers/topics/<t>                        PERSISTENT 0=1,2,3 / 1=2,3,1      assignment; topic commit point
/brokers/topics/<t>/partitions/<p>/state   PERSISTENT leader, leader_epoch, isr, controller_epoch
/controller                                EPHEMERAL  brokerid=<id>
/controller_epoch                          PERSISTENT <int>                  never deleted
/consumers/<g>/offsets/<t>/<p>              PERSISTENT <offset>
```

Payloads are the tiny sorted `k=v\n` codec (`KvCodec`), not `java.util.Properties` or JSON.
`/brokers/ids/<id>` is ephemeral, tied to the registering broker's ZK session — a broker's *broker
epoch* is that znode's `czxid` (creation transaction id): stable across ZK reconnects within the
same session, and guaranteed to change on any new registration (including a fast restart), which is
exactly the fencing property KIP-380 needs. `/controller` is likewise ephemeral (fast failover on
controller death/session loss); `/controller_epoch` is permanent and monotonically increasing,
serving as the fencing token described in KAFKA-6082.

## Wire protocol

Unchanged framing from the single-broker design:

```
Request:  size:int32 | apiKey:int16 | correlationId:int32 | body
Response: size:int32 | correlationId:int32 | body
```
All integers big-endian; strings are `length:int16 | utf8 bytes` (`-1` = null); byte arrays are
`length:int32 | bytes` (`-1` = null). `errorCode` `0` (`NONE`) means success; nonzero codes are
returned **in-band**, never as connection-level failures.

| apiKey | Name | Request body | Response body |
|---|---|---|---|
| 1 | CREATE_TOPIC | topic, numPartitions:int32, **replicationFactor:int32** | errorCode |
| 2 | METADATA | *(empty)* | **controllerId:int32**, brokers[**id, host, port**], topics[name, partitions[**p, leader(-1 if none), leaderEpoch(-1 if none), replicas[], isr[]**]] |
| 3 | PRODUCE | topic, **partition:int32**, key, value, **acks:int16 (1 or -1)**, **timeoutMs:int32** | errorCode, partition, offset |
| 4 | FETCH | topic, partition, offset, maxBytes, **replicaId:int32 (-1 = consumer)**, **currentLeaderEpoch:int32 (-1 = no check)** | errorCode, **highWatermark:int64 (-1 on error)**, records[offset, **crc:int32**, **leaderEpoch:int32**, timestamp, key, value] |
| 5 | OFFSET_COMMIT | group, topic, partition, offset | errorCode (now ZK-backed, wire-unchanged) |
| 6 | OFFSET_FETCH | group, topic, partition | errorCode, offset (now ZK-backed, wire-unchanged) |
| 7 | LEADER_AND_ISR *(new)* | controllerId, controllerEpoch, brokerEpoch:int64, partitions[topic, p, leader, leaderEpoch, isr[], replicas[], zkVersion] | errorCode |
| 8 | OFFSETS_FOR_LEADER_EPOCH *(new)* | topic, p, replicaId, currentLeaderEpoch, requestedEpoch | errorCode, leaderEpoch, endOffset |

Error codes (`minikafka.proto.ErrorCodes`), all distinct (enforced by a test):

| Code | Name | Meaning |
|---|---|---|
| 0 | NONE | success |
| 1 | UNKNOWN_TOPIC | topic does not exist |
| 2 | UNKNOWN_PARTITION | partition index out of range for the topic |
| 3 | OFFSET_OUT_OF_RANGE | fetch offset < 0 or > log end offset |
| 4 | TOPIC_ALREADY_EXISTS | `topics create` on an existing topic |
| 5 | NOT_LEADER_FOR_PARTITION | this broker isn't (or is no longer) the partition's leader |
| 6 | LEADER_NOT_AVAILABLE | partition has no leader yet (new topic, or all-ISR-dead) |
| 7 | NOT_ENOUGH_REPLICAS | acks=all produce rejected up front: `|committedIsr| < minISR` |
| 8 | NOT_ENOUGH_REPLICAS_AFTER_APPEND | append succeeded but ISR shrank below minISR before ack |
| 9 | REQUEST_TIMED_OUT | acks=all wait exceeded `timeoutMs` |
| 10 | STALE_CONTROLLER_EPOCH | LeaderAndIsr's `controllerEpoch` is behind what this broker has seen |
| 11 | STALE_BROKER_EPOCH | LeaderAndIsr's `brokerEpoch` doesn't match this broker's current ZK registration |
| 12 | FENCED_LEADER_EPOCH | request's `currentLeaderEpoch` is behind the partition's actual epoch |
| 13 | UNKNOWN_LEADER_EPOCH | request's `currentLeaderEpoch` is ahead of what this replica has seen |
| 14 | INVALID_REPLICATION_FACTOR | `replicationFactor` > live broker count (or < 1) |
| 15 | INVALID_REQUIRED_ACKS | `acks` not in `{1, -1}` |
| 16 | REPLICA_NOT_ASSIGNED | a FETCH's `replicaId` isn't in the partition's assigned replica set |

An unknown apiKey still closes the connection (unchanged from the single-broker design).

## On-disk format

Per topic-partition, a directory `<dataDir>/<topic>-<p>/` — unchanged naming/layout from the
single-broker design, but topic *metadata* (partition count, replica assignment) now lives only in
ZooKeeper; a broker's data directory holds only the partitions it is currently assigned, and an
unrecognized local directory is ignored rather than treated as a topic definition.

**`Record`** (one entry in a `.log` file):

```
offset:int64 | crc:int32 | leaderEpoch:int32 | timestamp:int64 | keyLen:int32(-1=null) | key | valueLen:int32 | value
```

`crc` is a `CRC32` over `leaderEpoch | timestamp | key | value` (not `offset` — an offset is
positional, not part of the record's content). The same CRC body/computation is shared with
`proto.FetchedRecord` via `minikafka.io.recordCrcBody`/`recordCrc`, so a fetched wire record and its
on-disk counterpart use one formula. `LogSegment` recovery replays the file from the start; on the
first `EOFException` (partial trailing write) *or* `CorruptRecordException` (bad CRC), it truncates
the file back to the last good record's end and rebuilds the index — both a torn write and a
corrupted-but-complete write are handled the same way, and never propagate to a reader.

`LeaderEpochCache` (in-memory, rebuilt from the segment's records during recovery — no checkpoint
file, since there's no retention/deletion to make a checkpoint necessary) maps `leaderEpoch → start
offset`, used by `OFFSETS_FOR_LEADER_EPOCH` and by `Log.appendAsLeader`'s bookkeeping.

`<dataDir>/.lock` (`FileChannel.tryLock`, held for the process lifetime) plus `<dataDir>/broker.id`
(written on first start, checked on every subsequent start — refuses to start if it doesn't match
`--broker-id`) prevent two processes from sharing a data directory, or a data directory silently
changing broker identity. The old single-broker `offsets.log` file is gone — consumer offsets moved
to ZooKeeper.

## Core algorithms

*(Full detail — including exact retry/backoff numbers, thread names, and every fenced-write case —
lives in the ADR's Decision table and its numbered Core algorithms 1–13; this section summarizes
the mechanisms an implementer/reader most needs, and calls out where the shipped code refined the
ADR.)*

**Controller election & fencing.** Election is one atomic
`multi(create EPHEMERAL /controller, setData(/controller_epoch, e+1, expectedVersion))`: only one
broker's multi can win a race on `/controller_epoch`'s version. The winner's `controller_epoch` is
a monotonically-increasing fencing token carried on every `LEADER_AND_ISR` request and every
controller ZK write (`multi(check(/controller_epoch, myEpochZkVersion), op)`); a broker or a losing
controller with a stale epoch is rejected (`STALE_CONTROLLER_EPOCH`) rather than corrupting state —
this is the KAFKA-6082 zombie-controller fence. A losing candidate watches `/controller` (one-shot
watch, re-armed only after it fires, never eagerly) and re-attempts election when it's deleted. On
becoming controller, the new controller reconciles the entire cluster once (creates missing state
for half-created topics, elects leaders for leaderless/offline partitions, re-elects where a
returned replica lets ISR recover, then pushes full `LEADER_AND_ISR` to every live broker) rather
than trusting any queued-but-undelivered state from its predecessor.

**Broker epoch.** A broker's epoch is the `czxid` of its own `/brokers/ids/<id>` ephemeral znode —
it changes on every fresh registration (including a fast restart under the same broker id) and
never changes across a mere ZK reconnect within one session. Every `LEADER_AND_ISR` the controller
sends carries the *recipient's* broker epoch as the controller last observed it; the broker rejects
the request (`STALE_BROKER_EPOCH`) if it doesn't exactly equal its own current epoch. This is
KIP-380's fix for a restarted broker being told to act on a stale, pre-restart command. **Ruling
R8:** the shipped code keeps the ADR's exact-match (not Kafka's real `<`-relaxed) check, and instead
fixes a registration race — a `LeaderAndIsr` racing a broker's own registration and being wrongly
rejected — by having `ControllerChannel` retry a `STALE_BROKER_EPOCH` response with backoff while
the same sender generation is still current; a real broker bounce discards the queue and resends
full state, so the retry loop is naturally bounded.

**Maximal ISR / high watermark (KIP-497).** Two ISR sets exist during a leader's tenure:
`committedIsr` (exactly what's durably written in the partition's ZK state znode) and
`maximalIsr = committedIsr ∪ pendingAdds` (every replica currently believed caught-up, whether or
not the ZK write proposing to add it has landed yet). The high watermark is computed over
`maximalIsr` (so a replica only just caught up already counts toward what's "acked", per the
Consistent-Core design this follows); `min.insync.replicas` checks (for `acks=all` produce) use
`committedIsr` only (so a still-pending add can't be used to satisfy the durability bar before its
ZK write actually commits). A replica joins `maximalIsr` the moment the leader observes its LEO
catch up to the leader's HW *and* to the epoch's start offset; it's only removed from `maximalIsr`
after a shrink CAS actually commits. This preserves the invariant **ZK ISR ⊆ maximalIsr** at every
instant. A per-broker `isr-updater` thread (one in-flight proposal per partition, computed under
the partition lock, released before the ZK CAS, re-applied under the lock only if nothing else
changed the epoch/zkVersion meanwhile) is the only thing that writes ISR changes for partitions this
broker leads; the controller only touches ISR when it elects a new leader. **Ruling R6:** on a
losing CAS (BadVersion), if a re-read shows the leader is still this broker at the same epoch and
the on-ZK ISR is already a subset of `maximalIsr`, the updater adopts that read (treats it as its
own earlier write that raced a retry) and re-proposes next round, instead of the ADR's original
"mark partition stale" — because only the current leader writes ISR within an epoch, such a
re-read can only be that leader's own committed write, and treating it as foreign would leave the
partition permanently stuck advancing HW. **Ruling R5:** if an ISR-expand CAS fails outright (not a
recognizable own-write), the pending member stays in `maximalIsr` and the partition is flagged stale
until the next `LEADER_AND_ISR` resolves it, rather than being dropped — dropping it could violate
the ZK-ISR-⊆-maximalIsr invariant if the write actually landed despite the client seeing a failure;
the cost is a liveness delay (HW stalls briefly), never a safety violation.

**Leader-epoch truncation (KIP-101/279).** Every record carries the `leaderEpoch` active when it
was written. On becoming (or re-becoming) a follower, a `ReplicaFetcher` runs a handshake before
fetching any data: send its own latest known epoch `E` to the leader via
`OFFSETS_FOR_LEADER_EPOCH`; the leader replies with `(E', end')` — either `E` itself if the leader's
epoch cache agrees, or the leader's own answer for what came right after `E` in its history
(`(-1, LEO)` if the leader has no epoch cache at all; the requested epoch's own earliest start if
`E` predates everything the leader still has). The follower truncates its log to
`min(end', its own endOffsetFor(E').offset, its own LEO)` and repeats with the new, strictly lower
value of `E` until the leader confirms agreement (`E' == E`) — guaranteed to terminate because
epochs strictly decrease each round. This never truncates to the high watermark (which would
silently drop acknowledged-but-not-yet-technically-committed data) — it truncates to the point
where the follower's and leader's histories are known to agree, which is what KIP-101/279 fix
relative to naive HW-based truncation. **Ruling R4 / accepted deviation:** `FETCH` responses carry
the record's stored CRC on the wire (`FetchedRecord.crc`); a follower verifies it at decode time
(`CorruptRecordException` ⇒ discard the response, back off, retry) rather than trusting whatever
`Log.appendAsFollower` is handed — the actual implementation recomputes the CRC from the
already-log-verified fields rather than literally forwarding a stored CRC field end-to-end, which
the review round accepted as equivalent (a `Log` read already verifies its own stored CRC before
the bytes ever reach the wire encoder).

**`acks=all` produce.** The connection-handler thread that received the produce request blocks on a
per-partition `Condition`, woken on every HW advance, until either the HW passes the produced
offset (respond `NONE` if `|committedIsr| ≥ minISR`, else `NOT_ENOUGH_REPLICAS_AFTER_APPEND`), the
partition stops being led by this broker or the epoch changes (`NOT_LEADER_FOR_PARTITION`), or
`timeoutMs` elapses (`REQUEST_TIMED_OUT`). **Ruling R7:** the deadline is measured against real
wall-clock time (`System.nanoTime` via `Condition.awaitNanos`), not the injectable `Clock` used
elsewhere for deterministic lag tests — this guarantees a waiter always terminates even if a test's
`MutableClock` is never advanced; the cost is that acks=all timeout tests use a short *real* timeout
(100ms) rather than simulated time.

**Consumer fetch.** Only the leader serves consumer fetches (`replicaId = -1`); an offset outside
`[0, LEO]` is `OFFSET_OUT_OF_RANGE`; a request inside `[HW, LEO]` returns an empty batch (data exists
but isn't yet acknowledged-safe to hand a consumer); otherwise records strictly below HW are
returned. **This is why `minikafka consume` always stops at the high watermark** rather than the
log's true end — it's consuming exactly what's safe to consider committed, the same distinction a
real Kafka consumer's `read_committed`/default isolation makes relative to `LEO`.

**Follower fetch / fencing (`UNKNOWN_LEADER_EPOCH`).** Every `FETCH` from a replica or a consumer
carries `currentLeaderEpoch` (`-1` = no check, used by consumers); the leader replies
`FENCED_LEADER_EPOCH` if the caller's epoch is behind the partition's actual epoch, or
`UNKNOWN_LEADER_EPOCH` if it's *ahead* (the leader hasn't caught up to a controller update the
caller already knows about). **Ruling R9:** on `UNKNOWN_LEADER_EPOCH` the fetcher keeps its
`Connection` open (the response is well-formed and the stream is still in sync — only the epoch
disagrees) rather than closing/reconnecting, a deliberate deviation from the general "close and
recreate on any error" binding note, since nothing about the connection itself is broken here.

## Guarantees (unclean leader election always off)

| acks | min.insync.replicas | Survives | Can still lose acked writes when |
|---|---|---|---|
| 1 | any | — | leader dies before any follower fetches the record |
| all | 1 | leader death, if ≥1 follower was in the ISR | ISR shrank to just the leader, then the leader's disk/host is lost |
| all | 2 (with RF=3) | any single broker failure | ≥2 simultaneous host/power losses (no fsync anywhere) |

The only configuration where `acks=all` survives *any* single-broker failure is **RF=3,
min.insync.replicas=2** — this is what `README.md`'s demo and the e2e failover tests use.
`--min-insync` is a per-broker startup flag; it **must be set the same on every broker in the
cluster** (nothing enforces or replicates this — a mismatch is a footgun, listed below).

## Known limitations (accepted, documented, not bugs)

- `acks=1` can lose the most recently acked write if the leader dies before any follower replicates
  it.
- No `fsync` anywhere — durability is purely a function of how many replicas have the data, not of
  what's flushed to disk.
- Delivery is at-least-once: a timeout, `NOT_ENOUGH_REPLICAS_AFTER_APPEND`, or I/O error leaves the
  outcome ambiguous to the producer; a retry may duplicate. No idempotent/transactional producer.
- A partition goes fully offline (`leader = -1`, `LEADER_NOT_AVAILABLE`) if every ISR member dies,
  by design (unclean election is never attempted) — it stays offline, ISR preserved, until a member
  returns.
- A ZooKeeper outage freezes failover, ISR changes, topic creation, and offset commits across the
  whole cluster — but does **not** stop already-assigned brokers from serving produce/fetch on
  their current partitions (`SUSPENDED` session state only logs; `LOST` makes the controller
  resign, but existing leaders/followers keep serving their existing roles).
- Every broker races to create `/controller` on startup — harmless with the broker counts this
  project targets, but an unbounded herd at large N.
- No preferred-leader rebalance: once a leader moves off a broker, nothing moves it back, so leader
  load can accumulate on the brokers that happened to survive failures.
- A single ZooKeeper node is a SPOF in the default local setup (`scripts/local-cluster.sh` runs one
  `minikafka zk`); production Kafka runs a 3+ node ensemble (session timeout ~6s, tickTime 2000ms —
  this project's tests use the same ballpark).
- Consumer group offsets are last-writer-wins across concurrent commits from the same group — no
  per-consumer ownership/fencing.
- Wiping a broker's data directory while that broker is (or was) in a partition's ISR can lose
  committed data that only existed on that broker — the same risk as wiping any Kafka broker's log
  directory.
- The on-disk `Record` format changed (added `crc`, `leaderEpoch`) in a breaking way relative to the
  single-broker design — old single-broker data directories are not readable; wipe them.
- `--min-insync` (min.insync.replicas) must be configured identically on every broker; nothing
  detects or prevents a mismatch.

**Non-goals** (see `PRD.md` for the full list): `acks=0`, unclean leader election,
`UpdateMetadata`/metadata cache, controlled-shutdown RPC, preferred-leader rebalance, partition
reassignment, adding partitions, topic deletion, retention, per-topic config, fetch batching/long
poll, fetch-from-follower, idempotent/transactional producer, consumer-group rebalancing, rack
awareness, ZooKeeper ACL/TLS, KRaft, old-format data migration.

## Concurrency model

- **Every thread is a daemon**, named `b<id>-<role>[-topic-p]` (e.g. `b2-fetcher-demo-0`,
  `b1-controller`, `b3-isr-updater`, `b1-acceptor`) — nothing spawned by a broker keeps a JVM (or a
  test) alive after `Server.stop()`.
- **Lock order: `Partition` lock → `Log` lock.** `Partition` methods call into `Log` with the
  partition lock held; `Log` never calls back out. **A `Partition` lock is never held across
  network or ZK I/O** — every algorithm that needs to both mutate partition state and do I/O (ISR
  CAS, sending `LEADER_AND_ISR`, the follower epoch handshake) prepares its intent under the lock,
  releases it, performs the I/O, then re-acquires the lock only to apply the (possibly
  now-stale-and-rechecked) result. This is what keeps a slow ZK write or a slow socket from
  blocking every produce/fetch on that partition.
  - `ReplicaManager.applyLeaderAndIsr` runs under a per-broker `leaderAndIsrLock` (distinct from any
    single partition's lock) while updating every partition's role in one `LeaderAndIsr` request;
    it releases every partition lock before rewiring fetchers (stopping/starting a
    `ReplicaFetcher` involves a bounded join, never done under a partition lock).
  - `IsrUpdater` (one thread per broker) builds an ISR proposal under a partition's lock, releases
    it, does the ZK CAS with no lock held, then re-acquires the lock to apply the outcome only if
    the epoch/zkVersion it captured is still current — otherwise it re-reads and either adopts
    (Ruling R6) or waits for the next `LeaderAndIsr`.
  - `ReplicaFetcher` (one thread per follower-side partition) never holds a partition lock across a
    network call — every local step (truncate, append, read epoch/LEO) goes through
    `ReplicaManager`, which locks only around that one local operation.
  - `Controller` runs its entire state machine (election, reconciliation, ISR-on-elect,
    `BrokersChanged`/`TopicsChanged`/`Elect` handling) on a **single event thread** draining a
    `LinkedBlockingQueue<ControllerEvent>` — controller state itself needs no locks because nothing
    but that thread ever touches it; only the elected controller watches ZK at all (no herd, no
    watch-ordering races between brokers).
  - `ControllerChannel` runs one FIFO sender thread per broker, so a slow/unreachable broker's
    retries never block delivery to any other broker.
- **`Log`/`LogSegment`** still synchronize their own append/read (one lock per partition's log, so
  partitions never contend with each other) — unchanged from the single-broker design.
- **Every ZK write is retry-safe** (tolerates Curator's replay of a write after `ConnectionLoss`):
  on `NodeExists`/`BadVersion`, the writer re-reads and checks whether the current state is
  recognizably its own already-committed write (by `ephemeralOwner`, or by leader+epoch+ISR
  matching what it intended) before treating the conflict as a real one. **Ruling R1:** this is
  tested by pre-applying the exact write a Curator replay would produce and asserting the
  recognize-own-write path classifies it correctly, rather than by literally forcing a Curator
  internal replay (not deterministically triggerable in a test) — the real `ConnectionLoss` path
  itself is exercised by the e2e nemesis tests, just not by this specific unit test.
- **Tests never use `Thread.sleep`** to wait for async state; they poll with
  `eventually`/`alwaysFor` helpers, and lag/backoff/timeouts are driven by an injectable `Clock`
  (`MutableClock` in tests) wherever that's compatible with also guaranteeing termination (the one
  documented exception being the real-time `acks=all` deadline, Ruling R7).
