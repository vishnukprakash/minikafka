# minikafka Multi-Broker Cluster (ZooKeeper Consistent Core) — ADR + Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: use `superpowers:subagent-driven-development` (or
> `superpowers:executing-plans`) to implement this plan task by task. Every task follows TDD:
> write failing test → run, expect FAIL → implement → run, expect PASS → commit. Each task ends with
> `./gradlew build` green.

The 2026-09-23 plan is historical and is left unedited; this ADR supersedes its
"no replication / no ZooKeeper" constraint and the `PRD.md` non-goals.

---

## ADR-001: Replicated multi-broker cluster with ZooKeeper as the consistent core

**Status:** Proposed (2026-09-28) → Accepted on approval. Supersedes PRD non-goals "Replication…",
"ZooKeeper or KRaft…", "Multi-broker deployment of any kind".

### Context

minikafka is a single-broker educational Kafka clone. The user wants a multi-broker setup that
keeps running when brokers fail, using ZooKeeper as the consistent core, following
distributed-consensus best practices, kept simple. User decisions: **cluster-only** (single-broker
mode removed; a 1-broker cluster with RF=1 replaces it) and **replication + failover** in scope.

Inputs: codebase map; sibling `ds-patterns-workshop/kafkalite` (reused: atomic controller-election
`multi`, watch→event-queue→re-read, only controller watches ZK; its gaps — no failover, ISR, leader
epochs — designed fresh); research (pre-KRaft Kafka znodes, KAFKA-6082 epoch-checked multi,
KIP-101/279 epoch truncation, KIP-380 broker epoch, KIP-497 maximal ISR, KAFKA-1382 CAS retry,
Jepsen Kafka, Curator docs, Unmesh Joshi's Consistent Core / Leader-Followers / High-Water Mark /
Generation Clock / Lease patterns); three design agents and three independent reviewers
(safety, feasibility, best-practice/test). Reviewer findings are folded in below.

### Decision

Pre-KRaft Kafka architecture, minimised:

| # | Decision | Rationale / tradeoff |
|---|---|---|
| D1 | **Curator 5.9.0** `curator-framework` (brings ZooKeeper 3.9.x; no separate pin, logback excluded), `curator-test` for tests. No recipes. | Curator handles connection retry; election hand-rolled so Kafka's mechanism stays visible. |
| D2 | **Controller election** = one `multi(create EPHEMERAL /controller, setData(/controller_epoch, e+1, ver))`. | Epoch is a generation clock / fencing token. |
| D3 | **Controller fencing**: every controller ZK write = `multi(check(/controller_epoch, myEpochZkVersion), op)`; every LeaderAndIsr carries `controllerEpoch`; brokers reject lower. Broker initialises its "seen controller epoch" from `/controller_epoch` at startup. | Zombie controller can neither write ZK nor command brokers (KAFKA-6082). |
| D4 | **Broker epoch** = czxid of `/brokers/ids/<id>`; LeaderAndIsr carries the recipient's broker epoch; broker rejects mismatch (`STALE_BROKER_EPOCH`). | Prevents a stale queued LeaderAndIsr from making a restarted broker a second leader (KIP-380). |
| D5 | **Controller→broker = push `LEADER_AND_ISR`** over the existing TCP protocol, one FIFO sender thread per broker, retry until delivered or broker leaves/bounces (then queue is discarded and full state resent). The controller sends to itself over TCP too (no in-process shortcut). **ZK first, RPC second**: never send state whose fenced write failed. | Only the controller watches ZK: no herd, no watch-ordering races. |
| D6 | **Client metadata**: any broker answers `METADATA` by reading ZK directly. No UpdateMetadata/cache. | Stale reads harmless — client refreshes on NOT_LEADER. |
| D7 | **ISR** changed by the leader via CAS on the state znode's zkVersion, using **maximal ISR** (KIP-497): `committedIsr` (as in ZK) and `maximalIsr = committedIsr ∪ pendingAdds`. HW uses maximalIsr; minISR checks use committedIsr; expansion joins maximalIsr *before* the CAS; shrink leaves maximalIsr only *after* CAS success. One in-flight ISR change per partition, executed on a per-broker `isr-updater` thread, never holding the partition lock. Controller edits ISR only when electing. **Invariant: ZK ISR ⊆ maximalIsr.** | Every electable replica was counted in HW ⇒ elected leader has all committed data. |
| D8 | **Leader-epoch truncation** (KIP-101 + iterative KIP-279): each record stores `leaderEpoch`; follower loops `OFFSETS_FOR_LEADER_EPOCH` until epochs agree. Never truncate to HW. | HW truncation loses acked data / diverges. |
| D9 | `LeaderEpochCache` rebuilt from records during segment recovery; no checkpoint file. `leaderEpochStartOffset` held in memory (set at becomeLeader). | Equivalent answers because there is no retention/deletion. |
| D10 | **No HW checkpoint**; HW = 0 on load, recomputed; unknown follower LEO = −1 included in the min. | Consumers briefly see less after restart; never wrong. |
| D11 | **Unclean leader election disabled** (not configurable). All-ISR-dead ⇒ `leader=-1`, ISR preserved. | Consistency over availability (Jepsen). |
| D12 | **acks ∈ {1, all}**; acks=0 out of scope. Defaults: RF=1, minISR=1; docs/e2e use RF=3/minISR=2 (the only config where acks=all survives any single failure). | acks=1 loss on leader crash is documented. |
| D13 | **Client-side partitioning** (existing `floorMod(key.contentHashCode(), n)` / round-robin moved from `Broker.choosePartition`); PRODUCE carries `partition`. | Must know partition before leader. |
| D14 | **Consumer offsets in ZK** `/consumers/<g>/offsets/<t>/<p>` via `create().orSetData().creatingParentsIfNeeded()`; `OffsetStore` deleted. | Cluster-wide, nothing to replicate; last-writer-wins. |
| D15 | **acks=all wait** on the connection-handler thread via per-partition `Condition`, with timeout. | Fits thread-per-connection; no purgatory. |
| D16 | **Retry-safe ZK writes**: every write tolerates Curator's replay after ConnectionLoss — on NodeExists/BadVersion re-read and recognise our own committed write (by `ephemeralOwner`, or leader+epoch+ISR match). | Otherwise liveness bugs (KAFKA-1382). |
| D17 | **Session states**: SUSPENDED = log only (fencing makes pausing unnecessary); LOST = controller resigns, brokers keep serving existing roles; RECONNECTED/new session = idempotently re-register (wait for stale ephemeral) + enqueue `Elect`. | ZK outage must not become total outage. |
| D18 | **Record integrity**: add `crc:int32` (CRC32 of epoch..value); recovery truncates at first unparsable/bad-CRC record and rebuilds the index; `appendAsFollower` rejects bad CRC. | Cheap now (format already breaking), stops torn writes/corruption spreading. |
| D19 | **Durability**: no fsync; durability via replication. | Power loss on all ISR replicas can lose acked data — documented. |
| D20 | **Delivery**: at-least-once (timeouts/NOT_ENOUGH_REPLICAS_AFTER_APPEND/IO errors = outcome unknown; retry may duplicate). | No idempotent producer. |
| D21 | **Znode payload**: tiny sorted `k=v\n` codec (not `java.util.Properties`, whose date comment makes bytes unstable). | Readable in `zkCli`, no JSON dependency. |
| D22 | **ZK chroot/namespace** supported: `--zk host:2181/minikafka` → Curator `namespace`. | Isolation; tests use a namespace per test on a shared server. |

### Consequences

Guarantees (unclean election off):

| acks | minISR | Survives | Can lose acked writes when |
|---|---|---|---|
| 1 | any | — | leader dies before followers fetch |
| all | 1 | leader death if ≥1 follower in ISR | ISR shrank to leader, then leader's disk/host lost |
| all | 2 (RF=3) | any single broker failure | ≥2 simultaneous host/power losses (no fsync) |

**Known limitations (documented):** acks=1 loss; no fsync; duplicates on retry; partition offline
while its whole ISR is dead; ZK outage freezes failover/ISR changes/topic creation/offset commits
(serving continues); herd on `/controller` (small N); leader imbalance (no preferred-leader
rebalance); single ZK node is a SPOF (production: 3-node ensemble, session ~6s, tickTime 2000);
last-writer-wins group offsets; wiping a broker's data dir while it is in an ISR can lose committed
data; breaking on-disk format (wipe old data dirs); `--min-insync` must match on all brokers.

**Non-goals:** acks=0, unclean election, UpdateMetadata/metadata cache, controlled-shutdown RPC,
preferred-leader rebalance, reassignment, adding partitions, topic deletion, retention, per-topic
config, fetch batching/long-poll, fetch-from-follower, idempotent/transactional producer,
consumer-group rebalancing, rack awareness, ZK ACL/TLS, KRaft, old-format migration.

### Alternatives considered

- **KRaft / own Raft** — rejected: user asked for ZooKeeper; far larger scope.
- **Curator `LeaderLatch`** — rejected: sequential-node recipe hides the epoch/fencing mechanism.
- **Brokers watch partition-state znodes** — rejected: herd, ordering races; kafkalite and Kafka use push.
- **UpdateMetadata RPC + broker metadata cache** — rejected: extra RPC/cache; reading ZK is enough at this scale.
- **HW-based truncation** — rejected: KIP-101 data-loss/divergence scenarios.
- **Unclean election option** — rejected: availability over safety not wanted for an educational reference.
- **Keep single-broker mode alongside** — rejected by user (cluster-only).
- **Separate `java-test-fixtures` / `e2eTest` source set** — rejected: Kotlin `internal` visibility friction and Gradle wiring; tags on one source set suffice.

---

## Goal / Architecture / Tech Stack

**Goal:** 3+ in-process or multi-process brokers coordinated by ZooKeeper; topics replicated with
RF≤live brokers; leader failover with zero acked (acks=all, minISR≥2) loss; clients route by metadata.

**Architecture (dependency graph — replaces CLAUDE.md graph):**
```
cli     → server, client, model
server  → broker, cluster, zk, net, proto, model   (composition root; IsrWriter impl over ZkStore)
cluster → zk, net, proto, model                    (controller)
broker  → log, net, proto, model                   (replica manager; no zk dependency)
client  → net, proto, model
zk      → model, Curator
net     → proto
proto, log → io        model → nothing             (proto and log still never depend on each other)
```

**Tech stack:** Kotlin 2.4.10, Gradle 9.7.1 wrapper (unchanged — see ruling in the 2026-09-23 plan),
JDK per existing ruling (26 default), Curator 5.9.0, slf4j 2.0.x + slf4j-simple, JUnit 5.10.3.
**Ruling carried forward:** do not change Gradle/Kotlin versions. Task 1 smoke-tests Curator +
`TestingServer` on this JDK (with `-Dzookeeper.sasl.client=false`); if it fails, fall back to a
`jvmToolchain(21)` for tests and record a new ruling here.

## Global Constraints

- Dependency graph above; never add an edge pointing the other way.
- Big-endian everywhere; every wire type in `proto` has `encode(DataOutput)` + companion `decode(DataInput)`.
- Every thread is a daemon and named `b<id>-<role>[-topic-p]` (e.g. `b2-fetcher-demo-0`, `b1-controller`, `b3-isr-updater`).
- **Never hold a Partition lock across network or ZK I/O.** Lock order: Partition → Log.
- Every ZK write is retry-safe (D16). Every controller write is epoch-fenced (D3).
- Tests: no `Thread.sleep`; wait with `eventually/alwaysFor`; injectable `Clock` for lag/backoff/timeouts.
- Timeout ordering: client socket timeout (40s / tests 10s) > produce `timeoutMs` (30s / 5s) > `replicaLagTimeMaxMs` (10s / 1s). Fetcher backoff 50ms on empty responses, capped exponential on errors.
- INFO logs for: controller elected/resigned (epoch), LeaderAndIsr applied/skipped (reason), ISR shrink/expand, truncation (from→to, epoch), session state changes (sessionId), every fenced rejection.

## ZooKeeper layout (under optional chroot)

```
/brokers/ids/<id>                          EPHEMERAL  host=..,port=..        czxid = broker epoch
/brokers/topics/<t>                        PERSISTENT 0=1,2,3 / 1=2,3,1      assignment; topic commit point
/brokers/topics/<t>/partitions/<p>/state   PERSISTENT leader, leader_epoch, isr, controller_epoch
/controller                                EPHEMERAL  brokerid=<id>
/controller_epoch                          PERSISTENT <int>                   never deleted
/consumers/<g>/offsets/<t>/<p>             PERSISTENT <offset>
```

## Wire protocol (changes)

| ApiKey | Request | Response | New errors |
|---|---|---|---|
| CREATE_TOPIC(1) | + `replicationFactor:int32` | errorCode | INVALID_REPLICATION_FACTOR |
| METADATA(2) | — | `controllerId`, `brokers[id,host,port]`, `topics[name, partitions[p, leader(-1), leaderEpoch, replicas[], isr[]]]` | — |
| PRODUCE(3) | + `partition:int32`, `acks:int16(1\|-1)`, `timeoutMs:int32` | errorCode, partition, offset | NOT_LEADER_FOR_PARTITION, NOT_ENOUGH_REPLICAS, NOT_ENOUGH_REPLICAS_AFTER_APPEND, REQUEST_TIMED_OUT, INVALID_REQUIRED_ACKS |
| FETCH(4) | + `replicaId:int32(-1 consumer)`, `currentLeaderEpoch:int32(-1 no check)` | + `highWatermark:int64`; record + `leaderEpoch` | FENCED_LEADER_EPOCH, UNKNOWN_LEADER_EPOCH, NOT_LEADER_FOR_PARTITION, REPLICA_NOT_ASSIGNED |
| OFFSET_COMMIT(5)/FETCH(6) | unchanged | unchanged | — (ZK-backed) |
| LEADER_AND_ISR(7) new | `controllerId, controllerEpoch, brokerEpoch:int64, partitions[topic, p, leader, leaderEpoch, isr[], replicas[], zkVersion]` | errorCode | STALE_CONTROLLER_EPOCH, STALE_BROKER_EPOCH |
| OFFSETS_FOR_LEADER_EPOCH(8) new | `topic, p, replicaId, currentLeaderEpoch, requestedEpoch` | errorCode, leaderEpoch, endOffset | FENCED/UNKNOWN_LEADER_EPOCH |

Also LEADER_NOT_AVAILABLE. New codes numbered from 5; test `every error code is distinct`.

## On-disk format

- `Record`: `offset:int64 | crc:int32 | leaderEpoch:int32 | timestamp:int64 | key | value` (CRC32 over leaderEpoch..value).
- Partition dirs unchanged `<dataDir>/<topic>-<p>/`; topic metadata only in ZK (unknown local dirs ignored).
- `<dataDir>/.lock` (`FileChannel.tryLock`) + `<dataDir>/broker.id` (refuse start on mismatch). `offsets.log` removed.

## Core algorithms

1. **Broker startup:** lock data dir, check `broker.id` → Curator connect (log negotiated session timeout) → read `/controller_epoch` into seenControllerEpoch → build ReplicaManager (partitions opened on LeaderAndIsr; until then NOT_LEADER) → bind TCP server → register `/brokers/ids/<id>` with actual bound port + `--advertised-host` (NodeExists: ok if `ephemeralOwner == my session`, else wait up to 2×session timeout for stale node, then fail "duplicate broker id") → enqueue `Elect`.
2. **Graceful stop:** stop fetchers → close Curator (ephemeral deleted ⇒ fast failover) → close socket/handlers → close logs. CLI installs a shutdown hook.
3. **Controller election (event thread):** ensure `/controller_epoch` (NodeExists ok) → read `(e, ver)` → election multi. Win ⇒ record `myEpoch`, `myEpochZkVersion` from the result. NodeExists/ConnectionLoss ⇒ read `/controller`; if `ephemeralOwner == my session` it's a win (re-read epoch), else watch `/controller` (read+watch in one call). On win: watch `/brokers/ids`, `/brokers/topics` (read+watch atomically) → **full reconcile**: create missing state znodes for half-created topics, elect leaders for partitions whose leader is dead/absent, re-elect offline partitions whose ISR member returned, send full LeaderAndIsr to every live broker.
4. **Fenced controller write:** `multi(check(epoch), op)`. BadVersion on the epoch check ⇒ resign (clear state, stop senders). BadVersion on the state op ⇒ re-read; if the znode already equals what I intended with my controller_epoch, treat as mine (still send LeaderAndIsr); else recompute.
5. **Topic creation (any broker):** `rf ≤ live brokers` else INVALID_REPLICATION_FACTOR → `ReplicaAssigner` → create assignment (NodeExists ⇒ TOPIC_ALREADY_EXISTS; documented that a Curator replay may report this for one's own create) → NONE. Controller `TopicsChanged`: per partition, if ≥1 assigned replica live, create parents (outside txn) then fenced `multi(check, create state{leader=first live replica, leader_epoch=0, isr=live replicas})` (NodeExists = done); if none live, leave state absent until a replica registers. Clients get LEADER_NOT_AVAILABLE and retry (budget ~10×200ms).
6. **BrokersChanged (level-triggered diff of `{id→czxid}`; czxid change = bounce = death + start):** discard dead/bounced brokers' sender queues. For partitions whose **leader** is dead/bounced, and offline partitions: re-read state; newLeader = first assigned replica live ∧ in ISR; write `leader, leader_epoch+1, isr ∩ live` (fenced) or `leader=-1, leader_epoch+1, isr unchanged`. Dead followers are left to the leader's lag shrink. Send LeaderAndIsr to live replicas of changed partitions; new/bounced brokers get full state.
7. **Apply LeaderAndIsr:** reject `brokerEpoch ≠ my czxid` (STALE_BROKER_EPOCH) or `controllerEpoch < seen` (STALE_CONTROLLER_EPOCH); update seen. Per partition, skip unless `leaderEpoch > local`. Under partition lock: set epoch/role/committedIsr=maximalIsr=isr/zkVersion; fail pending acks=all waiters (NOT_LEADER); **release lock**, then stop/join old fetcher. Leader: `leaderEpochStartOffset = LEO`, every follower `leo=-1, lastCaughtUp=now`, recompute HW (ISR={self} ⇒ HW=LEO). Follower: start new fetcher (also on every epoch bump with same leader).
8. **Follower fetcher:** if log empty skip handshake; else loop: send `latestEpoch E` → leader `(E', end')` (leader: empty cache ⇒ (−1, LEO); requested below earliest ⇒ (requested, earliestStart)) → truncate to `min(end', own.endOffsetFor(E').offset, LEO)` (E' = −1 ⇒ `min(end', LEO)`) → stop when `E' == E` (terminates: epochs strictly decrease). Then loop FETCH(replicaId, LEO, epoch): under lock, check `fetchEpoch == leaderEpoch` else drop; `appendAsFollower` (CRC checked); `HW = min(LEO, leaderHW)`. Empty ⇒ backoff 50ms. FENCED ⇒ wait for LeaderAndIsr; UNKNOWN_LEADER_EPOCH/IO ⇒ capped backoff; OFFSET_OUT_OF_RANGE ⇒ redo handshake.
9. **Leader on follower fetch:** replicaId ∉ replicas ⇒ REPLICA_NOT_ASSIGNED; epoch check (FENCED/UNKNOWN); only then record follower LEO. Caught-up rule: `o ≥ LEO ⇒ lastCaughtUp=now; else if o ≥ lastFetchLeaderLEO ⇒ lastCaughtUp=lastFetchTime`; store `lastFetchLeaderLEO=LEO, lastFetchTime=now`. If follower ∉ maximalIsr ∧ `leo ≥ HW` ∧ `leo ≥ leaderEpochStartOffset` ⇒ add to maximalIsr, flag expand for isr-updater. `HW = max(HW, min LEO over maximalIsr)` (−1 included); signal. Return records up to LEO + HW.
10. **isr-updater thread (per broker):** every lag/2 and on flag: under lock build proposal (shrink: followers with `now − lastCaughtUp > lag`; plus pending adds), capture leaderEpoch/zkVersion; release lock; `IsrWriter.cas` (preserving leader_epoch/controller_epoch); re-lock; apply only if epoch/zkVersion unchanged: success ⇒ committedIsr=proposal, maximalIsr updated (shrunk members removed now), zkVersion=new, recompute HW; BadVersion ⇒ re-read, if leader==me ∧ epoch==local ∧ isr==proposal adopt, else mark stale (await LeaderAndIsr); failure of an expand ⇒ drop pending add.
11. **Produce:** not leader ⇒ NOT_LEADER; acks ∉{1,-1} ⇒ INVALID_REQUIRED_ACKS; acks=all ∧ |committedIsr| < minISR ⇒ NOT_ENOUGH_REPLICAS (before append); `appendAsLeader(epoch)`; acks=1 ⇒ respond; acks=all ⇒ await: `HW > offset` ⇒ (|committedIsr| ≥ minISR ? NONE : NOT_ENOUGH_REPLICAS_AFTER_APPEND); epoch changed/not leader ⇒ NOT_LEADER; deadline ⇒ REQUEST_TIMED_OUT.
12. **Consumer fetch:** leader only; `offset < 0 ∨ > LEO` ⇒ OFFSET_OUT_OF_RANGE; `[HW, LEO]` ⇒ empty; else records `< HW`.
13. **Client:** bootstrap list (alias `--host/--port`); metadata cache; one `Connection` per broker; on NOT_LEADER/LEADER_NOT_AVAILABLE/FENCED/IO ⇒ refresh metadata, backoff, retry (≤10); on socket timeout close & recreate the Connection (stream desynced); unknown topic ⇒ UNKNOWN_TOPIC immediately.

## File structure (new/changed)

```
src/main/kotlin/minikafka/
  model/   TopicPartition.kt, PartitionState.kt, BrokerInfo.kt, Versioned.kt
  io/      Encoding.kt (unchanged)
  log/     Record.kt(+crc,epoch)  LogSegment.kt(truncateTo, epochStarts, rebuild index, robust recovery)
           OffsetIndex.kt(truncateTo)  Log.kt(appendAsLeader/Follower, truncateTo, read(max, maxOffsetExclusive))
           LeaderEpochCache.kt
  proto/   Protocol.kt(new keys/codes)  Requests.kt  Responses.kt  (LeaderAndIsr*, OffsetsForLeaderEpoch*)
  net/     Connection.kt (extracted from MiniKafkaClient.request, client/MiniKafkaClient.kt:35; timeouts)
  zk/      ZkStore.kt  ZkPaths.kt  KvCodec.kt
  cluster/ Controller.kt  ControllerEvent.kt  ControllerChannel.kt  PartitionLeaderElector.kt  ReplicaAssigner.kt
  broker/  BrokerConfig.kt  Partition.kt  ReplicaManager.kt  ReplicaFetcher.kt  IsrUpdater.kt  IsrWriter.kt  BrokerSnapshot.kt
           (Broker.kt, OffsetStore.kt deleted in Task 9)
  server/  ServerConfig.kt  Server.kt(BrokerConfig, bind→register, stop order)  ConnectionHandler.kt(new keys; unknown key still closes, :79)
  client/  MiniKafkaClient.kt(bootstrap, routing, retry)  Partitioner.kt
  cli/     Cli.kt  (server --broker-id --zk [--advertised-host --min-insync --port --data-dir]; zk --port --data-dir;
                    topics create --replication-factor | list | describe; --bootstrap; produce --acks; consume stops at HW)
src/test/kotlin/minikafka/...       unit + @Tag("zk") module tests
src/test/kotlin/minikafka/testing/  EmbeddedZk, TcpProxy, TestCluster, TestClusterExtension, Eventually, MutableClock, Workload, AckedWriteChecker
src/test/kotlin/minikafka/e2e/      @Tag("e2e") scenarios
scripts/local-cluster.sh
```

## Tasks

Each task: **Files** (create/modify/test), **Interfaces** (consumes/produces), checkbox TDD steps,
complete code for tests and public interfaces, `Run:`/`Expected:` lines, commit command. The plan
document should give full code for tests and interfaces and implementation outlines for bodies
(the plan would otherwise run to many thousands of lines).

1. **Build setup & JDK smoke test** — `build.gradle.kts`: `configurations.all { exclude(group="ch.qos.logback") }`; `curator-framework:5.9.0`, `slf4j-api:2.0.16`, `runtimeOnly slf4j-simple`; test: `curator-test:5.9.0`. `tasks.test { useJUnitPlatform { excludeTags("e2e", "slow") + optional -PexcludeTags wiring } }`; register `e2eTest` (`Test`, same classpath, `includeTags("e2e","slow")`, `maxParallelForks=1`, `dependsOn(installDist)`, sysprop `minikafka.home`); `check` depends on it. `junit-platform.properties`: timeout 60s, SEPARATE_THREAD. Sysprops `zookeeper.admin.enableServer=false`, `zookeeper.sasl.client=false`. `simplelogger.properties` (showThreadName, zookeeper/curator=warn). Test: `CuratorSmokeTest` — TestingServer(InstanceSpec tickTime=200) + client connects, creates/reads a znode, negotiated session timeout = requested.
2. **model + log part 1: Record format** — `model/*`; Record `crc`, `leaderEpoch`; CRC verify helper. Keep `Log.append(ts,key,value)` as wrapper calling `appendAsLeader(..., epoch=0)` so callers stay green. Tests: RecordTest epoch/crc round-trip, `sizeInBytes`, bad CRC detected; mechanical update of LogSegmentTest.
3. **log part 2: robust recovery, truncation, epochs** — LogSegment recovery truncates at first unparsable/bad-CRC record, rebuilds index from the scan, exposes `epochStarts()`; `OffsetIndex.truncateTo`; `LogSegment.truncateTo` (index before `setLength`); `Log.truncateTo`, `appendAsFollower`, `read(offset, maxBytes, maxOffsetExclusive)` (single-segment, so existing cross-segment test stays green); `LeaderEpochCache`. Tests: truncate within/across segments, index trimmed, append after truncate reuses offsets, truncate beyond end no-op, follower append keeps offsets/epochs & rejects gaps/bad CRC, epochs survive reopen, torn length field recovers, index rebuilt; `LeaderEpochCacheTest` (assign monotonic, endOffsetFor latest/unknown/below-earliest/empty, truncateFromEnd, rebuild).
4. **net + proto additions** — `Connection` (MiniKafkaClient refactored onto it, behaviour unchanged); new error codes; LEADER_AND_ISR, OFFSETS_FOR_LEADER_EPOCH types. Existing message shapes unchanged yet. Tests: codec round-trips, `every error code is distinct`, Connection timeout test.
5. **zk: ZkStore** (`@Tag("zk")`, shared TestingServer per class, namespace per test) — paths, KvCodec, registerBroker (czxid; NodeExists/ownership logic), liveBrokers, assignment create/read, state read/CAS/create, election multi with ownership recovery, fenced multi with op-level error discrimination, offsets, watches (read+watch), connection listener, session-expire test helper. Tests: assignment duplicate; CAS ok/stale; **stale controller-epoch write rejected**; epoch monotonic; election race — exactly one of 5 clients wins; registration removed on close; re-register after expiry (stale-node wait); duplicate id fails; offsets commit/fetch/−1 when none/visible from a new session (replaces OffsetStoreTest); KvCodec stable bytes; retry-after-commit recognised as own write (fault-injected Curator wrapper).
6. **cluster pure logic** — `ReplicaAssigner`, `PartitionLeaderElector`. Tests: round-robin, no duplicate replica, leaders spread, INVALID_REPLICATION_FACTOR, deterministic start; elector picks first live ISR replica; returns none (never out-of-ISR).
7. **broker: Partition + ReplicaManager + IsrUpdater** (alongside old Broker) — `MutableClock`, fake `IsrWriter`. `PartitionTest`: HW = min LEO over maximalIsr with −1 unknowns; HW never backward; ISR={self} ⇒ HW=LEO; lag-time shrink; **follower keeping pace under constant appends never shrunk**; rejoin on reaching HW and epoch start; **expansion racing a produce doesn't ack before follower has it**; acks=all completes / NOT_ENOUGH_REPLICAS / REQUEST_TIMED_OUT / NOT_LEADER on leadership loss / AFTER_APPEND; consumer fetch < HW; FENCED/UNKNOWN_LEADER_EPOCH; REPLICA_NOT_ASSIGNED; stale leaderEpoch skipped; STALE_CONTROLLER_EPOCH; STALE_BROKER_EPOCH; ISR CAS BadVersion-but-mine adopted; concurrency stress (8 producer threads + leadership flips + truncation, invariants hold). `client/Partitioner.kt` + `PartitionerTest` (migrated keyed/round-robin tests).
8. **follower truncation** — `FollowerTruncationTest` with two real Logs: KIP-101 scenario 1 (stale-HW restart keeps committed) and 2 (divergence converges); **multi-round KIP-279 case** (follower {1:0,3:5,4:8} vs leader {1:0,2:4,5:7}); leader returns lower epoch follower lacks; leader empty ⇒ truncate to 0; follower empty skips handshake; cache trimmed.
9. **single-broker cluster cut-over** — `ServerConfig`, `Server(config)` (lock, broker.id, bind→register, stop order), controller election + topic creation + LeaderAndIsr + channel, ConnectionHandler with new keys, **wire shape changes to CREATE_TOPIC/METADATA/PRODUCE/FETCH together with all callers**, METADATA/offsets from ZK, routing client, CLI flags + `zk` subcommand (`ZooKeeperServerMain`) + `topics describe` (under-replicated/offline marks) + shutdown hook. Delete `Broker.kt`, `OffsetStore.kt`, their tests. Test infra: `EmbeddedZk`, `TcpProxy`, `TestCluster` (N brokers, port 0, temp dirs, session 3s/tickTime 200, lag 1s; `stopBroker/crashBroker/restartBroker/expireSession/isolateFromZk/healZk/pauseFetchers` — expiry via `ZooKeeper(connect, t, w, sessionId, passwd).close()` for deterministic server-side expiry; fetcher pause via `internal` ReplicaManager hook), `TestClusterExtension` (dumps ZK tree + BrokerSnapshots before teardown), `eventually/alwaysFor`. `SingleBrokerClusterTest` (migrated 3 integration tests + **restart retains data for hyphenated topic 'my-topic'** + unknown topic immediate error + duplicate topic + offsets through ZK). Update ProtocolCodecTest for changed shapes.
10. **replication** — ReplicaFetcher, OFFSETS_FOR_LEADER_EPOCH handler, HW propagation, ISR via ZK. `ReplicationClusterTest` (3 brokers, RF=3): byte-identical replicas; paused follower shrinks ISR in ZK and rejoins; acks=all blocks then completes after shrink; minISR=2 rejects when only leader remains; idle followers back off (no busy-spin).
11. **failover & fencing** — BrokersChanged election, czxid bounce, controller failover + full reconcile, session listener, queue discard. `ControllerFailoverTest`: leader killed ⇒ epoch+1 new leader; controller killed ⇒ epoch+1, topic creation continues; half-created topic finished by new controller; topic with all replicas down gets state once one registers; **stale queued LeaderAndIsr to restarted broker rejected (STALE_BROKER_EPOCH)**; raw stale-controller-epoch LeaderAndIsr rejected; election retry-after-commit still yields a controller. `ClientRoutingTest`: bootstrap any broker, dead bootstrap, NOT_LEADER refresh+retry, give up after max retries, fetch from follower rejected, offset visible through any broker, graceful stop triggers failover well within session timeout.
12. **E2E suite** (`@Tag("e2e")`, 3 brokers, real TCP; `Workload` + `AckedWriteChecker`: every acked (p, offset) holds the acked value on every ISR replica; no fabricated values; duplicates only for retried/timed-out sends; first occurrences keep per-producer order; replicas equal below HW, fully equal after `awaitFullyReplicated`) — produce/consume across partitions; **kill leader under acks=all (RF=3, minISR=2): zero acked loss**; controller kill; zombie leader via TcpProxy (eventual: single controller, higher epoch, no violations); restarted follower truncates divergent tail; all-ISR-down ⇒ offline (`alwaysFor`, no unclean) ⇒ recovers; leader session expiry ⇒ steps down & rejoins; rolling restart; ZK stop/restart ⇒ serving continues, cluster recovers; **seeded nemesis** (30s random crash/restart/isolate/expire/pause, 2 acks=all producers, seed printed, `-Dnemesis.seed=`); `@Tag("slow")` CLI 3-process cluster via `installDist`, SIGKILL leader.
13. **docs & demo** — `PRD.md` goals/non-goals (link this ADR), `DESIGN.md` (architecture, znodes, protocol table, algorithms, guarantees, limitations), `CLAUDE.md` (graph incl. `model`/`net`/`zk`/`cluster`, commands incl. `e2eTest`, `zk` subcommand, consume stops at HW), `README.md` (quick start, failure demos), `scripts/local-cluster.sh start|stop|kill <id>|restart <id>|describe`.

## Test plan summary

| Level | Where | Proves |
|---|---|---|
| Unit | log, proto, net, cluster pure, broker Partition, truncation, partitioner | format, truncation (KIP-101/279), HW/ISR/maximal-ISR, acks, fencing, epochs |
| Module (`@Tag("zk")`) | ZkStore, election, registration, ReplicationClusterTest, ControllerFailoverTest, ClientRoutingTest, SingleBrokerClusterTest | ZK CAS/fencing, retry-safety, session handling, replication over TCP, failover |
| E2E (`@Tag("e2e")`/`slow`) | e2e/ | zero acked loss under crashes, zombies, rolling restart, ZK outage, randomized nemesis, CLI multi-process |

Est. runtime: `test` ~45s; `e2eTest` ~3–4 min.

## Final verification

- `./gradlew test` (fast loop `-PexcludeTags=zk`), `./gradlew e2eTest`, `./gradlew build` all green.
- Manual: `minikafka zk --port 2181` + `scripts/local-cluster.sh start` → `topics create --topic demo --partitions 3 --replication-factor 3` → `produce --acks all` ×N → `kill` leader → `topics describe` shows new leader & epoch+1 → `consume --from-beginning` returns every acked message → kill controller, `/controller_epoch` increments → restart all, ISR full again.
- Every e2e failure test asserts `AckedWriteChecker.violations(...)` empty and replica consistency.
