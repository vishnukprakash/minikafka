package minikafka.broker

import minikafka.log.Log
import minikafka.log.Record
import minikafka.model.TopicPartition
import minikafka.proto.ErrorCodes
import minikafka.proto.LeaderAndIsrRequest
import minikafka.proto.OffsetsForLeaderEpochResponse
import org.slf4j.LoggerFactory
import java.io.File
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

/**
 * Told about role transitions applied by LeaderAndIsr, after the partition lock is released
 * (algorithm 7) but while [ReplicaManager] still serialises LeaderAndIsr handling, so calls arrive
 * in application order. Task 10 plugs the fetcher manager in here: stop/join the old fetcher,
 * start a new one on every follower transition (including an epoch bump with the same leader).
 * Implementations must not call [ReplicaManager.applyLeaderAndIsr].
 */
interface ReplicaRoleListener {
    fun onBecomeLeader(tp: TopicPartition, leaderEpoch: Int)

    /** [leader] may be -1 (partition offline). */
    fun onBecomeFollower(tp: TopicPartition, leader: Int, leaderEpoch: Int)

    companion object {
        val NONE: ReplicaRoleListener = object : ReplicaRoleListener {
            override fun onBecomeLeader(tp: TopicPartition, leaderEpoch: Int) {}
            override fun onBecomeFollower(tp: TopicPartition, leader: Int, leaderEpoch: Int) {}
        }
    }
}

/**
 * The broker's replica layer: the registry of local [Partition]s (opened lazily on the first
 * LeaderAndIsr that names this broker in `replicas`, log dir `<dataDir>/<topic>-<p>`), controller
 * and broker-epoch fencing (D3, D4), and the per-broker [IsrUpdater]. No network, no ZooKeeper:
 * ISR writes go through the injected [IsrStore].
 *
 * @param startIsrUpdater false in unit tests, which call `isrUpdater.runOnce()` themselves.
 */
class ReplicaManager(
    val config: BrokerConfig,
    isrStore: IsrStore,
    private val clock: Clock = Clock.SYSTEM,
    startIsrUpdater: Boolean = true
) : AutoCloseable {
    private val logger = LoggerFactory.getLogger(ReplicaManager::class.java)
    private val brokerId = config.brokerId
    private val partitions = ConcurrentHashMap<TopicPartition, Partition>()
    private val leaderAndIsrLock = Any()

    /**
     * Highest controller epoch seen (D3). Task 9 initialises it from `/controller_epoch` at startup,
     * before the server accepts requests; afterwards only [applyLeaderAndIsr] raises it.
     */
    @Volatile
    var seenControllerEpoch: Int = 0

    @Volatile
    var roleListener: ReplicaRoleListener = ReplicaRoleListener.NONE

    @Volatile
    private var fetchersPaused = false

    @Volatile
    private var closed = false

    private val followerFetchRequests = AtomicLong()

    @Volatile
    private var fetcherManager: ReplicaFetcherManager? = null

    internal val isrUpdater = IsrUpdater(brokerId, config.replicaLagTimeMaxMs, isrStore) { partitions.values.toList() }

    init {
        if (startIsrUpdater) isrUpdater.start()
    }

    /**
     * Algorithm 7. Returns the request-level error code.
     *
     * Per-partition failures are isolated: if a partition's log cannot be opened (or applying the
     * state throws), it is logged at WARN and skipped *without advancing its leader epoch*, so a
     * retry of the same request — or any later LeaderAndIsr — re-attempts it. The request still
     * answers NONE, and the role listener is always told about every transition that did apply
     * (dispatched in `finally`), so one bad partition never leaves the others without fetchers
     * for their new epoch or head-of-line blocks the controller channel.
     */
    fun applyLeaderAndIsr(req: LeaderAndIsrRequest, myBrokerEpoch: Long): Short = synchronized(leaderAndIsrLock) {
        if (closed) return ErrorCodes.NONE
        if (req.brokerEpoch != myBrokerEpoch) {
            logger.info("b{}: LeaderAndIsr from controller {} rejected STALE_BROKER_EPOCH: {} != my {}", brokerId, req.controllerId, req.brokerEpoch, myBrokerEpoch)
            return ErrorCodes.STALE_BROKER_EPOCH
        }
        if (req.controllerEpoch < seenControllerEpoch) {
            logger.info("b{}: LeaderAndIsr from controller {} rejected STALE_CONTROLLER_EPOCH: {} < seen {}", brokerId, req.controllerId, req.controllerEpoch, seenControllerEpoch)
            return ErrorCodes.STALE_CONTROLLER_EPOCH
        }
        seenControllerEpoch = req.controllerEpoch
        val transitions = mutableListOf<Partition.RoleTransition>()
        try {
            for (p in req.partitions) {
                val tp = TopicPartition(p.topic, p.partition)
                if (brokerId !in p.replicas) {
                    logger.info("b{} {}: LeaderAndIsr skipped: not in replicas {}", brokerId, tp, p.replicas)
                    continue
                }
                try {
                    val partition = partitions.computeIfAbsent(tp, ::openPartition)
                    partition.applyLeaderAndIsr(p)?.let(transitions::add)
                } catch (e: Exception) {
                    logger.warn("b{} {}: LeaderAndIsr (leader={}, epoch={}) failed to apply; skipped, will retry on a later LeaderAndIsr", brokerId, tp, p.leader, p.leaderEpoch, e)
                }
            }
        } finally {
            // Every partition lock is released here; now (re)wire fetchers.
            val listener = roleListener
            for (t in transitions) {
                try {
                    if (t.isLeader) listener.onBecomeLeader(t.tp, t.leaderEpoch)
                    else listener.onBecomeFollower(t.tp, t.leader, t.leaderEpoch)
                } catch (e: Exception) {
                    logger.warn("b{} {}: role listener failed", brokerId, t.tp, e)
                }
            }
        }
        ErrorCodes.NONE
    }

    /**
     * Algorithm 11. acks=all blocks the calling (connection-handler) thread until committed, fenced,
     * or [timeoutMs] of real time (System.nanoTime, not the injected [Clock]) elapses (R7).
     */
    fun produce(tp: TopicPartition, key: ByteArray?, value: ByteArray, acks: Short, timeoutMs: Int): ProduceResult =
        partitions[tp]?.produce(key, value, acks, timeoutMs) ?: ProduceResult(ErrorCodes.NOT_LEADER_FOR_PARTITION, -1L)

    fun fetchAsConsumer(tp: TopicPartition, offset: Long, maxBytes: Int): FetchResult =
        partitions[tp]?.fetchAsConsumer(offset, maxBytes) ?: notLeaderFetch

    fun fetchAsFollower(tp: TopicPartition, replicaId: Int, offset: Long, currentLeaderEpoch: Int, maxBytes: Int): FetchResult {
        followerFetchRequests.incrementAndGet()
        return partitions[tp]?.fetchAsFollower(replicaId, offset, currentLeaderEpoch, maxBytes) ?: notLeaderFetch
    }

    fun offsetsForLeaderEpoch(tp: TopicPartition, replicaId: Int, currentLeaderEpoch: Int, requestedEpoch: Int): OffsetsForLeaderEpochResponse =
        partitions[tp]?.offsetsForLeaderEpoch(replicaId, currentLeaderEpoch, requestedEpoch)
            ?: OffsetsForLeaderEpochResponse(ErrorCodes.NOT_LEADER_FOR_PARTITION, -1, -1L)

    /** Follower append (Task 10's fetcher): NONE, NOT_LEADER (not a follower) or FENCED (epoch moved on). */
    fun appendAsFollower(tp: TopicPartition, expectedLeaderEpoch: Int, records: List<Record>, leaderHw: Long): Short =
        partitions[tp]?.appendAsFollower(expectedLeaderEpoch, records, leaderHw) ?: ErrorCodes.NOT_LEADER_FOR_PARTITION

    /** Follower truncation (Task 10's fetcher, offset from Task 8's logic). */
    fun truncateTo(tp: TopicPartition, expectedLeaderEpoch: Int, offset: Long): Short =
        partitions[tp]?.truncateTo(expectedLeaderEpoch, offset) ?: ErrorCodes.NOT_LEADER_FOR_PARTITION

    /**
     * Algorithm 8's leader-epoch handshake for a follower replica (Task 10's fetcher, before its
     * first FETCH of [expectedLeaderEpoch]): see [truncateForLeaderEpoch]. [queryLeader] sends
     * OFFSETS_FOR_LEADER_EPOCH to the leader and returns (leaderEpoch, endOffset); it runs with no
     * lock held and may throw (propagated). NOT_LEADER / FENCED_LEADER_EPOCH if this replica is no
     * longer a follower in [expectedLeaderEpoch].
     */
    fun truncateFollower(
        tp: TopicPartition,
        expectedLeaderEpoch: Int,
        queryLeader: (requestedEpoch: Int) -> Pair<Int, Long>
    ): FollowerTruncationOutcome =
        partitions[tp]?.truncateAsFollower(expectedLeaderEpoch, queryLeader)
            ?: FollowerTruncationOutcome(ErrorCodes.NOT_LEADER_FOR_PARTITION, null)

    /** This replica's log end offset, or null if the partition is not open here. */
    fun logEndOffset(tp: TopicPartition): Long? = partitions[tp]?.logEndOffset()

    /**
     * Turns on replication (Task 10): installs a [ReplicaFetcherManager] as the [roleListener], so
     * every follower transition applied from now on starts a [ReplicaFetcher]. Call once, before
     * the broker accepts LeaderAndIsr. [resolver] maps the leader's broker id to its endpoint.
     */
    fun startReplication(
        resolver: BrokerResolver,
        clientFactory: LeaderClientFactory = LeaderClientFactory.tcp(config.replicaSocketTimeoutMs),
        settings: FetcherSettings = FetcherSettings.from(config)
    ): Unit = synchronized(leaderAndIsrLock) {
        check(fetcherManager == null) { "replication already started" }
        if (closed) return
        val manager = ReplicaFetcherManager(this, resolver, clientFactory, settings)
        fetcherManager = manager
        roleListener = manager
    }

    internal fun fetcherFor(tp: TopicPartition): ReplicaFetcher? = fetcherManager?.fetcherFor(tp)

    /** Test hook (ruling R2): while paused, fetchers send no requests (roles are untouched); resume continues. */
    internal fun pauseFetchers(paused: Boolean) {
        fetchersPaused = paused
        logger.info("b{}: fetchers {}", brokerId, if (paused) "paused" else "resumed")
    }

    internal fun fetchersPaused(): Boolean = fetchersPaused

    /** Test/diagnostics hook: this replica's own records in `[from, untilExclusive)`, regardless of role or HW. */
    internal fun readLocal(tp: TopicPartition, from: Long, untilExclusive: Long): List<Record> =
        partitions[tp]?.readLocal(from, untilExclusive) ?: emptyList()

    fun snapshot(): BrokerSnapshot = BrokerSnapshot(
        brokerId, seenControllerEpoch, fetchersPaused,
        partitions.values.map { it.snapshot() }.sortedWith(compareBy({ it.tp.topic }, { it.tp.partition })),
        followerFetchRequests.get()
    )

    /**
     * Step 1 of the graceful stop (algorithm 2): stops the threads that write to ZooKeeper or
     * replicate (the fetchers, then the isr-updater) *before* the ZooKeeper client closes.
     * Idempotent; [close] calls it too. No fetcher starts afterwards.
     */
    fun stopReplication() {
        fetcherManager?.close()
        isrUpdater.close()
    }

    override fun close() {
        synchronized(leaderAndIsrLock) { closed = true }
        stopReplication()
        partitions.values.forEach { it.close() }
    }

    private fun openPartition(tp: TopicPartition): Partition {
        val log = Log(File(config.dataDir, "${tp.topic}-${tp.partition}"))
        logger.info("b{} {}: opened log (leo={})", brokerId, tp, log.logEndOffset())
        return Partition(tp, brokerId, log, clock, config.minInsyncReplicas, config.replicaLagTimeMaxMs, isrUpdater::requestRun)
    }

    private val notLeaderFetch = FetchResult(ErrorCodes.NOT_LEADER_FOR_PARTITION, emptyList(), -1L)
}
