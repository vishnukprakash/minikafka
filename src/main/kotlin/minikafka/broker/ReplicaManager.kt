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

    internal val isrUpdater = IsrUpdater(brokerId, config.replicaLagTimeMaxMs, isrStore) { partitions.values.toList() }

    init {
        if (startIsrUpdater) isrUpdater.start()
    }

    /** Algorithm 7. Returns the request-level error code. */
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
        for (p in req.partitions) {
            val tp = TopicPartition(p.topic, p.partition)
            if (brokerId !in p.replicas) {
                logger.info("b{} {}: LeaderAndIsr skipped: not in replicas {}", brokerId, tp, p.replicas)
                continue
            }
            val partition = partitions.computeIfAbsent(tp, ::openPartition)
            partition.applyLeaderAndIsr(p, req.controllerEpoch)?.let(transitions::add)
        }
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
        ErrorCodes.NONE
    }

    fun produce(tp: TopicPartition, key: ByteArray?, value: ByteArray, acks: Short, timeoutMs: Int): ProduceResult =
        partitions[tp]?.produce(key, value, acks, timeoutMs) ?: ProduceResult(ErrorCodes.NOT_LEADER_FOR_PARTITION, -1L)

    fun fetchAsConsumer(tp: TopicPartition, offset: Long, maxBytes: Int): FetchResult =
        partitions[tp]?.fetchAsConsumer(offset, maxBytes) ?: notLeaderFetch

    fun fetchAsFollower(tp: TopicPartition, replicaId: Int, offset: Long, currentLeaderEpoch: Int, maxBytes: Int): FetchResult =
        partitions[tp]?.fetchAsFollower(replicaId, offset, currentLeaderEpoch, maxBytes) ?: notLeaderFetch

    fun offsetsForLeaderEpoch(tp: TopicPartition, replicaId: Int, currentLeaderEpoch: Int, requestedEpoch: Int): OffsetsForLeaderEpochResponse =
        partitions[tp]?.offsetsForLeaderEpoch(replicaId, currentLeaderEpoch, requestedEpoch)
            ?: OffsetsForLeaderEpochResponse(ErrorCodes.NOT_LEADER_FOR_PARTITION, -1, -1L)

    /** Follower append (Task 10's fetcher): NONE, NOT_LEADER (not a follower) or FENCED (epoch moved on). */
    fun appendAsFollower(tp: TopicPartition, expectedLeaderEpoch: Int, records: List<Record>, leaderHw: Long): Short =
        partitions[tp]?.appendAsFollower(expectedLeaderEpoch, records, leaderHw) ?: ErrorCodes.NOT_LEADER_FOR_PARTITION

    /** Follower truncation (Task 10's fetcher, offset from Task 8's logic). */
    fun truncateTo(tp: TopicPartition, expectedLeaderEpoch: Int, offset: Long): Short =
        partitions[tp]?.truncateTo(expectedLeaderEpoch, offset) ?: ErrorCodes.NOT_LEADER_FOR_PARTITION

    /** Test hook (ruling R2): Task 10's fetchers consult it; no-op until fetchers exist. */
    internal fun pauseFetchers(paused: Boolean) {
        fetchersPaused = paused
        logger.info("b{}: fetchers {}", brokerId, if (paused) "paused" else "resumed")
    }

    internal fun fetchersPaused(): Boolean = fetchersPaused

    fun snapshot(): BrokerSnapshot = BrokerSnapshot(
        brokerId, seenControllerEpoch, fetchersPaused,
        partitions.values.map { it.snapshot() }.sortedWith(compareBy({ it.tp.topic }, { it.tp.partition }))
    )

    override fun close() {
        synchronized(leaderAndIsrLock) { closed = true }
        isrUpdater.close()
        partitions.values.forEach { it.close() }
    }

    private fun openPartition(tp: TopicPartition): Partition {
        val log = Log(File(config.dataDir, "${tp.topic}-${tp.partition}"))
        logger.info("b{} {}: opened log (leo={})", brokerId, tp, log.logEndOffset())
        return Partition(tp, brokerId, log, clock, config.minInsyncReplicas, config.replicaLagTimeMaxMs, isrUpdater::requestRun)
    }

    private val notLeaderFetch = FetchResult(ErrorCodes.NOT_LEADER_FOR_PARTITION, emptyList(), -1L)
}
