package minikafka.broker

import minikafka.model.TopicPartition
import org.slf4j.LoggerFactory

/**
 * The [ReplicaRoleListener] that owns this broker's fetchers: exactly one [ReplicaFetcher] per
 * followed partition (Task 8's truncation assumes a single appender). Called by
 * [ReplicaManager.applyLeaderAndIsr] in application order, after the partition lock is released but
 * under the replica manager's LeaderAndIsr lock — so stopping a fetcher must be bounded: the old
 * fetcher's connection is closed and its thread interrupted, then joined for at most
 * [joinTimeoutMs] (a fetcher that lingers past that is fenced by its epoch and exits on its own).
 */
class ReplicaFetcherManager internal constructor(
    private val replicaManager: ReplicaManager,
    private val resolver: BrokerResolver,
    private val clientFactory: LeaderClientFactory,
    private val settings: FetcherSettings,
    private val joinTimeoutMs: Long = 2_000
) : ReplicaRoleListener, AutoCloseable {
    private val logger = LoggerFactory.getLogger(ReplicaFetcherManager::class.java)
    private val brokerId = replicaManager.config.brokerId
    private val fetchers = HashMap<TopicPartition, ReplicaFetcher>()
    private var closed = false

    @Synchronized
    override fun onBecomeLeader(tp: TopicPartition, leaderEpoch: Int) {
        stop(fetchers.remove(tp))
    }

    /** Always replaces the fetcher, even for an epoch bump with the same leader; none if [leader] is -1. */
    @Synchronized
    override fun onBecomeFollower(tp: TopicPartition, leader: Int, leaderEpoch: Int) {
        stop(fetchers.remove(tp))
        if (closed || leader < 0 || leader == brokerId) return
        val fetcher = ReplicaFetcher(replicaManager, tp, leader, leaderEpoch, resolver, clientFactory, settings)
        fetchers[tp] = fetcher
        fetcher.start()
    }

    @Synchronized
    internal fun fetcherFor(tp: TopicPartition): ReplicaFetcher? = fetchers[tp]

    /** Stops every fetcher (bounded) and refuses to start new ones. Idempotent. */
    @Synchronized
    override fun close() {
        closed = true
        val all = fetchers.values.toList()
        fetchers.clear()
        all.forEach { it.shutdown() }
        all.forEach(::join)
    }

    private fun stop(fetcher: ReplicaFetcher?) {
        fetcher ?: return
        fetcher.shutdown()
        join(fetcher)
    }

    private fun join(fetcher: ReplicaFetcher) {
        if (!fetcher.awaitShutdown(joinTimeoutMs)) {
            logger.warn("b{} {}: fetcher for epoch {} did not stop within {}ms; continuing (it is fenced)", brokerId, fetcher.tp, fetcher.leaderEpoch, joinTimeoutMs)
        }
    }
}
