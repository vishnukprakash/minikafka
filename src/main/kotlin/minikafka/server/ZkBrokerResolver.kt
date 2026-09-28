package minikafka.server

import minikafka.broker.BrokerResolver
import minikafka.model.BrokerInfo
import minikafka.zk.ZkStore

/**
 * Broker id -> endpoint for the replica fetchers, from `/brokers/ids` (the broker package cannot
 * depend on ZooKeeper). Cached; a `refresh` request (after a failed connection — e.g. the leader
 * restarted on another port) or a miss re-reads the live brokers. A ZooKeeper failure throws, which
 * the fetcher treats like an I/O error (backoff, retry).
 */
internal class ZkBrokerResolver(private val zk: ZkStore) : BrokerResolver {
    @Volatile
    private var cache: Map<Int, BrokerInfo> = emptyMap()

    override fun resolve(brokerId: Int, refresh: Boolean): BrokerInfo? {
        if (!refresh) cache[brokerId]?.let { return it }
        val live = zk.liveBrokers().mapValues { it.value.first }
        cache = live
        return live[brokerId]
    }
}
