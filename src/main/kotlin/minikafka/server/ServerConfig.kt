package minikafka.server

import minikafka.broker.BrokerConfig
import java.io.File

/**
 * Everything one broker process needs. [zkConnect] is `host:port[,host:port...][/chroot]` (D22).
 * [port] 0 binds an ephemeral port; the *actual* bound port is what gets registered in ZooKeeper,
 * together with [advertisedHost]. [minInsyncReplicas] must be the same on every broker.
 *
 * Timeout ordering (Global Constraints): client socket timeout > [requestTimeoutMs] (the largest
 * produce timeout a client should ask for) > [replicaLagTimeMaxMs].
 */
data class ServerConfig(
    val brokerId: Int,
    val zkConnect: String,
    val dataDir: File,
    val port: Int = 9092,
    val advertisedHost: String = "localhost",
    val minInsyncReplicas: Int = 1,
    val zkSessionTimeoutMs: Int = 6_000,
    val replicaLagTimeMaxMs: Long = 10_000,
    val requestTimeoutMs: Int = 30_000,
    /** Read timeout of the controller's LeaderAndIsr connections to brokers. */
    val controllerSocketTimeoutMs: Int = 30_000
) {
    init {
        require(brokerId >= 0) { "broker id must be >= 0, was $brokerId" }
        require(minInsyncReplicas >= 1) { "min in-sync replicas must be >= 1, was $minInsyncReplicas" }
    }

    internal fun brokerConfig(): BrokerConfig = BrokerConfig(
        brokerId = brokerId,
        advertisedHost = advertisedHost,
        port = port,
        dataDir = dataDir,
        minInsyncReplicas = minInsyncReplicas,
        replicaLagTimeMaxMs = replicaLagTimeMaxMs,
        requestTimeoutMs = requestTimeoutMs
    )
}
