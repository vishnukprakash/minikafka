package minikafka.broker

import java.io.File

/**
 * Per-broker replication settings. `minInsyncReplicas` must be identical on every broker (documented
 * limitation); timeout ordering: client socket timeout > produce timeoutMs > replicaLagTimeMaxMs.
 */
data class BrokerConfig(
    val brokerId: Int,
    val advertisedHost: String,
    val port: Int,
    val dataDir: File,
    val minInsyncReplicas: Int = 1,
    val replicaLagTimeMaxMs: Long = 10_000,
    val replicaFetchBackoffMs: Long = 50,
    val requestTimeoutMs: Int = 30_000
)
