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
    /** Fetcher backoff after an empty response; errors back off exponentially from it up to [replicaFetchMaxBackoffMs]. */
    val replicaFetchBackoffMs: Long = 50,
    val replicaFetchMaxBackoffMs: Long = 1_000,
    val replicaFetchMaxBytes: Int = 1 shl 20,
    /** Read/connect timeout of a follower's connection to its leader (FETCH answers at once: no long-poll). */
    val replicaSocketTimeoutMs: Int = 10_000,
    /** Upper bound on a produce request's acks=all wait; the client's timeoutMs is clamped to `[0, this]`. */
    val requestTimeoutMs: Int = 30_000
)
