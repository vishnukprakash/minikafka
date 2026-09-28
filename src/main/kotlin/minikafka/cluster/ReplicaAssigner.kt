package minikafka.cluster

/**
 * Thrown by [ReplicaAssigner.assign] when the requested replication factor cannot be satisfied:
 * fewer than 1, or more than the number of live brokers. Task 9 (topic creation) maps this to
 * `INVALID_REPLICATION_FACTOR` (`minikafka.proto.ErrorCodes`) — this package depends only on
 * `model`/JDK, so the proto error code is not referenced here.
 */
class InvalidReplicationFactorException(message: String) : IllegalArgumentException(message)

/**
 * Assigns partitions to brokers round-robin (algorithm 5's `ReplicaAssigner` step), rotating the
 * preferred leader (the first replica) by partition so leaders spread evenly across brokers.
 */
object ReplicaAssigner {

    /**
     * @param brokerIds the live broker ids (sorted ascending internally; input order irrelevant).
     * @param numPartitions must be >= 1.
     * @param replicationFactor must be in `1..brokerIds.size`, else [InvalidReplicationFactorException].
     * @param startIndex rotates which broker leads partition 0; same inputs + [startIndex] always
     *   produce the same assignment, different [startIndex] values generally produce different ones.
     * @return partition -> ordered replica list (first entry is the preferred leader), for
     *   partitions `0 until numPartitions`.
     */
    fun assign(
        brokerIds: List<Int>,
        numPartitions: Int,
        replicationFactor: Int,
        startIndex: Int = 0
    ): Map<Int, List<Int>> {
        require(numPartitions >= 1) { "numPartitions must be >= 1, was $numPartitions" }
        val sorted = brokerIds.sorted()
        val n = sorted.size
        if (replicationFactor < 1 || replicationFactor > n) {
            throw InvalidReplicationFactorException(
                "replicationFactor must be in 1..$n (live brokers: $sorted), was $replicationFactor"
            )
        }

        return (0 until numPartitions).associateWith { p ->
            (0 until replicationFactor).map { i -> sorted[(startIndex + p + i) % n] }
        }
    }
}
