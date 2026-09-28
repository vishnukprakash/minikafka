package minikafka.broker

import minikafka.model.TopicPartition

/** Point-in-time view of one local partition replica, for tests, diagnostics and `describe`. */
data class PartitionSnapshot(
    val tp: TopicPartition,
    val role: Partition.Role,
    val leader: Int,
    val leaderEpoch: Int,
    val replicas: List<Int>,
    val committedIsr: List<Int>,
    val maximalIsr: List<Int>,
    val zkVersion: Int,
    val highWatermark: Long,
    val logEndOffset: Long,
    val leaderEpochStartOffset: Long,
    /** True after an ISR CAS conflict we could not recognise as ours; cleared by LeaderAndIsr. */
    val isrStale: Boolean,
    /** Leader only: follower id -> last reported LEO (-1 = unknown since becoming leader). */
    val followerLeos: Map<Int, Long>
)

data class BrokerSnapshot(
    val brokerId: Int,
    val seenControllerEpoch: Int,
    val fetchersPaused: Boolean,
    val partitions: List<PartitionSnapshot>
)
