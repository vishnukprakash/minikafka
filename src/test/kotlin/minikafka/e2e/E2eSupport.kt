package minikafka.e2e

import minikafka.client.MiniKafkaClient
import minikafka.model.PartitionState
import minikafka.proto.ErrorCodes
import minikafka.testing.AckedWriteChecker
import minikafka.testing.TestCluster
import minikafka.testing.Workload
import minikafka.testing.WorkloadHistory
import minikafka.testing.eventually
import minikafka.testing.replicaViews
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/** Creates [topic] with [partitions] partitions and replication factor [rf], and waits until it is fully replicated. */
internal fun TestCluster.createReplicatedTopic(topic: String, partitions: Int, rf: Int = 3) {
    assertEquals(ErrorCodes.NONE, client().createTopic(topic, partitions, replicationFactor = rf))
    (0 until partitions).forEach { awaitLeader(topic, it) }
    awaitFullyReplicated(topic)
}

/** A [Workload] against this cluster (bootstrap follows restarts), with the cluster's client timeouts. */
internal fun TestCluster.workload(topic: String, partitions: List<Int>, acks: Short = MiniKafkaClient.ACKS_ALL): Workload =
    Workload(topic, partitions, acks, ::bootstrap, clientConfig(), name = "wl-$topic")

/** The partition state in ZooKeeper, which must exist. */
internal fun TestCluster.state(topic: String, partition: Int): PartitionState =
    checkNotNull(partitionState(topic, partition)) { "no state for $topic-$partition" }.value

/** The first partition of [topic] (0 until [partitions]) whose current leader satisfies [predicate]. */
internal fun TestCluster.partitionLedBy(topic: String, partitions: Int, predicate: (Int) -> Boolean): Int =
    (0 until partitions).first { predicate(awaitLeader(topic, it)) }

/** Waits until ZooKeeper names a leader other than [old] (running, applied) with a higher leader epoch; returns the new state. */
internal fun TestCluster.awaitLeaderMovedFrom(topic: String, partition: Int, old: PartitionState, timeout: Duration = 20.seconds): PartitionState =
    eventually(timeout) {
        val s = state(topic, partition)
        assertTrue(s.leader >= 0 && s.leader != old.leader && s.leaderEpoch > old.leaderEpoch) { "$topic-$partition leader not moved from ${old.leader} yet: $s" }
        awaitLeader(topic, partition, timeout = Duration.ZERO)
        s
    }

/**
 * Stops [workload], waits until [topic] is fully replicated, and asserts the acked-write checker
 * finds no violation (the violations are the assertion message). Returns the history.
 */
internal fun TestCluster.stopAndCheck(workload: Workload, topic: String, context: String = ""): WorkloadHistory {
    val history = workload.stop()
    println("[$topic] ${history.summary()}")
    awaitFullyReplicated(topic, timeout = 30.seconds)
    val violations = AckedWriteChecker.check(history, replicaViews(topic), fullyReplicated = true)
    assertTrue(violations.isEmpty()) { context + AckedWriteChecker.describe(history, violations) }
    assertReplicasConsistent(topic)
    assertTrue(history.acks.isNotEmpty(), "the workload acknowledged nothing: ${history.summary()}")
    return history
}

/** Every value of [topic]-[partition] a consumer reads from offset 0 up to the high watermark. */
internal fun MiniKafkaClient.consumeAll(topic: String, partition: Int): List<String> {
    val out = mutableListOf<String>()
    var offset = 0L
    while (true) {
        val r = fetch(topic, partition, offset)
        assertEquals(ErrorCodes.NONE, r.errorCode, "fetch $topic-$partition at $offset")
        r.records.forEach { out += String(it.value); offset = it.offset + 1 }
        if (r.records.isEmpty() || offset >= r.highWatermark) return out
    }
}
