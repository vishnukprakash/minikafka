package minikafka.broker

import minikafka.client.MiniKafkaClient.Companion.ACKS_ALL
import minikafka.client.MiniKafkaClient.Companion.ACKS_LEADER
import minikafka.model.TopicPartition
import minikafka.proto.ErrorCodes
import minikafka.proto.ProduceResponse
import minikafka.testing.TestCluster
import minikafka.testing.TestClusterExtension
import minikafka.testing.alwaysFor
import minikafka.testing.eventually
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.RegisterExtension
import java.io.File
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

private const val FETCH_BACKOFF_MS = 50L

/** Creates a 1-partition RF=3 topic and waits until all three replicas are in the ISR; returns the leader. */
private fun TestCluster.replicatedTopic(topic: String, partitions: Int = 1): Int {
    assertEquals(ErrorCodes.NONE, client().createTopic(topic, partitions, replicationFactor = 3))
    (0 until partitions).forEach { awaitLeader(topic, it) }
    awaitFullyReplicated(topic)
    return awaitLeader(topic, 0)
}

private fun TestCluster.followersOf(topic: String, partition: Int, leader: Int) = replicasOf(topic, partition).filter { it != leader }

private fun TestCluster.produceAsync(topic: String, value: String, acks: Short = ACKS_ALL): CompletableFuture<ProduceResponse> {
    val client = client()
    return CompletableFuture.supplyAsync { client.produce(topic, null, value.toByteArray(), acks, partition = 0) }
}

/** A real 3-broker cluster (RF=3, minISR=1) replicating over TCP, with the ISR kept in ZooKeeper. */
@Tag("zk")
class ReplicationClusterTest {
    @JvmField
    @RegisterExtension
    val clusterExtension = TestClusterExtension { TestCluster(size = 3) }
    private val cluster get() = clusterExtension.cluster

    @Test
    fun `followers hold byte-identical copies of the leader's log and learn its high watermark`() {
        cluster.replicatedTopic("rep", partitions = 3)
        val client = cluster.client()
        repeat(30) { i ->
            val r = client.produce("rep", "k$i".toByteArray(), "value-$i".toByteArray(), ACKS_ALL)
            assertEquals(ErrorCodes.NONE, r.errorCode)
        }
        cluster.awaitFullyReplicated("rep")
        cluster.assertReplicasConsistent("rep")

        for (p in 0 until 3) {
            val tp = TopicPartition("rep", p)
            val leader = cluster.awaitLeader("rep", p)
            val leaderSnap = cluster.broker(leader).snapshot().partitions.single { it.tp == tp }
            assertTrue(leaderSnap.logEndOffset > 0)
            // HW propagation: every follower learns the leader's HW on its next fetch.
            eventually {
                for (f in cluster.followersOf("rep", p, leader)) {
                    val s = cluster.broker(f).snapshot().partitions.single { it.tp == tp }
                    assertEquals(leaderSnap.highWatermark, s.highWatermark, "HW of $tp on follower $f")
                }
            }
            // Byte-identical on disk: same record bytes (offset, crc, epoch, timestamp, key, value) and index.
            val leaderFiles = partitionFiles(leader, tp)
            assertTrue(leaderFiles.isNotEmpty())
            for (f in cluster.followersOf("rep", p, leader)) {
                val files = partitionFiles(f, tp)
                assertEquals(leaderFiles.keys, files.keys, "segment files of $tp on $f")
                for ((name, bytes) in leaderFiles) assertArrayEquals(bytes, files[name], "$name of $tp on $f")
            }
        }
    }

    private fun partitionFiles(broker: Int, tp: TopicPartition): Map<String, ByteArray> =
        (File(cluster.dataDir(broker), "${tp.topic}-${tp.partition}").listFiles() ?: emptyArray())
            .filter { it.isFile }.associate { it.name to it.readBytes() }

    @Test
    fun `a paused follower is shrunk out of the ISR in ZooKeeper and rejoins after resuming`() {
        val leader = cluster.replicatedTopic("pause")
        val before = cluster.partitionState("pause", 0)!!.value
        val (paused, other) = cluster.followersOf("pause", 0, leader)
        cluster.pauseFetchers(paused)
        val client = cluster.client()
        repeat(5) { assertEquals(ErrorCodes.NONE, client.produce("pause", null, "m$it".toByteArray(), ACKS_LEADER, partition = 0).errorCode) }

        cluster.awaitIsr("pause", 0, setOf(leader, other))
        val shrunk = cluster.partitionState("pause", 0)!!.value
        assertEquals(before.leader, shrunk.leader)
        assertEquals(before.leaderEpoch, shrunk.leaderEpoch, "a leader ISR change keeps the leader epoch")
        assertEquals(before.controllerEpoch, shrunk.controllerEpoch, "and the znode's controller epoch")
        assertEquals(listOf(leader, other).sorted(), cluster.broker(leader).snapshot().partitions.single().committedIsr)

        cluster.pauseFetchers(paused, false)
        cluster.awaitIsr("pause", 0, setOf(leader, paused, other))
        cluster.awaitFullyReplicated("pause")
        cluster.assertReplicasConsistent("pause")
        assertEquals(5L, cluster.broker(paused).snapshot().partitions.single().logEndOffset)
    }

    @Test
    fun `acks=all waits for a paused ISR follower and completes once the follower is shrunk out`() {
        val leader = cluster.replicatedTopic("acks")
        val (paused, other) = cluster.followersOf("acks", 0, leader)
        cluster.pauseFetchers(paused)
        val produce = cluster.produceAsync("acks", "waits")

        // The paused follower stays in the ISR for replicaLagTimeMaxMs (1s): the produce must wait.
        alwaysFor(300.milliseconds) { assertFalse(produce.isDone, "acks=all completed while a paused follower was in the ISR") }
        val response = produce.get(10, TimeUnit.SECONDS)
        assertEquals(ErrorCodes.NONE, response.errorCode)
        assertEquals(0L, response.offset)
        // It could only complete after the shrink was committed to ZooKeeper (shrunk members leave the maximal ISR after the CAS).
        assertEquals(setOf(leader, other), cluster.partitionState("acks", 0)!!.value.isr.toSet())
        assertEquals(0L, cluster.broker(paused).snapshot().partitions.single().logEndOffset)

        cluster.pauseFetchers(paused, false)
        cluster.awaitFullyReplicated("acks")
        cluster.assertReplicasConsistent("acks")
    }

    @Test
    fun `idle followers back off between empty fetches instead of busy-spinning`() {
        val leader = cluster.replicatedTopic("idle")
        val startCount = cluster.broker(leader).snapshot().followerFetchRequests
        val start = System.nanoTime()
        alwaysFor(1.seconds) {} // an idle window
        val elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start)
        val fetches = cluster.broker(leader).snapshot().followerFetchRequests - startCount

        // Each of the 2 followers sends at most one FETCH per 50ms backoff (+1 in flight at each edge).
        val upperBound = 2 * (elapsedMs / FETCH_BACKOFF_MS + 2)
        println("idle window: $fetches follower fetches in ${elapsedMs}ms (upper bound $upperBound)")
        assertTrue(fetches <= upperBound, "$fetches follower fetches in ${elapsedMs}ms (bound $upperBound): busy-spinning?")
        assertTrue(fetches >= 4, "followers kept fetching while idle: $fetches in ${elapsedMs}ms")
        assertEquals(setOf(1, 2, 3), cluster.partitionState("idle", 0)!!.value.isr.toSet(), "idle followers stay in the ISR")
    }

    @Test
    fun `a restarted follower truncation-checks its log and catches up`() {
        val leader = cluster.replicatedTopic("restart")
        val client = cluster.client()
        repeat(10) { assertEquals(ErrorCodes.NONE, client.produce("restart", null, "a$it".toByteArray(), ACKS_ALL, partition = 0).errorCode) }
        cluster.awaitFullyReplicated("restart")

        val (stopped, other) = cluster.followersOf("restart", 0, leader)
        cluster.stopBroker(stopped)
        repeat(10) { assertEquals(ErrorCodes.NONE, client.produce("restart", null, "b$it".toByteArray(), ACKS_LEADER, partition = 0).errorCode) }
        cluster.awaitIsr("restart", 0, setOf(leader, other)) // a dead follower is shrunk by the leader's lag check

        cluster.restartBroker(stopped)
        cluster.awaitIsr("restart", 0, setOf(leader, stopped, other))
        cluster.awaitFullyReplicated("restart")
        cluster.assertReplicasConsistent("restart")
        val values = cluster.broker(stopped).replicaManager.readLocal(TopicPartition("restart", 0), 0, 100).map { String(it.value) }
        assertEquals((0 until 10).map { "a$it" } + (0 until 10).map { "b$it" }, values)
    }
}

/** minISR=2 on RF=3: acks=all needs at least one follower in the ISR. */
@Tag("zk")
class ReplicationMinIsrClusterTest {
    @JvmField
    @RegisterExtension
    val clusterExtension = TestClusterExtension { TestCluster(size = 3, minInsyncReplicas = 2) }
    private val cluster get() = clusterExtension.cluster

    @Test
    fun `acks=all is rejected with NOT_ENOUGH_REPLICAS when only the leader remains in the ISR`() {
        val leader = cluster.replicatedTopic("min")
        val followers = cluster.followersOf("min", 0, leader)
        followers.forEach { cluster.pauseFetchers(it) }
        cluster.awaitIsr("min", 0, setOf(leader))

        val client = cluster.client()
        assertEquals(ErrorCodes.NOT_ENOUGH_REPLICAS, client.produce("min", null, "x".toByteArray(), ACKS_ALL, partition = 0).errorCode)
        assertEquals(0L, cluster.broker(leader).snapshot().partitions.single().logEndOffset, "rejected before the append")
        assertEquals(ErrorCodes.NONE, client.produce("min", null, "y".toByteArray(), ACKS_LEADER, partition = 0).errorCode)

        followers.forEach { cluster.pauseFetchers(it, false) }
        cluster.awaitIsr("min", 0, setOf(1, 2, 3))
        assertEquals(ErrorCodes.NONE, client.produce("min", null, "z".toByteArray(), ACKS_ALL, partition = 0).errorCode)
        cluster.awaitFullyReplicated("min")
        cluster.assertReplicasConsistent("min")
    }

    @Test
    fun `an acks=all produce in flight when the ISR shrinks below minISR fails with NOT_ENOUGH_REPLICAS_AFTER_APPEND`() {
        val leader = cluster.replicatedTopic("after")
        val followers = cluster.followersOf("after", 0, leader)
        followers.forEach { cluster.pauseFetchers(it) }
        val produce = cluster.produceAsync("after", "late")
        assertEquals(ErrorCodes.NOT_ENOUGH_REPLICAS_AFTER_APPEND, produce.get(10, TimeUnit.SECONDS).errorCode)
        assertEquals(setOf(leader), cluster.partitionState("after", 0)!!.value.isr.toSet())
    }

    @Test
    fun `with one follower paused acks=all still succeeds once the ISR shrinks to two`() {
        val leader = cluster.replicatedTopic("two")
        val (paused, other) = cluster.followersOf("two", 0, leader)
        cluster.pauseFetchers(paused)
        assertEquals(ErrorCodes.NONE, cluster.produceAsync("two", "ok").get(10, TimeUnit.SECONDS).errorCode)
        assertEquals(setOf(leader, other), cluster.partitionState("two", 0)!!.value.isr.toSet())
    }
}
