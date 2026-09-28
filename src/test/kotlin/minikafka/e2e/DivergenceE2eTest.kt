package minikafka.e2e

import minikafka.client.MiniKafkaClient
import minikafka.model.TopicPartition
import minikafka.proto.ErrorCodes
import minikafka.testing.TestCluster
import minikafka.testing.TestClusterExtension
import minikafka.testing.eventually
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.extension.RegisterExtension
import java.util.concurrent.TimeUnit
import kotlin.time.Duration.Companion.seconds

/**
 * A restarted former leader truncates the divergent, uncommitted tail it wrote with acks=1 (the
 * documented acks=1 loss) and converges on the new leader's log, via the leader-epoch handshake
 * (D8) over the network.
 *
 * The replica lag time is 30s here (not 1s) so that the paused follower stays in the ISR, and so
 * stays electable, while the leader writes the tail it alone holds; the timeout ordering of the
 * global constraints only concerns acks=all, which is not used for the divergent writes.
 */
@Tag("e2e")
@Timeout(value = 120, unit = TimeUnit.SECONDS)
class DivergenceE2eTest {
    @JvmField
    @RegisterExtension
    val clusterExtension = TestClusterExtension { TestCluster(size = 3, minInsyncReplicas = 1, replicaLagTimeMaxMs = 30_000) }
    private val cluster get() = clusterExtension.cluster

    private fun values(broker: Int) =
        cluster.broker(broker).replicaManager.readLocal(TopicPartition("div", 0), 0, Long.MAX_VALUE).map { String(it.value) }

    private fun records(broker: Int) =
        cluster.broker(broker).replicaManager.readLocal(TopicPartition("div", 0), 0, Long.MAX_VALUE)
            .map { "${it.offset}/e${it.leaderEpoch}/${String(it.value)}" }

    @Test
    fun `a restarted leader truncates its uncommitted tail and ends up identical to the new leader`() {
        val client = cluster.client()
        assertEquals(ErrorCodes.NONE, client.createTopic("div", 1, replicationFactor = 2))
        val a = cluster.awaitLeader("div", 0)
        val b = cluster.replicasOf("div", 0).single { it != a }
        repeat(5) { assertEquals(ErrorCodes.NONE, client.produce("div", null, "c$it".toByteArray(), partition = 0).errorCode) }
        cluster.awaitFullyReplicated("div")
        val before = cluster.state("div", 0)

        // B stops fetching; A (still with ISR {A, B}) takes acks=1 writes only it holds.
        cluster.pauseFetchers(b)
        repeat(3) {
            val r = client.produce("div", null, "uncommitted-$it".toByteArray(), acks = MiniKafkaClient.ACKS_LEADER, partition = 0)
            assertEquals(ErrorCodes.NONE, r.errorCode)
        }
        assertEquals(8L, cluster.broker(a).replicaManager.logEndOffset(TopicPartition("div", 0)))
        assertEquals(5L, cluster.broker(b).replicaManager.logEndOffset(TopicPartition("div", 0)))
        assertEquals(setOf(a, b), cluster.state("div", 0).isr.toSet(), "B still in the ISR, so electable")

        cluster.crashBroker(a)
        val after = cluster.awaitLeaderMovedFrom("div", 0, before)
        assertEquals(b, after.leader)
        cluster.pauseFetchers(b, paused = false)
        repeat(2) {
            val r = client.produce("div", null, "b$it".toByteArray(), acks = MiniKafkaClient.ACKS_LEADER, partition = 0)
            assertEquals(ErrorCodes.NONE, r.errorCode)
            assertEquals(5L + it, r.offset, "B appends right after the committed prefix")
        }

        cluster.restartBroker(a)
        eventually(20.seconds) { assertEquals(records(b), records(a), "A's log equals B's") }
        cluster.awaitFullyReplicated("div")
        assertEquals((0 until 5).map { "c$it" } + listOf("b0", "b1"), values(a))
        assertTrue(values(a).none { it.startsWith("uncommitted") }, "A's uncommitted records are gone")
        assertEquals(records(b), records(a))
        cluster.assertReplicasConsistent("div")
    }
}
