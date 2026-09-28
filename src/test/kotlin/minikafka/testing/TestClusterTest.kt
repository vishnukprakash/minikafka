package minikafka.testing

import minikafka.client.MiniKafkaClient
import minikafka.model.TopicPartition
import minikafka.proto.ErrorCodes
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.RegisterExtension
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/**
 * The multi-broker test harness itself (used by Tasks 10–12): three in-process brokers, RF=1 so
 * nothing depends on replication yet, and each failure-injection hook doing what it promises.
 */
@Tag("zk")
class TestClusterTest {
    @JvmField
    @RegisterExtension
    val clusterExtension = TestClusterExtension { TestCluster(size = 3) }
    private val cluster get() = clusterExtension.cluster

    @Test
    fun `three brokers elect exactly one controller and the client routes to every partition leader`() {
        val controller = cluster.awaitController()
        assertEquals(1, cluster.brokerIds.count { cluster.broker(it).isController() })
        assertEquals(1, cluster.controllerEpoch())

        val client = cluster.client()
        assertEquals(ErrorCodes.NONE, client.createTopic("spread", 3, replicationFactor = 1))
        val leaders = (0 until 3).map { cluster.awaitLeader("spread", it) }
        assertEquals(setOf(1, 2, 3), leaders.toSet(), "RF=1 leaders spread over all brokers")
        for (p in 0 until 3) {
            val r = client.produce("spread", null, "v$p".toByteArray(), partition = p)
            assertEquals(ErrorCodes.NONE, r.errorCode)
            assertEquals("v$p", String(client.fetch("spread", p, 0L).records.single().value))
        }
        // every broker answers metadata from ZooKeeper
        for (id in cluster.brokerIds) {
            MiniKafkaClient("127.0.0.1", cluster.broker(id).port()).use { direct ->
                val md = direct.metadata()
                assertEquals(controller, md.controllerId)
                assertEquals(setOf(1, 2, 3), md.brokers.map { it.id }.toSet())
                assertEquals(leaders, md.topic("spread")!!.partitions.map { it.leader })
            }
        }
        cluster.awaitFullyReplicated("spread")
        cluster.assertReplicasConsistent("spread")
    }

    @Test
    fun `a restarted broker gets the leadership of its partitions back`() {
        val controller = cluster.awaitController()
        val client = cluster.client()
        client.createTopic("back", 3, replicationFactor = 1)
        val victim = cluster.brokerIds.first { it != controller }
        val p = (0 until 3).first { cluster.awaitLeader("back", it) == victim }
        val epoch = cluster.partitionState("back", p)!!.value.leaderEpoch

        cluster.stopBroker(victim)
        eventually { assertEquals(-1, cluster.partitionState("back", p)!!.value.leader, "RF=1 partition goes offline") }
        // The controller sees the new registration (and sends LeaderAndIsr) possibly before the
        // restarted broker has recorded its own broker epoch: R8 retries until it is accepted.
        cluster.restartBroker(victim)
        assertEquals(victim, cluster.awaitLeader("back", p))
        assertEquals(epoch + 2, cluster.partitionState("back", p)!!.value.leaderEpoch)
        cluster.client().use { assertEquals(ErrorCodes.NONE, it.produce("back", null, "v".toByteArray(), partition = p).errorCode) }
    }

    @Test
    fun `stopping the controller removes its registration at once and another broker takes over`() {
        val first = cluster.awaitController()
        cluster.stopBroker(first)
        assertFalse(first in cluster.admin().liveBrokers().keys, "graceful stop deletes the ephemeral registration")
        val next = cluster.awaitController()
        assertNotEquals(first, next)
        assertEquals(2, cluster.controllerEpoch())
    }

    @Test
    fun `a crashed broker stays registered until its session expires and can then be restarted`() {
        val victim = cluster.brokerIds.first { it != cluster.awaitController() }
        val epochBefore = cluster.admin().liveBrokers().getValue(victim).second
        cluster.crashBroker(victim)
        assertTrue(victim in cluster.admin().liveBrokers().keys, "crash leaves the ephemeral behind")
        eventually(10.seconds) { assertFalse(victim in cluster.admin().liveBrokers().keys) }

        cluster.restartBroker(victim)
        eventually { assertTrue(victim in cluster.admin().liveBrokers().keys) }
        assertNotEquals(epochBefore, cluster.admin().liveBrokers().getValue(victim).second)
    }

    @Test
    fun `an expired session re-registers the broker with a new broker epoch`() {
        val victim = cluster.brokerIds.first { it != cluster.awaitController() }
        val epochBefore = cluster.broker(victim).brokerEpoch
        cluster.expireSession(victim)
        eventually(15.seconds) {
            val registered = cluster.admin().liveBrokers()[victim]
            assertTrue(registered != null && registered.second != epochBefore) { "not re-registered yet: $registered" }
            assertEquals(registered!!.second, cluster.broker(victim).brokerEpoch)
        }
    }

    @Test
    fun `a controller cut off from ZooKeeper loses the role and rejoins once healed`() {
        val first = cluster.awaitController()
        cluster.isolateFromZk(first)
        eventually(15.seconds) {
            val now = cluster.controllerId()
            assertTrue(now != null && now != first) { "controller still $now" }
        }
        eventually(15.seconds) { assertFalse(cluster.broker(first).isController(), "isolated broker resigned") }
        assertEquals(2, cluster.controllerEpoch())

        cluster.healZk(first)
        eventually(15.seconds) { assertTrue(first in cluster.admin().liveBrokers().keys, "re-registered after heal") }
        assertEquals(1, cluster.brokerIds.count { cluster.broker(it).isController() })
    }

    @Test
    fun `ZooKeeper can be restarted on the same port and the cluster keeps working`() {
        cluster.client().createTopic("t", 1)
        cluster.awaitLeader("t", 0)
        cluster.zk.stop()
        cluster.zk.restart()
        eventually(15.seconds) { assertEquals(listOf("t"), cluster.admin().allTopics()) }
        val client = cluster.client()
        eventually(15.seconds) { assertEquals(ErrorCodes.NONE, client.produce("t", null, "after".toByteArray()).errorCode) }
    }

    @Test
    fun `pauseFetchers is reflected in the broker snapshot and diagnostics`() {
        cluster.pauseFetchers(2)
        assertTrue(cluster.broker(2).snapshot().fetchersPaused)
        cluster.pauseFetchers(2, paused = false)
        assertFalse(cluster.broker(2).snapshot().fetchersPaused)

        cluster.client().createTopic("diag", 1)
        cluster.awaitLeader("diag", 0)
        val dump = cluster.dumpDiagnostics()
        assertTrue(dump.contains("controller_epoch"), dump)
        assertTrue(dump.contains("state  [controller_epoch=1, isr="), dump)
        for (id in cluster.brokerIds) assertTrue(dump.contains("broker $id: port"), dump)
        assertTrue(dump.contains(TopicPartition("diag", 0).toString()), dump)
    }

    @Test
    fun `alwaysFor holds a property for the whole window`() {
        val start = System.nanoTime()
        alwaysFor(200.milliseconds) { assertEquals(1, cluster.controllerEpoch()) }
        assertTrue(System.nanoTime() - start >= 200_000_000L)
    }
}
