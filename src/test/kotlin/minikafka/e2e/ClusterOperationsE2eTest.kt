package minikafka.e2e

import minikafka.client.ClientConfig
import minikafka.client.MiniKafkaClient
import minikafka.proto.ErrorCodes
import minikafka.testing.AckedWriteChecker
import minikafka.testing.TestCluster
import minikafka.testing.TestClusterExtension
import minikafka.testing.alwaysFor
import minikafka.testing.eventually
import minikafka.testing.replicaViews
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.extension.RegisterExtension
import java.util.concurrent.TimeUnit
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/**
 * Whole-cluster behaviour on 3 brokers, RF=3, minISR=2: produce/consume across many partitions,
 * a partition whose whole ISR is down (offline, no unclean election, even across a controller
 * failover), a rolling restart, and a ZooKeeper outage.
 */
@Tag("e2e")
@Timeout(value = 150, unit = TimeUnit.SECONDS)
class ClusterOperationsE2eTest {
    @JvmField
    @RegisterExtension
    val clusterExtension = TestClusterExtension { TestCluster(size = 3, minInsyncReplicas = 2) }
    private val cluster get() = clusterExtension.cluster

    @Test
    fun `producers on 6 partitions are consumed back in order from every partition, replicated on all 3 brokers`() {
        val partitions = (0 until 6).toList()
        cluster.createReplicatedTopic("pc", 6)
        assertEquals(setOf(1, 2, 3), partitions.map { cluster.awaitLeader("pc", it) }.toSet(), "leaders spread over the brokers")
        val workload = cluster.workload("pc", partitions).start()
        workload.awaitAcks(40, on = partitions)
        val history = workload.stop()
        cluster.awaitFullyReplicated("pc")

        val consumer = cluster.client()
        val consumed = partitions.associateWith { consumer.consumeAll("pc", it) }
        val views = cluster.replicaViews("pc")
        val violations = AckedWriteChecker.check(history, views, fullyReplicated = true, consumed = consumed)
        assertTrue(violations.isEmpty()) { AckedWriteChecker.describe(history, violations) }
        for (p in partitions) {
            val view = views.single { it.partition == p }
            assertEquals(view.logs.getValue(view.leader).map { String(it.value) }, consumed[p], "consumer sees the committed log of p$p")
            val acked = history.sendsFor(p).mapNotNull { it.ack?.value }
            assertEquals(acked, consumed.getValue(p).distinct().filter { it in acked }, "p$p acked values in order")
            assertEquals(3, view.logs.size)
        }
        cluster.assertReplicasConsistent("pc")
    }

    @Test
    fun `with the whole ISR down a partition stays offline, even across a controller failover, and recovers when an ISR member returns`() {
        cluster.createReplicatedTopic("down", 1)
        val client = cluster.client()
        repeat(3) { assertEquals(ErrorCodes.NONE, client.produce("down", null, "committed-$it".toByteArray(), partition = 0).errorCode) }
        val leader = cluster.awaitLeader("down", 0)
        val followers = cluster.replicasOf("down", 0).filter { it != leader }
        followers.forEach { cluster.pauseFetchers(it) }
        cluster.awaitIsr("down", 0, setOf(leader))
        // minISR=2 refuses acks=all now; acks=1 lands on the sole ISR member only.
        assertEquals(ErrorCodes.NOT_ENOUGH_REPLICAS, client.produce("down", null, "refused".toByteArray(), partition = 0).errorCode)
        assertEquals(ErrorCodes.NONE, client.produce("down", null, "only-on-leader".toByteArray(), acks = MiniKafkaClient.ACKS_LEADER, partition = 0).errorCode)
        val before = cluster.state("down", 0)

        cluster.stopBroker(leader)
        eventually(20.seconds) {
            val s = cluster.state("down", 0)
            assertEquals(-1, s.leader, "offline: $s")
            assertEquals(before.leaderEpoch + 1, s.leaderEpoch)
            assertEquals(listOf(leader), s.isr, "ISR preserved")
        }
        followers.forEach { cluster.pauseFetchers(it, paused = false) } // live, but not in the ISR
        val offline = { what: String ->
            val s = cluster.state("down", 0)
            assertEquals(-1, s.leader, "$what: no out-of-ISR replica may be elected: $s")
            assertEquals(listOf(leader), s.isr, "$what: ISR preserved")
            assertEquals(before.leaderEpoch + 1, s.leaderEpoch, "$what: an offline partition is not rewritten")
        }
        alwaysFor(1.seconds) { offline("before the controller failover") }

        // Force a new controller to re-evaluate the partition from scratch (full reconcile), then
        // bring the old controller back too (another broker change): still offline.
        val controller = cluster.awaitController()
        val controllerEpoch = cluster.controllerEpoch()
        cluster.stopBroker(controller)
        eventually(20.seconds) { assertNotEquals(controller, cluster.awaitController(1.seconds)) }
        assertEquals(controllerEpoch + 1, cluster.controllerEpoch())
        alwaysFor(1500.milliseconds) { offline("after the controller failover") }
        cluster.restartBroker(controller)
        eventually(20.seconds) { assertTrue(controller in cluster.admin().liveBrokers().keys) }
        alwaysFor(1500.milliseconds) { offline("after the old controller rejoined") }
        val quick = cluster.client(ClientConfig(socketTimeoutMs = 5_000, produceTimeoutMs = 2_000, maxRetries = 2, retryBackoffMs = 20))
        assertEquals(ErrorCodes.LEADER_NOT_AVAILABLE, quick.produce("down", null, "x".toByteArray(), partition = 0).errorCode)

        cluster.restartBroker(leader)
        assertEquals(leader, cluster.awaitLeader("down", 0, timeout = 20.seconds))
        assertEquals(before.leaderEpoch + 2, cluster.state("down", 0).leaderEpoch)
        cluster.awaitFullyReplicated("down")
        assertEquals((0 until 3).map { "committed-$it" } + "only-on-leader", cluster.client().consumeAll("down", 0))
        cluster.assertReplicasConsistent("down")
    }

    @Test
    fun `a rolling restart with the controller last loses no acknowledged write`() {
        cluster.createReplicatedTopic("roll", 3)
        val partitions = listOf(0, 1, 2)
        val workload = cluster.workload("roll", partitions).start()
        workload.awaitAcks(20, on = partitions)
        val controller = cluster.awaitController()
        val order = cluster.brokerIds.filter { it != controller } + controller

        for (id in order) {
            cluster.restartBroker(id) // graceful stop (fast failover), start on the same data dir
            eventually(30.seconds) {
                for (p in partitions) {
                    val s = cluster.state("roll", p)
                    assertTrue(s.leader >= 0 && cluster.isRunning(s.leader)) { "roll-$p has no running leader: $s" }
                    assertEquals(cluster.replicasOf("roll", p).toSet(), s.isr.toSet(), "broker $id back in the ISR of roll-$p")
                }
            }
            workload.awaitAcks(10, on = partitions)
        }
        cluster.awaitController()
        assertNotEquals(controller, cluster.controllerId(), "the controller moved when its broker restarted")
        cluster.stopAndCheck(workload, "roll")
    }

    @Test
    fun `during a ZooKeeper outage existing leaders keep serving produce and fetch, and the cluster recovers afterwards`() {
        cluster.createReplicatedTopic("zk", 3)
        val partitions = listOf(0, 1, 2)
        val workload = cluster.workload("zk", partitions).start()
        workload.awaitAcks(20, on = partitions)
        val reader = cluster.client().also { it.metadata() } // routes from cached metadata during the outage

        val roles = cluster.runningBrokers().associateWith { id ->
            cluster.broker(id).snapshot().partitions.filter { it.tp.topic == "zk" }.associate { it.tp.partition to it.role }
        }
        cluster.zk.stop()
        // Longer than the 3s session timeout: every broker's Curator declares the session LOST, the
        // controller resigns, but roles stay (D17) and the ISR needs no change, so acks=all commits.
        workload.awaitAcks(30, on = partitions, timeout = 20.seconds)
        eventually(15.seconds) {
            assertTrue(cluster.runningBrokers().none { cluster.broker(it).isController() }, "controller resigned on session LOST")
        }
        // From here on every session is lost: serving must continue with the roles unchanged.
        for ((id, before) in roles) {
            val now = cluster.broker(id).snapshot().partitions.filter { it.tp.topic == "zk" }.associate { it.tp.partition to it.role }
            assertEquals(before, now, "broker $id keeps its roles after session loss (D17)")
        }
        workload.awaitAcks(10, on = partitions, timeout = 20.seconds)
        alwaysFor(3500.milliseconds) {
            for (p in partitions) {
                val r = reader.fetch("zk", p, 0)
                assertEquals(ErrorCodes.NONE, r.errorCode, "fetch zk-$p during the outage")
                assertTrue(r.records.isNotEmpty())
            }
        }
        workload.awaitAcks(10, on = partitions, timeout = 20.seconds)

        cluster.zk.restart()
        cluster.awaitController(timeout = 30.seconds)
        eventually(30.seconds) { assertEquals(cluster.brokerIds.toSet(), cluster.admin().liveBrokers().keys) }
        val client = cluster.client()
        eventually(30.seconds) { assertEquals(ErrorCodes.NONE, client.createTopic("zk-after", 1, replicationFactor = 3)) }
        cluster.awaitLeader("zk-after", 0, timeout = 20.seconds)
        assertEquals(ErrorCodes.NONE, client.produce("zk-after", null, "v".toByteArray(), partition = 0).errorCode)
        workload.awaitAcks(10, on = partitions, timeout = 30.seconds)
        cluster.stopAndCheck(workload, "zk")
    }
}
