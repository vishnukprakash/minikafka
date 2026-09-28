package minikafka.e2e

import minikafka.broker.Partition
import minikafka.cluster.local
import minikafka.proto.ErrorCodes
import minikafka.testing.TestCluster
import minikafka.testing.TestClusterExtension
import minikafka.testing.alwaysFor
import minikafka.testing.eventually
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
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
 * Failures of single brokers under a running acks=all workload, on the configuration the design
 * recommends (3 brokers, RF=3, minISR=2: acks=all survives any single failure). Every test ends
 * with the acked-write checker: zero violations.
 */
@Tag("e2e")
@Timeout(value = 120, unit = TimeUnit.SECONDS)
class FailoverE2eTest {
    @JvmField
    @RegisterExtension
    val clusterExtension = TestClusterExtension { TestCluster(size = 3, minInsyncReplicas = 2) }
    private val cluster get() = clusterExtension.cluster

    @Test
    fun `killing a partition leader under acks=all loses no acknowledged write and the partition stays available`() {
        cluster.createReplicatedTopic("kill", 3)
        val controller = cluster.awaitController()
        val p = cluster.partitionLedBy("kill", 3) { it != controller }
        val before = cluster.state("kill", p)
        val workload = cluster.workload("kill", listOf(0, 1, 2)).start()
        workload.awaitAcks(30, on = listOf(p))

        cluster.crashBroker(before.leader) // no graceful close: failover waits for the session to expire
        val after = cluster.awaitLeaderMovedFrom("kill", p, before)
        assertTrue(after.leaderEpoch > before.leaderEpoch, "leader epoch increased: $before -> $after")
        assertFalse(before.leader in after.isr, "the dead leader left the ISR: $after")
        workload.awaitAcks(30, on = listOf(p)) // available again after the failover

        cluster.restartBroker(before.leader)
        workload.awaitAcks(10, on = listOf(0, 1, 2))
        cluster.stopAndCheck(workload, "kill")
    }

    @Test
    fun `killing the controller elects a new one with epoch+1, work continues, and the old one does not take over`() {
        cluster.createReplicatedTopic("ctl", 3)
        val first = cluster.awaitController()
        val epoch = cluster.controllerEpoch()
        val workload = cluster.workload("ctl", listOf(0, 1, 2)).start()
        workload.awaitAcks(20)

        cluster.crashBroker(first)
        val next = eventually(20.seconds) {
            val id = cluster.awaitController(1.seconds)
            assertNotEquals(first, id)
            id
        }
        assertEquals(epoch + 1, cluster.controllerEpoch())
        assertEquals(epoch + 1, cluster.broker(next).controller.activeEpoch)

        // Topic creation and produce continue under the new controller (including on the
        // partitions the dead controller led).
        val client = cluster.client()
        assertEquals(ErrorCodes.NONE, client.createTopic("ctl-after", 2, replicationFactor = 2))
        for (q in 0 until 2) {
            cluster.awaitLeader("ctl-after", q)
            assertEquals(epoch + 1, cluster.state("ctl-after", q).controllerEpoch)
            assertEquals(ErrorCodes.NONE, client.produce("ctl-after", null, "v$q".toByteArray(), partition = q).errorCode)
        }
        workload.awaitAcks(20, on = listOf(0, 1, 2))

        // The old controller comes back as an ordinary broker.
        cluster.restartBroker(first)
        eventually(20.seconds) { assertTrue(first in cluster.admin().liveBrokers().keys) }
        alwaysFor(1500.milliseconds) {
            assertEquals(next, cluster.controllerId())
            assertEquals(epoch + 1, cluster.controllerEpoch())
            assertFalse(cluster.broker(first).isController())
        }
        workload.awaitAcks(10)
        cluster.stopAndCheck(workload, "ctl")
    }

    @Test
    fun `a zombie controller and leader cut off from ZooKeeper converge to one controller with a higher epoch and lose nothing`() {
        cluster.createReplicatedTopic("zombie", 3)
        val zombie = cluster.awaitController()
        val controllerEpoch = cluster.controllerEpoch()
        val p = cluster.partitionLedBy("zombie", 3) { it == zombie }
        val before = cluster.state("zombie", p)
        val workload = cluster.workload("zombie", listOf(0, 1, 2)).start()
        workload.awaitAcks(20)

        // Cut its ZooKeeper link only: it keeps its TCP listener and, for a while, its roles. How
        // long the zombie window lasts is not deterministic, so only eventual outcomes are asserted.
        cluster.isolateFromZk(zombie)
        eventually(20.seconds) {
            val id = cluster.controllerId()
            assertTrue(id != null && id != zombie && cluster.isRunning(id) && cluster.broker(id).isController()) { "controller still $id" }
        }
        val after = cluster.awaitLeaderMovedFrom("zombie", p, before)
        assertTrue(after.leaderEpoch > before.leaderEpoch)
        workload.awaitAcks(20, on = listOf(p))

        cluster.healZk(zombie)
        eventually(30.seconds) {
            val controllers = cluster.runningBrokers().filter { cluster.broker(it).isController() }
            assertEquals(1, controllers.size, "exactly one live controller: $controllers")
            assertEquals(cluster.controllerId(), controllers.single())
            assertTrue(cluster.controllerEpoch() > controllerEpoch, "controller epoch moved on")
            assertTrue(zombie in cluster.admin().liveBrokers().keys, "the zombie re-registered")
        }
        workload.awaitAcks(10, on = listOf(0, 1, 2))
        cluster.stopAndCheck(workload, "zombie")
        assertEquals(1, cluster.runningBrokers().count { cluster.broker(it).isController() })
    }

    @Test
    fun `a leader whose session expires steps down, rejoins as a follower, and loses nothing`() {
        cluster.createReplicatedTopic("exp", 3)
        val controller = cluster.awaitController()
        val p = cluster.partitionLedBy("exp", 3) { it != controller }
        val before = cluster.state("exp", p)
        val victim = before.leader
        val oldBrokerEpoch = cluster.broker(victim).brokerEpoch
        val workload = cluster.workload("exp", listOf(0, 1, 2)).start()
        workload.awaitAcks(20, on = listOf(p))

        cluster.expireSession(victim)
        val after = cluster.awaitLeaderMovedFrom("exp", p, before)
        eventually(20.seconds) {
            val registered = cluster.admin().liveBrokers()[victim]
            assertTrue(registered != null && registered.second != oldBrokerEpoch) { "not re-registered yet: $registered" }
            assertEquals(registered!!.second, cluster.broker(victim).brokerEpoch)
            val local = cluster.local(victim, "exp", p)!!
            assertEquals(Partition.Role.FOLLOWER, local.role, "stepped down locally: $local")
            assertEquals(cluster.state("exp", p).leaderEpoch, local.leaderEpoch)
        }
        assertTrue(after.leaderEpoch > before.leaderEpoch)
        workload.awaitAcks(20, on = listOf(p))
        cluster.stopAndCheck(workload, "exp") // includes: the victim is back in every ISR
    }
}
