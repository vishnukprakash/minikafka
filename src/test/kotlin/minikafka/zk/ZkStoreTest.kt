package minikafka.zk

import minikafka.model.BrokerInfo
import minikafka.model.PartitionState
import minikafka.model.TopicPartition
import minikafka.model.Versioned
import minikafka.testing.eventually
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger

@Tag("zk")
class ZkStoreTest : ZkTestBase() {

    private val tp = TopicPartition("demo", 0)
    private val state = PartitionState(leader = 1, leaderEpoch = 0, isr = listOf(1, 2, 3), controllerEpoch = 1)

    @Test
    fun `topic assignment is created once and a duplicate create returns false`() {
        val zk = newStore()
        val assignment = mapOf(0 to listOf(1, 2, 3), 1 to listOf(2, 3, 1))

        assertTrue(zk.createTopicAssignment("demo", assignment))
        assertFalse(zk.createTopicAssignment("demo", mapOf(0 to listOf(9))))

        assertEquals(assignment, zk.readAssignment("demo"))
        assertNull(zk.readAssignment("missing"))
        assertEquals(listOf("demo"), zk.allTopics())
    }

    @Test
    fun `allTopics is empty before any topic exists`() {
        assertEquals(emptyList<String>(), newStore().allTopics())
    }

    @Test
    fun `CAS succeeds on the current version and returns null on a stale version`() {
        val zk = newStoreWithTopic()
        val election = zk.electController(1)!!
        val v0 = (zk.fencedCreatePartitionState(tp, state, election.epochZkVersion) as FencedWriteResult.Ok).zkVersion
        assertEquals(Versioned(state, v0), zk.readPartitionState(tp))

        val shrunk = state.copy(isr = listOf(1, 2))
        val v1 = zk.casPartitionState(tp, shrunk, v0)
        assertEquals(v0 + 1, v1)
        assertEquals(shrunk, zk.readPartitionState(tp)!!.value)

        // A second writer still holding v0 loses, and the znode is untouched.
        assertNull(zk.casPartitionState(tp, state.copy(isr = listOf(1)), v0))
        assertEquals(shrunk, zk.readPartitionState(tp)!!.value)
    }

    @Test
    fun `CAS BadVersion whose znode already equals the intended state is recognised as our replayed write`() {
        val zk = newStoreWithTopic()
        val election = zk.electController(1)!!
        val v0 = (zk.fencedCreatePartitionState(tp, state, election.epochZkVersion) as FencedWriteResult.Ok).zkVersion
        val shrunk = state.copy(isr = listOf(1, 2))

        // Simulate "committed, connection lost, Curator replays": the exact write is already applied...
        val committed = zk.casPartitionState(tp, shrunk, v0)!!
        // ...so the replay hits BadVersion, but must be classified as our own write.
        assertEquals(committed, zk.casPartitionState(tp, shrunk, v0))
    }

    @Test
    fun `CAS BadVersion whose znode has the same ISR but a newer leader, epoch or controller epoch is not our write`() {
        val zk = newStoreWithTopic()
        val election = zk.electController(1)!!
        val v0 = (zk.fencedCreatePartitionState(tp, state, election.epochZkVersion) as FencedWriteResult.Ok).zkVersion
        // The old leader (1, epoch 0) wants to shrink to [1, 2]...
        val ourShrink = state.copy(isr = listOf(1, 2))

        // ...but a controller failover already elected 2 in epoch 1 with that same ISR.
        val failover = PartitionState(leader = 2, leaderEpoch = 1, isr = listOf(1, 2), controllerEpoch = 1)
        val v1 = (zk.fencedSetPartitionState(tp, failover, v0, election.epochZkVersion) as FencedWriteResult.Ok).zkVersion
        assertNull(zk.casPartitionState(tp, ourShrink, v0), "same ISR, different leader and leader_epoch")
        assertEquals(Versioned(failover, v1), zk.readPartitionState(tp))

        // Same leader, newer leader_epoch (1 re-elected), same ISR.
        val reelected = PartitionState(leader = 1, leaderEpoch = 2, isr = listOf(1, 2), controllerEpoch = 1)
        val v2 = (zk.fencedSetPartitionState(tp, reelected, v1, election.epochZkVersion) as FencedWriteResult.Ok).zkVersion
        assertNull(zk.casPartitionState(tp, reelected.copy(leaderEpoch = 1), v1), "same leader and ISR, newer leader_epoch")
        assertEquals(Versioned(reelected, v2), zk.readPartitionState(tp))

        // Differs only in controller_epoch.
        assertNull(zk.casPartitionState(tp, reelected.copy(controllerEpoch = 7), v1), "differs only in controller_epoch")
        assertEquals(Versioned(reelected, v2), zk.readPartitionState(tp))
    }

    @Test
    fun `fenced create refuses to invent a topic znode that has no assignment`() {
        val zk = newStore()
        val e = zk.electController(1)!!
        assertThrows<IllegalStateException> { zk.fencedCreatePartitionState(tp, state, e.epochZkVersion) }
        assertNull(zk.readAssignment(tp.topic))
    }

    @Test
    fun `CAS on a missing state znode returns null`() {
        assertNull(newStore().casPartitionState(tp, state, 0))
    }

    @Test
    fun `fenced write from a controller whose epoch was superseded is rejected as ControllerFenced`() {
        val a = newStoreWithTopic()
        val b = newStore()
        val aElection = a.electController(1)!!
        assertEquals(1, aElection.epoch)

        // A becomes a zombie: its session expires (so /controller goes away) and B is elected.
        val oldSession = a.sessionId()
        expire(a)
        eventually { assertTrue(a.sessionId() != oldSession && a.isConnected()) }
        val bElection = eventually { checkNotNull(b.electController(2)) { "B not elected yet" } }
        assertEquals(2, bElection.epoch)

        // A still believes in its old epoch: every fenced write must fail on the epoch check.
        assertEquals(FencedWriteResult.ControllerFenced, a.fencedCreatePartitionState(tp, state, aElection.epochZkVersion))
        assertNull(a.readPartitionState(tp))

        val bState = state.copy(controllerEpoch = 2)
        val v0 = (b.fencedCreatePartitionState(tp, bState, bElection.epochZkVersion) as FencedWriteResult.Ok).zkVersion
        assertEquals(
            FencedWriteResult.ControllerFenced,
            a.fencedSetPartitionState(tp, state.copy(leaderEpoch = 1), v0, aElection.epochZkVersion)
        )
        assertEquals(bState, a.readPartitionState(tp)!!.value)
    }

    @Test
    fun `fenced set with a stale state version is a StateConflict, fenced create of an existing different state is AlreadyExists`() {
        val zk = newStoreWithTopic()
        val e = zk.electController(1)!!
        val v0 = (zk.fencedCreatePartitionState(tp, state, e.epochZkVersion) as FencedWriteResult.Ok).zkVersion
        val next = state.copy(leader = 2, leaderEpoch = 1, isr = listOf(2, 3))
        val v1 = (zk.fencedSetPartitionState(tp, next, v0, e.epochZkVersion) as FencedWriteResult.Ok).zkVersion
        assertEquals(v0 + 1, v1)

        val other = state.copy(leader = 3, leaderEpoch = 2, isr = listOf(3))
        assertEquals(FencedWriteResult.StateConflict, zk.fencedSetPartitionState(tp, other, v0, e.epochZkVersion))
        assertEquals(FencedWriteResult.AlreadyExists, zk.fencedCreatePartitionState(tp, other, e.epochZkVersion))
        assertEquals(next, zk.readPartitionState(tp)!!.value)
    }

    @Test
    fun `fenced writes that were already committed are recognised as our own on replay`() {
        val zk = newStoreWithTopic()
        val e = zk.electController(1)!!
        val v0 = (zk.fencedCreatePartitionState(tp, state, e.epochZkVersion) as FencedWriteResult.Ok).zkVersion
        // Replayed create: NodeExists, but content is exactly ours.
        assertEquals(FencedWriteResult.Ok(v0), zk.fencedCreatePartitionState(tp, state, e.epochZkVersion))

        val next = state.copy(leader = 2, leaderEpoch = 1, isr = listOf(2, 3))
        val v1 = (zk.fencedSetPartitionState(tp, next, v0, e.epochZkVersion) as FencedWriteResult.Ok).zkVersion
        // Replayed set: BadVersion on the state op, but content is exactly ours.
        assertEquals(FencedWriteResult.Ok(v1), zk.fencedSetPartitionState(tp, next, v0, e.epochZkVersion))
    }

    @Test
    fun `offsets commit, overwrite, report -1 when none, and are visible from another session`() {
        val a = newStore()
        assertEquals(-1L, a.fetchOffset("g1", tp))

        a.commitOffset("g1", tp, 5)
        assertEquals(5L, a.fetchOffset("g1", tp))
        a.commitOffset("g1", tp, 42)
        assertEquals(42L, a.fetchOffset("g1", tp))

        assertEquals(-1L, a.fetchOffset("g2", tp))
        assertEquals(-1L, a.fetchOffset("g1", TopicPartition("demo", 1)))

        a.close()
        val b = newStore()
        assertEquals(42L, b.fetchOffset("g1", tp))
    }

    @Test
    fun `watchChildren returns current children and fires once when a child is added`() {
        val watcher = newStore()
        val other = newStore()
        val fired = AtomicInteger()

        assertEquals(emptyList<String>(), watcher.watchChildren(ZkPaths.BROKER_IDS) { fired.incrementAndGet() })
        other.registerBroker(BrokerInfo(1, "localhost", 9092))
        eventually { assertEquals(1, fired.get()) }

        // One-shot: re-arm by calling again; the read reflects the change.
        assertEquals(listOf("1"), watcher.watchChildren(ZkPaths.BROKER_IDS) { fired.incrementAndGet() })
        other.registerBroker(BrokerInfo(2, "localhost", 9093))
        eventually { assertEquals(2, fired.get()) }
    }

    @Test
    fun `watchNode returns null for a missing node and fires on creation, then on deletion`() {
        val watcher = newStore()
        val other = newStore()
        val fired = AtomicInteger()

        assertNull(watcher.watchNode(ZkPaths.CONTROLLER) { fired.incrementAndGet() })
        other.electController(7)
        eventually { assertEquals(1, fired.get()) }

        assertEquals(mapOf("brokerid" to "7"), KvCodec.decode(watcher.watchNode(ZkPaths.CONTROLLER) { fired.incrementAndGet() }!!))
        other.close() // ephemeral /controller deleted
        eventually { assertEquals(2, fired.get()) }
        assertNull(watcher.currentController())
    }

    @Test
    fun `watchNode fires on a data change`() {
        val watcher = newStore()
        val other = newStore()
        watcher.ensureControllerEpochNode()
        val fired = AtomicInteger()

        assertEquals("0", String(watcher.watchNode(ZkPaths.CONTROLLER_EPOCH) { fired.incrementAndGet() }!!))
        other.electController(1) // bumps /controller_epoch to 1
        eventually { assertEquals(1, fired.get()) }
        assertEquals("1", String(watcher.watchNode(ZkPaths.CONTROLLER_EPOCH) { fired.incrementAndGet() }!!))
    }

    @Test
    fun `connection listener reports LOST then RECONNECTED across a session expiry`() {
        val zk = newStore()
        val events = CopyOnWriteArrayList<ConnectionStateKind>()
        zk.onConnectionStateChanged { events += it }

        expire(zk)
        eventually {
            assertTrue(events.contains(ConnectionStateKind.LOST), "events=$events")
            assertTrue(events.contains(ConnectionStateKind.RECONNECTED), "events=$events")
        }
        assertTrue(events.indexOf(ConnectionStateKind.LOST) < events.lastIndexOf(ConnectionStateKind.RECONNECTED))
    }

    private fun newStoreWithTopic(): ZkStore =
        newStore().also { assertTrue(it.createTopicAssignment(tp.topic, mapOf(tp.partition to listOf(1, 2, 3)))) }
}
