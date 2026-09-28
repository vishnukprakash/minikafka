package minikafka.cluster

import minikafka.broker.Partition
import minikafka.broker.PartitionSnapshot
import minikafka.client.ClientConfig
import minikafka.model.PartitionState
import minikafka.model.TopicPartition
import minikafka.net.Connection
import minikafka.proto.ApiKeys
import minikafka.proto.ErrorCodes
import minikafka.proto.FetchRequest
import minikafka.proto.FetchResponse
import minikafka.proto.LeaderAndIsrPartition
import minikafka.proto.LeaderAndIsrRequest
import minikafka.proto.LeaderAndIsrResponse
import minikafka.testing.TestCluster
import minikafka.testing.TestClusterExtension
import minikafka.testing.alwaysFor
import minikafka.testing.eventually
import minikafka.zk.FencedWriteResult
import minikafka.zk.KvCodec
import minikafka.zk.ZkPaths
import minikafka.zk.ZkStore
import org.apache.zookeeper.CreateMode
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.RegisterExtension
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/** Sends one raw LEADER_AND_ISR straight to broker [id] (as a zombie controller / stale queue would). */
internal fun TestCluster.rawLeaderAndIsr(id: Int, request: LeaderAndIsrRequest): Short =
    Connection("127.0.0.1", broker(id).port(), 5_000).use { conn ->
        conn.request(ApiKeys.LEADER_AND_ISR, { request.encode(it) }) { LeaderAndIsrResponse.decode(it).errorCode }
    }

/** Broker [id]'s local view of [topic]-[partition] (null = never opened there). */
internal fun TestCluster.local(id: Int, topic: String, partition: Int): PartitionSnapshot? =
    broker(id).snapshot().partitions.firstOrNull { it.tp == TopicPartition(topic, partition) }

/** Creates an RF=3 topic and waits until every partition is fully replicated. */
internal fun TestCluster.replicatedTopic(topic: String, partitions: Int = 1) {
    assertEquals(ErrorCodes.NONE, client().createTopic(topic, partitions, replicationFactor = 3))
    (0 until partitions).forEach { awaitLeader(topic, it) }
    awaitFullyReplicated(topic)
}

/**
 * Failover and fencing (algorithms 3, 4, 6, 7; D2–D4, D11, D17) on a real 3-broker cluster:
 * leader and controller failure, reconcile by a new controller, czxid bounce detection, and the
 * broker-side rejection of stale LeaderAndIsr requests.
 */
@Tag("zk")
class ControllerFailoverTest {
    @JvmField
    @RegisterExtension
    val clusterExtension = TestClusterExtension { TestCluster(size = 3) }
    private val cluster get() = clusterExtension.cluster

    @Test
    fun `a crashed leader is replaced by the first live ISR replica with leader_epoch+1`() {
        cluster.replicatedTopic("f", partitions = 3)
        val controller = cluster.awaitController()
        val p = (0 until 3).first { cluster.awaitLeader("f", it) != controller }
        val before = cluster.partitionState("f", p)!!.value
        val replicas = cluster.replicasOf("f", p)
        val client = cluster.client()
        repeat(5) { assertEquals(ErrorCodes.NONE, client.produce("f", null, "m$it".toByteArray(), partition = p).errorCode) }

        cluster.crashBroker(before.leader) // its registration lives on until the session expires
        val after = eventually(15.seconds) {
            val s = cluster.partitionState("f", p)!!.value
            assertNotEquals(before.leader, s.leader, "leader not moved yet: $s")
            s
        }
        assertEquals(replicas.first { it != before.leader }, after.leader, "first assigned replica that is live and in ISR")
        assertEquals(before.leaderEpoch + 1, after.leaderEpoch)
        assertEquals(replicas.filter { it != before.leader }.toSet(), after.isr.toSet(), "isr ∩ live")
        assertEquals(1, after.controllerEpoch)

        assertEquals(after.leader, cluster.awaitLeader("f", p))
        assertEquals(ErrorCodes.NONE, client.produce("f", null, "m5".toByteArray(), partition = p).errorCode)
        eventually { assertEquals((0..5).map { "m$it" }, client.fetch("f", p, 0).records.map { String(it.value) }) }
    }

    @Test
    fun `a crashed controller is replaced with controller_epoch+1 and topic creation continues`() {
        val first = cluster.awaitController()
        assertEquals(1, cluster.controllerEpoch())
        cluster.crashBroker(first)
        val next = eventually(15.seconds) {
            val id = cluster.awaitController(1.seconds)
            assertNotEquals(first, id)
            id
        }
        assertEquals(2, cluster.controllerEpoch())
        assertEquals(2, cluster.broker(next).controller.activeEpoch)

        val client = cluster.client()
        assertEquals(ErrorCodes.NONE, client.createTopic("after", 2, replicationFactor = 2))
        for (p in 0 until 2) {
            cluster.awaitLeader("after", p)
            val s = cluster.partitionState("after", p)!!.value
            assertEquals(0, s.leaderEpoch)
            assertEquals(2, s.controllerEpoch, "state written by the new controller")
            assertFalse(first in s.isr)
            assertEquals(ErrorCodes.NONE, client.produce("after", null, "v".toByteArray(), partition = p).errorCode)
        }
    }

    @Test
    fun `a half-created topic is finished by the next controller`() {
        val first = cluster.awaitController()
        val holding = CountDownLatch(1)
        cluster.broker(first).controller.beforeEvent = { event ->
            if (event is ControllerEvent.TopicsChanged) {
                holding.countDown()
                CountDownLatch(1).await() // until interrupted by the controller's close()
            }
        }
        assertEquals(ErrorCodes.NONE, cluster.client().createTopic("half", 2, replicationFactor = 3))
        assertTrue(holding.await(10, TimeUnit.SECONDS))
        // The old controller got as far as partition 0 before dying.
        val replicas0 = cluster.replicasOf("half", 0)
        val admin = cluster.admin()
        val written = admin.fencedCreatePartitionState(
            TopicPartition("half", 0), PartitionState(replicas0.first(), 0, replicas0, 1), admin.controllerEpoch().zkVersion
        )
        assertTrue(written is FencedWriteResult.Ok, "$written")
        alwaysFor(300.milliseconds) { assertNull(cluster.partitionState("half", 1), "old controller is held") }

        cluster.stopBroker(first)
        cluster.awaitController()
        assertEquals(2, cluster.controllerEpoch())
        val client = cluster.client()
        for (p in 0 until 2) {
            val leader = cluster.awaitLeader("half", p)
            assertNotEquals(first, leader)
            assertEquals(ErrorCodes.NONE, client.produce("half", null, "v$p".toByteArray(), partition = p).errorCode)
        }
        val created = cluster.partitionState("half", 1)!!.value
        assertEquals(2, created.controllerEpoch, "missing state created by the new controller")
        assertEquals(0, created.leaderEpoch)
        assertEquals(cluster.replicasOf("half", 1).filter { it != first }.toSet(), created.isr.toSet())
    }

    @Test
    fun `a topic whose replicas are all down gets its state once one of them registers`() {
        val controller = cluster.awaitController()
        val victim = cluster.brokerIds.first { it != controller }
        cluster.stopBroker(victim)
        assertTrue(cluster.admin().createTopicAssignment("orphan", mapOf(0 to listOf(victim))))
        alwaysFor(500.milliseconds) { assertNull(cluster.partitionState("orphan", 0), "no live replica: state stays absent") }
        val quick = cluster.client(ClientConfig(socketTimeoutMs = 5_000, produceTimeoutMs = 2_000, maxRetries = 2, retryBackoffMs = 20))
        assertEquals(ErrorCodes.LEADER_NOT_AVAILABLE, quick.produce("orphan", null, "x".toByteArray(), partition = 0).errorCode)

        cluster.restartBroker(victim)
        assertEquals(victim, cluster.awaitLeader("orphan", 0))
        assertEquals(PartitionState(victim, 0, listOf(victim), 1), cluster.partitionState("orphan", 0)!!.value)
        assertEquals(ErrorCodes.NONE, cluster.client().produce("orphan", null, "x".toByteArray(), partition = 0).errorCode)
    }

    @Test
    fun `a stale LeaderAndIsr aimed at a broker's previous incarnation is rejected with STALE_BROKER_EPOCH`() {
        cluster.replicatedTopic("s")
        val leader = cluster.awaitLeader("s", 0)
        val follower = cluster.replicasOf("s", 0).first { it != leader }
        val oldEpoch = cluster.broker(follower).brokerEpoch
        cluster.restartBroker(follower)
        val newEpoch = cluster.broker(follower).brokerEpoch
        assertNotEquals(oldEpoch, newEpoch, "a new registration has a new czxid")
        cluster.awaitFullyReplicated("s")
        val localBefore = cluster.local(follower, "s", 0)!!
        val state = cluster.partitionState("s", 0)!!

        // What a queue built for the old incarnation would still hold: "you are the leader now".
        val stale = LeaderAndIsrRequest(
            cluster.awaitController(), cluster.controllerEpoch(), oldEpoch,
            listOf(LeaderAndIsrPartition("s", 0, follower, state.value.leaderEpoch + 5, listOf(follower), cluster.replicasOf("s", 0), state.zkVersion))
        )
        assertEquals(ErrorCodes.STALE_BROKER_EPOCH, cluster.rawLeaderAndIsr(follower, stale))
        val localAfter = cluster.local(follower, "s", 0)!!
        assertEquals(Partition.Role.FOLLOWER, localAfter.role)
        assertEquals(localBefore.leaderEpoch, localAfter.leaderEpoch)
        assertEquals(leader, localAfter.leader)
        // Only the broker epoch was wrong: the same controller epoch with the current one is accepted.
        assertEquals(ErrorCodes.NONE, cluster.rawLeaderAndIsr(follower, stale.copy(brokerEpoch = newEpoch, partitions = emptyList())))
    }

    @Test
    fun `a LeaderAndIsr from a stale controller epoch is rejected`() {
        cluster.replicatedTopic("z")
        val first = cluster.awaitController()
        cluster.stopBroker(first)
        eventually(15.seconds) { assertTrue(cluster.awaitController(1.seconds) != first) }
        cluster.awaitFullyReplicated("z")
        val target = cluster.replicasOf("z", 0).first { cluster.isRunning(it) && it != cluster.leaderOf("z", 0) }
        eventually { assertEquals(2, cluster.broker(target).snapshot().seenControllerEpoch) }
        val localBefore = cluster.local(target, "z", 0)!!
        val state = cluster.partitionState("z", 0)!!

        val zombie = LeaderAndIsrRequest(
            first, 1, cluster.broker(target).brokerEpoch,
            listOf(LeaderAndIsrPartition("z", 0, target, state.value.leaderEpoch + 5, listOf(target), cluster.replicasOf("z", 0), state.zkVersion))
        )
        assertEquals(ErrorCodes.STALE_CONTROLLER_EPOCH, cluster.rawLeaderAndIsr(target, zombie))
        val localAfter = cluster.local(target, "z", 0)!!
        assertEquals(Partition.Role.FOLLOWER, localAfter.role)
        assertEquals(localBefore.leader, localAfter.leader)
        assertEquals(localBefore.leaderEpoch, localAfter.leaderEpoch)
        assertEquals(2, cluster.broker(target).snapshot().seenControllerEpoch)
    }

    /**
     * A newer controller (id 99, not a broker of this cluster, held by the admin session) takes
     * over behind the active controller's back: one transaction replaces `/controller` and bumps
     * `/controller_epoch`, exactly what a real successor's election commits. The old controller is
     * not watching `/controller`, so only its next fenced write can tell it. Returns the new epoch.
     */
    private fun takeOverControllerExternally(): Int {
        val admin = cluster.admin()
        val next = admin.controllerEpoch().value + 1
        admin.curator.transaction().forOperations(
            admin.curator.transactionOp().delete().forPath(ZkPaths.CONTROLLER),
            admin.curator.transactionOp().create().withMode(CreateMode.EPHEMERAL)
                .forPath(ZkPaths.CONTROLLER, KvCodec.encode(mapOf("brokerid" to "99"))),
            admin.curator.transactionOp().setData().forPath(ZkPaths.CONTROLLER_EPOCH, next.toString().toByteArray())
        )
        return next
    }

    /** After the foreign controller goes away, the brokers compete again: one wins with [foreignEpoch] + 1. */
    private fun releaseForeignControllerAndAwaitSuccessor(foreignEpoch: Int): Int {
        cluster.admin().curator.delete().forPath(ZkPaths.CONTROLLER)
        val next = cluster.awaitController()
        assertEquals(foreignEpoch + 1, cluster.controllerEpoch())
        assertEquals(listOf(next), cluster.runningBrokers().filter { cluster.broker(it).isController() }, "exactly one active controller")
        return next
    }

    @Test
    fun `a controller fenced while electing a leader resigns and competes again`() {
        cluster.replicatedTopic("fence", partitions = 3)
        val old = cluster.awaitController()
        val p = (0 until 3).first { cluster.awaitLeader("fence", it) != old }
        val before = cluster.partitionState("fence", p)!!
        val foreignEpoch = takeOverControllerExternally()

        cluster.stopBroker(before.value.leader) // the old controller's BrokersChanged needs an election for p

        eventually { assertFalse(cluster.broker(old).isController(), "the fenced controller must resign") }
        assertEquals(-1, cluster.broker(old).controller.activeEpoch)
        alwaysFor(300.milliseconds) {
            assertEquals(before, cluster.partitionState("fence", p), "the fenced write left the state untouched")
            assertTrue(cluster.runningBrokers().none { cluster.broker(it).isController() }, "only the foreign controller is active")
        }

        // Only the resigned controller is left to compete: it must be watching /controller again.
        cluster.runningBrokers().filter { it != old }.forEach(cluster::stopBroker)
        assertEquals(old, releaseForeignControllerAndAwaitSuccessor(foreignEpoch))
        assertEquals(old, cluster.awaitLeader("fence", p), "the only live ISR member")
        val after = cluster.partitionState("fence", p)!!.value
        assertEquals(before.value.leaderEpoch + 1, after.leaderEpoch)
        assertEquals(foreignEpoch + 1, after.controllerEpoch)
    }

    @Test
    fun `a controller fenced while creating a new topic's state resigns and competes again`() {
        val old = cluster.awaitController()
        val foreignEpoch = takeOverControllerExternally()

        assertEquals(ErrorCodes.NONE, cluster.client().createTopic("fresh", 1, replicationFactor = 3))

        eventually { assertFalse(cluster.broker(old).isController(), "the fenced controller must resign") }
        alwaysFor(300.milliseconds) { assertNull(cluster.partitionState("fresh", 0), "the fenced create wrote nothing") }

        // Only the resigned controller is left to compete: it must be watching /controller again.
        cluster.brokerIds.filter { it != old }.forEach(cluster::stopBroker)
        assertEquals(old, releaseForeignControllerAndAwaitSuccessor(foreignEpoch))
        assertEquals(old, cluster.awaitLeader("fresh", 0))
        val state = cluster.partitionState("fresh", 0)!!.value
        assertEquals(0, state.leaderEpoch)
        assertEquals(foreignEpoch + 1, state.controllerEpoch)
    }

    @Test
    fun `a broker seeds its seen controller epoch from ZooKeeper at startup`() {
        cluster.brokerIds.forEach(cluster::stopBroker)
        // A controller (not a broker of this cluster) holds /controller at epoch 7: nobody will send
        // LeaderAndIsr, so whatever the restarted broker has seen can only come from ZooKeeper.
        val admin = cluster.admin()
        admin.curator.transaction().forOperations(
            admin.curator.transactionOp().create().withMode(CreateMode.EPHEMERAL)
                .forPath(ZkPaths.CONTROLLER, KvCodec.encode(mapOf("brokerid" to "99"))),
            admin.curator.transactionOp().setData().forPath(ZkPaths.CONTROLLER_EPOCH, "7".toByteArray())
        )
        cluster.startBroker(1)
        assertEquals(7, cluster.broker(1).snapshot().seenControllerEpoch)
        assertFalse(cluster.broker(1).isController())
        val epoch = cluster.broker(1).brokerEpoch
        assertEquals(ErrorCodes.STALE_CONTROLLER_EPOCH, cluster.rawLeaderAndIsr(1, LeaderAndIsrRequest(99, 6, epoch, emptyList())))
        assertEquals(ErrorCodes.NONE, cluster.rawLeaderAndIsr(1, LeaderAndIsrRequest(99, 7, epoch, emptyList())))

        admin.curator.delete().forPath(ZkPaths.CONTROLLER) // the foreign controller goes away
        assertEquals(1, cluster.awaitController())
        assertEquals(8, cluster.controllerEpoch())
    }

    @Test
    fun `an election whose transaction committed before the reply was lost still yields a controller`() {
        // Its own chroot on the cluster's ZooKeeper: this test drives one Controller directly.
        val store = ZkStore(cluster.zk.connectString + "/e" + UUID.randomUUID().toString().take(8), 3_000).also { it.start() }
        val controller = Controller(1, store, ControllerChannel(1), retryBackoffMs = 50)
        try {
            store.ensureControllerEpochNode()
            // The election multi committed in our session, but (as after a ConnectionLoss) we never
            // saw the reply: Curator's replay then fails with NodeExists.
            store.curator.transaction().forOperations(
                store.curator.transactionOp().create().withMode(CreateMode.EPHEMERAL)
                    .forPath(ZkPaths.CONTROLLER, KvCodec.encode(mapOf("brokerid" to "1"))),
                store.curator.transactionOp().setData().withVersion(0).forPath(ZkPaths.CONTROLLER_EPOCH, "1".toByteArray())
            )
            controller.start()
            controller.enqueue(ControllerEvent.Elect)
            eventually { assertTrue(controller.isActive, "recognised its own committed election") }
            assertEquals(1, controller.activeEpoch)
            assertEquals(1, store.controllerEpoch().value, "no second epoch bump")
            assertEquals(1, store.currentController())
        } finally {
            controller.close()
            store.close()
        }
    }

    @Test
    fun `with the whole ISR dead the partition stays offline until an ISR member returns (no unclean election)`() {
        cluster.replicatedTopic("u")
        val leader = cluster.awaitLeader("u", 0)
        val followers = cluster.replicasOf("u", 0).filter { it != leader }
        followers.forEach { cluster.pauseFetchers(it) }
        cluster.awaitIsr("u", 0, setOf(leader))
        val client = cluster.client()
        assertEquals(ErrorCodes.NONE, client.produce("u", null, "only-on-leader".toByteArray(), partition = 0).errorCode)
        val before = cluster.partitionState("u", 0)!!.value

        cluster.stopBroker(leader)
        eventually(15.seconds) {
            val s = cluster.partitionState("u", 0)!!.value
            assertEquals(-1, s.leader, "offline: $s")
            assertEquals(before.leaderEpoch + 1, s.leaderEpoch, "leader=-1 is a new leader epoch too")
            assertEquals(listOf(leader), s.isr, "ISR unchanged")
        }
        followers.forEach { cluster.pauseFetchers(it, paused = false) } // live, but out of the ISR
        val quick = cluster.client(ClientConfig(socketTimeoutMs = 5_000, produceTimeoutMs = 2_000, maxRetries = 2, retryBackoffMs = 20))
        alwaysFor(1500.milliseconds) {
            val s = cluster.partitionState("u", 0)!!.value
            assertEquals(-1, s.leader, "no out-of-ISR replica may be elected: $s")
            assertEquals(listOf(leader), s.isr, "ISR preserved")
        }
        assertEquals(ErrorCodes.LEADER_NOT_AVAILABLE, quick.produce("u", null, "x".toByteArray(), partition = 0).errorCode)

        cluster.restartBroker(leader)
        assertEquals(leader, cluster.awaitLeader("u", 0))
        assertEquals(before.leaderEpoch + 2, cluster.partitionState("u", 0)!!.value.leaderEpoch)
        eventually { assertEquals(listOf("only-on-leader"), client.fetch("u", 0, 0).records.map { String(it.value) }) }
        cluster.awaitFullyReplicated("u")
        cluster.assertReplicasConsistent("u")
    }

    @Test
    fun `a quick restart is detected by its new czxid as a bounce and leadership moves off it`() = bounce(failFirstHandling = false)

    @Test
    fun `a bounce is still handled when its BrokersChanged fails and a reconcile retries it`() = bounce(failFirstHandling = true)

    /**
     * [failFirstHandling]: the controller's handling of the change fails (as on a ZooKeeper error)
     * before it looked at anything; the retry is a full reconcile, which must still see the bounce.
     */
    private fun bounce(failFirstHandling: Boolean) {
        cluster.replicatedTopic("b", partitions = 3)
        val controller = cluster.awaitController()
        val p = (0 until 3).first { cluster.awaitLeader("b", it) != controller }
        val before = cluster.partitionState("b", p)!!.value
        val victim = before.leader
        val replicas = cluster.replicasOf("b", p)
        assertEquals(replicas.first(), victim, "the preferred replica leads: an election among all live ISR members would pick it again")
        val oldEpoch = cluster.broker(victim).brokerEpoch

        // Hold the controller while the broker goes away and comes back, so that it sees one
        // change of /brokers/ids: the same id, with a new czxid.
        val held = CountDownLatch(1)
        val release = CountDownLatch(1)
        val once = AtomicBoolean()
        cluster.broker(controller).controller.beforeEvent = { event ->
            if (event is ControllerEvent.BrokersChanged && once.compareAndSet(false, true)) {
                held.countDown()
                release.await()
                if (failFirstHandling) throw IllegalStateException("injected failure")
            }
        }
        cluster.stopBroker(victim)
        assertTrue(held.await(10, TimeUnit.SECONDS))
        cluster.startBroker(victim)
        val newEpoch = cluster.admin().liveBrokers().getValue(victim).second
        assertNotEquals(oldEpoch, newEpoch)
        cluster.pauseFetchers(victim) // so the new leader cannot re-admit it before we look at the ISR
        release.countDown()

        val after = eventually(15.seconds) {
            val s = cluster.partitionState("b", p)!!.value
            assertNotEquals(before.leaderEpoch, s.leaderEpoch, "not re-elected yet: $s")
            s
        }
        assertEquals(before.leaderEpoch + 1, after.leaderEpoch, "one election (death), no offline step")
        assertEquals(replicas.first { it != victim }, after.leader, "the bounced incarnation is not eligible")
        assertFalse(victim in after.isr, "isr ∩ (live − bounced)")
        // The new incarnation got its full state (it has no partitions open after a restart).
        eventually {
            for (q in 0 until 3) {
                val local = cluster.local(victim, "b", q)
                assertEquals(cluster.partitionState("b", q)!!.value.leaderEpoch, local?.leaderEpoch, "partition $q on the bounced broker")
            }
        }
        assertEquals(Partition.Role.FOLLOWER, cluster.local(victim, "b", p)!!.role)
        cluster.pauseFetchers(victim, paused = false)
        cluster.awaitFullyReplicated("b")
    }

    @Test
    fun `a bounce of the sole ISR member is handled in one pass as death then start (epoch+2, leader back)`() {
        cluster.replicatedTopic("solo", partitions = 3)
        val controller = cluster.awaitController()
        val p = (0 until 3).first { cluster.awaitLeader("solo", it) != controller }
        val victim = cluster.awaitLeader("solo", p)
        val followers = cluster.replicasOf("solo", p).filter { it != victim }
        followers.forEach { cluster.pauseFetchers(it) }
        cluster.awaitIsr("solo", p, setOf(victim))
        assertEquals(ErrorCodes.NONE, cluster.client().produce("solo", null, "kept".toByteArray(), acks = 1, partition = p).errorCode)
        val before = cluster.partitionState("solo", p)!!.value

        // One BrokersChanged sees the victim with a new czxid: death (no eligible ISR member ⇒
        // offline, epoch+1) and then start (the returned sole ISR member is elected, epoch+2).
        val held = CountDownLatch(1)
        val release = CountDownLatch(1)
        val once = AtomicBoolean()
        cluster.broker(controller).controller.beforeEvent = { event ->
            if (event is ControllerEvent.BrokersChanged && once.compareAndSet(false, true)) {
                held.countDown()
                release.await()
            }
        }
        try {
            cluster.stopBroker(victim)
            assertTrue(held.await(10, TimeUnit.SECONDS))
            cluster.startBroker(victim)
        } finally {
            release.countDown() // never leave the controller's event thread parked
        }

        assertEquals(victim, cluster.awaitLeader("solo", p))
        val after = cluster.partitionState("solo", p)!!.value
        assertEquals(before.leaderEpoch + 2, after.leaderEpoch, "offline step then re-election, in one pass")
        assertEquals(listOf(victim), after.isr, "ISR = the sole member (no unclean election)")
        assertEquals(1, after.controllerEpoch)
        eventually { assertEquals(listOf("kept"), cluster.client().fetch("solo", p, 0).records.map { String(it.value) }) }
        followers.forEach { cluster.pauseFetchers(it, paused = false) }
        cluster.awaitFullyReplicated("solo")
        cluster.assertReplicasConsistent("solo")
    }

    @Test
    fun `a broker cut off from ZooKeeper keeps serving its partitions and rejoins when healed`() {
        val controller = cluster.awaitController()
        val victim = cluster.brokerIds.first { it != controller }
        val client = cluster.client()
        assertEquals(ErrorCodes.NONE, client.createTopic("iso", 3, replicationFactor = 1))
        val p = (0 until 3).first { cluster.awaitLeader("iso", it) == victim }
        assertEquals(ErrorCodes.NONE, client.produce("iso", null, "before".toByteArray(), partition = p).errorCode)
        val epoch = cluster.partitionState("iso", p)!!.value.leaderEpoch

        cluster.isolateFromZk(victim)
        // The controller sees the session expire: RF=1 ⇒ the partition goes offline in ZooKeeper...
        eventually(15.seconds) { assertEquals(-1, cluster.partitionState("iso", p)!!.value.leader) }
        // ...but the broker keeps its role (D17) and still answers a direct fetch.
        assertEquals(Partition.Role.LEADER, cluster.local(victim, "iso", p)!!.role)
        val fetched = Connection("127.0.0.1", cluster.broker(victim).port(), 5_000).use { conn ->
            conn.request(ApiKeys.FETCH, { FetchRequest("iso", p, 0, 1 shl 20).encode(it) }) { FetchResponse.decode(it) }
        }
        assertEquals(ErrorCodes.NONE, fetched.errorCode)
        assertEquals(listOf("before"), fetched.records.map { String(it.value) })

        cluster.healZk(victim)
        assertEquals(victim, cluster.awaitLeader("iso", p))
        assertEquals(epoch + 2, cluster.partitionState("iso", p)!!.value.leaderEpoch)
        assertEquals(ErrorCodes.NONE, client.produce("iso", null, "after".toByteArray(), partition = p).errorCode)
    }
}
