package minikafka.zk

import minikafka.testing.eventually
import org.apache.zookeeper.CreateMode
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

@Tag("zk")
class ZkElectionTest : ZkTestBase() {

    @Test
    fun `controller epoch increases monotonically across successive elections`() {
        val observer = newStore()
        assertEquals(0, observer.controllerEpoch().value)

        for ((i, expectedEpoch) in listOf(1, 2, 3).withIndex()) {
            val brokerId = i + 1
            val zk = newStore()
            val election = zk.electController(brokerId)!!
            assertEquals(expectedEpoch, election.epoch)
            assertEquals(brokerId, observer.currentController())
            assertEquals(election.epoch, observer.controllerEpoch().value)
            assertEquals(election.epochZkVersion, observer.controllerEpoch().zkVersion)
            zk.close() // ephemeral /controller removed with the session
            eventually { assertNull(observer.currentController()) }
        }
        assertEquals(3, observer.controllerEpoch().value)
    }

    @Test
    fun `a second candidate loses while a controller exists`() {
        val a = newStore()
        val b = newStore()
        assertEquals(1, a.electController(1)!!.epoch)
        assertNull(b.electController(2))
        assertEquals(1, b.currentController())
        assertEquals(1, b.controllerEpoch().value)
    }

    @Test
    fun `election race - exactly one of five candidates wins`() {
        val stores = (1..5).map { newStore() }
        val go = CountDownLatch(1)
        val pool = Executors.newFixedThreadPool(5)
        try {
            val futures = stores.mapIndexed { i, zk ->
                pool.submit<ControllerElection?> { go.await(); zk.electController(i + 1) }
            }
            go.countDown()
            val results = futures.map { it.get(20, TimeUnit.SECONDS) }
            val winners = results.withIndex().filter { it.value != null }
            assertEquals(1, winners.size, "results=$results")
            assertEquals(1, winners.single().value!!.epoch)
            assertEquals(winners.single().index + 1, stores[0].currentController())
        } finally {
            pool.shutdownNow()
        }
    }

    @Test
    fun `election whose multi already committed (replayed after connection loss) is recognised as a win`() {
        val zk = newStore()
        zk.ensureControllerEpochNode()
        val before = zk.controllerEpoch()

        // Pre-apply exactly the election multi, with this store's own session, as if the first
        // attempt had committed and Curator were now replaying it.
        zk.curator.transaction().forOperations(
            zk.curator.transactionOp().create().withMode(CreateMode.EPHEMERAL)
                .forPath(ZkPaths.CONTROLLER, KvCodec.encode(mapOf("brokerid" to "4"))),
            zk.curator.transactionOp().setData().withVersion(before.zkVersion)
                .forPath(ZkPaths.CONTROLLER_EPOCH, "1".toByteArray())
        )

        val election = zk.electController(4)!!
        assertEquals(1, election.epoch)
        assertEquals(zk.controllerEpoch().zkVersion, election.epochZkVersion)
    }
}
