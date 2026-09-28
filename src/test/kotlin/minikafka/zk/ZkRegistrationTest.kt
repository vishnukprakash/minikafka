package minikafka.zk

import minikafka.model.BrokerInfo
import minikafka.testing.eventually
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

@Tag("zk")
class ZkRegistrationTest : ZkTestBase() {

    private val info = BrokerInfo(1, "localhost", 9092)

    @Test
    fun `registration creates an ephemeral node that is removed when the store closes`() {
        val broker = newStore()
        val observer = newStore()

        val czxid = broker.registerBroker(info)
        assertEquals(mapOf(1 to (info to czxid)), observer.liveBrokers())

        broker.close()
        eventually { assertEquals(emptyMap<Int, Pair<BrokerInfo, Long>>(), observer.liveBrokers()) }
    }

    @Test
    fun `registering again with the same session is idempotent and returns the same czxid`() {
        val broker = newStore()
        val czxid = broker.registerBroker(info)
        assertEquals(czxid, broker.registerBroker(info))
    }

    @Test
    fun `liveBrokers czxid (broker epoch) changes when a broker re-registers in a new session`() {
        val observer = newStore()
        val first = newStore()
        val czxid1 = first.registerBroker(info)
        first.close()
        eventually { assertTrue(observer.liveBrokers().isEmpty()) }

        val czxid2 = newStore().registerBroker(info)
        assertNotEquals(czxid1, czxid2)
        assertEquals(czxid2, observer.liveBrokers()[1]!!.second)
    }

    @Test
    fun `re-registers after a server-side session expiry in a new session`() {
        val broker = newStore()
        val observer = newStore()
        val czxid1 = broker.registerBroker(info)
        val oldSession = broker.sessionId()

        expire(broker)
        eventually { assertTrue(broker.sessionId() != oldSession && broker.isConnected()) }

        val czxid2 = broker.registerBroker(info)
        assertNotEquals(czxid1, czxid2)
        assertEquals(mapOf(1 to (info to czxid2)), observer.liveBrokers())
    }

    @Test
    fun `waits for a stale ephemeral from a previous session to disappear, then registers`() {
        val stale = newStore()
        stale.registerBroker(info)

        val restarted = newStore()
        val waiting = CountDownLatch(1)
        restarted.onWaitingForStaleRegistration = { owner ->
            if (owner == stale.sessionId()) waiting.countDown()
        }
        val registration = CompletableFuture.supplyAsync { restarted.registerBroker(info, timeoutMs = 15_000) }
        assertTrue(waiting.await(10, TimeUnit.SECONDS), "registration never started waiting for the stale node")
        assertFalse(registration.isDone)

        // The previous incarnation's session dies server-side; the waiting registration proceeds.
        expire(stale)
        val czxid = registration.get(20, TimeUnit.SECONDS)
        assertEquals(czxid, restarted.liveBrokers()[1]!!.second)
    }

    @Test
    fun `duplicate broker id held by another live session fails after the timeout`() {
        newStore().registerBroker(info)
        val impostor = newStore()
        val e = assertThrows<DuplicateBrokerIdException> { impostor.registerBroker(info, timeoutMs = 500) }
        assertTrue(e.message!!.contains("1"), e.message)
    }
}
