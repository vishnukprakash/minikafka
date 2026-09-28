package minikafka.cluster

import minikafka.testing.eventually
import minikafka.zk.DuplicateBrokerIdException
import minikafka.zk.ZkTestBase
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import java.util.concurrent.atomic.AtomicInteger

@Tag("zk")
class ControllerRetryTest : ZkTestBase() {
    private val reconnected = ControllerEvent.SessionReconnected

    @Test
    fun `a pending SessionReconnected retry is never downgraded by a later failure`() {
        for (active in listOf(true, false)) {
            assertEquals(reconnected, Controller.retryAfterFailure(ControllerEvent.Elect, reconnected, active))
            assertEquals(reconnected, Controller.retryAfterFailure(ControllerEvent.Reconcile, reconnected, active))
            assertEquals(reconnected, Controller.retryAfterFailure(ControllerEvent.BrokersChanged(1), reconnected, active))
            assertEquals(reconnected, Controller.retryAfterFailure(reconnected, null, active))
            assertEquals(reconnected, Controller.retryAfterFailure(reconnected, ControllerEvent.Elect, active))
        }
        assertEquals(ControllerEvent.Reconcile, Controller.retryAfterFailure(ControllerEvent.TopicsChanged(1), null, active = true))
        assertEquals(ControllerEvent.Elect, Controller.retryAfterFailure(ControllerEvent.Elect, ControllerEvent.Reconcile, active = false))
    }

    @Test
    fun `a failing re-registration after reconnect is retried until it succeeds, then the broker elects`() {
        val store = newStore()
        val attempts = AtomicInteger()
        val controller = Controller(
            brokerId = 1,
            zk = store,
            channel = ControllerChannel(1),
            onSessionReconnected = {
                // e.g. our previous session's ephemeral is still there
                if (attempts.incrementAndGet() < 3) throw DuplicateBrokerIdException(1, 10)
            },
            retryBackoffMs = 50
        )
        try {
            controller.start()
            controller.enqueue(ControllerEvent.SessionReconnected)
            eventually { assertEquals(3, attempts.get()) }
            eventually { assertTrue(controller.isActive) }
            assertEquals(1, store.controllerEpoch().value)
        } finally {
            controller.close()
        }
    }
}
