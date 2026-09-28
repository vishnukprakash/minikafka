package minikafka.testing

import minikafka.broker.Clock
import java.util.concurrent.atomic.AtomicLong

/** A [Clock] that only moves when a test calls [advance] / [set]. Thread-safe. */
class MutableClock(startMillis: Long = 1_000_000L) : Clock {
    private val now = AtomicLong(startMillis)

    override fun millis(): Long = now.get()

    fun advance(millis: Long) {
        now.addAndGet(millis)
    }

    fun set(millis: Long) {
        now.set(millis)
    }
}
