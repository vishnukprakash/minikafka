package minikafka.testing

import java.util.concurrent.locks.LockSupport
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/**
 * Polls [block] every [pollInterval] until it completes without throwing, or rethrows the last
 * failure once [timeout] has elapsed. The only sanctioned way for tests to wait for asynchronous
 * effects (tests never call `Thread.sleep` directly).
 */
fun <T> eventually(
    timeout: Duration = 10.seconds,
    pollInterval: Duration = 25.milliseconds,
    block: () -> T
): T {
    val deadline = System.nanoTime() + timeout.inWholeNanoseconds
    while (true) {
        try {
            return block()
        } catch (e: Throwable) {
            if (e !is AssertionError && e !is Exception) throw e
            if (System.nanoTime() >= deadline) throw e
        }
        LockSupport.parkNanos(pollInterval.inWholeNanoseconds)
    }
}

/**
 * Asserts that [block] keeps succeeding (does not throw) for the whole of [duration], polling every
 * [pollInterval]; rethrows the first failure. For "this must never happen" properties (e.g. no
 * unclean leader is ever elected while the ISR is down).
 */
fun alwaysFor(
    duration: Duration,
    pollInterval: Duration = 25.milliseconds,
    block: () -> Unit
) {
    val deadline = System.nanoTime() + duration.inWholeNanoseconds
    while (true) {
        block()
        if (System.nanoTime() >= deadline) return
        LockSupport.parkNanos(pollInterval.inWholeNanoseconds)
    }
}
