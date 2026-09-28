package minikafka.broker

/**
 * Millisecond wall clock used for replica-lag bookkeeping (lastCaughtUp / lastFetchTime) and
 * record timestamps. Injected so tests can drive lag deterministically with a MutableClock.
 * Note: acks=all *wait deadlines* deliberately use real time (System.nanoTime), not this clock,
 * so a waiting producer always terminates even when a test never advances its clock.
 */
fun interface Clock {
    fun millis(): Long

    companion object {
        val SYSTEM: Clock = Clock { System.currentTimeMillis() }
    }
}
