package minikafka.client

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

/**
 * Client-side partition choice (D13), moved from the old `Broker.choosePartition`: a keyed record
 * goes to `floorMod(key.contentHashCode(), numPartitions)`; an unkeyed record round-robins using a
 * per-topic counter. Thread-safe.
 */
class Partitioner {
    private val roundRobinCounters = ConcurrentHashMap<String, AtomicInteger>()

    fun partition(topic: String, key: ByteArray?, numPartitions: Int): Int {
        require(numPartitions > 0) { "numPartitions must be positive, got $numPartitions" }
        if (key != null) return Math.floorMod(key.contentHashCode(), numPartitions)
        val counter = roundRobinCounters.computeIfAbsent(topic) { AtomicInteger(0) }
        return Math.floorMod(counter.getAndIncrement(), numPartitions)
    }
}
