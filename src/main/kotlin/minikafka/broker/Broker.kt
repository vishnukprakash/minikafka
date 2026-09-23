package minikafka.broker

import minikafka.log.Log
import minikafka.log.Record
import minikafka.proto.ErrorCodes
import minikafka.proto.TopicMetadata
import java.io.File
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

class Broker(private val dataDir: File) {
    private data class TopicState(val numPartitions: Int, val logs: List<Log>)

    private val topics = ConcurrentHashMap<String, TopicState>()
    private val roundRobinCounters = ConcurrentHashMap<String, AtomicInteger>()
    private val offsetStore = OffsetStore(File(dataDir, "offsets.log"))

    init {
        dataDir.mkdirs()
        val partitionCounts = mutableMapOf<String, Int>()
        dataDir.listFiles { f -> f.isDirectory }?.forEach { partitionDir ->
            val separatorIndex = partitionDir.name.lastIndexOf('-')
            if (separatorIndex > 0) {
                val topic = partitionDir.name.substring(0, separatorIndex)
                val partition = partitionDir.name.substring(separatorIndex + 1).toIntOrNull()
                if (partition != null) {
                    partitionCounts[topic] = maxOf(partitionCounts.getOrDefault(topic, 0), partition + 1)
                }
            }
        }
        partitionCounts.forEach { (topic, numPartitions) ->
            val logs = (0 until numPartitions).map { p -> Log(File(dataDir, "$topic-$p")) }
            topics[topic] = TopicState(numPartitions, logs)
        }
    }

    fun createTopic(topic: String, numPartitions: Int): Short {
        var created = false
        topics.computeIfAbsent(topic) {
            created = true
            val logs = (0 until numPartitions).map { p -> Log(File(dataDir, "$topic-$p")) }
            TopicState(numPartitions, logs)
        }
        return if (created) ErrorCodes.NONE else ErrorCodes.TOPIC_ALREADY_EXISTS
    }

    fun listTopics(): List<TopicMetadata> =
        topics.map { (name, state) -> TopicMetadata(name, state.numPartitions) }

    fun produce(topic: String, key: ByteArray?, value: ByteArray): Triple<Short, Int, Long> {
        val state = topics[topic] ?: return Triple(ErrorCodes.UNKNOWN_TOPIC, -1, -1L)
        val partition = choosePartition(topic, key, state.numPartitions)
        val offset = state.logs[partition].append(System.currentTimeMillis(), key, value)
        return Triple(ErrorCodes.NONE, partition, offset)
    }

    fun fetch(topic: String, partition: Int, offset: Long, maxBytes: Int): Pair<Short, List<Record>> {
        val state = topics[topic] ?: return ErrorCodes.UNKNOWN_TOPIC to emptyList()
        if (partition < 0 || partition >= state.numPartitions) return ErrorCodes.UNKNOWN_PARTITION to emptyList()
        val logEndOffset = state.logs[partition].logEndOffset()
        if (offset < 0 || offset > logEndOffset) {
            return ErrorCodes.OFFSET_OUT_OF_RANGE to emptyList()
        }
        return ErrorCodes.NONE to state.logs[partition].read(offset, maxBytes)
    }

    fun commitOffset(group: String, topic: String, partition: Int, offset: Long): Short {
        offsetStore.commit(group, topic, partition, offset)
        return ErrorCodes.NONE
    }

    fun fetchOffset(group: String, topic: String, partition: Int): Pair<Short, Long> =
        ErrorCodes.NONE to offsetStore.fetch(group, topic, partition)

    fun close() {
        topics.values.forEach { state -> state.logs.forEach { log -> log.close() } }
    }

    private fun choosePartition(topic: String, key: ByteArray?, numPartitions: Int): Int {
        if (key != null) {
            return Math.floorMod(key.contentHashCode(), numPartitions)
        }
        val counter = roundRobinCounters.computeIfAbsent(topic) { AtomicInteger(0) }
        return Math.floorMod(counter.getAndIncrement(), numPartitions)
    }
}
