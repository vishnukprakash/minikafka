package minikafka.log

import java.io.File

class Log(
    private val dir: File,
    private val segmentMaxBytes: Long = 10 * 1024 * 1024,
    private val indexIntervalBytes: Int = 4096
) {
    private val segments = mutableListOf<LogSegment>()

    init {
        dir.mkdirs()
        val baseOffsets = dir.listFiles { f -> f.name.endsWith(".log") }
            ?.map { it.name.removeSuffix(".log").toLong() }
            ?.sorted()
            ?: emptyList()
        if (baseOffsets.isEmpty()) {
            segments.add(LogSegment(dir, 0L, indexIntervalBytes))
        } else {
            baseOffsets.forEach { base -> segments.add(LogSegment(dir, base, indexIntervalBytes)) }
        }
    }

    private fun activeSegment(): LogSegment = segments.last()

    @Synchronized
    fun append(timestamp: Long, key: ByteArray?, value: ByteArray): Long =
        appendAsLeader(timestamp, key, value, leaderEpoch = 0)

    @Synchronized
    fun appendAsLeader(timestamp: Long, key: ByteArray?, value: ByteArray, leaderEpoch: Int): Long {
        var active = activeSegment()
        if (active.sizeInBytes >= segmentMaxBytes) {
            active = LogSegment(dir, active.nextOffset, indexIntervalBytes)
            segments.add(active)
        }
        val offset = active.nextOffset
        active.append(Record(offset, leaderEpoch, timestamp, key, value))
        return offset
    }

    @Synchronized
    fun read(offset: Long, maxBytes: Int): List<Record> {
        val segment = segments.lastOrNull { it.baseOffset <= offset } ?: return emptyList()
        return segment.read(offset, maxBytes)
    }

    @Synchronized
    fun logEndOffset(): Long = activeSegment().nextOffset

    @Synchronized
    fun close() {
        segments.forEach { it.close() }
    }
}
