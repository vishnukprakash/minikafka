package minikafka.log

import java.io.File

class Log(
    private val dir: File,
    private val segmentMaxBytes: Long = 10 * 1024 * 1024,
    private val indexIntervalBytes: Int = 4096
) {
    private val segments = mutableListOf<LogSegment>()
    private val epochCache = LeaderEpochCache()

    init {
        dir.mkdirs()
        val baseOffsets = dir.listFiles { f -> f.name.endsWith(".log") }
            ?.map { it.name.removeSuffix(".log").toLong() }
            ?.sorted()
            ?: emptyList()
        for (base in baseOffsets) {
            val previous = segments.lastOrNull()
            if (previous != null && previous.nextOffset != base) {
                // An earlier segment was truncated by recovery (or a segment is missing): every
                // later segment is no longer contiguous with the log, so drop it.
                LogSegment(dir, base, indexIntervalBytes).delete()
                continue
            }
            segments.add(LogSegment(dir, base, indexIntervalBytes))
        }
        if (segments.isEmpty()) segments.add(LogSegment(dir, 0L, indexIntervalBytes))
        segments.forEach { segment ->
            segment.epochStarts().forEach { (epoch, start) -> epochCache.assign(epoch, start) }
        }
    }

    private fun activeSegment(): LogSegment = segments.last()

    private fun rollIfFull(): LogSegment {
        var active = activeSegment()
        if (active.sizeInBytes >= segmentMaxBytes) {
            active = LogSegment(dir, active.nextOffset, indexIntervalBytes)
            segments.add(active)
        }
        return active
    }

    @Synchronized
    fun append(timestamp: Long, key: ByteArray?, value: ByteArray): Long =
        appendAsLeader(timestamp, key, value, leaderEpoch = 0)

    @Synchronized
    fun appendAsLeader(timestamp: Long, key: ByteArray?, value: ByteArray, leaderEpoch: Int): Long {
        val active = rollIfFull()
        val offset = active.nextOffset
        active.append(Record(offset, leaderEpoch, timestamp, key, value))
        epochCache.assign(leaderEpoch, offset)
        return offset
    }

    /**
     * Appends records replicated from the leader, keeping their own offsets and leader epochs.
     * The first record must be at the current log end offset and the batch must be contiguous,
     * otherwise [IllegalArgumentException] is thrown and nothing is written.
     *
     * CRC: a [Record] is an already-decoded value that does not carry its stored CRC, so there is
     * nothing left to verify here; this method *relies on* integrity having been checked where the
     * bytes were decoded — on disk by [Record.readFrom], on the wire by `proto.FetchedRecord.decode`
     * (ruling R4), both of which throw [CorruptRecordException]. [Record.writeTo] then computes a
     * fresh CRC from the fields.
     */
    @Synchronized
    fun appendAsFollower(records: List<Record>) {
        if (records.isEmpty()) return
        val leo = logEndOffset()
        require(records.first().offset == leo) {
            "follower append must start at log end offset $leo, got ${records.first().offset}"
        }
        records.zipWithNext().forEach { (a, b) ->
            require(b.offset == a.offset + 1) { "non-contiguous follower batch: ${a.offset} then ${b.offset}" }
        }
        for (record in records) {
            rollIfFull().append(record)
            epochCache.assign(record.leaderEpoch, record.offset)
        }
    }

    @Synchronized
    fun read(offset: Long, maxBytes: Int): List<Record> = read(offset, maxBytes, Long.MAX_VALUE)

    /**
     * Like [read] (returns records from a single segment only) but never returns a record whose
     * offset is >= [maxOffsetExclusive] (e.g. the high watermark for consumers).
     */
    @Synchronized
    fun read(offset: Long, maxBytes: Int, maxOffsetExclusive: Long): List<Record> {
        val segment = segments.lastOrNull { it.baseOffset <= offset } ?: return emptyList()
        return segment.read(offset, maxBytes, maxOffsetExclusive)
    }

    /**
     * Removes every record with offset >= [offset]: whole segments whose base offset is >= [offset]
     * are closed and deleted (the first segment is always kept, emptied if needed), the new active
     * segment is truncated, and the leader-epoch cache is trimmed. No-op if [offset] >= LEO.
     */
    @Synchronized
    fun truncateTo(offset: Long) {
        if (offset >= logEndOffset()) return
        while (segments.size > 1 && segments.last().baseOffset >= offset) {
            segments.removeAt(segments.lastIndex).delete()
        }
        activeSegment().truncateTo(offset)
        epochCache.truncateFromEnd(offset)
    }

    @Synchronized
    fun logEndOffset(): Long = activeSegment().nextOffset

    /** The newest leader epoch in this log, or -1 if the log has no records. */
    @Synchronized
    fun latestEpoch(): Int = epochCache.latestEpoch()

    /** See [LeaderEpochCache.endOffsetFor]; uses this log's current end offset. */
    @Synchronized
    fun endOffsetForEpoch(epoch: Int): Pair<Int, Long> = epochCache.endOffsetFor(epoch, logEndOffset())

    /** (epoch, startOffset) entries of the leader-epoch cache, for inspection. */
    @Synchronized
    fun epochEntries(): List<Pair<Int, Long>> = epochCache.entries()

    @Synchronized
    fun close() {
        segments.forEach { it.close() }
    }
}
