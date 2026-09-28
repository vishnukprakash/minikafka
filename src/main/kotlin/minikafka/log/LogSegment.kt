package minikafka.log

import java.io.File
import java.io.IOException
import java.io.RandomAccessFile

class LogSegment(
    dir: File,
    val baseOffset: Long,
    private val indexIntervalBytes: Int = 4096
) {
    private val logFile = File(dir, "%020d.log".format(baseOffset))
    private val indexFile = File(dir, "%020d.index".format(baseOffset))
    private val index = OffsetIndex(indexFile, baseOffset)
    private val raf = RandomAccessFile(logFile, "rw")

    var nextOffset: Long = baseOffset
        private set
    var sizeInBytes: Long = 0L
        private set
    private var bytesSinceLastIndex: Int = 0

    /**
     * (epoch, firstOffset) each time a record's epoch rises above every earlier one in this
     * segment. For a well-formed log (epochs never decrease) this is exactly "each point the epoch
     * increases vs. the previous record".
     */
    private val epochStarts = mutableListOf<Pair<Int, Long>>()

    init {
        recover()
    }

    /**
     * Scans the segment from the start, stopping at the first record that cannot be parsed for any
     * reason (short read, bad CRC, a length field that is negative or larger than the rest of the
     * file, or an offset that is not the next expected one). The file is truncated there, and the
     * sparse index is rebuilt from the same scan (the on-disk index is never trusted), using the
     * same [indexIntervalBytes] rule as [append].
     */
    private fun recover() {
        index.truncateTo(baseOffset) // discard whatever the old index said
        val length = raf.length()
        var pos = 0L
        while (pos < length) {
            val record = tryReadRecordAt(pos, length) ?: break
            val recordSize = (raf.filePointer - pos).toInt()
            track(record, pos, recordSize)
            pos += recordSize
        }
        if (pos < length) raf.setLength(pos)
        sizeInBytes = pos
        raf.seek(pos)
    }

    /** Returns the record at [pos] if it is complete, intact and next in sequence; else null. */
    private fun tryReadRecordAt(pos: Long, fileLength: Long): Record? {
        if (!lengthFieldsFit(pos, fileLength)) return null
        raf.seek(pos)
        val record = try {
            Record.readFrom(raf)
        } catch (e: IOException) { // EOFException, CorruptRecordException, ...
            return null
        }
        return if (record.offset == nextOffset) record else null
    }

    /**
     * Validates the key/value length fields of the record at [pos] against the bytes remaining in
     * the file *before* [Record.readFrom] allocates arrays for them, so a torn or garbage length
     * (e.g. Int.MAX_VALUE) cannot trigger an OutOfMemoryError or NegativeArraySizeException.
     */
    private fun lengthFieldsFit(pos: Long, fileLength: Long): Boolean {
        val keyLengthPos = pos + KEY_LENGTH_FIELD_POSITION
        if (keyLengthPos + 4 > fileLength) return false
        raf.seek(keyLengthPos)
        val keyLength = raf.readInt()
        if (keyLength < -1) return false
        val valueLengthPos = keyLengthPos + 4 + maxOf(keyLength, 0)
        if (valueLengthPos + 4 > fileLength) return false
        raf.seek(valueLengthPos)
        val valueLength = raf.readInt()
        return valueLength >= 0 && valueLengthPos + 4 + valueLength <= fileLength
    }

    /** Shared bookkeeping for [append] and recovery: offsets, size, sparse index, epoch starts. */
    private fun track(record: Record, position: Long, recordSize: Int) {
        sizeInBytes = position + recordSize
        bytesSinceLastIndex += recordSize
        if (bytesSinceLastIndex >= indexIntervalBytes) {
            index.append(record.offset, position.toInt())
            bytesSinceLastIndex = 0
        }
        if (epochStarts.isEmpty() || record.leaderEpoch > epochStarts.last().first) {
            epochStarts.add(record.leaderEpoch to record.offset)
        }
        nextOffset = record.offset + 1
    }

    @Synchronized
    fun append(record: Record) {
        require(record.offset == nextOffset) {
            "out-of-order append: expected offset $nextOffset, got ${record.offset}"
        }
        val position = sizeInBytes
        raf.seek(position)
        record.writeTo(raf)
        track(record, position, record.sizeInBytes())
    }

    /**
     * Records from [offset] onward, stopping once [maxBytes] have been collected (at least one
     * record is always returned if any is available) and never returning an offset >=
     * [maxOffsetExclusive].
     */
    @Synchronized
    fun read(offset: Long, maxBytes: Int, maxOffsetExclusive: Long = Long.MAX_VALUE): List<Record> {
        val end = minOf(nextOffset, maxOffsetExclusive)
        if (offset < baseOffset || offset >= end) return emptyList()
        val startPosition = index.lookup(offset)
        raf.seek(startPosition.toLong())
        val records = mutableListOf<Record>()
        var bytesRead = 0
        while (raf.filePointer < sizeInBytes && bytesRead < maxBytes) {
            val record = Record.readFrom(raf)
            if (record.offset >= end) break
            if (record.offset >= offset) {
                records.add(record)
                bytesRead += record.sizeInBytes()
            }
        }
        return records
    }

    /** (epoch, firstOffset) at each point the leader epoch increases within this segment. */
    @Synchronized
    fun epochStarts(): List<Pair<Int, Long>> = epochStarts.toList()

    /**
     * Removes every record with offset >= [offset]. [offset] <= [baseOffset] empties the segment;
     * [offset] >= [nextOffset] is a no-op. The index is trimmed before the file is shortened so it
     * never points past the end of the data.
     */
    @Synchronized
    fun truncateTo(offset: Long) {
        if (offset >= nextOffset) return
        val target = maxOf(offset, baseOffset)
        val position = positionOf(target)
        index.truncateTo(target)
        raf.setLength(position)
        raf.seek(position)
        sizeInBytes = position
        nextOffset = target
        // The exact byte count since the last surviving index entry is not cheaply recoverable;
        // restarting the count only delays the next sparse entry, which is always safe.
        bytesSinceLastIndex = 0
        epochStarts.removeAll { it.second >= target }
    }

    /** Byte position of the record with [offset], which must be in [baseOffset, nextOffset). */
    private fun positionOf(offset: Long): Long {
        if (offset == baseOffset) return 0L
        var pos = index.lookup(offset).toLong()
        raf.seek(pos)
        while (pos < sizeInBytes) {
            val record = Record.readFrom(raf)
            if (record.offset >= offset) return pos
            pos = raf.filePointer
        }
        return sizeInBytes
    }

    @Synchronized
    fun close() {
        raf.close()
    }

    /** Closes the segment and deletes its log and index files. */
    @Synchronized
    fun delete() {
        raf.close()
        logFile.delete()
        indexFile.delete()
    }

    companion object {
        /** offset:int64 | crc:int32 | leaderEpoch:int32 | timestamp:int64 | keyLength:int32 ... */
        private const val KEY_LENGTH_FIELD_POSITION = 8 + 4 + 4 + 8
    }
}
