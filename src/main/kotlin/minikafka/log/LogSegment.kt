package minikafka.log

import java.io.EOFException
import java.io.File
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

    init {
        val length = raf.length()
        raf.seek(0)
        var pos = 0L
        while (pos < length) {
            val recordStart = pos
            try {
                val record = Record.readFrom(raf)
                pos = raf.filePointer
                nextOffset = record.offset + 1
            } catch (e: EOFException) {
                raf.setLength(recordStart)
                pos = recordStart
                break
            }
        }
        sizeInBytes = pos
        raf.seek(pos)
    }

    @Synchronized
    fun append(record: Record) {
        require(record.offset == nextOffset) {
            "out-of-order append: expected offset $nextOffset, got ${record.offset}"
        }
        val position = sizeInBytes
        raf.seek(position)
        record.writeTo(raf)
        val recordSize = record.sizeInBytes()
        sizeInBytes += recordSize
        bytesSinceLastIndex += recordSize
        if (bytesSinceLastIndex >= indexIntervalBytes) {
            index.append(record.offset, position.toInt())
            bytesSinceLastIndex = 0
        }
        nextOffset = record.offset + 1
    }

    @Synchronized
    fun read(offset: Long, maxBytes: Int): List<Record> {
        if (offset < baseOffset || offset >= nextOffset) return emptyList()
        val startPosition = index.lookup(offset)
        raf.seek(startPosition.toLong())
        val records = mutableListOf<Record>()
        var bytesRead = 0
        while (raf.filePointer < sizeInBytes && bytesRead < maxBytes) {
            val record = Record.readFrom(raf)
            if (record.offset >= offset) {
                records.add(record)
                bytesRead += record.sizeInBytes()
            }
        }
        return records
    }

    fun close() {
        raf.close()
    }
}
