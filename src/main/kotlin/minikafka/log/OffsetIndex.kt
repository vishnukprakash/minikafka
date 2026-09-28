package minikafka.log

import java.io.File
import java.io.RandomAccessFile

class OffsetIndex(private val file: File, private val baseOffset: Long) {
    private val entries = mutableListOf<Pair<Int, Int>>() // relativeOffset to filePosition, in ascending order

    init {
        if (file.exists() && file.length() > 0) {
            RandomAccessFile(file, "r").use { raf ->
                val count = (raf.length() / ENTRY_SIZE).toInt()
                for (i in 0 until count) {
                    raf.seek(i.toLong() * ENTRY_SIZE)
                    val relativeOffset = raf.readInt()
                    val position = raf.readInt()
                    entries.add(relativeOffset to position)
                }
            }
        }
    }

    @Synchronized
    fun append(offset: Long, position: Int) {
        val relativeOffset = (offset - baseOffset).toInt()
        RandomAccessFile(file, "rw").use { raf ->
            raf.seek(raf.length())
            raf.writeInt(relativeOffset)
            raf.writeInt(position)
        }
        entries.add(relativeOffset to position)
    }

    @Synchronized
    fun lookup(targetOffset: Long): Int {
        val relativeTarget = (targetOffset - baseOffset).toInt()
        var low = 0
        var high = entries.size - 1
        var result = 0
        while (low <= high) {
            val mid = (low + high) / 2
            val (relativeOffset, position) = entries[mid]
            if (relativeOffset <= relativeTarget) {
                result = position
                low = mid + 1
            } else {
                high = mid - 1
            }
        }
        return result
    }

    /**
     * Removes every entry for an offset >= [offset] and shrinks the file to match, so a lookup can
     * never return a position past the (truncated) end of the segment. [offset] <= baseOffset
     * empties the index. Always leaves the file in place (creating it if missing).
     */
    @Synchronized
    fun truncateTo(offset: Long) {
        val relativeTarget = offset - baseOffset
        val keep = entries.count { it.first < relativeTarget }
        while (entries.size > keep) entries.removeAt(entries.lastIndex)
        RandomAccessFile(file, "rw").use { it.setLength(keep.toLong() * ENTRY_SIZE) }
    }

    companion object {
        private const val ENTRY_SIZE = 8
    }
}
