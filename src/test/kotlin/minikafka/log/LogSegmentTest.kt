package minikafka.log

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.fail
import org.junit.jupiter.api.io.TempDir
import java.io.DataOutputStream
import java.io.File
import java.io.FileOutputStream
import java.io.IOException
import java.io.RandomAccessFile

class LogSegmentTest {
    @Test
    fun `appends and reads back records in order`(@TempDir tempDir: File) {
        val segment = LogSegment(tempDir, baseOffset = 0L)
        segment.append(Record(0L, 0, 1000L, null, "a".toByteArray()))
        segment.append(Record(1L, 0, 1001L, null, "b".toByteArray()))
        segment.append(Record(2L, 0, 1002L, null, "c".toByteArray()))

        val records = segment.read(0L, maxBytes = 1024)
        assertEquals(3, records.size)
        assertEquals("a", String(records[0].value))
        assertEquals("b", String(records[1].value))
        assertEquals("c", String(records[2].value))
        segment.close()
    }

    @Test
    fun `reads starting from a middle offset`(@TempDir tempDir: File) {
        val segment = LogSegment(tempDir, baseOffset = 0L)
        for (i in 0 until 5) {
            segment.append(Record(i.toLong(), 0, 1000L + i, null, "v$i".toByteArray()))
        }
        val records = segment.read(3L, maxBytes = 1024)
        assertEquals(2, records.size)
        assertEquals("v3", String(records[0].value))
        assertEquals("v4", String(records[1].value))
        segment.close()
    }

    @Test
    fun `recovers by truncating a partial trailing write after an unclean shutdown`(@TempDir tempDir: File) {
        val segment = LogSegment(tempDir, baseOffset = 0L)
        segment.append(Record(0L, 0, 1000L, null, "a".toByteArray()))
        segment.append(Record(1L, 0, 1001L, null, "b".toByteArray()))
        segment.close()

        // Simulate a crash mid-write: a complete offset field followed by an
        // incomplete timestamp field (4 of the 8 bytes it needs).
        val logFile = File(tempDir, "%020d.log".format(0L))
        RandomAccessFile(logFile, "rw").use { raf ->
            raf.seek(raf.length())
            raf.writeLong(2L)
            raf.writeInt(42)
        }

        val recovered = LogSegment(tempDir, baseOffset = 0L)
        assertEquals(2L, recovered.nextOffset)
        val records = recovered.read(0L, maxBytes = 1024)
        assertEquals(2, records.size)
        recovered.close()
    }

    @Test
    fun `exposes epoch start offsets recovered from its records`(@TempDir tempDir: File) {
        val segment = LogSegment(tempDir, baseOffset = 0L)
        val epochs = listOf(0, 0, 2, 2, 2, 5)
        epochs.forEachIndexed { i, e -> segment.append(Record(i.toLong(), e, 1000L, null, "v$i".toByteArray())) }
        assertEquals(listOf(0 to 0L, 2 to 2L, 5 to 5L), segment.epochStarts())
        segment.close()

        val reopened = LogSegment(tempDir, baseOffset = 0L)
        assertEquals(listOf(0 to 0L, 2 to 2L, 5 to 5L), reopened.epochStarts())
        reopened.truncateTo(3L)
        assertEquals(listOf(0 to 0L, 2 to 2L), reopened.epochStarts())
        assertEquals(3L, reopened.nextOffset)
        reopened.close()
    }

    @Test
    fun `delete removes both files and fails loudly when a file survives`(@TempDir tempDir: File) {
        val ok = LogSegment(tempDir, baseOffset = 0L)
        ok.append(Record(0L, 0, 1000L, null, "a".toByteArray()))
        ok.delete()
        assertFalse(File(tempDir, "%020d.log".format(0L)).exists())

        val dir = File(tempDir, "ro").apply { mkdirs() }
        val segment = LogSegment(dir, baseOffset = 5L)
        segment.append(Record(5L, 0, 1000L, null, "a".toByteArray()))
        assumeTrue(dir.setWritable(false), "cannot make the directory read-only here")
        try {
            // Running as root would let the delete succeed anyway.
            assumeTrue(!File(dir, "probe").let { runCatching { it.createNewFile() }.getOrDefault(false) })
            val e = assertThrows(IOException::class.java) { segment.delete() }
            assertTrue(File(dir, "%020d.log".format(5L)).path in (e.message ?: ""), e.message)
        } finally {
            dir.setWritable(true)
        }
    }

    // ---- recovery edge cases ----

    private fun record(offset: Long, epoch: Int = 0, value: String = "v$offset") =
        Record(offset, epoch, 1000L + offset, null, value.toByteArray())

    private fun appendRawBytes(dir: File, write: (DataOutputStream) -> Unit) =
        DataOutputStream(FileOutputStream(File(dir, "%020d.log".format(0L)), true)).use(write)

    /** The fixed header of a record: offset, crc, leaderEpoch, timestamp (24 bytes, up to the key length). */
    private fun DataOutputStream.writeHeader(offset: Long) {
        writeLong(offset); writeInt(0); writeInt(0); writeLong(0L)
    }

    /** Recovery must never throw anything, OutOfMemoryError included (turned into a test failure). */
    private fun reopen(dir: File): LogSegment =
        try { LogSegment(dir, baseOffset = 0L) } catch (t: Throwable) { fail("recovery threw $t") }

    @Test
    fun `recovery keeps records with an empty value, a null key and an empty key, and every record after them`(@TempDir tempDir: File) {
        LogSegment(tempDir, baseOffset = 0L).apply {
            append(record(0))
            append(Record(1L, 0, 1001L, null, ByteArray(0)))
            append(Record(2L, 0, 1002L, ByteArray(0), ByteArray(0)))
            append(record(3))
            append(Record(4L, 0, 1004L, null, ByteArray(0))) // the last record, too
            close()
        }
        val recovered = reopen(tempDir)
        assertEquals(5L, recovered.nextOffset)
        assertEquals(listOf(0L, 1L, 2L, 3L, 4L), recovered.read(0L, maxBytes = 1 shl 20).map { it.offset })
        recovered.close()
    }

    @Test
    fun `recovery truncates a torn tail shorter than a record header`(@TempDir tempDir: File) {
        LogSegment(tempDir, baseOffset = 0L).apply { append(record(0)); close() }
        appendRawBytes(tempDir) { it.write(ByteArray(10) { 7 }) }
        val recovered = reopen(tempDir)
        assertEquals(1L, recovered.nextOffset)
        assertEquals(record(0).sizeInBytes().toLong(), recovered.sizeInBytes)
        recovered.close()
    }

    @Test
    fun `recovery truncates torn tails that end inside the key or the value length field`(@TempDir tempDir: File) {
        LogSegment(tempDir, baseOffset = 0L).apply { append(record(0)); close() }
        val good = record(0).sizeInBytes().toLong()

        appendRawBytes(tempDir) { it.writeHeader(1L); it.writeShort(0) } // 2 of the key length's 4 bytes
        reopen(tempDir).apply { assertEquals(good, sizeInBytes); close() }

        appendRawBytes(tempDir) { it.writeHeader(1L); it.writeInt(-1); it.writeShort(0) } // 2 of the value length's 4 bytes
        reopen(tempDir).apply { assertEquals(good, sizeInBytes); close() }
    }

    @Test
    fun `recovery truncates a trailing record whose key length field is garbage`(@TempDir tempDir: File) {
        LogSegment(tempDir, baseOffset = 0L).apply { append(record(0)); close() }

        appendRawBytes(tempDir) { it.writeHeader(1L); it.writeInt(Int.MAX_VALUE); it.write(ByteArray(16)) }
        reopen(tempDir).apply { assertEquals(1L, nextOffset); close() }

        appendRawBytes(tempDir) { it.writeHeader(1L); it.writeInt(-7); it.writeInt(Int.MAX_VALUE); it.write(ByteArray(16)) }
        reopen(tempDir).apply { assertEquals(1L, nextOffset); close() }
    }

    @Test
    fun `recovery truncates a trailing record whose value length field is garbage`(@TempDir tempDir: File) {
        LogSegment(tempDir, baseOffset = 0L).apply { append(record(0)); append(record(1)); close() }
        appendRawBytes(tempDir) { it.writeHeader(2L); it.writeInt(-1); it.writeInt(Int.MAX_VALUE); it.write(ByteArray(16)) }
        val recovered = reopen(tempDir)
        assertEquals(2L, recovered.nextOffset)
        assertEquals(listOf(0L, 1L), recovered.read(0L, maxBytes = 1 shl 20).map { it.offset })
        recovered.close()
    }

    // ---- append / read / truncate edges ----

    @Test
    fun `append rejects an out-of-order offset`(@TempDir tempDir: File) {
        val segment = LogSegment(tempDir, baseOffset = 0L)
        segment.append(record(0))
        assertThrows(IllegalArgumentException::class.java) { segment.append(record(5)) }
        assertThrows(IllegalArgumentException::class.java) { segment.append(record(0)) }
        assertEquals(1L, segment.nextOffset)
        segment.close()
    }

    @Test
    fun `read with maxBytes equal to one record returns exactly that record`(@TempDir tempDir: File) {
        val segment = LogSegment(tempDir, baseOffset = 0L)
        repeat(3) { segment.append(record(it.toLong())) }
        assertEquals(listOf(0L), segment.read(0L, maxBytes = record(0).sizeInBytes()).map { it.offset })
        assertEquals(listOf(0L, 1L), segment.read(0L, maxBytes = record(0).sizeInBytes() + 1).map { it.offset })
        segment.close()
    }

    @Test
    fun `truncateTo drops an epoch that starts exactly at the truncation offset`(@TempDir tempDir: File) {
        val segment = LogSegment(tempDir, baseOffset = 0L)
        listOf(0, 0, 1, 1).forEachIndexed { i, e -> segment.append(record(i.toLong(), e)) }
        segment.truncateTo(2L)
        assertEquals(listOf(0 to 0L), segment.epochStarts())
        segment.close()
    }

    @Test
    fun `truncateTo at or beyond nextOffset is a no-op`(@TempDir tempDir: File) {
        val segment = LogSegment(tempDir, baseOffset = 0L)
        segment.append(record(0)); segment.append(record(1))
        val size = segment.sizeInBytes
        segment.truncateTo(2L)
        segment.truncateTo(10L)
        assertEquals(2L, segment.nextOffset)
        assertEquals(size, segment.sizeInBytes)
        segment.append(record(2))
        assertEquals(listOf(0L, 1L, 2L), segment.read(0L, maxBytes = 1 shl 20).map { it.offset })
        segment.close()
    }

    @Test
    fun `read below the segment's base offset is empty`(@TempDir tempDir: File) {
        val segment = LogSegment(tempDir, baseOffset = 5L)
        segment.append(record(5)); segment.append(record(6))
        assertTrue(segment.read(4L, maxBytes = 1 shl 20).isEmpty())
        assertTrue(segment.read(-1L, maxBytes = 1 shl 20).isEmpty())
        assertEquals(listOf(5L, 6L), segment.read(5L, maxBytes = 1 shl 20).map { it.offset })
        segment.close()
    }
}
