package minikafka.log

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.io.TempDir
import java.io.File
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
}
