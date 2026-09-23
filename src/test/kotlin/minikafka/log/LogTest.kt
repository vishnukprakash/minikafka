package minikafka.log

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.io.TempDir
import java.io.File

class LogTest {
    @Test
    fun `appends records and assigns sequential offsets`(@TempDir tempDir: File) {
        val log = Log(File(tempDir, "t-0"))
        val offset0 = log.append(1000L, null, "a".toByteArray())
        val offset1 = log.append(1001L, null, "b".toByteArray())
        assertEquals(0L, offset0)
        assertEquals(1L, offset1)
        log.close()
    }

    @Test
    fun `reads back records appended earlier`(@TempDir tempDir: File) {
        val log = Log(File(tempDir, "t-0"))
        log.append(1000L, "k".toByteArray(), "a".toByteArray())
        log.append(1001L, "k".toByteArray(), "b".toByteArray())

        val records = log.read(0L, maxBytes = 1024)
        assertEquals(2, records.size)
        assertEquals("a", String(records[0].value))
        assertEquals("b", String(records[1].value))
        log.close()
    }

    @Test
    fun `rolls to a new segment once the active one exceeds the size threshold`(@TempDir tempDir: File) {
        val dir = File(tempDir, "t-0")
        val log = Log(dir, segmentMaxBytes = 40L)
        repeat(5) { i -> log.append(1000L + i, null, "value$i".toByteArray()) }
        log.close()

        val segmentFiles = dir.listFiles { f -> f.name.endsWith(".log") }!!
        assertTrue(segmentFiles.size > 1, "expected more than one segment file, found ${segmentFiles.size}")
    }

    @Test
    fun `reads across segment roll boundaries and exercises the sparse index`(@TempDir tempDir: File) {
        val dir = File(tempDir, "t-0")
        val log = Log(dir, segmentMaxBytes = 200L, indexIntervalBytes = 64)
        val count = 50
        for (i in 0 until count) {
            log.append(1000L + i, null, "value-%02d".format(i).toByteArray())
        }
        log.close()

        val segmentFiles = dir.listFiles { f -> f.name.endsWith(".log") }!!
        assertTrue(segmentFiles.size > 1, "expected multiple segment files, found ${segmentFiles.size}")

        // At least one segment's index file must have more than one entry (8 bytes each) for
        // OffsetIndex.lookup's binary search to actually be exercised rather than always
        // hitting the empty-index "no entries" case.
        val indexFiles = dir.listFiles { f -> f.name.endsWith(".index") }!!
        assertTrue(
            indexFiles.any { it.length() >= 16 },
            "expected at least one index file with multiple entries"
        )

        val reopened = Log(dir, segmentMaxBytes = 200L, indexIntervalBytes = 64)

        // Read starting partway through the middle of the data (not offset 0), which lands
        // partway into a later segment, past at least one roll boundary. A single read()
        // call only ever returns records from the segment it lands in (mirroring how a
        // real consumer loop re-fetches with the next offset), so drain the rest with
        // successive reads and verify every record comes back, in order.
        val midOffset = 30L
        val drained = mutableListOf<Record>()
        var nextOffset = midOffset
        var reads = 0
        while (nextOffset < count) {
            val batch = reopened.read(nextOffset, maxBytes = 1024 * 1024)
            assertTrue(batch.isNotEmpty(), "expected at least one record from offset $nextOffset")
            drained.addAll(batch)
            nextOffset = batch.last().offset + 1
            reads++
            assertTrue(reads <= count, "too many read iterations, possible infinite loop")
        }
        assertTrue(reads > 1, "expected draining from offset $midOffset to require reads spanning multiple segments")

        assertEquals(count - midOffset.toInt(), drained.size)
        drained.forEachIndexed { idx, record ->
            val expectedOffset = midOffset + idx
            assertEquals(expectedOffset, record.offset)
            assertEquals("value-%02d".format(expectedOffset.toInt()), String(record.value))
        }
        reopened.close()
    }

    @Test
    fun `survives reopening and keeps reading from where it left off`(@TempDir tempDir: File) {
        val dir = File(tempDir, "t-0")
        val log = Log(dir)
        log.append(1000L, null, "a".toByteArray())
        log.close()

        val reopened = Log(dir)
        val offset = reopened.append(1001L, null, "b".toByteArray())
        assertEquals(1L, offset)
        val records = reopened.read(0L, maxBytes = 1024)
        assertEquals(2, records.size)
        reopened.close()
    }
}
