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
