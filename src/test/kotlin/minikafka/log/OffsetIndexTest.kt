package minikafka.log

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.io.TempDir
import java.io.File

class OffsetIndexTest {
    @Test
    fun `lookup returns 0 when no entries exist`(@TempDir tempDir: File) {
        val index = OffsetIndex(File(tempDir, "00000000000000000000.index"), baseOffset = 0L)
        assertEquals(0, index.lookup(100L))
    }

    @Test
    fun `lookup returns the nearest position at or before the target offset`(@TempDir tempDir: File) {
        val index = OffsetIndex(File(tempDir, "00000000000000000000.index"), baseOffset = 0L)
        index.append(offset = 10L, position = 100)
        index.append(offset = 20L, position = 250)
        index.append(offset = 30L, position = 400)

        assertEquals(0, index.lookup(5L))
        assertEquals(100, index.lookup(15L))
        assertEquals(250, index.lookup(25L))
        assertEquals(400, index.lookup(1000L))
    }

    @Test
    fun `reloads entries from an existing index file`(@TempDir tempDir: File) {
        val file = File(tempDir, "00000000000000000000.index")
        val first = OffsetIndex(file, baseOffset = 0L)
        first.append(offset = 10L, position = 100)

        val reloaded = OffsetIndex(file, baseOffset = 0L)
        assertEquals(100, reloaded.lookup(50L))
    }
}
