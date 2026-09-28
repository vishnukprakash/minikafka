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

    @Test
    fun `truncateTo removes entries at or after the target and shrinks the file`(@TempDir tempDir: File) {
        val file = File(tempDir, "00000000000000000000.index")
        val index = OffsetIndex(file, baseOffset = 0L)
        index.append(offset = 10L, position = 100)
        index.append(offset = 20L, position = 250)
        index.append(offset = 30L, position = 400)

        index.truncateTo(20L)
        assertEquals(8L, file.length())
        assertEquals(100, index.lookup(1000L))

        val reloaded = OffsetIndex(file, baseOffset = 0L)
        assertEquals(100, reloaded.lookup(1000L))

        index.truncateTo(0L)
        assertEquals(0L, file.length())
        assertEquals(0, index.lookup(1000L))
    }

    @Test
    fun `an index with a non-zero base offset looks up and truncates by relative offset`(@TempDir tempDir: File) {
        val file = File(tempDir, "00000000000000000100.index")
        val index = OffsetIndex(file, baseOffset = 100L)
        index.append(offset = 110L, position = 50)
        index.append(offset = 120L, position = 90)
        assertEquals(0, index.lookup(105L))
        assertEquals(50, index.lookup(115L))
        assertEquals(90, index.lookup(120L))
        val reloaded = OffsetIndex(file, baseOffset = 100L)
        assertEquals(50, reloaded.lookup(115L))
        assertEquals(90, reloaded.lookup(125L))

        index.truncateTo(115L) // keeps 110, drops 120
        assertEquals(8L, file.length())
        assertEquals(50, index.lookup(130L))
        assertEquals(50, OffsetIndex(file, baseOffset = 100L).lookup(130L))

        index.truncateTo(110L)
        assertEquals(0L, file.length())
    }
}
