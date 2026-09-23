package minikafka.broker

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.io.TempDir
import java.io.File

class OffsetStoreTest {
    @Test
    fun `fetch returns -1 when nothing has been committed`(@TempDir tempDir: File) {
        val store = OffsetStore(File(tempDir, "offsets.log"))
        assertEquals(-1L, store.fetch("g", "t", 0))
    }

    @Test
    fun `commit then fetch returns the committed offset`(@TempDir tempDir: File) {
        val store = OffsetStore(File(tempDir, "offsets.log"))
        store.commit("g", "t", 0, 42L)
        assertEquals(42L, store.fetch("g", "t", 0))
    }

    @Test
    fun `reloads committed offsets from disk`(@TempDir tempDir: File) {
        val file = File(tempDir, "offsets.log")
        val store = OffsetStore(file)
        store.commit("g", "t", 0, 42L)

        val reloaded = OffsetStore(file)
        assertEquals(42L, reloaded.fetch("g", "t", 0))
    }
}
