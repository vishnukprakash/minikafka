package minikafka.log

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class LeaderEpochCacheTest {
    private fun cacheOf(vararg entries: Pair<Int, Long>): LeaderEpochCache {
        val cache = LeaderEpochCache()
        entries.forEach { (epoch, start) -> cache.assign(epoch, start) }
        return cache
    }

    @Test
    fun `assign is monotonic and ignores equal or lower epochs`() {
        val cache = LeaderEpochCache()
        cache.assign(0, 0L)
        cache.assign(0, 5L)   // equal: ignored
        cache.assign(2, 10L)
        cache.assign(1, 12L)  // lower: ignored
        cache.assign(2, 15L)  // equal: ignored
        cache.assign(3, 20L)
        assertEquals(listOf(0 to 0L, 2 to 10L, 3 to 20L), cache.entries())
        assertEquals(3, cache.latestEpoch())
    }

    @Test
    fun `latestEpoch is -1 when empty`() {
        assertEquals(-1, LeaderEpochCache().latestEpoch())
    }

    @Test
    fun `endOffsetFor the latest epoch returns the log end offset`() {
        val cache = cacheOf(0 to 0L, 2 to 10L, 3 to 20L)
        assertEquals(3 to 25L, cache.endOffsetFor(3, logEndOffset = 25L))
    }

    @Test
    fun `endOffsetFor a middle epoch returns the next entry's start offset`() {
        val cache = cacheOf(0 to 0L, 2 to 10L, 3 to 20L)
        assertEquals(0 to 10L, cache.endOffsetFor(0, logEndOffset = 25L))
        assertEquals(2 to 20L, cache.endOffsetFor(2, logEndOffset = 25L))
    }

    @Test
    fun `endOffsetFor an unknown epoch between entries returns the largest lower epoch and its end`() {
        val cache = cacheOf(0 to 0L, 2 to 10L, 5 to 20L)
        assertEquals(0 to 10L, cache.endOffsetFor(1, logEndOffset = 25L))
        assertEquals(2 to 20L, cache.endOffsetFor(4, logEndOffset = 25L))
    }

    @Test
    fun `endOffsetFor an epoch above the latest returns the latest epoch and LEO`() {
        val cache = cacheOf(0 to 0L, 2 to 10L)
        assertEquals(2 to 25L, cache.endOffsetFor(7, logEndOffset = 25L))
    }

    @Test
    fun `endOffsetFor an epoch below the earliest returns the requested epoch and the earliest start`() {
        val cache = cacheOf(3 to 4L, 5 to 10L)
        assertEquals(1 to 4L, cache.endOffsetFor(1, logEndOffset = 25L))
    }

    @Test
    fun `endOffsetFor on an empty cache returns -1 and LEO`() {
        assertEquals(-1 to 7L, LeaderEpochCache().endOffsetFor(3, logEndOffset = 7L))
    }

    @Test
    fun `truncateFromEnd removes entries starting at or after the offset`() {
        val cache = cacheOf(0 to 0L, 2 to 10L, 3 to 20L)
        cache.truncateFromEnd(20L)
        assertEquals(listOf(0 to 0L, 2 to 10L), cache.entries())
        cache.truncateFromEnd(11L)
        assertEquals(listOf(0 to 0L, 2 to 10L), cache.entries())
        cache.truncateFromEnd(0L)
        assertEquals(emptyList<Pair<Int, Long>>(), cache.entries())
        assertEquals(-1, cache.latestEpoch())
    }

    @Test
    fun `after truncateFromEnd a lower epoch than the removed one can be assigned again`() {
        val cache = cacheOf(0 to 0L, 4 to 10L)
        cache.truncateFromEnd(10L)
        cache.assign(2, 10L)
        assertEquals(listOf(0 to 0L, 2 to 10L), cache.entries())
    }

    @Test
    fun `a newer epoch whose start offset precedes the latest start is rejected, an equal start is allowed`() {
        val cache = cacheOf(1 to 5L)
        assertThrows<IllegalArgumentException> { cache.assign(2, 3L) }
        assertEquals(listOf(1 to 5L), cache.entries())
        cache.assign(2, 5L)
        assertEquals(listOf(1 to 5L, 2 to 5L), cache.entries())
    }
}
