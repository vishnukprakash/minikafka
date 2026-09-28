package minikafka.client

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class PartitionerTest {
    @Test
    fun `a key always routes to the same partition`() {
        val partitioner = Partitioner()
        val key = "user-123".toByteArray()
        val a = partitioner.partition("t", key, 4)
        val b = partitioner.partition("t", key, 4)
        assertEquals(a, b)
        assertEquals(Math.floorMod(key.contentHashCode(), 4), a)
    }

    @Test
    fun `a key with a negative hash maps into range`() {
        val partitioner = Partitioner()
        val key = (0 until 10_000).map { "some-longer-key-$it".toByteArray() }.first { it.contentHashCode() < 0 }
        val p = partitioner.partition("t", key, 3)
        assertTrue(p in 0 until 3)
    }

    @Test
    fun `no key round-robins across partitions`() {
        val partitioner = Partitioner()
        val partitions = (0 until 3).map { partitioner.partition("t", null, 3) }
        assertEquals(setOf(0, 1, 2), partitions.toSet())
    }

    @Test
    fun `round-robin counters are per topic`() {
        val partitioner = Partitioner()
        assertEquals(0, partitioner.partition("a", null, 3))
        assertEquals(1, partitioner.partition("a", null, 3))
        assertEquals(0, partitioner.partition("b", null, 3))
        assertEquals(2, partitioner.partition("a", null, 3))
    }

    @Test
    fun `zero partitions is rejected with IllegalArgumentException, keyed or not`() {
        val partitioner = Partitioner()
        assertThrows<IllegalArgumentException> { partitioner.partition("t", "k".toByteArray(), 0) }
        assertThrows<IllegalArgumentException> { partitioner.partition("t", null, 0) }
    }

    @Test
    fun `an empty key is a key, hashed like any other, not round-robined like null`() {
        val partitioner = Partitioner()
        val expected = Math.floorMod(ByteArray(0).contentHashCode(), 3)
        repeat(3) { assertEquals(expected, partitioner.partition("t", ByteArray(0), 3)) }
    }
}
