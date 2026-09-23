package minikafka.log

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream

class RecordTest {
    @Test
    fun `round trips a record with a key`() {
        val record = Record(offset = 5L, timestamp = 1000L, key = "k".toByteArray(), value = "hello".toByteArray())
        val buffer = ByteArrayOutputStream()
        record.writeTo(DataOutputStream(buffer))
        val decoded = Record.readFrom(DataInputStream(ByteArrayInputStream(buffer.toByteArray())))
        assertEquals(record.offset, decoded.offset)
        assertEquals(record.timestamp, decoded.timestamp)
        assertArrayEquals(record.key, decoded.key)
        assertArrayEquals(record.value, decoded.value)
    }

    @Test
    fun `round trips a record without a key`() {
        val record = Record(offset = 0L, timestamp = 2000L, key = null, value = "world".toByteArray())
        val buffer = ByteArrayOutputStream()
        record.writeTo(DataOutputStream(buffer))
        val decoded = Record.readFrom(DataInputStream(ByteArrayInputStream(buffer.toByteArray())))
        assertNull(decoded.key)
        assertArrayEquals(record.value, decoded.value)
    }

    @Test
    fun `sizeInBytes matches the encoded length`() {
        val record = Record(offset = 0L, timestamp = 0L, key = "k".toByteArray(), value = "value".toByteArray())
        val buffer = ByteArrayOutputStream()
        record.writeTo(DataOutputStream(buffer))
        assertEquals(buffer.size(), record.sizeInBytes())
    }
}
