package minikafka.log

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertThrows
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream

class RecordTest {
    @Test
    fun `round trips a record with a key`() {
        val record = Record(offset = 5L, leaderEpoch = 3, timestamp = 1000L, key = "k".toByteArray(), value = "hello".toByteArray())
        val buffer = ByteArrayOutputStream()
        record.writeTo(DataOutputStream(buffer))
        val decoded = Record.readFrom(DataInputStream(ByteArrayInputStream(buffer.toByteArray())))
        assertEquals(record.offset, decoded.offset)
        assertEquals(record.leaderEpoch, decoded.leaderEpoch)
        assertEquals(record.timestamp, decoded.timestamp)
        assertArrayEquals(record.key, decoded.key)
        assertArrayEquals(record.value, decoded.value)
    }

    @Test
    fun `round trips a record without a key`() {
        val record = Record(offset = 0L, leaderEpoch = 0, timestamp = 2000L, key = null, value = "world".toByteArray())
        val buffer = ByteArrayOutputStream()
        record.writeTo(DataOutputStream(buffer))
        val decoded = Record.readFrom(DataInputStream(ByteArrayInputStream(buffer.toByteArray())))
        assertEquals(record.leaderEpoch, decoded.leaderEpoch)
        assertNull(decoded.key)
        assertArrayEquals(record.value, decoded.value)
    }

    @Test
    fun `sizeInBytes matches the encoded length`() {
        val record = Record(offset = 0L, leaderEpoch = 0, timestamp = 0L, key = "k".toByteArray(), value = "value".toByteArray())
        val buffer = ByteArrayOutputStream()
        record.writeTo(DataOutputStream(buffer))
        assertEquals(buffer.size(), record.sizeInBytes())
    }

    @Test
    fun `detects a bad crc caused by corrupted bytes on disk`() {
        val record = Record(offset = 0L, leaderEpoch = 0, timestamp = 0L, key = "k".toByteArray(), value = "value".toByteArray())
        val buffer = ByteArrayOutputStream()
        record.writeTo(DataOutputStream(buffer))
        val bytes = buffer.toByteArray()
        // Flip a byte inside the value payload (last byte of the encoded record).
        bytes[bytes.size - 1] = (bytes[bytes.size - 1].toInt() xor 0xFF).toByte()

        assertThrows(CorruptRecordException::class.java) {
            Record.readFrom(DataInputStream(ByteArrayInputStream(bytes)))
        }
    }
}
