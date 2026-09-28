package minikafka.io

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream

class EncodingTest {
    @Test
    fun `round trips a non-null string`() {
        val buffer = ByteArrayOutputStream()
        DataOutputStream(buffer).writeNullableString("hello")
        val input = DataInputStream(ByteArrayInputStream(buffer.toByteArray()))
        assertEquals("hello", input.readNullableString())
    }

    @Test
    fun `round trips a null string`() {
        val buffer = ByteArrayOutputStream()
        DataOutputStream(buffer).writeNullableString(null)
        val input = DataInputStream(ByteArrayInputStream(buffer.toByteArray()))
        assertNull(input.readNullableString())
    }

    @Test
    fun `round trips non-null bytes`() {
        val bytes = byteArrayOf(1, 2, 3, 4)
        val buffer = ByteArrayOutputStream()
        DataOutputStream(buffer).writeNullableBytesAsInt32(bytes)
        val input = DataInputStream(ByteArrayInputStream(buffer.toByteArray()))
        assertArrayEquals(bytes, input.readNullableBytesAsInt32())
    }

    @Test
    fun `round trips null bytes`() {
        val buffer = ByteArrayOutputStream()
        DataOutputStream(buffer).writeNullableBytesAsInt32(null)
        val input = DataInputStream(ByteArrayInputStream(buffer.toByteArray()))
        assertNull(input.readNullableBytesAsInt32())
    }

    @Test
    fun `an empty string and empty bytes round trip as empty, not null`() {
        val buffer = ByteArrayOutputStream()
        DataOutputStream(buffer).apply {
            writeNullableString("")
            writeNullableBytesAsInt32(ByteArray(0))
        }
        val input = DataInputStream(ByteArrayInputStream(buffer.toByteArray()))
        assertEquals("", input.readNullableString())
        assertArrayEquals(ByteArray(0), input.readNullableBytesAsInt32())
    }
}
