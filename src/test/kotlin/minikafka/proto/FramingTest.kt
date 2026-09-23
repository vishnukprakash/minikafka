package minikafka.proto

import minikafka.io.readNullableString
import minikafka.io.writeNullableString
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.assertEquals
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream

class FramingTest {
    @Test
    fun `round trips a request frame`() {
        val buffer = ByteArrayOutputStream()
        writeFrame(DataOutputStream(buffer), apiKey = 3, correlationId = 7) { out ->
            out.writeNullableString("hello")
        }
        val (header, body) = readFrameHeader(DataInputStream(ByteArrayInputStream(buffer.toByteArray())))
        assertEquals(3.toShort(), header.apiKey)
        assertEquals(7, header.correlationId)
        assertEquals("hello", body.readNullableString())
    }

    @Test
    fun `round trips a response frame`() {
        val buffer = ByteArrayOutputStream()
        writeResponseFrame(DataOutputStream(buffer), correlationId = 9) { out ->
            out.writeShort(0)
        }
        val (correlationId, body) = readResponseFrame(DataInputStream(ByteArrayInputStream(buffer.toByteArray())))
        assertEquals(9, correlationId)
        assertEquals(0, body.readShort().toInt())
    }
}
