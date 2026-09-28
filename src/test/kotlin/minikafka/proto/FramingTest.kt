package minikafka.proto

import minikafka.io.readNullableString
import minikafka.io.writeNullableString
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.assertEquals
import java.io.BufferedOutputStream
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

    @Test
    fun `request and response frames are flushed through a buffered stream`() {
        val sink = ByteArrayOutputStream()
        val out = DataOutputStream(BufferedOutputStream(sink, 8192))
        writeFrame(out, apiKey = 1, correlationId = 7) { it.writeInt(42) }
        assertEquals(4 + 2 + 4 + 4, sink.size(), "size + apiKey + correlationId + body reached the sink")
        writeResponseFrame(out, correlationId = 7) { it.writeInt(42) }
        assertEquals(14 + 4 + 4 + 4, sink.size(), "size + correlationId + body reached the sink")
    }
}
