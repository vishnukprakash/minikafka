package minikafka.proto

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInput
import java.io.DataInputStream
import java.io.DataOutput
import java.io.DataOutputStream

data class FrameHeader(val apiKey: Short, val correlationId: Int)

fun writeFrame(out: DataOutputStream, apiKey: Short, correlationId: Int, body: (DataOutput) -> Unit) {
    val buffer = ByteArrayOutputStream()
    val bodyOut = DataOutputStream(buffer)
    bodyOut.writeShort(apiKey.toInt())
    bodyOut.writeInt(correlationId)
    body(bodyOut)
    bodyOut.flush()
    val bytes = buffer.toByteArray()
    out.writeInt(bytes.size)
    out.write(bytes)
    out.flush()
}

fun readFrameHeader(input: DataInputStream): Pair<FrameHeader, DataInputStream> {
    val size = input.readInt()
    val bytes = ByteArray(size)
    input.readFully(bytes)
    val bodyIn = DataInputStream(ByteArrayInputStream(bytes))
    val apiKey = bodyIn.readShort()
    val correlationId = bodyIn.readInt()
    return FrameHeader(apiKey, correlationId) to bodyIn
}

fun writeResponseFrame(out: DataOutputStream, correlationId: Int, body: (DataOutput) -> Unit) {
    val buffer = ByteArrayOutputStream()
    val bodyOut = DataOutputStream(buffer)
    bodyOut.writeInt(correlationId)
    body(bodyOut)
    bodyOut.flush()
    val bytes = buffer.toByteArray()
    out.writeInt(bytes.size)
    out.write(bytes)
    out.flush()
}

fun readResponseFrame(input: DataInputStream): Pair<Int, DataInputStream> {
    val size = input.readInt()
    val bytes = ByteArray(size)
    input.readFully(bytes)
    val bodyIn = DataInputStream(ByteArrayInputStream(bytes))
    val correlationId = bodyIn.readInt()
    return correlationId to bodyIn
}
