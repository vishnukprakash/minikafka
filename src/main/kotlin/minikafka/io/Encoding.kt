package minikafka.io

import java.io.DataInput
import java.io.DataOutput

fun DataOutput.writeNullableString(value: String?) {
    if (value == null) {
        writeShort(-1)
    } else {
        val bytes = value.toByteArray(Charsets.UTF_8)
        writeShort(bytes.size)
        write(bytes)
    }
}

fun DataInput.readNullableString(): String? {
    val length = readShort().toInt()
    if (length < 0) return null
    val bytes = ByteArray(length)
    readFully(bytes)
    return String(bytes, Charsets.UTF_8)
}

fun DataOutput.writeNullableBytesAsInt32(value: ByteArray?) {
    if (value == null) {
        writeInt(-1)
    } else {
        writeInt(value.size)
        write(value)
    }
}

fun DataInput.readNullableBytesAsInt32(): ByteArray? {
    val length = readInt()
    if (length < 0) return null
    val bytes = ByteArray(length)
    readFully(bytes)
    return bytes
}
