package minikafka.log

import minikafka.io.readNullableBytesAsInt32
import minikafka.io.writeNullableBytesAsInt32
import java.io.DataInput
import java.io.DataOutput

data class Record(
    val offset: Long,
    val timestamp: Long,
    val key: ByteArray?,
    val value: ByteArray
) {
    fun writeTo(out: DataOutput) {
        out.writeLong(offset)
        out.writeLong(timestamp)
        out.writeNullableBytesAsInt32(key)
        out.writeInt(value.size)
        out.write(value)
    }

    fun sizeInBytes(): Int = 8 + 8 + 4 + (key?.size ?: 0) + 4 + value.size

    companion object {
        fun readFrom(input: DataInput): Record {
            val offset = input.readLong()
            val timestamp = input.readLong()
            val key = input.readNullableBytesAsInt32()
            val valueLength = input.readInt()
            val value = ByteArray(valueLength)
            input.readFully(value)
            return Record(offset, timestamp, key, value)
        }
    }
}
