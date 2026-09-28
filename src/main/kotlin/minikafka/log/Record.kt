package minikafka.log

import minikafka.io.readNullableBytesAsInt32
import minikafka.io.writeNullableBytesAsInt32
import java.io.ByteArrayOutputStream
import java.io.DataInput
import java.io.DataOutput
import java.io.DataOutputStream
import java.io.IOException
import java.util.zip.CRC32

/**
 * Thrown by [Record.readFrom] when the stored CRC does not match the record's bytes, i.e. the
 * record was torn or corrupted on disk. Recovery (see [LogSegment]) truncates the log at the
 * first record that raises this.
 */
class CorruptRecordException(message: String) : IOException(message)

data class Record(
    val offset: Long,
    val leaderEpoch: Int,
    val timestamp: Long,
    val key: ByteArray?,
    val value: ByteArray
) {
    fun writeTo(out: DataOutput) {
        val body = encodeBody(leaderEpoch, timestamp, key, value)
        out.writeLong(offset)
        out.writeInt(crc32Of(body))
        out.write(body)
    }

    fun sizeInBytes(): Int = 8 + 4 + 4 + 8 + 4 + (key?.size ?: 0) + 4 + value.size

    companion object {
        fun readFrom(input: DataInput): Record {
            val offset = input.readLong()
            val expectedCrc = input.readInt()
            val leaderEpoch = input.readInt()
            val timestamp = input.readLong()
            val key = input.readNullableBytesAsInt32()
            val valueLength = input.readInt()
            val value = ByteArray(valueLength)
            input.readFully(value)

            val actualCrc = crc32Of(encodeBody(leaderEpoch, timestamp, key, value))
            if (actualCrc != expectedCrc) {
                throw CorruptRecordException(
                    "corrupt record at offset $offset: expected crc $expectedCrc, computed $actualCrc"
                )
            }
            return Record(offset, leaderEpoch, timestamp, key, value)
        }

        private fun encodeBody(leaderEpoch: Int, timestamp: Long, key: ByteArray?, value: ByteArray): ByteArray {
            val buffer = ByteArrayOutputStream()
            val out = DataOutputStream(buffer)
            out.writeInt(leaderEpoch)
            out.writeLong(timestamp)
            out.writeNullableBytesAsInt32(key)
            out.writeInt(value.size)
            out.write(value)
            return buffer.toByteArray()
        }

        private fun crc32Of(bytes: ByteArray): Int {
            val crc = CRC32()
            crc.update(bytes)
            return crc.value.toInt()
        }
    }
}
