package minikafka.log

import minikafka.io.readNullableBytesAsInt32
import minikafka.io.recordCrcBody
import java.io.DataInput
import java.io.DataOutput
import java.util.zip.CRC32

/** Kept under its historical name; the class itself lives in `io` so `proto` can raise it too (R4). */
typealias CorruptRecordException = minikafka.io.CorruptRecordException

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

        private fun encodeBody(leaderEpoch: Int, timestamp: Long, key: ByteArray?, value: ByteArray): ByteArray =
            recordCrcBody(leaderEpoch, timestamp, key, value)

        private fun crc32Of(bytes: ByteArray): Int {
            val crc = CRC32()
            crc.update(bytes)
            return crc.value.toInt()
        }
    }
}
