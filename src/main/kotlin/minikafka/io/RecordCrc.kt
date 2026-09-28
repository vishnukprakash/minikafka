package minikafka.io

import java.io.ByteArrayOutputStream
import java.io.DataOutputStream
import java.io.IOException
import java.util.zip.CRC32

/**
 * Thrown when a record's stored CRC does not match its bytes: torn or corrupted on disk (log
 * recovery truncates at the first such record) or on the wire (a FETCH response carrying one is
 * discarded, R4). Lives in `io` so both `log` and `proto` can raise it without depending on each other.
 */
class CorruptRecordException(message: String) : IOException(message)

/**
 * CRC32 over a record's `leaderEpoch | timestamp | key | value` (D18) — the same bytes and order
 * as the on-disk [minikafka.log.Record] body and the wire `FetchedRecord`, so a CRC computed on
 * disk is valid on the wire and vice versa.
 */
fun recordCrc(leaderEpoch: Int, timestamp: Long, key: ByteArray?, value: ByteArray): Int {
    val crc = CRC32()
    crc.update(recordCrcBody(leaderEpoch, timestamp, key, value))
    return crc.value.toInt() // CRC32 is 32 bits: narrowing to Int keeps every bit (sign aside)
}

/** The exact bytes [recordCrc] covers. */
fun recordCrcBody(leaderEpoch: Int, timestamp: Long, key: ByteArray?, value: ByteArray): ByteArray {
    val buffer = ByteArrayOutputStream()
    val out = DataOutputStream(buffer)
    out.writeInt(leaderEpoch)
    out.writeLong(timestamp)
    out.writeNullableBytesAsInt32(key)
    out.writeInt(value.size)
    out.write(value)
    return buffer.toByteArray()
}
