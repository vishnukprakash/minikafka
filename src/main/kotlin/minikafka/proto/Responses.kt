package minikafka.proto

import minikafka.io.CorruptRecordException
import minikafka.io.readNullableBytesAsInt32
import minikafka.io.recordCrc
import minikafka.io.readNullableString
import minikafka.io.writeNullableBytesAsInt32
import minikafka.io.writeNullableString
import java.io.DataInput
import java.io.DataOutput

data class CreateTopicResponse(val errorCode: Short) {
    fun encode(out: DataOutput) { out.writeShort(errorCode.toInt()) }
    companion object {
        fun decode(input: DataInput): CreateTopicResponse = CreateTopicResponse(input.readShort())
    }
}

internal fun DataOutput.writeIntList(values: List<Int>) {
    writeInt(values.size)
    values.forEach { writeInt(it) }
}

internal fun DataInput.readIntList(): List<Int> {
    val count = readInt()
    return (0 until count).map { readInt() }
}

data class BrokerMetadata(val id: Int, val host: String, val port: Int) {
    fun encode(out: DataOutput) {
        out.writeInt(id)
        out.writeNullableString(host)
        out.writeInt(port)
    }
    companion object {
        fun decode(input: DataInput): BrokerMetadata {
            val id = input.readInt()
            val host = input.readNullableString()!!
            val port = input.readInt()
            return BrokerMetadata(id, host, port)
        }
    }
}

/** [leader] is -1 when the partition has no leader (state absent or all ISR down); [leaderEpoch] -1 when state is absent. */
data class PartitionMetadata(
    val partition: Int,
    val leader: Int,
    val leaderEpoch: Int,
    val replicas: List<Int>,
    val isr: List<Int>
) {
    fun encode(out: DataOutput) {
        out.writeInt(partition)
        out.writeInt(leader)
        out.writeInt(leaderEpoch)
        out.writeIntList(replicas)
        out.writeIntList(isr)
    }
    companion object {
        fun decode(input: DataInput): PartitionMetadata {
            val partition = input.readInt()
            val leader = input.readInt()
            val leaderEpoch = input.readInt()
            val replicas = input.readIntList()
            val isr = input.readIntList()
            return PartitionMetadata(partition, leader, leaderEpoch, replicas, isr)
        }
    }
}

data class TopicMetadata(val name: String, val partitions: List<PartitionMetadata>) {
    /** Derived (not on the wire): the topic's partition count. */
    val numPartitions: Int get() = partitions.size

    fun partition(p: Int): PartitionMetadata? = partitions.firstOrNull { it.partition == p }

    fun encode(out: DataOutput) {
        out.writeNullableString(name)
        out.writeInt(partitions.size)
        partitions.forEach { it.encode(out) }
    }
    companion object {
        fun decode(input: DataInput): TopicMetadata {
            val name = input.readNullableString()!!
            val count = input.readInt()
            val partitions = (0 until count).map { PartitionMetadata.decode(input) }
            return TopicMetadata(name, partitions)
        }
    }
}

/** [controllerId] is -1 when no controller is currently elected. */
data class MetadataResponse(
    val controllerId: Int,
    val brokers: List<BrokerMetadata>,
    val topics: List<TopicMetadata>
) {
    fun topic(name: String): TopicMetadata? = topics.firstOrNull { it.name == name }

    fun broker(id: Int): BrokerMetadata? = brokers.firstOrNull { it.id == id }

    fun encode(out: DataOutput) {
        out.writeInt(controllerId)
        out.writeInt(brokers.size)
        brokers.forEach { it.encode(out) }
        out.writeInt(topics.size)
        topics.forEach { it.encode(out) }
    }
    companion object {
        fun decode(input: DataInput): MetadataResponse {
            val controllerId = input.readInt()
            val brokerCount = input.readInt()
            val brokers = (0 until brokerCount).map { BrokerMetadata.decode(input) }
            val count = input.readInt()
            val topics = (0 until count).map { TopicMetadata.decode(input) }
            return MetadataResponse(controllerId, brokers, topics)
        }
    }
}

data class ProduceResponse(val errorCode: Short, val partition: Int, val offset: Long) {
    fun encode(out: DataOutput) {
        out.writeShort(errorCode.toInt())
        out.writeInt(partition)
        out.writeLong(offset)
    }
    companion object {
        fun decode(input: DataInput): ProduceResponse {
            val errorCode = input.readShort()
            val partition = input.readInt()
            val offset = input.readLong()
            return ProduceResponse(errorCode, partition, offset)
        }
    }
}

/**
 * One record on the wire. [crc] is the record's CRC32 over `leaderEpoch | timestamp | key | value`
 * ([recordCrc], identical to the on-disk CRC). The default recomputes it from the fields — for a
 * record read from the log that equals the stored CRC, since log reads verify it. [decode] verifies
 * it and throws [CorruptRecordException] on a mismatch, so a corrupted response is discarded
 * rather than appended by a follower (R4, D18).
 */
data class FetchedRecord(
    val offset: Long,
    val leaderEpoch: Int,
    val timestamp: Long,
    val key: ByteArray?,
    val value: ByteArray,
    val crc: Int = recordCrc(leaderEpoch, timestamp, key, value)
) {
    fun encode(out: DataOutput) {
        out.writeLong(offset)
        out.writeInt(crc)
        out.writeInt(leaderEpoch)
        out.writeLong(timestamp)
        out.writeNullableBytesAsInt32(key)
        out.writeInt(value.size)
        out.write(value)
    }
    companion object {
        fun decode(input: DataInput): FetchedRecord {
            val offset = input.readLong()
            val crc = input.readInt()
            val leaderEpoch = input.readInt()
            val timestamp = input.readLong()
            val key = input.readNullableBytesAsInt32()
            val valueLength = input.readInt()
            val value = ByteArray(valueLength)
            input.readFully(value)
            val actual = recordCrc(leaderEpoch, timestamp, key, value)
            if (actual != crc) {
                throw CorruptRecordException("corrupt fetched record at offset $offset: expected crc $crc, computed $actual")
            }
            return FetchedRecord(offset, leaderEpoch, timestamp, key, value, crc)
        }
    }
}

/** [highWatermark] is -1 when the broker could not serve the partition (errors other than OFFSET_OUT_OF_RANGE). */
data class FetchResponse(val errorCode: Short, val highWatermark: Long, val records: List<FetchedRecord>) {
    fun encode(out: DataOutput) {
        out.writeShort(errorCode.toInt())
        out.writeLong(highWatermark)
        out.writeInt(records.size)
        records.forEach { it.encode(out) }
    }
    companion object {
        fun decode(input: DataInput): FetchResponse {
            val errorCode = input.readShort()
            val highWatermark = input.readLong()
            val count = input.readInt()
            val records = (0 until count).map { FetchedRecord.decode(input) }
            return FetchResponse(errorCode, highWatermark, records)
        }
    }
}

data class OffsetCommitResponse(val errorCode: Short) {
    fun encode(out: DataOutput) { out.writeShort(errorCode.toInt()) }
    companion object {
        fun decode(input: DataInput): OffsetCommitResponse = OffsetCommitResponse(input.readShort())
    }
}

data class OffsetFetchResponse(val errorCode: Short, val offset: Long) {
    fun encode(out: DataOutput) {
        out.writeShort(errorCode.toInt())
        out.writeLong(offset)
    }
    companion object {
        fun decode(input: DataInput): OffsetFetchResponse {
            val errorCode = input.readShort()
            val offset = input.readLong()
            return OffsetFetchResponse(errorCode, offset)
        }
    }
}

data class LeaderAndIsrResponse(val errorCode: Short) {
    fun encode(out: DataOutput) { out.writeShort(errorCode.toInt()) }
    companion object {
        fun decode(input: DataInput): LeaderAndIsrResponse = LeaderAndIsrResponse(input.readShort())
    }
}

data class OffsetsForLeaderEpochResponse(val errorCode: Short, val leaderEpoch: Int, val endOffset: Long) {
    fun encode(out: DataOutput) {
        out.writeShort(errorCode.toInt())
        out.writeInt(leaderEpoch)
        out.writeLong(endOffset)
    }
    companion object {
        fun decode(input: DataInput): OffsetsForLeaderEpochResponse {
            val errorCode = input.readShort()
            val leaderEpoch = input.readInt()
            val endOffset = input.readLong()
            return OffsetsForLeaderEpochResponse(errorCode, leaderEpoch, endOffset)
        }
    }
}
