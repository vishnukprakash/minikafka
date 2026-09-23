package minikafka.proto

import minikafka.io.readNullableBytesAsInt32
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

data class TopicMetadata(val name: String, val numPartitions: Int) {
    fun encode(out: DataOutput) {
        out.writeNullableString(name)
        out.writeInt(numPartitions)
    }
    companion object {
        fun decode(input: DataInput): TopicMetadata {
            val name = input.readNullableString()!!
            val numPartitions = input.readInt()
            return TopicMetadata(name, numPartitions)
        }
    }
}

data class MetadataResponse(val topics: List<TopicMetadata>) {
    fun encode(out: DataOutput) {
        out.writeInt(topics.size)
        topics.forEach { it.encode(out) }
    }
    companion object {
        fun decode(input: DataInput): MetadataResponse {
            val count = input.readInt()
            val topics = (0 until count).map { TopicMetadata.decode(input) }
            return MetadataResponse(topics)
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

data class FetchedRecord(val offset: Long, val timestamp: Long, val key: ByteArray?, val value: ByteArray) {
    fun encode(out: DataOutput) {
        out.writeLong(offset)
        out.writeLong(timestamp)
        out.writeNullableBytesAsInt32(key)
        out.writeInt(value.size)
        out.write(value)
    }
    companion object {
        fun decode(input: DataInput): FetchedRecord {
            val offset = input.readLong()
            val timestamp = input.readLong()
            val key = input.readNullableBytesAsInt32()
            val valueLength = input.readInt()
            val value = ByteArray(valueLength)
            input.readFully(value)
            return FetchedRecord(offset, timestamp, key, value)
        }
    }
}

data class FetchResponse(val errorCode: Short, val records: List<FetchedRecord>) {
    fun encode(out: DataOutput) {
        out.writeShort(errorCode.toInt())
        out.writeInt(records.size)
        records.forEach { it.encode(out) }
    }
    companion object {
        fun decode(input: DataInput): FetchResponse {
            val errorCode = input.readShort()
            val count = input.readInt()
            val records = (0 until count).map { FetchedRecord.decode(input) }
            return FetchResponse(errorCode, records)
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
