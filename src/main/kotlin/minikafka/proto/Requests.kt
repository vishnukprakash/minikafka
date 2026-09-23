package minikafka.proto

import minikafka.io.readNullableBytesAsInt32
import minikafka.io.readNullableString
import minikafka.io.writeNullableBytesAsInt32
import minikafka.io.writeNullableString
import java.io.DataInput
import java.io.DataOutput

data class CreateTopicRequest(val topic: String, val numPartitions: Int) {
    fun encode(out: DataOutput) {
        out.writeNullableString(topic)
        out.writeInt(numPartitions)
    }
    companion object {
        fun decode(input: DataInput): CreateTopicRequest {
            val topic = input.readNullableString()!!
            val numPartitions = input.readInt()
            return CreateTopicRequest(topic, numPartitions)
        }
    }
}

class MetadataRequest {
    fun encode(out: DataOutput) {}
    companion object {
        fun decode(input: DataInput): MetadataRequest = MetadataRequest()
    }
}

data class ProduceRequest(val topic: String, val key: ByteArray?, val value: ByteArray) {
    fun encode(out: DataOutput) {
        out.writeNullableString(topic)
        out.writeNullableBytesAsInt32(key)
        out.writeInt(value.size)
        out.write(value)
    }
    companion object {
        fun decode(input: DataInput): ProduceRequest {
            val topic = input.readNullableString()!!
            val key = input.readNullableBytesAsInt32()
            val valueLength = input.readInt()
            val value = ByteArray(valueLength)
            input.readFully(value)
            return ProduceRequest(topic, key, value)
        }
    }
}

data class FetchRequest(val topic: String, val partition: Int, val offset: Long, val maxBytes: Int) {
    fun encode(out: DataOutput) {
        out.writeNullableString(topic)
        out.writeInt(partition)
        out.writeLong(offset)
        out.writeInt(maxBytes)
    }
    companion object {
        fun decode(input: DataInput): FetchRequest {
            val topic = input.readNullableString()!!
            val partition = input.readInt()
            val offset = input.readLong()
            val maxBytes = input.readInt()
            return FetchRequest(topic, partition, offset, maxBytes)
        }
    }
}

data class OffsetCommitRequest(val group: String, val topic: String, val partition: Int, val offset: Long) {
    fun encode(out: DataOutput) {
        out.writeNullableString(group)
        out.writeNullableString(topic)
        out.writeInt(partition)
        out.writeLong(offset)
    }
    companion object {
        fun decode(input: DataInput): OffsetCommitRequest {
            val group = input.readNullableString()!!
            val topic = input.readNullableString()!!
            val partition = input.readInt()
            val offset = input.readLong()
            return OffsetCommitRequest(group, topic, partition, offset)
        }
    }
}

data class OffsetFetchRequest(val group: String, val topic: String, val partition: Int) {
    fun encode(out: DataOutput) {
        out.writeNullableString(group)
        out.writeNullableString(topic)
        out.writeInt(partition)
    }
    companion object {
        fun decode(input: DataInput): OffsetFetchRequest {
            val group = input.readNullableString()!!
            val topic = input.readNullableString()!!
            val partition = input.readInt()
            return OffsetFetchRequest(group, topic, partition)
        }
    }
}
