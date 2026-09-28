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

data class LeaderAndIsrPartition(
    val topic: String,
    val partition: Int,
    val leader: Int,
    val leaderEpoch: Int,
    val isr: List<Int>,
    val replicas: List<Int>,
    val zkVersion: Int
) {
    fun encode(out: DataOutput) {
        out.writeNullableString(topic)
        out.writeInt(partition)
        out.writeInt(leader)
        out.writeInt(leaderEpoch)
        out.writeInt(isr.size)
        isr.forEach { out.writeInt(it) }
        out.writeInt(replicas.size)
        replicas.forEach { out.writeInt(it) }
        out.writeInt(zkVersion)
    }
    companion object {
        fun decode(input: DataInput): LeaderAndIsrPartition {
            val topic = input.readNullableString()!!
            val partition = input.readInt()
            val leader = input.readInt()
            val leaderEpoch = input.readInt()
            val isrCount = input.readInt()
            val isr = (0 until isrCount).map { input.readInt() }
            val replicasCount = input.readInt()
            val replicas = (0 until replicasCount).map { input.readInt() }
            val zkVersion = input.readInt()
            return LeaderAndIsrPartition(topic, partition, leader, leaderEpoch, isr, replicas, zkVersion)
        }
    }
}

data class LeaderAndIsrRequest(
    val controllerId: Int,
    val controllerEpoch: Int,
    val brokerEpoch: Long,
    val partitions: List<LeaderAndIsrPartition>
) {
    fun encode(out: DataOutput) {
        out.writeInt(controllerId)
        out.writeInt(controllerEpoch)
        out.writeLong(brokerEpoch)
        out.writeInt(partitions.size)
        partitions.forEach { it.encode(out) }
    }
    companion object {
        fun decode(input: DataInput): LeaderAndIsrRequest {
            val controllerId = input.readInt()
            val controllerEpoch = input.readInt()
            val brokerEpoch = input.readLong()
            val count = input.readInt()
            val partitions = (0 until count).map { LeaderAndIsrPartition.decode(input) }
            return LeaderAndIsrRequest(controllerId, controllerEpoch, brokerEpoch, partitions)
        }
    }
}

data class OffsetsForLeaderEpochRequest(
    val topic: String,
    val partition: Int,
    val replicaId: Int,
    val currentLeaderEpoch: Int,
    val requestedEpoch: Int
) {
    fun encode(out: DataOutput) {
        out.writeNullableString(topic)
        out.writeInt(partition)
        out.writeInt(replicaId)
        out.writeInt(currentLeaderEpoch)
        out.writeInt(requestedEpoch)
    }
    companion object {
        fun decode(input: DataInput): OffsetsForLeaderEpochRequest {
            val topic = input.readNullableString()!!
            val partition = input.readInt()
            val replicaId = input.readInt()
            val currentLeaderEpoch = input.readInt()
            val requestedEpoch = input.readInt()
            return OffsetsForLeaderEpochRequest(topic, partition, replicaId, currentLeaderEpoch, requestedEpoch)
        }
    }
}
