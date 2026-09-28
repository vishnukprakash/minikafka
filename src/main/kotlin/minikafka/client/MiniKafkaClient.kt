package minikafka.client

import minikafka.net.Connection
import minikafka.proto.ApiKeys
import minikafka.proto.CreateTopicRequest
import minikafka.proto.CreateTopicResponse
import minikafka.proto.FetchRequest
import minikafka.proto.FetchResponse
import minikafka.proto.MetadataRequest
import minikafka.proto.MetadataResponse
import minikafka.proto.OffsetCommitRequest
import minikafka.proto.OffsetCommitResponse
import minikafka.proto.OffsetFetchRequest
import minikafka.proto.OffsetFetchResponse
import minikafka.proto.ProduceRequest
import minikafka.proto.ProduceResponse
import minikafka.proto.TopicMetadata
import java.io.Closeable

class MiniKafkaClient(host: String, port: Int) : Closeable {
    private val connection = Connection(host, port)

    fun createTopic(topic: String, numPartitions: Int): Short =
        connection.request(ApiKeys.CREATE_TOPIC, { CreateTopicRequest(topic, numPartitions).encode(it) }) {
            CreateTopicResponse.decode(it).errorCode
        }

    fun metadata(): List<TopicMetadata> =
        connection.request(ApiKeys.METADATA, { MetadataRequest().encode(it) }) {
            MetadataResponse.decode(it).topics
        }

    fun produce(topic: String, key: ByteArray?, value: ByteArray): ProduceResponse =
        connection.request(ApiKeys.PRODUCE, { ProduceRequest(topic, key, value).encode(it) }) {
            ProduceResponse.decode(it)
        }

    fun fetch(topic: String, partition: Int, offset: Long, maxBytes: Int = 1024 * 1024): FetchResponse =
        connection.request(ApiKeys.FETCH, { FetchRequest(topic, partition, offset, maxBytes).encode(it) }) {
            FetchResponse.decode(it)
        }

    fun commitOffset(group: String, topic: String, partition: Int, offset: Long): Short =
        connection.request(ApiKeys.OFFSET_COMMIT, { OffsetCommitRequest(group, topic, partition, offset).encode(it) }) {
            OffsetCommitResponse.decode(it).errorCode
        }

    fun fetchOffset(group: String, topic: String, partition: Int): Long =
        connection.request(ApiKeys.OFFSET_FETCH, { OffsetFetchRequest(group, topic, partition).encode(it) }) {
            OffsetFetchResponse.decode(it).offset
        }

    override fun close() {
        connection.close()
    }
}
