package minikafka.client

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
import minikafka.proto.readResponseFrame
import minikafka.proto.writeFrame
import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.Closeable
import java.io.DataInput
import java.io.DataInputStream
import java.io.DataOutput
import java.io.DataOutputStream
import java.net.Socket
import java.util.concurrent.atomic.AtomicInteger

class MiniKafkaClient(host: String, port: Int) : Closeable {
    private val socket = Socket(host, port)
    private val input = DataInputStream(BufferedInputStream(socket.getInputStream()))
    private val output = DataOutputStream(BufferedOutputStream(socket.getOutputStream()))
    private val correlationIds = AtomicInteger(0)

    private fun <T> request(apiKey: Short, encodeBody: (DataOutput) -> Unit, decodeResponse: (DataInput) -> T): T {
        val correlationId = correlationIds.getAndIncrement()
        writeFrame(output, apiKey, correlationId, encodeBody)
        val (responseCorrelationId, body) = readResponseFrame(input)
        check(responseCorrelationId == correlationId) {
            "correlation id mismatch: expected $correlationId, got $responseCorrelationId"
        }
        return decodeResponse(body)
    }

    fun createTopic(topic: String, numPartitions: Int): Short =
        request(ApiKeys.CREATE_TOPIC, { CreateTopicRequest(topic, numPartitions).encode(it) }) {
            CreateTopicResponse.decode(it).errorCode
        }

    fun metadata(): List<TopicMetadata> =
        request(ApiKeys.METADATA, { MetadataRequest().encode(it) }) {
            MetadataResponse.decode(it).topics
        }

    fun produce(topic: String, key: ByteArray?, value: ByteArray): ProduceResponse =
        request(ApiKeys.PRODUCE, { ProduceRequest(topic, key, value).encode(it) }) {
            ProduceResponse.decode(it)
        }

    fun fetch(topic: String, partition: Int, offset: Long, maxBytes: Int = 1024 * 1024): FetchResponse =
        request(ApiKeys.FETCH, { FetchRequest(topic, partition, offset, maxBytes).encode(it) }) {
            FetchResponse.decode(it)
        }

    fun commitOffset(group: String, topic: String, partition: Int, offset: Long): Short =
        request(ApiKeys.OFFSET_COMMIT, { OffsetCommitRequest(group, topic, partition, offset).encode(it) }) {
            OffsetCommitResponse.decode(it).errorCode
        }

    fun fetchOffset(group: String, topic: String, partition: Int): Long =
        request(ApiKeys.OFFSET_FETCH, { OffsetFetchRequest(group, topic, partition).encode(it) }) {
            OffsetFetchResponse.decode(it).offset
        }

    override fun close() {
        socket.close()
    }
}
