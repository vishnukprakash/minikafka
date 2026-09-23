package minikafka.server

import minikafka.broker.Broker
import minikafka.proto.ApiKeys
import minikafka.proto.CreateTopicRequest
import minikafka.proto.CreateTopicResponse
import minikafka.proto.FetchRequest
import minikafka.proto.FetchResponse
import minikafka.proto.FetchedRecord
import minikafka.proto.MetadataRequest
import minikafka.proto.MetadataResponse
import minikafka.proto.OffsetCommitRequest
import minikafka.proto.OffsetCommitResponse
import minikafka.proto.OffsetFetchRequest
import minikafka.proto.OffsetFetchResponse
import minikafka.proto.ProduceRequest
import minikafka.proto.ProduceResponse
import minikafka.proto.readFrameHeader
import minikafka.proto.writeResponseFrame
import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.EOFException
import java.io.IOException
import java.net.Socket

class ConnectionHandler(private val socket: Socket, private val broker: Broker) {
    fun handle() {
        val input = DataInputStream(BufferedInputStream(socket.getInputStream()))
        val output = DataOutputStream(BufferedOutputStream(socket.getOutputStream()))
        try {
            while (true) {
                val (header, body) = readFrameHeader(input)
                when (header.apiKey) {
                    ApiKeys.CREATE_TOPIC -> {
                        val request = CreateTopicRequest.decode(body)
                        val errorCode = broker.createTopic(request.topic, request.numPartitions)
                        writeResponseFrame(output, header.correlationId) { out ->
                            CreateTopicResponse(errorCode).encode(out)
                        }
                    }
                    ApiKeys.METADATA -> {
                        MetadataRequest.decode(body)
                        val topics = broker.listTopics()
                        writeResponseFrame(output, header.correlationId) { out ->
                            MetadataResponse(topics).encode(out)
                        }
                    }
                    ApiKeys.PRODUCE -> {
                        val request = ProduceRequest.decode(body)
                        val (errorCode, partition, offset) = broker.produce(request.topic, request.key, request.value)
                        writeResponseFrame(output, header.correlationId) { out ->
                            ProduceResponse(errorCode, partition, offset).encode(out)
                        }
                    }
                    ApiKeys.FETCH -> {
                        val request = FetchRequest.decode(body)
                        val (errorCode, records) = broker.fetch(request.topic, request.partition, request.offset, request.maxBytes)
                        val fetched = records.map { FetchedRecord(it.offset, it.timestamp, it.key, it.value) }
                        writeResponseFrame(output, header.correlationId) { out ->
                            FetchResponse(errorCode, fetched).encode(out)
                        }
                    }
                    ApiKeys.OFFSET_COMMIT -> {
                        val request = OffsetCommitRequest.decode(body)
                        val errorCode = broker.commitOffset(request.group, request.topic, request.partition, request.offset)
                        writeResponseFrame(output, header.correlationId) { out ->
                            OffsetCommitResponse(errorCode).encode(out)
                        }
                    }
                    ApiKeys.OFFSET_FETCH -> {
                        val request = OffsetFetchRequest.decode(body)
                        val (errorCode, offset) = broker.fetchOffset(request.group, request.topic, request.partition)
                        writeResponseFrame(output, header.correlationId) { out ->
                            OffsetFetchResponse(errorCode, offset).encode(out)
                        }
                    }
                    else -> throw IOException("unknown apiKey: ${header.apiKey}")
                }
            }
        } catch (e: EOFException) {
            // client closed the connection
        } catch (e: IOException) {
            // connection error; drop silently for a mini broker
        } finally {
            socket.close()
        }
    }
}
