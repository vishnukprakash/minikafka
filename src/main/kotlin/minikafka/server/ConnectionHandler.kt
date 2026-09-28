package minikafka.server

import minikafka.proto.ApiKeys
import minikafka.proto.CreateTopicRequest
import minikafka.proto.FetchRequest
import minikafka.proto.LeaderAndIsrRequest
import minikafka.proto.MetadataRequest
import minikafka.proto.OffsetCommitRequest
import minikafka.proto.OffsetFetchRequest
import minikafka.proto.OffsetsForLeaderEpochRequest
import minikafka.proto.ProduceRequest
import minikafka.proto.readFrameHeader
import minikafka.proto.writeResponseFrame
import org.slf4j.LoggerFactory
import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.DataInputStream
import java.io.DataOutput
import java.io.DataOutputStream
import java.io.EOFException
import java.io.IOException
import java.net.Socket

/**
 * Serves one client connection (thread-per-connection): reads a request frame, dispatches on its
 * apiKey to [BrokerApis], writes the response frame, repeats. An unknown apiKey closes the
 * connection, as does any failure while handling a request (e.g. ZooKeeper unreachable) — the
 * client sees an I/O error and retries on a fresh connection.
 */
internal class ConnectionHandler(private val socket: Socket, private val apis: BrokerApis, private val brokerId: Int) {
    private val log = LoggerFactory.getLogger(ConnectionHandler::class.java)

    fun handle() {
        try {
            val input = DataInputStream(BufferedInputStream(socket.getInputStream()))
            val output = DataOutputStream(BufferedOutputStream(socket.getOutputStream()))
            while (true) {
                val (header, body) = readFrameHeader(input)
                val respond: (DataOutput) -> Unit = when (header.apiKey) {
                    ApiKeys.CREATE_TOPIC -> apis.createTopic(CreateTopicRequest.decode(body))::encode
                    ApiKeys.METADATA -> {
                        MetadataRequest.decode(body)
                        apis.metadata()::encode
                    }
                    ApiKeys.PRODUCE -> apis.produce(ProduceRequest.decode(body))::encode
                    ApiKeys.FETCH -> apis.fetch(FetchRequest.decode(body))::encode
                    ApiKeys.OFFSET_COMMIT -> apis.offsetCommit(OffsetCommitRequest.decode(body))::encode
                    ApiKeys.OFFSET_FETCH -> apis.offsetFetch(OffsetFetchRequest.decode(body))::encode
                    ApiKeys.LEADER_AND_ISR -> apis.leaderAndIsr(LeaderAndIsrRequest.decode(body))::encode
                    ApiKeys.OFFSETS_FOR_LEADER_EPOCH -> apis.offsetsForLeaderEpoch(OffsetsForLeaderEpochRequest.decode(body))::encode
                    else -> throw IOException("unknown apiKey: ${header.apiKey}")
                }
                writeResponseFrame(output, header.correlationId, respond)
            }
        } catch (_: EOFException) {
            // client closed the connection
        } catch (e: IOException) {
            log.debug("b{}: connection from {} closed: {}", brokerId, socket.remoteSocketAddress, e.toString())
        } catch (e: Exception) {
            log.warn("b{}: request from {} failed; closing the connection: {}", brokerId, socket.remoteSocketAddress, e.toString())
        } finally {
            runCatching { socket.close() }
        }
    }
}
