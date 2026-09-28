package minikafka.cluster

import minikafka.model.BrokerInfo
import minikafka.proto.ApiKeys
import minikafka.proto.ErrorCodes
import minikafka.proto.LeaderAndIsrPartition
import minikafka.proto.LeaderAndIsrRequest
import minikafka.proto.LeaderAndIsrResponse
import minikafka.proto.readFrameHeader
import minikafka.proto.writeResponseFrame
import minikafka.testing.alwaysFor
import minikafka.testing.eventually
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.IOException
import java.net.InetAddress
import java.net.ServerSocket
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.time.Duration.Companion.milliseconds

class ControllerChannelTest {
    /** A fake broker: records every LeaderAndIsr and answers with [answer] (called with the 0-based request index). */
    private class FakeBroker(private val answer: (Int) -> Short) : AutoCloseable {
        private val server = ServerSocket(0, 50, InetAddress.getLoopbackAddress())
        val received = CopyOnWriteArrayList<LeaderAndIsrRequest>()
        val port = server.localPort

        init {
            Thread({
                try {
                    while (true) {
                        val socket = server.accept()
                        Thread({
                            try {
                                val input = DataInputStream(socket.getInputStream())
                                val output = DataOutputStream(socket.getOutputStream())
                                while (true) {
                                    val (header, body) = readFrameHeader(input)
                                    check(header.apiKey == ApiKeys.LEADER_AND_ISR)
                                    val request = LeaderAndIsrRequest.decode(body)
                                    val code = answer(received.size)
                                    received += request
                                    writeResponseFrame(output, header.correlationId) { LeaderAndIsrResponse(code).encode(it) }
                                }
                            } catch (_: IOException) {
                            } finally {
                                socket.close()
                            }
                        }, "fake-broker-conn").apply { isDaemon = true }.start()
                    }
                } catch (_: IOException) {
                }
            }, "fake-broker-accept").apply { isDaemon = true }.start()
        }

        override fun close() = server.close()
    }

    private val closeables = mutableListOf<AutoCloseable>()

    @AfterEach
    fun cleanup() = closeables.reversed().forEach { runCatching { it.close() } }

    private val partition = LeaderAndIsrPartition("t", 0, 2, 3, listOf(2), listOf(2), 4)

    @Test
    fun `a LeaderAndIsr rejected with STALE_BROKER_EPOCH is redelivered once the broker knows its epoch (R8)`() {
        // First attempt races the broker's registration: it has not recorded epoch 42 yet.
        val broker = FakeBroker { index -> if (index == 0) ErrorCodes.STALE_BROKER_EPOCH else ErrorCodes.NONE }
        closeables += broker
        val channel = ControllerChannel(myBrokerId = 1, socketTimeoutMs = 5_000, retryBackoffMs = 20).also { closeables += it }
        channel.addBroker(BrokerInfo(2, "127.0.0.1", broker.port), brokerEpoch = 42)

        assertTrue(channel.send(2, controllerEpoch = 7, partitions = listOf(partition)))
        eventually { assertEquals(2, broker.received.size) }
        assertTrue(broker.received.all { it == LeaderAndIsrRequest(1, 7, 42, listOf(partition)) })

        // Delivered: the next request is sent exactly once.
        channel.send(2, 7, emptyList())
        eventually { assertEquals(3, broker.received.size) }
        alwaysFor(200.milliseconds) { assertEquals(3, broker.received.size) }
    }

    @Test
    fun `retrying a stale-epoch request stops when the broker's sender is removed`() {
        val broker = FakeBroker { ErrorCodes.STALE_BROKER_EPOCH }
        closeables += broker
        val channel = ControllerChannel(myBrokerId = 1, socketTimeoutMs = 5_000, retryBackoffMs = 20).also { closeables += it }
        channel.addBroker(BrokerInfo(2, "127.0.0.1", broker.port), brokerEpoch = 42)
        channel.send(2, 7, listOf(partition))
        eventually { assertTrue(broker.received.size >= 2) }

        channel.removeBroker(2) // e.g. the controller saw the broker bounce
        val afterRemoval = broker.received.size
        alwaysFor(500.milliseconds) { assertTrue(broker.received.size <= afterRemoval + 1) }
    }

    @Test
    fun `other errors are not retried`() {
        val broker = FakeBroker { ErrorCodes.STALE_CONTROLLER_EPOCH }
        closeables += broker
        val channel = ControllerChannel(myBrokerId = 1, socketTimeoutMs = 5_000, retryBackoffMs = 20).also { closeables += it }
        channel.addBroker(BrokerInfo(2, "127.0.0.1", broker.port), brokerEpoch = 42)
        channel.send(2, 7, listOf(partition))
        eventually { assertEquals(1, broker.received.size) }
        alwaysFor(200.milliseconds) { assertEquals(1, broker.received.size) }
    }
}
