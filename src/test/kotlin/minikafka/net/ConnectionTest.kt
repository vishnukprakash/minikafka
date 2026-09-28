package minikafka.net

import minikafka.proto.readFrameHeader
import minikafka.proto.writeResponseFrame
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.IOException
import java.net.ServerSocket
import java.net.SocketTimeoutException

class ConnectionTest {
    /** Reads exactly one request frame from [server]'s next accepted connection, then hands it to [respond]. */
    private fun acceptOnce(server: ServerSocket, respond: (correlationId: Int, out: DataOutputStream) -> Unit) {
        Thread {
            server.accept().use { socket ->
                val input = DataInputStream(BufferedInputStream(socket.getInputStream()))
                val output = DataOutputStream(BufferedOutputStream(socket.getOutputStream()))
                val (header, _) = readFrameHeader(input)
                respond(header.correlationId, output)
            }
        }.apply { isDaemon = true; start() }
    }

    @Test
    fun `round trips a request through a real socket`() {
        ServerSocket(0).use { server ->
            acceptOnce(server) { correlationId, output ->
                writeResponseFrame(output, correlationId) { it.writeInt(42) }
            }

            Connection("localhost", server.localPort).use { connection ->
                val result = connection.request(1, { it.writeInt(7) }) { it.readInt() }
                assertEquals(42, result)
            }
        }
    }

    @Test
    fun `throws IOException on correlation id mismatch`() {
        ServerSocket(0).use { server ->
            acceptOnce(server) { correlationId, output ->
                // Respond with a correlation id that does not match what was requested.
                writeResponseFrame(output, correlationId + 1) { it.writeInt(0) }
            }

            Connection("localhost", server.localPort).use { connection ->
                assertThrows(IOException::class.java) {
                    connection.request(1, { it.writeInt(7) }) { it.readInt() }
                }
            }
        }
    }

    @Test
    fun `read timeout throws SocketTimeoutException within a bounded time`() {
        ServerSocket(0).use { server ->
            // Accept the connection but never respond, so the client's read blocks until the
            // socket timeout fires. No Thread.sleep here: the fake server simply stays silent.
            Thread {
                server.accept()
            }.apply { isDaemon = true; start() }

            Connection("localhost", server.localPort, readTimeoutMs = 200).use { connection ->
                val start = System.nanoTime()
                assertThrows(SocketTimeoutException::class.java) {
                    connection.request(1, { it.writeInt(7) }) { it.readInt() }
                }
                val elapsedMs = (System.nanoTime() - start) / 1_000_000
                assert(elapsedMs < 5_000) { "expected timeout well under 5s, took ${elapsedMs}ms" }
            }
        }
    }
}
