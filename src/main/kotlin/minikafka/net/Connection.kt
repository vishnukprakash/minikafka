package minikafka.net

import minikafka.proto.readResponseFrame
import minikafka.proto.writeFrame
import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.Closeable
import java.io.DataInput
import java.io.DataInputStream
import java.io.DataOutput
import java.io.DataOutputStream
import java.io.IOException
import java.net.InetSocketAddress
import java.net.Socket
import java.util.concurrent.atomic.AtomicInteger

/**
 * One TCP connection to a broker, speaking minikafka's length-prefixed request/response framing.
 *
 * Holds a single socket; not safe for concurrent use by multiple threads. `readTimeoutMs` bounds
 * how long [request] will block waiting for a response (via `Socket.soTimeout`) — a slow or dead
 * peer causes a [java.net.SocketTimeoutException] rather than blocking forever — and also bounds
 * the TCP connect.
 *
 * **Never reuse a Connection after [request] throws.** A timeout or I/O error can leave a partial
 * frame or a late response on the stream, so the next request would read the wrong bytes (or fail
 * the correlation-id check). Callers must [close] it and open a new one.
 */
class Connection(host: String, port: Int, readTimeoutMs: Int = DEFAULT_READ_TIMEOUT_MS) : Closeable {
    private val socket = Socket().apply {
        try {
            connect(InetSocketAddress(host, port), readTimeoutMs)
            soTimeout = readTimeoutMs
            tcpNoDelay = true
        } catch (e: IOException) {
            close()
            throw e
        }
    }
    private val input = DataInputStream(BufferedInputStream(socket.getInputStream()))
    private val output = DataOutputStream(BufferedOutputStream(socket.getOutputStream()))
    private val correlationIds = AtomicInteger(0)

    fun <T> request(apiKey: Short, encodeBody: (DataOutput) -> Unit, decodeResponse: (DataInput) -> T): T {
        val correlationId = correlationIds.getAndIncrement()
        writeFrame(output, apiKey, correlationId, encodeBody)
        val (responseCorrelationId, body) = readResponseFrame(input)
        if (responseCorrelationId != correlationId) {
            throw IOException(
                "correlation id mismatch: expected $correlationId, got $responseCorrelationId"
            )
        }
        return decodeResponse(body)
    }

    override fun close() {
        socket.close()
    }

    companion object {
        const val DEFAULT_READ_TIMEOUT_MS = 40_000
    }
}
