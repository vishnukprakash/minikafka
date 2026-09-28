package minikafka.testing

import java.io.IOException
import java.net.InetAddress
import java.net.ServerSocket
import java.net.Socket
import java.net.SocketException
import java.util.concurrent.ConcurrentHashMap

/**
 * A byte-pumping TCP proxy on 127.0.0.1 in front of [targetHost]:[targetPort] — in [TestCluster],
 * one sits between each broker's ZooKeeper client and ZooKeeper, so a test can cut that one link.
 *
 * [cut] closes every proxied connection and, until [heal], closes new ones as soon as they are
 * accepted: the client sees connection loss and, if the cut lasts longer than its session
 * timeout, ZooKeeper expires the session (it receives no heartbeats). Threads are daemons.
 */
class TcpProxy(private val targetHost: String, private val targetPort: Int, private val name: String = "tcp-proxy") : AutoCloseable {
    private val serverSocket = ServerSocket(0, 50, InetAddress.getLoopbackAddress())
    private val open = ConcurrentHashMap.newKeySet<Socket>()

    @Volatile
    private var isCut = false

    @Volatile
    private var closed = false

    val port: Int = serverSocket.localPort

    init {
        Thread(::acceptLoop, "$name-accept").apply { isDaemon = true }.start()
    }

    fun cut() {
        isCut = true
        closeAll()
    }

    fun heal() {
        isCut = false
    }

    fun isCut(): Boolean = isCut

    override fun close() {
        closed = true
        runCatching { serverSocket.close() }
        closeAll()
    }

    private fun closeAll() {
        open.toList().forEach { runCatching { it.close() } }
        open.clear()
    }

    private fun acceptLoop() {
        while (!closed) {
            val client = try {
                serverSocket.accept()
            } catch (_: SocketException) {
                return
            }
            if (isCut) {
                runCatching { client.close() }
                continue
            }
            val upstream = try {
                Socket(targetHost, targetPort)
            } catch (_: IOException) {
                runCatching { client.close() }
                continue
            }
            open += client
            open += upstream
            if (isCut) { // cut raced with the accept
                runCatching { client.close() }
                runCatching { upstream.close() }
                continue
            }
            pump(client, upstream, "$name-up")
            pump(upstream, client, "$name-down")
        }
    }

    private fun pump(from: Socket, to: Socket, threadName: String) {
        Thread({
            val buffer = ByteArray(8192)
            try {
                val input = from.getInputStream()
                val output = to.getOutputStream()
                while (true) {
                    val n = input.read(buffer)
                    if (n < 0) break
                    output.write(buffer, 0, n)
                    output.flush()
                }
            } catch (_: IOException) {
            } finally {
                runCatching { from.close() }
                runCatching { to.close() }
                open -= from
                open -= to
            }
        }, threadName).apply { isDaemon = true }.start()
    }
}
