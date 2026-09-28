package minikafka.testing

import org.apache.curator.test.InstanceSpec
import org.apache.curator.test.TestingServer
import org.apache.zookeeper.Watcher
import org.apache.zookeeper.ZooKeeper
import org.slf4j.LoggerFactory
import java.io.IOException
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/**
 * An in-process single-node ZooKeeper (Curator `TestingServer`) with a short tickTime (200ms), so
 * test sessions can be as short as 400ms; the maximum session timeout is raised to 60s. [stop] +
 * [restart] keeps the port and the data, so clients reconnect to the same address (ZooKeeper
 * outage scenarios).
 *
 * **Hardened against the start-up flake** seen in Tasks 10/11 ("could not connect to ZooKeeper",
 * every connection closed or reset). Root cause: `InstanceSpec` picks the port by binding and
 * closing a wildcard socket, and ZooKeeper binds `*:port` explicitly only later. If in that gap any
 * process (another test JVM, a proxy, …) binds `127.0.0.1:0` and gets that port, ZooKeeper's
 * wildcard bind still succeeds (SO_REUSEADDR), but every connection to `127.0.0.1:port` — the
 * address all clients use — goes to the more specific listener instead. So after starting, the
 * constructor checks that no other socket holds `127.0.0.1:port` ([isShadowed]) and that a probe
 * client can open a real session; otherwise the server is closed and replaced by one on a fresh
 * port (at most [START_ATTEMPTS] times). The per-IP connection limit is disabled too
 * (`maxClientCnxns=0`): every test client, broker and proxy connects from 127.0.0.1.
 *
 * @param firstPort the port of the first attempt (-1 = random); for tests of this class.
 */
class EmbeddedZk(tickTimeMs: Int = 200, firstPort: Int = -1) : AutoCloseable {
    private val server: TestingServer = startServing(tickTimeMs, firstPort)

    /** `host:port` — no chroot. */
    val connectString: String get() = server.connectString

    val port: Int get() = server.port

    fun stop() = server.stop()

    fun restart() = server.restart()

    override fun close() = server.close()

    internal companion object {
        const val START_ATTEMPTS = 3
        const val PROBE_TIMEOUT_MS = 5_000L
        private val log = LoggerFactory.getLogger(EmbeddedZk::class.java)

        private fun startServing(tickTimeMs: Int, firstPort: Int): TestingServer {
            var last: Exception? = null
            repeat(START_ATTEMPTS) { attempt ->
                val spec = InstanceSpec(
                    null, if (attempt == 0) firstPort else -1, -1, -1, true, -1, tickTimeMs, 0,
                    mapOf<String, Any>("maxSessionTimeout" to "60000")
                )
                val server = try {
                    TestingServer(spec, true)
                } catch (e: Exception) {
                    last = e
                    log.warn("EmbeddedZk: start attempt {} failed: {}", attempt + 1, e.toString())
                    return@repeat
                }
                try {
                    check(!isShadowed(server.port)) { "another socket holds 127.0.0.1:${server.port}" }
                    probe(server.connectString)
                    return server
                } catch (e: Exception) {
                    last = e
                    log.warn("EmbeddedZk: server {} does not serve (attempt {}): {}", server.connectString, attempt + 1, e.toString())
                    runCatching { server.close() }
                }
            }
            throw IllegalStateException("could not start a serving ZooKeeper in $START_ATTEMPTS attempts", last)
        }

        /**
         * True if a socket other than our wildcard listener is bound to `127.0.0.1:[port]` (and so
         * receives the connections meant for ZooKeeper): an exact bind of that address then fails,
         * whereas next to only our `*:port` listener it succeeds (and is closed again at once).
         */
        fun isShadowed(port: Int): Boolean = try {
            ServerSocket().use {
                it.reuseAddress = true
                it.bind(InetSocketAddress(InetAddress.getLoopbackAddress(), port))
            }
            false
        } catch (_: IOException) {
            true
        }

        /** Opens a real session (SyncConnected) and reads `/`; throws if that does not happen in time. */
        fun probe(connectString: String) {
            val connected = CountDownLatch(1)
            val zk = ZooKeeper(connectString, 10_000) { event ->
                if (event.state == Watcher.Event.KeeperState.SyncConnected) connected.countDown()
            }
            try {
                check(connected.await(PROBE_TIMEOUT_MS, TimeUnit.MILLISECONDS)) { "no session within ${PROBE_TIMEOUT_MS}ms" }
                checkNotNull(zk.exists("/", false)) { "root znode missing" }
            } finally {
                zk.close()
            }
        }
    }
}
