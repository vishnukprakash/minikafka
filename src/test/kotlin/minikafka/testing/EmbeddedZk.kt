package minikafka.testing

import org.apache.curator.test.InstanceSpec
import org.apache.curator.test.TestingServer
import org.apache.zookeeper.Watcher
import org.apache.zookeeper.ZooKeeper
import org.slf4j.LoggerFactory
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/**
 * An in-process single-node ZooKeeper (Curator `TestingServer`) with a short tickTime (200ms), so
 * test sessions can be as short as 400ms; the maximum session timeout is raised to 60s. [stop] +
 * [restart] keeps the port and the data, so clients reconnect to the same address (ZooKeeper
 * outage scenarios).
 *
 * Hardened against start-up flakes: the per-IP connection limit is disabled (`maxClientCnxns=0`;
 * every test client, broker and proxy connects from 127.0.0.1), and the constructor only returns
 * once a probe client has opened (and closed) a real session. A server that does not serve within
 * [PROBE_TIMEOUT_MS] is closed and replaced by one on a fresh port (at most [START_ATTEMPTS] times).
 */
class EmbeddedZk(tickTimeMs: Int = 200) : AutoCloseable {
    private val server: TestingServer = startServing(tickTimeMs)

    /** `host:port` — no chroot. */
    val connectString: String get() = server.connectString

    val port: Int get() = server.port

    fun stop() = server.stop()

    fun restart() = server.restart()

    override fun close() = server.close()

    private companion object {
        const val START_ATTEMPTS = 3
        const val PROBE_TIMEOUT_MS = 5_000L
        private val log = LoggerFactory.getLogger(EmbeddedZk::class.java)

        fun startServing(tickTimeMs: Int): TestingServer {
            var last: Exception? = null
            repeat(START_ATTEMPTS) { attempt ->
                val spec = InstanceSpec(
                    null, -1, -1, -1, true, -1, tickTimeMs, 0,
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
