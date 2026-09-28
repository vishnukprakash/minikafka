package minikafka.testing

import org.apache.zookeeper.Watcher
import org.apache.zookeeper.ZooKeeper
import org.apache.zookeeper.server.embedded.ExitHandler
import org.apache.zookeeper.server.embedded.ZooKeeperServerEmbedded
import org.slf4j.LoggerFactory
import java.io.File
import java.io.IOException
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.nio.file.Files
import java.util.Properties
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/**
 * An in-process single-node ZooKeeper (`ZooKeeperServerEmbedded`, standalone) with a short
 * tickTime (200ms), so test sessions can be as short as 400ms; the maximum session timeout is
 * raised to 60s. [stop] + [restart] keeps the port and the data, so clients reconnect to the same
 * address (ZooKeeper outage scenarios).
 *
 * **Hardened against the start-up flake** seen in Tasks 10/11 ("could not connect to ZooKeeper",
 * every connection closed or reset). Root cause: the port is chosen by binding and closing a
 * socket, and ZooKeeper used to bind `*:port` only later. If in that gap any process (another test
 * JVM, a proxy, …) bound `127.0.0.1:0` and got that port, ZooKeeper's wildcard bind still
 * succeeded (SO_REUSEADDR), but every connection to `127.0.0.1:port` — the address all clients
 * use — went to the more specific listener instead.
 *
 * Now ZooKeeper binds **`127.0.0.1:port` explicitly** (`clientPortAddress`), the exact address
 * clients use: a squatter on that address makes the bind fail loudly, and a later wildcard
 * squatter is less specific, so it never receives our clients' connections. A start that fails
 * (bind or session probe) is closed and retried on a fresh port, at most [START_ATTEMPTS] times.
 * [restart] cannot change port, so it fails loudly instead: it throws if the port is held, if the
 * server does not come up, or if it cannot open a probe session.
 *
 * Built directly on `ZooKeeperServerEmbedded` with `ExitHandler.LOG_ONLY` rather than Curator's
 * `TestingServer`, whose embedded server uses the default `EXIT` handler: a failed bind there calls
 * `System.exit(2)` and kills the whole test JVM. The per-IP connection limit is disabled
 * (`maxClientCnxns=0`): every test client, broker and proxy connects from 127.0.0.1.
 *
 * @param firstPort the port of the first attempt (-1 = random); for tests of this class.
 */
class EmbeddedZk(private val tickTimeMs: Int = 200, firstPort: Int = -1) : AutoCloseable {
    private val dataDir: File = Files.createTempDirectory("embedded-zk").toFile()

    /** The client port (fixed for the lifetime of this object, across [stop]/[restart]). */
    val port: Int

    private var server: ZooKeeperServerEmbedded?

    init {
        var last: Exception? = null
        var started: Pair<Int, ZooKeeperServerEmbedded>? = null
        for (attempt in 0 until START_ATTEMPTS) {
            val candidate = if (attempt == 0 && firstPort > 0) firstPort else freePort()
            try {
                started = candidate to startOn(candidate)
                break
            } catch (e: Exception) {
                last = e
                log.warn("EmbeddedZk: start attempt {} on port {} failed: {}", attempt + 1, candidate, e.toString())
            }
        }
        if (started == null) {
            dataDir.deleteRecursively()
            throw IllegalStateException("could not start a serving ZooKeeper in $START_ATTEMPTS attempts", last)
        }
        port = started.first
        server = started.second
    }

    /** `127.0.0.1:port` — no chroot. */
    val connectString: String get() = "$LOOPBACK:$port"

    /** Stops serving (clients see connection loss); the data survives for [restart]. */
    @Synchronized
    fun stop() {
        server?.close()
        server = null
    }

    /** Restarts on the same port and data; throws if the port was taken meanwhile or the server does not serve. */
    @Synchronized
    fun restart() {
        stop()
        server = startOn(port)
    }

    @Synchronized
    override fun close() {
        stop()
        dataDir.deleteRecursively()
    }

    /** Starts a server on 127.0.0.1:[port] and probes it; on any failure closes it and throws. */
    private fun startOn(port: Int): ZooKeeperServerEmbedded {
        check(!isLoopbackPortHeld(port)) { "another socket holds $LOOPBACK:$port" }
        val props = Properties().apply {
            setProperty("tickTime", tickTimeMs.toString())
            setProperty("minSessionTimeout", (2 * tickTimeMs).toString())
            setProperty("maxSessionTimeout", "60000")
            setProperty("maxClientCnxns", "0")
            setProperty("clientPort", port.toString())
            setProperty("clientPortAddress", LOOPBACK)
            setProperty("dataDir", dataDir.absolutePath)
            setProperty("admin.enableServer", "false")
            setProperty("4lw.commands.whitelist", "*")
        }
        val zk = ZooKeeperServerEmbedded.builder()
            .configuration(props)
            .baseDir(dataDir.toPath())
            .exitHandler(ExitHandler.LOG_ONLY)
            .build()
        try {
            zk.start(START_TIMEOUT_MS)
            probe(connectString(port))
            return zk
        } catch (e: Exception) {
            runCatching { zk.close() }
            throw e
        }
    }

    internal companion object {
        const val START_ATTEMPTS = 3
        const val START_TIMEOUT_MS = 10_000L
        const val PROBE_TIMEOUT_MS = 5_000L
        const val LOOPBACK = "127.0.0.1"
        private val log = LoggerFactory.getLogger(EmbeddedZk::class.java)

        private fun connectString(port: Int) = "$LOOPBACK:$port"

        private fun freePort(): Int = ServerSocket(0, 1, InetAddress.getByName(LOOPBACK)).use { it.localPort }

        /**
         * True if [port] on 127.0.0.1 is held by some socket (an exact bind of that address fails).
         * A running [EmbeddedZk] holds its own port this way, so for its port this is always true.
         */
        fun isLoopbackPortHeld(port: Int): Boolean = try {
            ServerSocket().use {
                it.reuseAddress = true
                it.bind(InetSocketAddress(InetAddress.getByName(LOOPBACK), port))
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
