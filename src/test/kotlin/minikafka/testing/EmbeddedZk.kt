package minikafka.testing

import org.apache.curator.test.InstanceSpec
import org.apache.curator.test.TestingServer

/**
 * An in-process single-node ZooKeeper (Curator `TestingServer`) with a short tickTime (200ms), so
 * test sessions can be as short as 400ms–4s. [stop] + [restart] keeps the port and the data, so
 * clients reconnect to the same address (ZooKeeper outage scenarios).
 */
class EmbeddedZk(tickTimeMs: Int = 200) : AutoCloseable {
    private val server = TestingServer(InstanceSpec(null, -1, -1, -1, true, -1, tickTimeMs, -1), true)

    /** `127.0.0.1:<port>` — no chroot. */
    val connectString: String get() = server.connectString

    val port: Int get() = server.port

    fun stop() = server.stop()

    fun restart() = server.restart()

    override fun close() = server.close()
}
