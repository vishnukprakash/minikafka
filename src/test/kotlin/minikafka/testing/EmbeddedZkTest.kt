package minikafka.testing

import org.apache.zookeeper.ZooKeeper
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import java.net.InetAddress
import java.net.ServerSocket
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/** The start-up hardening of [EmbeddedZk] (the Task 10/11 "could not connect to ZooKeeper" flake). */
@Tag("zk")
class EmbeddedZkTest {
    @Test
    fun `a port shadowed by a loopback listener is detected and ZooKeeper is started on another one`() {
        // What the flake looked like: in the gap between InstanceSpec choosing a port and ZooKeeper
        // binding it, another socket took 127.0.0.1:<port>. ZooKeeper's wildcard bind still
        // succeeds, but every client connection to 127.0.0.1:<port> reaches the other listener.
        ServerSocket(0, 50, InetAddress.getLoopbackAddress()).use { squatter ->
            val taken = squatter.localPort
            EmbeddedZk(firstPort = taken).use { zk ->
                assertNotEquals(taken, zk.port, "the shadowed port was given up")
                assertFalse(EmbeddedZk.isShadowed(zk.port))
                val connected = CountDownLatch(1)
                val client = ZooKeeper(zk.connectString, 5_000) { connected.countDown() }
                try {
                    assertTrue(connected.await(5, TimeUnit.SECONDS))
                    assertNotNull(client.exists("/", false))
                } finally {
                    client.close()
                }
            }
        }
    }

    @Test
    fun `an unshadowed server is not flagged`() {
        EmbeddedZk().use { zk -> assertFalse(EmbeddedZk.isShadowed(zk.port)) }
    }
}
