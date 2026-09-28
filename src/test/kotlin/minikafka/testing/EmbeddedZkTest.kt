package minikafka.testing

import org.apache.zookeeper.ZooKeeper
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/** The start-up/restart hardening of [EmbeddedZk] (the Task 10/11 "could not connect to ZooKeeper" flake). */
@Tag("zk")
class EmbeddedZkTest {
    @Test
    fun `a port squatted on 127_0_0_1 fails the bind and ZooKeeper is started on another one`() {
        // What the flake looked like: in the gap between InstanceSpec choosing a port and ZooKeeper
        // binding it, another socket took 127.0.0.1:<port>. A wildcard bind used to succeed anyway
        // and be shadowed; the explicit loopback bind now fails and the next attempt uses a new port.
        ServerSocket(0, 50, InetAddress.getLoopbackAddress()).use { squatter ->
            val taken = squatter.localPort
            EmbeddedZk(firstPort = taken).use { zk ->
                assertNotEquals(taken, zk.port, "the squatted port was given up")
                assertServes(zk)
            }
        }
    }

    @Test
    fun `ZooKeeper holds the exact loopback address, so a later squatter cannot shadow it`() {
        EmbeddedZk().use { zk ->
            assertTrue(EmbeddedZk.isLoopbackPortHeld(zk.port), "bound to 127.0.0.1 explicitly")
            // A wildcard listener on the same port is less specific: clients of 127.0.0.1 still reach ZooKeeper.
            val wildcard = runCatching {
                ServerSocket().apply { reuseAddress = true; bind(InetSocketAddress(zk.port)) }
            }.getOrNull()
            try {
                assertServes(zk)
            } finally {
                wildcard?.close()
            }
        }
    }

    @Test
    fun `restart on a port taken meanwhile fails loudly instead of being shadowed`() {
        EmbeddedZk().use { zk ->
            val port = zk.port
            zk.stop()
            val squatter = ServerSocket(port, 50, InetAddress.getLoopbackAddress())
            try {
                assertThrows(Exception::class.java) { zk.restart() }
            } finally {
                squatter.close()
            }
            assertDoesNotThrow { zk.restart() } // the port is free again: same port, serving
            assertEquals(port, zk.port)
            assertServes(zk)
        }
    }

    private fun assertServes(zk: EmbeddedZk) {
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
