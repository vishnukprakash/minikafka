package minikafka.zk

import minikafka.testing.EmbeddedZk
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.RetryOneTime
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import java.nio.charset.StandardCharsets

/**
 * Task 1 smoke test: proves Curator + an in-process ZooKeeper server work on this JDK. Originally
 * on Curator's raw `TestingServer`; now on [EmbeddedZk] (the hardened server every other ZooKeeper
 * test uses, tickTime 200ms), so this test cannot hit the port-shadowing flake either.
 */
@Tag("zk")
class CuratorSmokeTest {

    @Test
    fun `embedded ZooKeeper starts, Curator client connects, and negotiated session timeout matches request`() {
        val requestedSessionTimeoutMs = 3000

        EmbeddedZk().use { zkServer ->
            val client = CuratorFrameworkFactory.builder()
                .connectString(zkServer.connectString)
                .retryPolicy(RetryOneTime(100))
                .sessionTimeoutMs(requestedSessionTimeoutMs)
                .build()
            client.use {
                client.start()
                client.blockUntilConnected()

                val path = "/minikafka-smoke-test"
                val payload = "hello-zk".toByteArray(StandardCharsets.UTF_8)
                client.create().forPath(path, payload)

                val readBack = client.data.forPath(path)
                assertEquals("hello-zk", String(readBack, StandardCharsets.UTF_8))

                val negotiatedSessionTimeout = client.zookeeperClient.zooKeeper.sessionTimeout
                assertEquals(requestedSessionTimeoutMs, negotiatedSessionTimeout)
            }
        }
    }
}
