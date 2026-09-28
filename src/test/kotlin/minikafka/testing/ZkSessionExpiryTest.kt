package minikafka.testing

import org.apache.zookeeper.Watcher
import org.apache.zookeeper.ZooKeeper
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

@Tag("zk")
class ZkSessionExpiryTest {
    @Test
    fun `expiring a session expires it, and expiring an already-expired session is a no-op`() {
        EmbeddedZk().use { zk ->
            val connected = CountDownLatch(1)
            val expired = CountDownLatch(1)
            val victim = ZooKeeper(zk.connectString, 3_000) { event ->
                when (event.state) {
                    Watcher.Event.KeeperState.SyncConnected -> connected.countDown()
                    Watcher.Event.KeeperState.Expired -> expired.countDown()
                    else -> {}
                }
            }
            try {
                assertTrue(connected.await(5, TimeUnit.SECONDS))
                val id = victim.sessionId
                val password = victim.sessionPasswd
                expireZkSession(zk.connectString, id, password)
                assertTrue(expired.await(10, TimeUnit.SECONDS), "the victim learns its session expired")
                // The nemesis race: a second expiry of the same (dead) session used to throw
                // "could not attach to session"; the session is already gone, which is the goal.
                assertDoesNotThrow { expireZkSession(zk.connectString, id, password, timeoutMs = 5_000) }
            } finally {
                victim.close()
            }
        }
    }
}
