package minikafka.testing

import org.apache.zookeeper.Watcher
import org.apache.zookeeper.ZooKeeper
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/**
 * Deterministically expires a ZooKeeper session **server-side**: opens a second handle on the
 * victim's session (same id + password), waits until it is connected, then closes it. Closing a
 * session deletes its ephemerals atomically; the victim's own client later reconnects, is told
 * its session is expired, and (Curator) starts a brand-new session.
 *
 * [connectString] must be the bare `host:port` list (no chroot).
 */
fun expireZkSession(connectString: String, sessionId: Long, password: ByteArray, timeoutMs: Long = 10_000) {
    val connected = CountDownLatch(1)
    val zk = ZooKeeper(connectString, 30_000, { event ->
        if (event.state == Watcher.Event.KeeperState.SyncConnected) connected.countDown()
    }, sessionId, password)
    try {
        check(connected.await(timeoutMs, TimeUnit.MILLISECONDS)) {
            "could not attach to session 0x${sessionId.toString(16)} to expire it"
        }
    } finally {
        zk.close()
    }
}
