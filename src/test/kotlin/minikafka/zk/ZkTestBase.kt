package minikafka.zk

import minikafka.testing.expireZkSession
import org.apache.curator.test.InstanceSpec
import org.apache.curator.test.TestingServer
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.TestInstance
import java.util.UUID

/**
 * One in-process ZooKeeper per test class (tickTime 200ms so sessions can be short), and a
 * fresh chroot namespace per test so tests never see each other's znodes.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class ZkTestBase {
    protected lateinit var server: TestingServer
    private val stores = mutableListOf<ZkStore>()

    /** `host:port/t<uuid>` — unique per test. */
    protected lateinit var connect: String

    @BeforeAll
    fun startServer() {
        server = TestingServer(InstanceSpec(null, -1, -1, -1, true, -1, 200, -1), true)
    }

    @AfterAll
    fun stopServer() {
        server.close()
    }

    @BeforeEach
    fun newNamespace() {
        connect = server.connectString + "/t" + UUID.randomUUID().toString().replace("-", "")
    }

    @AfterEach
    fun closeStores() {
        stores.forEach { runCatching { it.close() } }
        stores.clear()
    }

    /** A started store on this test's namespace; closed automatically after the test. */
    protected fun newStore(sessionTimeoutMs: Int = 2000): ZkStore =
        ZkStore(connect, sessionTimeoutMs).also { stores += it; it.start() }

    protected fun expire(store: ZkStore) {
        val (id, password) = store.sessionIdAndPassword()
        expireZkSession(server.connectString, id, password)
    }
}
