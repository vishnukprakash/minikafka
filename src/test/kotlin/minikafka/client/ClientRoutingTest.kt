package minikafka.client

import minikafka.broker.Partition
import minikafka.cluster.local
import minikafka.cluster.replicatedTopic
import minikafka.model.TopicPartition
import minikafka.net.Connection
import minikafka.proto.ApiKeys
import minikafka.proto.ErrorCodes
import minikafka.proto.FetchRequest
import minikafka.proto.FetchResponse
import minikafka.proto.ProduceRequest
import minikafka.proto.ProduceResponse
import minikafka.testing.TestCluster
import minikafka.testing.TestClusterExtension
import minikafka.testing.eventually
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.RegisterExtension
import java.io.IOException
import java.net.InetAddress
import java.net.ServerSocket
import kotlin.time.Duration.Companion.seconds

/**
 * The routing client (algorithm 13) against a real 3-broker cluster: bootstrap, metadata-driven
 * routing, refresh-and-retry on leadership changes, the retry budget, and failover speed on a
 * graceful stop. The ZooKeeper session is 10s here, so a failover that only a session expiry
 * could trigger would be obvious.
 */
@Tag("zk")
class ClientRoutingTest {
    private val sessionTimeoutMs = 10_000

    @JvmField
    @RegisterExtension
    val clusterExtension = TestClusterExtension { TestCluster(size = 3, zkSessionTimeoutMs = sessionTimeoutMs) }
    private val cluster get() = clusterExtension.cluster

    private fun address(id: Int) = HostPort("127.0.0.1", cluster.broker(id).port())

    private fun client(bootstrap: List<HostPort>, config: ClientConfig = cluster.clientConfig()) =
        MiniKafkaClient(bootstrap, config)

    /** Listeners holding the ports handed out by [deadAddress]; closed after each test. */
    private val held = mutableListOf<ServerSocket>()

    @AfterEach
    fun releaseDeadPorts() {
        held.forEach { runCatching { it.close() } }
        held.clear()
    }

    /**
     * A dead broker address, guaranteed for the rest of the test: the port stays held by a listener
     * of ours (so no broker or other socket can take it over) that resets every connection at once,
     * so every request to it fails with an IOException. (A bound but non-listening socket would not
     * do: on macOS a SYN to it is dropped, so connects hang until their timeout instead.)
     */
    private fun deadAddress(): HostPort {
        val listener = ServerSocket(0, 50, InetAddress.getLoopbackAddress())
        held += listener
        Thread({
            while (!listener.isClosed) {
                val conn = runCatching { listener.accept() }.getOrNull() ?: break
                runCatching { conn.setSoLinger(true, 0); conn.close() } // RST
            }
        }, "dead-broker-${listener.localPort}").apply { isDaemon = true }.start()
        return HostPort("127.0.0.1", listener.localPort)
    }

    @Test
    fun `a client bootstrapped from any single broker reaches every partition leader`() {
        cluster.replicatedTopic("any", partitions = 3)
        val leaders = (0 until 3).map { cluster.awaitLeader("any", it) }
        assertEquals(setOf(1, 2, 3), leaders.toSet())
        for (id in cluster.brokerIds) {
            client(listOf(address(id))).use { c ->
                assertEquals(setOf(1, 2, 3), c.metadata().brokers.map { it.id }.toSet())
                for (p in 0 until 3) {
                    val r = c.produce("any", null, "from-$id".toByteArray(), partition = p)
                    assertEquals(ErrorCodes.NONE, r.errorCode, "via bootstrap $id to partition $p")
                }
            }
        }
        cluster.client().use { c ->
            for (p in 0 until 3) {
                assertEquals(listOf("from-1", "from-2", "from-3"), c.fetch("any", p, 0).records.map { String(it.value) })
            }
        }
    }

    @Test
    fun `a dead bootstrap broker is skipped`() {
        client(listOf(deadAddress(), address(2))).use { c ->
            assertEquals(ErrorCodes.NONE, c.createTopic("skip", 1))
            cluster.awaitLeader("skip", 0)
            assertEquals(ErrorCodes.NONE, c.produce("skip", null, "v".toByteArray()).errorCode)
        }
        // A known broker that has died since the last metadata is skipped too.
        client(listOf(address(1), address(2), address(3))).use { c ->
            c.metadata()
            val controller = cluster.awaitController()
            val victim = cluster.brokerIds.first { it != controller && it != cluster.leaderOf("skip", 0) }
            cluster.stopBroker(victim)
            assertTrue(victim !in c.metadata().brokers.map { it.id })
        }
        client(listOf(deadAddress(), deadAddress())).use { c ->
            assertThrows(IOException::class.java) { c.metadata() }
        }
    }

    @Test
    fun `NOT_LEADER makes the client refresh its metadata and retry on the new leader`() {
        cluster.replicatedTopic("moved", partitions = 3)
        val controller = cluster.awaitController()
        // Not the controller's partition: its session expiry would also move the controller.
        val p = (0 until 3).first { cluster.awaitLeader("moved", it) != controller }
        val oldLeader = cluster.awaitLeader("moved", p)
        val c = cluster.client()
        assertEquals(oldLeader, c.metadata().topic("moved")!!.partition(p)!!.leader) // cached: points at oldLeader

        // A new session for the leader = a bounce: leadership moves, the broker stays up (same port) as a follower.
        val epoch = cluster.partitionState("moved", p)!!.value.leaderEpoch
        cluster.expireSession(oldLeader)
        val newLeader = eventually(15.seconds) {
            val s = cluster.partitionState("moved", p)!!.value
            assertTrue(s.leader >= 0 && s.leader != oldLeader && s.leaderEpoch > epoch) { "not moved yet: $s" }
            s.leader
        }
        eventually(15.seconds) {
            val local = cluster.local(oldLeader, "moved", p)!!
            assertEquals(Partition.Role.FOLLOWER, local.role, "old leader demoted: $local")
            assertEquals(newLeader, local.leader)
        }
        cluster.awaitLeader("moved", p)
        // The old leader itself now answers NOT_LEADER...
        val direct = Connection("127.0.0.1", cluster.broker(oldLeader).port(), 5_000).use { conn ->
            conn.request(ApiKeys.PRODUCE, { ProduceRequest("moved", p, null, "direct".toByteArray(), -1, 1_000).encode(it) }) {
                ProduceResponse.decode(it)
            }
        }
        assertEquals(ErrorCodes.NOT_LEADER_FOR_PARTITION, direct.errorCode)
        // ...so the client, still routing by its cached metadata, is told NOT_LEADER, refreshes and retries.
        val r = c.produce("moved", null, "after-move".toByteArray(), partition = p)
        assertEquals(ErrorCodes.NONE, r.errorCode)
        val onNewLeader = cluster.broker(newLeader).replicaManager.readLocal(TopicPartition("moved", p), 0, Long.MAX_VALUE)
        assertEquals("after-move", String(onNewLeader.single { it.offset == r.offset }.value))
    }

    @Test
    fun `the client gives up after its retry budget and returns the last error`() {
        val controller = cluster.awaitController()
        cluster.client().use { assertEquals(ErrorCodes.NONE, it.createTopic("gone", 3, replicationFactor = 1)) }
        val victim = cluster.brokerIds.first { it != controller }
        val p = (0 until 3).first { cluster.awaitLeader("gone", it) == victim }
        cluster.stopBroker(victim)
        eventually { assertEquals(-1, cluster.partitionState("gone", p)!!.value.leader) }

        val config = cluster.clientConfig().copy(maxRetries = 3, retryBackoffMs = 100)
        client(cluster.bootstrap(), config).use { c ->
            val start = System.nanoTime()
            val r = c.produce("gone", null, "x".toByteArray(), partition = p)
            val elapsedMs = (System.nanoTime() - start) / 1_000_000
            assertEquals(ErrorCodes.LEADER_NOT_AVAILABLE, r.errorCode)
            assertTrue(elapsedMs >= 3 * 100) { "3 retries, 100ms apart: only ${elapsedMs}ms" }
            assertTrue(elapsedMs < 5_000) { "gave up in ${elapsedMs}ms" }
            assertEquals(ErrorCodes.LEADER_NOT_AVAILABLE, c.fetch("gone", p, 0).errorCode)
            assertEquals(ErrorCodes.UNKNOWN_TOPIC, c.produce("no-such-topic", null, "x".toByteArray(), partition = 0).errorCode)
        }
    }

    @Test
    fun `a consumer fetch from a follower is rejected with NOT_LEADER_FOR_PARTITION`() {
        cluster.replicatedTopic("ff")
        cluster.client().use { assertEquals(ErrorCodes.NONE, it.produce("ff", null, "v".toByteArray(), partition = 0).errorCode) }
        cluster.awaitFullyReplicated("ff")
        val leader = cluster.awaitLeader("ff", 0)
        for (follower in cluster.replicasOf("ff", 0).filter { it != leader }) {
            val r = Connection("127.0.0.1", cluster.broker(follower).port(), 5_000).use { conn ->
                conn.request(ApiKeys.FETCH, { FetchRequest("ff", 0, 0, 1 shl 20).encode(it) }) { FetchResponse.decode(it) }
            }
            assertEquals(ErrorCodes.NOT_LEADER_FOR_PARTITION, r.errorCode, "follower $follower holds the data but must not serve it")
            assertTrue(r.records.isEmpty())
        }
    }

    @Test
    fun `a committed offset is visible through any broker`() {
        cluster.client().use { assertEquals(ErrorCodes.NONE, it.createTopic("off", 1)) }
        client(listOf(address(1))).use { assertEquals(ErrorCodes.NONE, it.commitOffset("g", "off", 0, 42)) }
        for (id in cluster.brokerIds) {
            client(listOf(address(id))).use { assertEquals(42L, it.fetchOffset("g", "off", 0), "via broker $id") }
        }
        client(listOf(address(3))).use { assertEquals(ErrorCodes.NONE, it.commitOffset("g", "off", 0, 43)) }
        client(listOf(address(2))).use { assertEquals(43L, it.fetchOffset("g", "off", 0)) }
    }

    @Test
    fun `a graceful stop fails over well within the session timeout`() {
        cluster.replicatedTopic("fast")
        val c = cluster.client()
        assertEquals(ErrorCodes.NONE, c.produce("fast", null, "before".toByteArray(), partition = 0).errorCode)
        cluster.awaitFullyReplicated("fast")
        val leader = cluster.awaitLeader("fast", 0)

        val start = System.nanoTime()
        cluster.stopBroker(leader) // deletes its registration at once (algorithm 2)
        val newLeader = cluster.awaitLeader("fast", 0, timeout = (sessionTimeoutMs / 2 / 1000).seconds)
        val elapsedMs = (System.nanoTime() - start) / 1_000_000
        assertNotEquals(leader, newLeader)
        assertTrue(elapsedMs < sessionTimeoutMs / 2) { "failover took ${elapsedMs}ms (session timeout ${sessionTimeoutMs}ms)" }

        assertEquals(ErrorCodes.NONE, c.produce("fast", null, "after".toByteArray(), partition = 0).errorCode)
        eventually { assertEquals(listOf("before", "after"), c.fetch("fast", 0, 0).records.map { String(it.value) }) }
    }
}
