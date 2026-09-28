package minikafka.testing

import minikafka.client.ClientConfig
import minikafka.client.HostPort
import minikafka.client.MiniKafkaClient
import minikafka.log.Record
import minikafka.model.PartitionState
import minikafka.model.TopicPartition
import minikafka.model.Versioned
import minikafka.server.Server
import minikafka.server.ServerConfig
import minikafka.zk.ZkStore
import org.apache.zookeeper.KeeperException
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import java.io.File
import java.nio.file.Files
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
 * N in-process brokers (ids 1..[size]) on ephemeral ports, each with its own temp data dir, all
 * coordinated by one [EmbeddedZk] under a per-cluster chroot. Each broker reaches ZooKeeper through
 * its own [TcpProxy], so one broker can be isolated from ZooKeeper ([isolateFromZk]) or crashed
 * without closing its session ([crashBroker]).
 *
 * Test timings: ZooKeeper session 3s (tickTime 200ms), replica lag 1s, produce timeout 5s, client
 * socket timeout 10s. Teardown ([close]) order: clients → brokers → proxies → admin store → ZooKeeper.
 */
class TestCluster(
    val size: Int = 1,
    val minInsyncReplicas: Int = 1,
    val zkSessionTimeoutMs: Int = 3_000,
    val replicaLagTimeMaxMs: Long = 1_000,
    val produceTimeoutMs: Int = 5_000,
    val clientSocketTimeoutMs: Int = 10_000
) : AutoCloseable {
    val zk = EmbeddedZk()
    private val chroot = "/c" + UUID.randomUUID().toString().replace("-", "").take(12)
    private val root: File = Files.createTempDirectory("minikafka-cluster").toFile()
    private val proxies: Map<Int, TcpProxy> = (1..size).associateWith { TcpProxy("127.0.0.1", zk.port, "zk-proxy-b$it") }
    private val servers = ConcurrentHashMap<Int, Server>()
    private val clients = CopyOnWriteArrayList<MiniKafkaClient>()
    private var adminStore: ZkStore? = null

    val brokerIds: List<Int> = (1..size).toList()

    /** Starts every broker and waits until a controller is elected and every broker is registered. */
    fun start(): TestCluster {
        brokerIds.forEach(::startBroker)
        awaitController()
        eventually(15.seconds) { assertEquals(brokerIds.toSet(), admin().liveBrokers().keys) }
        return this
    }

    // ------------------------------------------------------------------ brokers

    fun dataDir(id: Int): File = File(root, "b$id")

    fun config(id: Int): ServerConfig = ServerConfig(
        brokerId = id,
        zkConnect = "127.0.0.1:${proxy(id).port}$chroot",
        dataDir = dataDir(id),
        port = 0,
        advertisedHost = "127.0.0.1",
        minInsyncReplicas = minInsyncReplicas,
        zkSessionTimeoutMs = zkSessionTimeoutMs,
        replicaLagTimeMaxMs = replicaLagTimeMaxMs,
        requestTimeoutMs = produceTimeoutMs,
        controllerSocketTimeoutMs = clientSocketTimeoutMs
    )

    fun startBroker(id: Int): Server {
        check(!servers.containsKey(id)) { "broker $id is already running" }
        val server = Server(config(id))
        server.start()
        servers[id] = server
        return server
    }

    fun broker(id: Int): Server = servers[id] ?: throw IllegalStateException("broker $id is not running")

    fun isRunning(id: Int): Boolean = servers.containsKey(id)

    fun runningBrokers(): List<Int> = servers.keys.sorted()

    /** Graceful stop (algorithm 2): the registration disappears at once. */
    fun stopBroker(id: Int) {
        servers.remove(id)?.stop()
    }

    /**
     * Crash: cut the broker's ZooKeeper link first, so its session is *not* closed, then tear the
     * broker down. Its registration (and `/controller`, if it was controller) stays until ZooKeeper
     * expires the session (~[zkSessionTimeoutMs]).
     */
    fun crashBroker(id: Int) {
        proxy(id).cut()
        servers.remove(id)?.stop()
    }

    /** Stops the broker if running (gracefully), heals its ZooKeeper link, and starts it on the same data dir. */
    fun restartBroker(id: Int): Server {
        stopBroker(id)
        proxy(id).heal()
        return startBroker(id)
    }

    /** Expires the broker's ZooKeeper session server-side (deterministic; the broker then gets a new session). */
    fun expireSession(id: Int) {
        val (sessionId, password) = broker(id).zkStore.sessionIdAndPassword()
        expireZkSession(zk.connectString, sessionId, password)
    }

    fun isolateFromZk(id: Int) = proxy(id).cut()

    fun healZk(id: Int) = proxy(id).heal()

    /** Pauses/resumes the broker's replica fetchers (ruling R2 hook): while paused they send no requests; roles are untouched. */
    fun pauseFetchers(id: Int, paused: Boolean = true) = broker(id).replicaManager.pauseFetchers(paused)

    // ------------------------------------------------------------------ clients & ZooKeeper views

    fun bootstrap(): List<HostPort> = runningBrokers().map { HostPort("127.0.0.1", broker(it).port()) }

    fun clientConfig(): ClientConfig = ClientConfig(socketTimeoutMs = clientSocketTimeoutMs, produceTimeoutMs = produceTimeoutMs)

    /** A client bootstrapped on the running brokers; closed by [close]. */
    fun client(config: ClientConfig = clientConfig()): MiniKafkaClient =
        MiniKafkaClient(bootstrap(), config).also { clients += it }

    /** A ZooKeeper store on the cluster's chroot, connected directly (not through any proxy). */
    @Synchronized
    fun admin(): ZkStore = adminStore ?: ZkStore(zk.connectString + chroot, 10_000).also {
        it.start()
        adminStore = it
    }

    fun controllerId(): Int? = admin().currentController()

    fun controllerEpoch(): Int = admin().controllerEpoch().value

    fun partitionState(topic: String, partition: Int): Versioned<PartitionState>? =
        admin().readPartitionState(TopicPartition(topic, partition))

    fun leaderOf(topic: String, partition: Int): Int? = partitionState(topic, partition)?.value?.leader?.takeIf { it >= 0 }

    fun replicasOf(topic: String, partition: Int): List<Int> =
        admin().readAssignment(topic)?.get(partition) ?: emptyList()

    fun partitionsOf(topic: String): List<Int> = admin().readAssignment(topic)?.keys?.sorted() ?: emptyList()

    // ------------------------------------------------------------------ waits & assertions

    /** Waits for a controller that is running and considers itself active; returns its id. */
    fun awaitController(timeout: Duration = 15.seconds): Int = eventually(timeout) {
        val id = controllerId()
        assertTrue(id != null && isRunning(id) && broker(id).isController()) { "no active controller yet (znode says $id)" }
        id!!
    }

    /** Waits until ZooKeeper names a leader and that (running) broker has applied it; returns the leader id. */
    fun awaitLeader(topic: String, partition: Int, timeout: Duration = 15.seconds): Int = eventually(timeout) {
        val state = checkNotNull(partitionState(topic, partition)) { "no state for $topic-$partition" }.value
        assertTrue(state.leader >= 0 && isRunning(state.leader)) { "no running leader for $topic-$partition: $state" }
        val local = broker(state.leader).snapshot().partitions.firstOrNull { it.tp == TopicPartition(topic, partition) }
        assertTrue(local != null && local.leader == state.leader && local.leaderEpoch == state.leaderEpoch) {
            "leader ${state.leader} has not applied $state yet: $local"
        }
        state.leader
    }

    /**
     * Waits until ZooKeeper's ISR is [expected] *and* the (running) leader has applied it locally
     * (its committedIsr): the isr-updater CASes ZooKeeper first and updates the leader afterwards.
     */
    fun awaitIsr(topic: String, partition: Int, expected: Set<Int>, timeout: Duration = 15.seconds) = eventually(timeout) {
        val state = partitionState(topic, partition)?.value
        assertEquals(expected, state?.isr?.toSet(), "ISR of $topic-$partition in ZooKeeper")
        if (state!!.leader >= 0 && isRunning(state.leader)) {
            val local = broker(state.leader).snapshot().partitions.firstOrNull { it.tp == TopicPartition(topic, partition) }
            assertEquals(expected, local?.committedIsr?.toSet(), "committed ISR of $topic-$partition on leader ${state.leader}")
        }
    }

    /**
     * Waits until every partition of [topic] has ISR == replicas in ZooKeeper, its leader's HW ==
     * LEO, and every running replica has the leader's LEO.
     */
    fun awaitFullyReplicated(topic: String, timeout: Duration = 20.seconds) = eventually(timeout) {
        for (p in partitionsOf(topic)) {
            val tp = TopicPartition(topic, p)
            val replicas = replicasOf(topic, p)
            val leader = awaitLeader(topic, p, timeout = Duration.ZERO)
            assertEquals(replicas.toSet(), partitionState(topic, p)!!.value.isr.toSet(), "ISR of $tp")
            val leaderSnap = broker(leader).snapshot().partitions.first { it.tp == tp }
            assertEquals(leaderSnap.logEndOffset, leaderSnap.highWatermark, "leader HW of $tp")
            for (r in replicas.filter(::isRunning)) {
                val snap = broker(r).snapshot().partitions.firstOrNull { it.tp == tp }
                assertEquals(leaderSnap.logEndOffset, snap?.logEndOffset, "LEO of $tp on broker $r")
            }
        }
    }

    /** Every running ISR replica holds exactly the leader's records below the leader's HW. */
    fun assertReplicasConsistent(topic: String) {
        for (p in partitionsOf(topic)) {
            val tp = TopicPartition(topic, p)
            val state = partitionState(topic, p)?.value ?: continue
            if (state.leader < 0 || !isRunning(state.leader)) continue
            val hw = broker(state.leader).snapshot().partitions.first { it.tp == tp }.highWatermark
            val expected = broker(state.leader).replicaManager.readLocal(tp, 0, hw).map(::describe)
            for (r in state.isr.filter { it != state.leader && isRunning(it) }) {
                assertEquals(expected, broker(r).replicaManager.readLocal(tp, 0, hw).map(::describe), "replica $r of $tp below HW $hw")
            }
        }
    }

    private fun describe(r: Record) = "${r.offset}/${r.leaderEpoch}/${r.key?.let(::String)}/${String(r.value)}"

    /** The ZooKeeper tree under the chroot (with payloads) plus every running broker's snapshot. */
    fun dumpDiagnostics(): String = buildString {
        appendLine("===== TestCluster diagnostics (chroot $chroot) =====")
        appendLine("--- ZooKeeper ---")
        try {
            dumpZk(this, "/", 0)
        } catch (e: Exception) {
            appendLine("(ZooKeeper unavailable: $e)")
        }
        appendLine("--- brokers (running: ${runningBrokers()}) ---")
        for (id in runningBrokers()) {
            try {
                val server = broker(id)
                appendLine("broker $id: port ${server.port()} epoch ${server.brokerEpoch} controller=${server.isController()}")
                val snap = server.snapshot()
                appendLine("  seenControllerEpoch=${snap.seenControllerEpoch} fetchersPaused=${snap.fetchersPaused}")
                snap.partitions.forEach { appendLine("  $it") }
            } catch (e: Exception) {
                appendLine("broker $id: (snapshot failed: $e)")
            }
        }
    }

    private fun dumpZk(sb: StringBuilder, path: String, depth: Int) {
        val curator = admin().curator
        val data = try {
            curator.data.forPath(path)
        } catch (_: KeeperException.NoNodeException) {
            return
        }
        val payload = data?.let { String(it, Charsets.UTF_8).trim().replace("\n", ", ") } ?: ""
        sb.append("  ".repeat(depth)).append(if (depth == 0) "/" else path.substringAfterLast('/'))
        if (payload.isNotEmpty()) sb.append("  [").append(payload).append(']')
        sb.appendLine()
        val children = try {
            curator.children.forPath(path).sorted()
        } catch (_: KeeperException.NoNodeException) {
            emptyList()
        }
        for (child in children) dumpZk(sb, if (path == "/") "/$child" else "$path/$child", depth + 1)
    }

    // ------------------------------------------------------------------ teardown

    override fun close() {
        clients.forEach { runCatching { it.close() } }
        clients.clear()
        runningBrokers().forEach { id -> runCatching { servers.remove(id)?.stop() } }
        proxies.values.forEach { runCatching { it.close() } }
        adminStore?.let { runCatching { it.close() } }
        runCatching { zk.close() }
        root.deleteRecursively()
    }

    private fun proxy(id: Int): TcpProxy = proxies[id] ?: throw IllegalArgumentException("no broker $id in a $size-broker cluster")
}
