package minikafka.zk

import minikafka.model.BrokerInfo
import minikafka.model.PartitionState
import minikafka.model.TopicPartition
import minikafka.model.Versioned
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.framework.api.transaction.CuratorOp
import org.apache.curator.framework.api.transaction.OperationType
import org.apache.curator.framework.state.ConnectionState
import org.apache.curator.framework.state.ConnectionStateListener
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.KeeperException
import org.apache.zookeeper.OpResult
import org.apache.zookeeper.Watcher
import org.apache.zookeeper.data.Stat
import org.slf4j.LoggerFactory
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/** Connection states callers see, so they never depend on Curator types. */
enum class ConnectionStateKind { CONNECTED, SUSPENDED, LOST, RECONNECTED }

/** A won controller election: the new epoch and the zkVersion of `/controller_epoch` that fences it (D2, D3). */
data class ControllerElection(val epoch: Int, val epochZkVersion: Int)

/** Outcome of an epoch-fenced controller write (`multi(check(/controller_epoch), op)`, D3). */
sealed interface FencedWriteResult {
    /** The state znode now holds exactly what was written, at [zkVersion]. */
    data class Ok(val zkVersion: Int) : FencedWriteResult

    /** The epoch check failed: a newer controller exists; the caller must resign. */
    data object ControllerFenced : FencedWriteResult

    /** The state op failed on its version (or the znode vanished) and holds something else: re-read and recompute. */
    data object StateConflict : FencedWriteResult

    /** A fenced create found a state znode that already exists with different content. */
    data object AlreadyExists : FencedWriteResult
}

class DuplicateBrokerIdException(brokerId: Int, timeoutMs: Long) : RuntimeException(
    "duplicate broker id $brokerId: /brokers/ids/$brokerId is held by another live session " +
        "(waited ${timeoutMs}ms for it to disappear)"
)

/**
 * The only code in minikafka that talks to ZooKeeper (via Curator). See [ZkPaths] for the layout
 * and [KvCodec] for payloads.
 *
 * **Retry-safety invariant (D16):** Curator transparently replays an operation after a
 * ConnectionLoss, and the first attempt may already have committed. Every write here therefore
 * tolerates its own replay: on `NodeExists`/`BadVersion` it re-reads and recognises its own
 * committed write — by `ephemeralOwner == my session` for ephemerals, or by the znode already
 * holding exactly the intended content (leader, leader_epoch, isr, controller_epoch) for state —
 * and reports success in that case.
 *
 * **Watches** are one-shot and are registered atomically with the read that returns the current
 * value (`watchChildren`/`watchNode`); re-arm by calling again. Callbacks run on ZooKeeper's
 * event thread and must not block or do ZK work themselves — enqueue an event and return. After a
 * session expiry all watches are gone: re-arm on [ConnectionStateKind.RECONNECTED].
 *
 * @param connectString `host:port[,host:port...][/chroot]`; a chroot becomes the Curator namespace (D22).
 */
class ZkStore(
    connectString: String,
    val sessionTimeoutMs: Int,
    private val connectionTimeoutMs: Int = 10_000
) : AutoCloseable {

    private val log = LoggerFactory.getLogger(ZkStore::class.java)

    internal val curator: CuratorFramework

    init {
        val slash = connectString.indexOf('/')
        val hosts = if (slash < 0) connectString else connectString.substring(0, slash)
        val namespace = if (slash < 0) null else connectString.substring(slash + 1).trim('/').ifEmpty { null }
        curator = CuratorFrameworkFactory.builder()
            .connectString(hosts)
            .namespace(namespace)
            .sessionTimeoutMs(sessionTimeoutMs)
            .connectionTimeoutMs(connectionTimeoutMs)
            .retryPolicy(ExponentialBackoffRetry(100, 5, 1000))
            .build()
    }

    /** Starts the client and blocks (bounded by the connection timeout) until connected. */
    fun start() {
        curator.connectionStateListenable.addListener(ConnectionStateListener { _, state ->
            log.info("ZooKeeper connection state {} (session 0x{})", state, sessionIdOrNull()?.toString(16))
        })
        curator.start()
        if (!curator.blockUntilConnected(connectionTimeoutMs, TimeUnit.MILLISECONDS)) {
            curator.close()
            throw IllegalStateException("could not connect to ZooKeeper within ${connectionTimeoutMs}ms")
        }
        log.info(
            "Connected to ZooKeeper: session 0x{}, negotiated session timeout {}ms",
            sessionId().toString(16), curator.zookeeperClient.zooKeeper.sessionTimeout
        )
    }

    override fun close() {
        curator.close()
    }

    fun sessionId(): Long = curator.zookeeperClient.zooKeeper.sessionId

    fun isConnected(): Boolean = curator.zookeeperClient.isConnected

    /** For tests that expire the session server-side. */
    internal fun sessionIdAndPassword(): Pair<Long, ByteArray> =
        curator.zookeeperClient.zooKeeper.let { it.sessionId to it.sessionPasswd }

    /** Test hook: invoked (with the owning session id) each time [registerBroker] starts waiting for a stale node. */
    @Volatile
    internal var onWaitingForStaleRegistration: (Long) -> Unit = {}

    private fun sessionIdOrNull(): Long? = runCatching { sessionId() }.getOrNull()

    /** Invokes [listener] on every connection state change (on Curator's listener thread; don't block). */
    fun onConnectionStateChanged(listener: (ConnectionStateKind) -> Unit) {
        curator.connectionStateListenable.addListener(ConnectionStateListener { _, state ->
            val kind = when (state) {
                ConnectionState.CONNECTED -> ConnectionStateKind.CONNECTED
                ConnectionState.SUSPENDED -> ConnectionStateKind.SUSPENDED
                ConnectionState.LOST -> ConnectionStateKind.LOST
                ConnectionState.RECONNECTED -> ConnectionStateKind.RECONNECTED
                else -> null // READ_ONLY: never enabled
            }
            if (kind != null) listener(kind)
        })
    }

    // ------------------------------------------------------------------ brokers

    /**
     * Creates the ephemeral `/brokers/ids/<id>` and returns its czxid (the broker epoch, D4).
     * If the node exists and is owned by this session (a replay, or re-registering) its czxid is
     * returned. If another session owns it (typically the stale ephemeral of this broker's previous
     * incarnation) waits up to [timeoutMs] for it to be deleted, then retries; still held ⇒
     * [DuplicateBrokerIdException].
     */
    fun registerBroker(info: BrokerInfo, timeoutMs: Long = 2L * sessionTimeoutMs): Long {
        val path = ZkPaths.brokerId(info.id)
        val deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs)
        while (true) {
            try {
                curator.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL)
                    .forPath(path, KvCodec.encodeBrokerInfo(info))
            } catch (_: KeeperException.NodeExistsException) {
                // ours (replay / re-register) or someone else's: decided below
            }
            val changed = CountDownLatch(1)
            val stat = curator.checkExists().usingWatcher(Watcher { changed.countDown() }).forPath(path)
                ?: continue // deleted in between: try again
            if (stat.ephemeralOwner == sessionId()) {
                log.info("Registered broker {} at {}:{} (broker epoch / czxid {})", info.id, info.host, info.port, stat.czxid)
                return stat.czxid
            }
            val remainingNanos = deadline - System.nanoTime()
            if (remainingNanos <= 0) throw DuplicateBrokerIdException(info.id, timeoutMs)
            log.info(
                "Broker id {} is held by session 0x{}; waiting up to {}ms for it to expire",
                info.id, stat.ephemeralOwner.toString(16), TimeUnit.NANOSECONDS.toMillis(remainingNanos)
            )
            onWaitingForStaleRegistration(stat.ephemeralOwner)
            if (!changed.await(remainingNanos, TimeUnit.NANOSECONDS)) throw DuplicateBrokerIdException(info.id, timeoutMs)
        }
    }

    /** Live brokers: id → (info, czxid = broker epoch). */
    fun liveBrokers(): Map<Int, Pair<BrokerInfo, Long>> {
        val ids = childrenOrEmpty(ZkPaths.BROKER_IDS)
        val result = sortedMapOf<Int, Pair<BrokerInfo, Long>>()
        for (child in ids) {
            val id = child.toIntOrNull() ?: continue
            val stat = Stat()
            val data = try {
                curator.data.storingStatIn(stat).forPath(ZkPaths.brokerId(id))
            } catch (_: KeeperException.NoNodeException) {
                continue // broker left between the list and the read
            }
            result[id] = KvCodec.decodeBrokerInfo(id, data) to stat.czxid
        }
        return result
    }

    // ------------------------------------------------------------------ topics

    /**
     * Creates `/brokers/topics/<t>` (the topic's commit point). Returns false if it already
     * exists — note a Curator replay of our own successful create also reports false.
     */
    fun createTopicAssignment(topic: String, assignment: Map<Int, List<Int>>): Boolean = try {
        curator.create().creatingParentsIfNeeded().forPath(ZkPaths.topic(topic), KvCodec.encodeAssignment(assignment))
        true
    } catch (_: KeeperException.NodeExistsException) {
        false
    }

    fun readAssignment(topic: String): Map<Int, List<Int>>? =
        dataOrNull(ZkPaths.topic(topic))?.let { KvCodec.decodeAssignment(it) }

    fun allTopics(): List<String> = childrenOrEmpty(ZkPaths.BROKER_TOPICS).sorted()

    // ------------------------------------------------------------------ partition state

    fun readPartitionState(tp: TopicPartition): Versioned<PartitionState>? {
        val stat = Stat()
        return try {
            val data = curator.data.storingStatIn(stat).forPath(ZkPaths.partitionState(tp))
            Versioned(KvCodec.decodePartitionState(data), stat.version)
        } catch (_: KeeperException.NoNodeException) {
            null
        }
    }

    /**
     * Unfenced compare-and-set of a partition state (the leader's ISR change, algorithm 10).
     * Returns the new zkVersion, or null if [expectedZkVersion] is stale (or the znode is missing).
     * On BadVersion, a znode that already equals [state] exactly is our own replayed write (D16):
     * its version is returned.
     */
    fun casPartitionState(tp: TopicPartition, state: PartitionState, expectedZkVersion: Int): Int? = try {
        curator.setData().withVersion(expectedZkVersion)
            .forPath(ZkPaths.partitionState(tp), KvCodec.encodePartitionState(state)).version
    } catch (_: KeeperException.BadVersionException) {
        readPartitionState(tp)?.takeIf { it.value == state }?.zkVersion
    } catch (_: KeeperException.NoNodeException) {
        null
    }

    /**
     * Fenced `multi(check(/controller_epoch, epochZkVersion), create(state))`. Parent znodes
     * (`partitions`, `partitions/<p>`) are created outside the transaction, but never the topic
     * znode itself (it is the topic's commit point and must already exist).
     *
     * @throws IllegalStateException if the topic's assignment znode does not exist.
     * @throws KeeperException (e.g. ConnectionLoss, SessionExpired) if ZooKeeper stays unreachable
     *   after Curator's retries; the write's outcome is then unknown.
     */
    fun fencedCreatePartitionState(tp: TopicPartition, state: PartitionState, epochZkVersion: Int): FencedWriteResult {
        try {
            for (parent in listOf(ZkPaths.partitions(tp.topic), ZkPaths.partition(tp))) {
                try {
                    curator.create().forPath(parent, ByteArray(0))
                } catch (_: KeeperException.NodeExistsException) {
                }
            }
        } catch (e: KeeperException.NoNodeException) {
            throw IllegalStateException("topic '${tp.topic}' has no assignment znode", e)
        }
        val op = curator.transactionOp().create().forPath(ZkPaths.partitionState(tp), KvCodec.encodePartitionState(state))
        return fencedWrite(tp, state, epochZkVersion, op, FencedWriteResult.AlreadyExists)
    }

    /**
     * Fenced `multi(check(/controller_epoch, epochZkVersion), setData(state, expectedStateVersion))`.
     *
     * @throws KeeperException (e.g. ConnectionLoss, SessionExpired) if ZooKeeper stays unreachable
     *   after Curator's retries; the write's outcome is then unknown.
     */
    fun fencedSetPartitionState(
        tp: TopicPartition,
        state: PartitionState,
        expectedStateVersion: Int,
        epochZkVersion: Int
    ): FencedWriteResult {
        val op = curator.transactionOp().setData().withVersion(expectedStateVersion)
            .forPath(ZkPaths.partitionState(tp), KvCodec.encodePartitionState(state))
        return fencedWrite(tp, state, epochZkVersion, op, FencedWriteResult.StateConflict)
    }

    private fun fencedWrite(
        tp: TopicPartition,
        state: PartitionState,
        epochZkVersion: Int,
        stateOp: CuratorOp,
        stateOpFailure: FencedWriteResult
    ): FencedWriteResult {
        val check = curator.transactionOp().check().withVersion(epochZkVersion).forPath(ZkPaths.CONTROLLER_EPOCH)
        return try {
            val results = curator.transaction().forOperations(check, stateOp)
            // A fresh create is always version 0; a setData reports its new stat.
            FencedWriteResult.Ok(results[1].resultStat?.version ?: 0)
        } catch (e: KeeperException) {
            when (failedOpIndex(e) ?: throw e) {
                0 -> {
                    log.info("Fenced write to {} rejected: controller epoch zkVersion {} is stale", tp, epochZkVersion)
                    FencedWriteResult.ControllerFenced
                }
                else -> {
                    // Replay of our own committed write? (D16)
                    val current = readPartitionState(tp)
                    if (current != null && current.value == state) FencedWriteResult.Ok(current.zkVersion) else stateOpFailure
                }
            }
        }
    }

    /** Index of the op that failed a multi (ops before it report OK, ops after it RUNTIMEINCONSISTENCY). */
    private fun failedOpIndex(e: KeeperException): Int? =
        e.results?.indexOfFirst { it is OpResult.ErrorResult && it.err != KeeperException.Code.OK.intValue() }
            ?.takeIf { it >= 0 }

    // ------------------------------------------------------------------ controller

    /** Creates `/controller_epoch` = 0 if absent (NodeExists is fine). */
    fun ensureControllerEpochNode() {
        createIfMissing(ZkPaths.CONTROLLER_EPOCH, "0".toByteArray(Charsets.UTF_8))
    }

    /** Current controller epoch and its zkVersion (creates the node at 0 if absent). */
    fun controllerEpoch(): Versioned<Int> {
        val stat = Stat()
        val data = try {
            curator.data.storingStatIn(stat).forPath(ZkPaths.CONTROLLER_EPOCH)
        } catch (_: KeeperException.NoNodeException) {
            ensureControllerEpochNode()
            curator.data.storingStatIn(stat).forPath(ZkPaths.CONTROLLER_EPOCH)
        }
        return Versioned(String(data, Charsets.UTF_8).trim().toInt(), stat.version)
    }

    fun currentController(): Int? =
        dataOrNull(ZkPaths.CONTROLLER)?.let { KvCodec.decode(it)["brokerid"]?.toIntOrNull() }

    /**
     * Controller election (D2): `multi(create EPHEMERAL /controller, setData(/controller_epoch, e+1, ver))`.
     *
     * Returns the won epoch, or null if `/controller` exists and belongs to another session (the
     * caller should then watch `/controller`). If the multi fails (NodeExists, BadVersion,
     * ConnectionLoss) it is recognised as our own committed election (D16) only if `/controller` is
     * owned by this session **and** `/controller_epoch` was last modified by the very transaction
     * that created `/controller` (epoch `mzxid` == controller `czxid`); the epoch returned comes from
     * that same read, so a newer controller's epoch can never be mistaken for ours. If the multi
     * fails and `/controller` is absent (someone won, then died), the attempt is retried once;
     * a second such failure also returns null.
     *
     * @throws KeeperException if ZooKeeper stays unreachable after Curator's retries.
     */
    fun electController(brokerId: Int): ControllerElection? {
        repeat(2) {
            when (val outcome = tryElect(brokerId)) {
                is ElectionAttempt.Won -> return outcome.election
                ElectionAttempt.OtherController -> return null
                ElectionAttempt.NoController -> {} // retry once
            }
        }
        return null
    }

    private sealed interface ElectionAttempt {
        data class Won(val election: ControllerElection) : ElectionAttempt
        data object OtherController : ElectionAttempt
        data object NoController : ElectionAttempt
    }

    private fun tryElect(brokerId: Int): ElectionAttempt {
        val current = controllerEpoch()
        val newEpoch = current.value + 1
        try {
            val results = curator.transaction().forOperations(
                curator.transactionOp().create().withMode(CreateMode.EPHEMERAL)
                    .forPath(ZkPaths.CONTROLLER, KvCodec.encode(mapOf("brokerid" to brokerId.toString()))),
                curator.transactionOp().setData().withVersion(current.zkVersion)
                    .forPath(ZkPaths.CONTROLLER_EPOCH, newEpoch.toString().toByteArray(Charsets.UTF_8))
            )
            val epochStat = results.first { it.type == OperationType.SET_DATA }.resultStat
            log.info("Broker {} elected controller with epoch {}", brokerId, newEpoch)
            return ElectionAttempt.Won(ControllerElection(newEpoch, epochStat.version))
        } catch (e: KeeperException) {
            if (e !is KeeperException.NodeExistsException &&
                e !is KeeperException.BadVersionException &&
                e !is KeeperException.ConnectionLossException
            ) throw e
        }
        // Read the epoch first, then /controller: both must come from the same (our) transaction.
        val epochStat = Stat()
        val epochData = curator.data.storingStatIn(epochStat).forPath(ZkPaths.CONTROLLER_EPOCH)
        val controllerStat = curator.checkExists().forPath(ZkPaths.CONTROLLER) ?: return ElectionAttempt.NoController
        if (controllerStat.ephemeralOwner == sessionId() && epochStat.mzxid == controllerStat.czxid) {
            val epoch = String(epochData, Charsets.UTF_8).trim().toInt()
            log.info("Broker {} recognised its own committed election: controller with epoch {}", brokerId, epoch)
            return ElectionAttempt.Won(ControllerElection(epoch, epochStat.version))
        }
        return ElectionAttempt.OtherController
    }

    // ------------------------------------------------------------------ consumer offsets

    /** Last-writer-wins group offset commit (D14). */
    fun commitOffset(group: String, tp: TopicPartition, offset: Long) {
        curator.create().orSetData().creatingParentsIfNeeded()
            .forPath(ZkPaths.consumerOffset(group, tp), offset.toString().toByteArray(Charsets.UTF_8))
    }

    /** The committed offset, or -1 if none. */
    fun fetchOffset(group: String, tp: TopicPartition): Long =
        dataOrNull(ZkPaths.consumerOffset(group, tp))?.let { String(it, Charsets.UTF_8).trim().toLong() } ?: -1L

    // ------------------------------------------------------------------ watches

    /**
     * Reads the children of [path] and leaves a one-shot watch in the same call; [onChange] fires
     * once on the next child change. Creates [path] (persistent, empty) if it does not exist yet.
     */
    fun watchChildren(path: String, onChange: () -> Unit): List<String> {
        val watcher = oneShot(path, onChange)
        return try {
            curator.children.usingWatcher(watcher).forPath(path)
        } catch (_: KeeperException.NoNodeException) {
            createIfMissing(path)
            curator.children.usingWatcher(watcher).forPath(path)
        }.sorted()
    }

    /**
     * Reads [path]'s data (null if absent) and leaves a one-shot exists-watch registered *before*
     * the read, so [onChange] fires on the next creation, data change or deletion.
     */
    fun watchNode(path: String, onChange: () -> Unit): ByteArray? {
        curator.checkExists().usingWatcher(oneShot(path, onChange)).forPath(path) ?: return null
        return dataOrNull(path)
    }

    private fun oneShot(path: String, onChange: () -> Unit) = Watcher { event ->
        // Connection-state (type None) events are not node changes; RECONNECTED is the re-arm signal.
        if (event.type != Watcher.Event.EventType.None) {
            try {
                onChange()
            } catch (e: Exception) {
                log.warn("Watch callback for {} threw", path, e)
            }
        }
    }

    // ------------------------------------------------------------------ helpers

    private fun dataOrNull(path: String): ByteArray? = try {
        curator.data.forPath(path)
    } catch (_: KeeperException.NoNodeException) {
        null
    }

    private fun childrenOrEmpty(path: String): List<String> = try {
        curator.children.forPath(path)
    } catch (_: KeeperException.NoNodeException) {
        emptyList()
    }

    /** Persistent create; NodeExists is fine (idempotent, so replay-safe). */
    private fun createIfMissing(path: String, data: ByteArray = ByteArray(0)) {
        try {
            curator.create().creatingParentsIfNeeded().forPath(path, data)
        } catch (_: KeeperException.NodeExistsException) {
        }
    }
}
