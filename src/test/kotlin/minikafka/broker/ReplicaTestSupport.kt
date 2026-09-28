package minikafka.broker

import minikafka.model.PartitionState
import minikafka.model.TopicPartition
import minikafka.model.Versioned
import minikafka.proto.LeaderAndIsrPartition
import minikafka.proto.LeaderAndIsrRequest
import minikafka.testing.MutableClock
import java.io.File
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList

const val ACKS_ONE: Short = 1
const val ACKS_ALL: Short = -1

/**
 * In-memory stand-in for the ZooKeeper state znodes: versioned CAS like `setData().withVersion`.
 * [onWrite], when set, replaces the CAS behaviour entirely (fault / race injection); it runs on
 * the caller's thread (the isr-updater), with no partition lock held.
 */
class FakeIsrStore : IsrStore {
    val states = ConcurrentHashMap<TopicPartition, Versioned<PartitionState>>()
    val writes = CopyOnWriteArrayList<Pair<TopicPartition, PartitionState>>()

    @Volatile
    var onWrite: ((TopicPartition, PartitionState, Int) -> Int?)? = null

    /** Unconditional controller-style write; returns the new zkVersion. */
    @Synchronized
    fun controllerSet(tp: TopicPartition, state: PartitionState): Int {
        val next = (states[tp]?.zkVersion ?: -1) + 1
        states[tp] = Versioned(state, next)
        return next
    }

    override fun write(tp: TopicPartition, state: PartitionState, expectedZkVersion: Int): Int? {
        writes.add(tp to state)
        onWrite?.let { return it(tp, state, expectedZkVersion) }
        synchronized(this) {
            val current = states[tp] ?: return null
            if (current.zkVersion != expectedZkVersion) return null
            states[tp] = Versioned(state, current.zkVersion + 1)
            return current.zkVersion + 1
        }
    }

    override fun read(tp: TopicPartition): Versioned<PartitionState>? = states[tp]
}

/** One broker's ReplicaManager (no background isr-updater thread) plus a fake controller. */
class ReplicaHarness(
    dataDir: File,
    val brokerId: Int = 1,
    minInsyncReplicas: Int = 1,
    val lagMs: Long = 1_000,
    val tp: TopicPartition = TopicPartition("t", 0),
    requestTimeoutMs: Int = 30_000
) : AutoCloseable {
    val clock = MutableClock()
    val store = FakeIsrStore()
    val brokerEpoch = 100L
    var controllerEpoch = 1
    val rm = ReplicaManager(
        BrokerConfig(brokerId, "localhost", 0, dataDir, minInsyncReplicas, replicaLagTimeMaxMs = lagMs, requestTimeoutMs = requestTimeoutMs),
        store, clock, startIsrUpdater = false
    )

    fun partitionRequest(leader: Int, epoch: Int, isr: List<Int>, replicas: List<Int>, zkVersion: Int, tp: TopicPartition = this.tp) =
        LeaderAndIsrPartition(tp.topic, tp.partition, leader, epoch, isr, replicas, zkVersion)

    /** Writes the state znode like the controller would, then sends LeaderAndIsr. */
    fun leaderAndIsr(leader: Int, epoch: Int, isr: List<Int>, replicas: List<Int>, tp: TopicPartition = this.tp): Short {
        val v = store.controllerSet(tp, PartitionState(leader, epoch, isr, controllerEpoch))
        return rm.applyLeaderAndIsr(
            LeaderAndIsrRequest(0, controllerEpoch, brokerEpoch, listOf(partitionRequest(leader, epoch, isr, replicas, v, tp))),
            brokerEpoch
        )
    }

    fun produce(value: String, acks: Short = ACKS_ONE, timeoutMs: Int = 5_000): ProduceResult =
        rm.produce(tp, null, value.toByteArray(), acks, timeoutMs)

    fun produceAsync(value: String, acks: Short = ACKS_ALL, timeoutMs: Int = 5_000): CompletableFuture<ProduceResult> =
        async { produce(value, acks, timeoutMs) }

    fun followerFetch(replicaId: Int, offset: Long, epoch: Int = state().leaderEpoch): FetchResult =
        rm.fetchAsFollower(tp, replicaId, offset, epoch, 1 shl 20)

    fun consumerFetch(offset: Long): FetchResult = rm.fetchAsConsumer(tp, offset, 1 shl 20)

    fun state(tp: TopicPartition = this.tp): PartitionSnapshot = rm.snapshot().partitions.single { it.tp == tp }

    fun runIsrUpdater() = rm.isrUpdater.runOnce()

    override fun close() = rm.close()
}

fun <T> async(block: () -> T): CompletableFuture<T> {
    val future = CompletableFuture<T>()
    val thread = Thread {
        try {
            future.complete(block())
        } catch (t: Throwable) {
            future.completeExceptionally(t)
        }
    }
    thread.isDaemon = true
    thread.start()
    return future
}
