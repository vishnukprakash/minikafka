package minikafka.broker

import org.slf4j.LoggerFactory
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * Per-broker ISR change executor (algorithm 10): a daemon thread `b<id>-isr-updater` that runs
 * [runOnce] every `replicaLagTimeMaxMs / 2` (real time) and whenever [requestRun] flags it (a
 * follower joined a maximal ISR). Lag itself is measured by the partition's injected [Clock], so
 * tests drive it deterministically by advancing a MutableClock and calling [runOnce] directly.
 *
 * [runOnce] is serialised (one ISR change in flight at a time per broker, so at most one per
 * partition). For each leader partition: prepare a proposal under the partition lock, release it,
 * CAS via [IsrStore.write] (and on conflict re-read via [IsrStore.read]) with no lock held, then
 * complete under the lock.
 */
class IsrUpdater internal constructor(
    private val brokerId: Int,
    replicaLagTimeMaxMs: Long,
    private val store: IsrStore,
    private val partitions: () -> Collection<Partition>
) : AutoCloseable {
    private val logger = LoggerFactory.getLogger(IsrUpdater::class.java)
    private val intervalMs = maxOf(1L, replicaLagTimeMaxMs / 2)
    private val signalLock = ReentrantLock()
    private val wakeup = signalLock.newCondition()
    private var flagged = false

    @Volatile
    private var running = false
    private val thread = Thread(::loop, "b$brokerId-isr-updater").apply { isDaemon = true }

    internal fun start() {
        running = true
        thread.start()
    }

    /** Asks the thread to run soon (never blocks; safe to call from any thread without locks). */
    fun requestRun() = signalLock.withLock {
        flagged = true
        wakeup.signal()
    }

    @Synchronized
    internal fun runOnce() {
        for (partition in partitions()) {
            val proposal = partition.prepareIsrChange() ?: continue
            val outcome = try {
                val version = store.write(partition.tp, proposal.state, proposal.zkVersion)
                if (version != null) Partition.IsrWriteOutcome.Written(version)
                else Partition.IsrWriteOutcome.Conflict(store.read(partition.tp))
            } catch (e: Exception) {
                Partition.IsrWriteOutcome.Failed(e)
            }
            partition.completeIsrChange(proposal, outcome)
        }
    }

    private fun loop() {
        while (running) {
            signalLock.withLock {
                if (!flagged && running) wakeup.await(intervalMs, TimeUnit.MILLISECONDS)
                flagged = false
            }
            if (!running) break
            try {
                runOnce()
            } catch (e: Exception) {
                logger.warn("b{} isr-updater round failed", brokerId, e)
            }
        }
    }

    override fun close() {
        running = false
        signalLock.withLock { wakeup.signalAll() }
        if (thread.isAlive) thread.join(5_000)
    }
}
