package minikafka.broker

import minikafka.log.Log
import minikafka.log.Record
import minikafka.model.PartitionState
import minikafka.model.TopicPartition
import minikafka.model.Versioned
import minikafka.proto.ErrorCodes
import minikafka.proto.LeaderAndIsrPartition
import minikafka.proto.OffsetsForLeaderEpochResponse
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

data class ProduceResult(val errorCode: Short, val offset: Long)

data class FetchResult(val errorCode: Short, val records: List<Record>, val highWatermark: Long)

/**
 * One local replica of a partition: role, leader epoch, ISR (committed + maximal, D7), follower
 * progress, and the high watermark. Implements shared-context algorithms 7 (per-partition part),
 * 9, 10 (the parts that run under the lock), 11 and 12.
 *
 * Concurrency: every field is guarded by [lock]. The lock is never held across network or
 * ZooKeeper I/O — the ISR CAS is split into [prepareIsrChange] / [completeIsrChange] with the
 * write done by the [IsrUpdater] in between. Lock order: Partition lock -> Log lock (Log methods
 * are called with the partition lock held; Log never calls back).
 */
class Partition internal constructor(
    val tp: TopicPartition,
    private val brokerId: Int,
    private val log: Log,
    private val clock: Clock,
    private val minInsyncReplicas: Int,
    private val replicaLagTimeMaxMs: Long,
    private val onExpandRequested: () -> Unit
) {
    enum class Role { NONE, LEADER, FOLLOWER }

    private class FollowerState(
        var leo: Long,
        var lastCaughtUpMs: Long,
        var lastFetchLeaderLeo: Long,
        var lastFetchTimeMs: Long
    )

    /** Role change produced by an applied LeaderAndIsr, reported to the listener after unlocking. */
    internal data class RoleTransition(val tp: TopicPartition, val isLeader: Boolean, val leader: Int, val leaderEpoch: Int)

    /**
     * An ISR change prepared under the lock, CAS-written at [zkVersion]. [knownControllerEpoch] is
     * the state znode's controller_epoch if this partition knows it (it wrote, adopted or pre-read
     * the znode during this leader epoch), else null: the updater then reads the znode first and
     * preserves its controller_epoch (algorithm 10), treating a version mismatch as a conflict.
     */
    internal data class IsrProposal(
        val leader: Int,
        val leaderEpoch: Int,
        val zkVersion: Int,
        val isr: Set<Int>,
        val shrink: Set<Int>,
        val knownControllerEpoch: Int?
    ) {
        fun stateWith(controllerEpoch: Int) = PartitionState(leader, leaderEpoch, isr.sorted(), controllerEpoch)
    }

    internal sealed interface IsrWriteOutcome {
        data class Written(val zkVersion: Int, val controllerEpoch: Int) : IsrWriteOutcome
        /** The CAS saw a version conflict; [current] is the state znode re-read afterwards. */
        data class Conflict(val current: Versioned<PartitionState>?) : IsrWriteOutcome
        data class Failed(val error: Exception) : IsrWriteOutcome
    }

    private val logger = LoggerFactory.getLogger(Partition::class.java)
    private val lock = ReentrantLock()
    /** Signalled on HW advance, role/epoch change, ISR change and close: wakes acks=all waiters. */
    private val stateChanged = lock.newCondition()

    private var role = Role.NONE
    private var leader = -1
    private var leaderEpoch = -1
    /** controller_epoch of the state znode at [zkVersion], or null if not known yet this epoch. */
    private var znodeControllerEpoch: Int? = null
    private var replicas: List<Int> = emptyList()
    private var committedIsr: Set<Int> = emptySet()
    private var maximalIsr: Set<Int> = emptySet()
    private var zkVersion = -1
    private var highWatermark = 0L // D10: no checkpoint, 0 on load
    private var leaderEpochStartOffset = -1L
    private var isrStale = false
    private val followers = HashMap<Int, FollowerState>()
    private var closed = false

    // ---- algorithm 7 (per partition) ----

    /** Applies one partition of a LeaderAndIsr; returns null if skipped (leader epoch not newer). */
    internal fun applyLeaderAndIsr(p: LeaderAndIsrPartition): RoleTransition? = lock.withLock {
        if (closed) return null
        if (p.leaderEpoch <= leaderEpoch) {
            logger.info("b{} {}: LeaderAndIsr skipped: leader epoch {} not newer than local {}", brokerId, tp, p.leaderEpoch, leaderEpoch)
            return null
        }
        leaderEpoch = p.leaderEpoch
        leader = p.leader
        replicas = p.replicas.toList()
        committedIsr = p.isr.toSet()
        maximalIsr = committedIsr
        zkVersion = p.zkVersion
        // LeaderAndIsr carries no per-partition controller_epoch, and a newer controller may resend
        // a state an older one wrote: learn the znode's value by reading it before the first ISR write.
        znodeControllerEpoch = null
        isrStale = false
        followers.clear()
        if (p.leader == brokerId) {
            role = Role.LEADER
            val now = clock.millis()
            val leo = log.logEndOffset()
            leaderEpochStartOffset = leo
            for (r in replicas) if (r != brokerId) followers[r] = FollowerState(-1L, now, leo, now)
            maybeAdvanceHwLocked()
        } else {
            role = Role.FOLLOWER
            leaderEpochStartOffset = -1L
        }
        stateChanged.signalAll() // pending acks=all waiters see the epoch change => NOT_LEADER
        logger.info(
            "b{} {}: LeaderAndIsr applied: {} leader={} epoch={} isr={} replicas={} zkVersion={} hw={} leo={}",
            brokerId, tp, role, leader, leaderEpoch, committedIsr.sorted(), replicas, zkVersion, highWatermark, log.logEndOffset()
        )
        RoleTransition(tp, role == Role.LEADER, leader, leaderEpoch)
    }

    // ---- algorithm 11 ----

    internal fun produce(key: ByteArray?, value: ByteArray, acks: Short, timeoutMs: Int): ProduceResult = lock.withLock {
        if (closed || role != Role.LEADER) return ProduceResult(ErrorCodes.NOT_LEADER_FOR_PARTITION, -1L)
        if (acks != ACKS_LEADER && acks != ACKS_ALL) return ProduceResult(ErrorCodes.INVALID_REQUIRED_ACKS, -1L)
        if (acks == ACKS_ALL && committedIsr.size < minInsyncReplicas) {
            return ProduceResult(ErrorCodes.NOT_ENOUGH_REPLICAS, -1L)
        }
        val epoch = leaderEpoch
        val offset = log.appendAsLeader(clock.millis(), key, value, epoch)
        maybeAdvanceHwLocked()
        if (acks == ACKS_LEADER) return ProduceResult(ErrorCodes.NONE, offset)

        awaitAckLocked(offset, epoch, timeoutMs)
    }

    /**
     * D15: waits on this partition's Condition (lock held on entry, released while waiting). The
     * deadline is real time (not the injected Clock) so a waiter always terminates; every signal
     * re-checks all exit conditions. Leadership is checked before HW so a waiter never reports
     * success for an epoch it lost (NOT_LEADER = outcome unknown, safe to retry).
     */
    private fun awaitAckLocked(offset: Long, epoch: Int, timeoutMs: Int): ProduceResult {
        var remainingNanos = TimeUnit.MILLISECONDS.toNanos(timeoutMs.toLong())
        while (true) {
            if (closed || role != Role.LEADER || leaderEpoch != epoch) {
                return ProduceResult(ErrorCodes.NOT_LEADER_FOR_PARTITION, -1L)
            }
            if (highWatermark > offset) {
                return if (committedIsr.size >= minInsyncReplicas) ProduceResult(ErrorCodes.NONE, offset)
                else ProduceResult(ErrorCodes.NOT_ENOUGH_REPLICAS_AFTER_APPEND, -1L)
            }
            if (remainingNanos <= 0L) return ProduceResult(ErrorCodes.REQUEST_TIMED_OUT, -1L)
            remainingNanos = stateChanged.awaitNanos(remainingNanos)
        }
    }

    // ---- algorithm 12 ----

    internal fun fetchAsConsumer(offset: Long, maxBytes: Int): FetchResult = lock.withLock {
        if (closed || role != Role.LEADER) return FetchResult(ErrorCodes.NOT_LEADER_FOR_PARTITION, emptyList(), -1L)
        val leo = log.logEndOffset()
        if (offset < 0 || offset > leo) return FetchResult(ErrorCodes.OFFSET_OUT_OF_RANGE, emptyList(), highWatermark)
        if (offset >= highWatermark) return FetchResult(ErrorCodes.NONE, emptyList(), highWatermark)
        FetchResult(ErrorCodes.NONE, log.read(offset, maxBytes, highWatermark), highWatermark)
    }

    // ---- algorithm 9 ----

    internal fun fetchAsFollower(replicaId: Int, offset: Long, currentLeaderEpoch: Int, maxBytes: Int): FetchResult {
        var expand = false
        val result = lock.withLock {
            if (closed || role == Role.NONE) return FetchResult(ErrorCodes.NOT_LEADER_FOR_PARTITION, emptyList(), -1L)
            if (replicaId == brokerId || replicaId !in replicas) {
                return FetchResult(ErrorCodes.REPLICA_NOT_ASSIGNED, emptyList(), -1L)
            }
            checkEpochLocked(currentLeaderEpoch, "fetch from replica $replicaId")?.let {
                return FetchResult(it, emptyList(), -1L)
            }
            if (role != Role.LEADER) return FetchResult(ErrorCodes.NOT_LEADER_FOR_PARTITION, emptyList(), -1L)
            val leo = log.logEndOffset()
            if (offset < 0 || offset > leo) return FetchResult(ErrorCodes.OFFSET_OUT_OF_RANGE, emptyList(), highWatermark)

            val f = followers.getValue(replicaId)
            val now = clock.millis()
            if (offset >= leo) {
                f.lastCaughtUpMs = now
            } else if (offset >= f.lastFetchLeaderLeo) {
                f.lastCaughtUpMs = f.lastFetchTimeMs
            }
            f.lastFetchLeaderLeo = leo
            f.lastFetchTimeMs = now
            f.leo = offset
            if (replicaId !in maximalIsr && offset >= highWatermark && offset >= leaderEpochStartOffset) {
                maximalIsr = maximalIsr + replicaId // joins maximal ISR before the CAS (D7)
                expand = true
                logger.info("b{} {}: replica {} caught up at {} (hw={}): pending ISR expand", brokerId, tp, replicaId, offset, highWatermark)
            }
            maybeAdvanceHwLocked()
            FetchResult(ErrorCodes.NONE, log.read(offset, maxBytes, leo), highWatermark)
        }
        if (expand) onExpandRequested() // outside the partition lock
        return result
    }

    internal fun offsetsForLeaderEpoch(replicaId: Int, currentLeaderEpoch: Int, requestedEpoch: Int): OffsetsForLeaderEpochResponse = lock.withLock {
        if (closed || role == Role.NONE) return epochError(ErrorCodes.NOT_LEADER_FOR_PARTITION)
        if (replicaId >= 0 && replicaId !in replicas) return epochError(ErrorCodes.REPLICA_NOT_ASSIGNED)
        checkEpochLocked(currentLeaderEpoch, "OffsetsForLeaderEpoch from replica $replicaId")?.let { return epochError(it) }
        if (role != Role.LEADER) return epochError(ErrorCodes.NOT_LEADER_FOR_PARTITION)
        val (epoch, endOffset) = log.endOffsetForEpoch(requestedEpoch)
        OffsetsForLeaderEpochResponse(ErrorCodes.NONE, epoch, endOffset)
    }

    // ---- follower side (called by Task 10's fetcher) ----

    /**
     * Appends records fetched from the leader, only if we are still a follower in
     * [expectedLeaderEpoch] (a response from an older epoch is dropped); then HW = min(LEO, leaderHw).
     */
    internal fun appendAsFollower(expectedLeaderEpoch: Int, records: List<Record>, leaderHw: Long): Short = lock.withLock {
        if (closed || role != Role.FOLLOWER) return ErrorCodes.NOT_LEADER_FOR_PARTITION
        if (expectedLeaderEpoch != leaderEpoch) return ErrorCodes.FENCED_LEADER_EPOCH
        log.appendAsFollower(records)
        highWatermark = minOf(log.logEndOffset(), leaderHw)
        ErrorCodes.NONE
    }

    /** Follower truncation (the offset is computed by Task 8's logic); caps HW at the new LEO. */
    internal fun truncateTo(expectedLeaderEpoch: Int, offset: Long): Short = lock.withLock {
        if (closed || role != Role.FOLLOWER) return ErrorCodes.NOT_LEADER_FOR_PARTITION
        if (expectedLeaderEpoch != leaderEpoch) return ErrorCodes.FENCED_LEADER_EPOCH
        val before = log.logEndOffset()
        log.truncateTo(offset)
        highWatermark = minOf(highWatermark, log.logEndOffset())
        logger.info("b{} {}: truncated {} -> {} (epoch {})", brokerId, tp, before, log.logEndOffset(), leaderEpoch)
        ErrorCodes.NONE
    }

    /**
     * Algorithm 8's handshake for this follower replica ([truncateForLeaderEpoch]). The partition
     * lock is held only for the initial role/epoch check and around each truncation (which
     * re-checks that we are still a follower in [expectedLeaderEpoch]); never while [queryLeader]
     * runs. Returns FENCED_LEADER_EPOCH / NOT_LEADER_FOR_PARTITION (log untouched from that point)
     * if the role or epoch moved on. Exceptions from [queryLeader] propagate.
     */
    internal fun truncateAsFollower(
        expectedLeaderEpoch: Int,
        queryLeader: (requestedEpoch: Int) -> Pair<Int, Long>
    ): FollowerTruncationOutcome {
        lock.withLock { followerCheckLocked(expectedLeaderEpoch)?.let { return FollowerTruncationOutcome(it, null) } }
        val result = try {
            truncateForLeaderEpoch(log, queryLeader, truncate = { offset ->
                lock.withLock {
                    followerCheckLocked(expectedLeaderEpoch)?.let { throw FencedTruncation(it) }
                    log.truncateTo(offset)
                    highWatermark = minOf(highWatermark, log.logEndOffset())
                }
            })
        } catch (e: FencedTruncation) {
            logger.info("b{} {}: truncation for epoch {} abandoned: role/epoch changed", brokerId, tp, expectedLeaderEpoch)
            return FollowerTruncationOutcome(e.code, null)
        }
        if (result.truncated) {
            logger.info(
                "b{} {}: truncated {} -> {} (leader epoch {}, final log epoch {}, {} round(s))",
                brokerId, tp, result.fromOffset, result.toOffset, expectedLeaderEpoch, result.finalEpoch, result.rounds
            )
        }
        return FollowerTruncationOutcome(ErrorCodes.NONE, result)
    }

    private fun followerCheckLocked(expectedLeaderEpoch: Int): Short? = when {
        closed || role != Role.FOLLOWER -> ErrorCodes.NOT_LEADER_FOR_PARTITION
        expectedLeaderEpoch != leaderEpoch -> ErrorCodes.FENCED_LEADER_EPOCH
        else -> null
    }

    private class FencedTruncation(val code: Short) : RuntimeException(null, null, false, false)

    // ---- algorithm 10 (the parts under the lock) ----

    /**
     * Builds the next ISR proposal, or null if nothing to change (not leader, stale, or no change).
     * Shrink: maximal-ISR followers with `now - lastCaughtUp > replicaLagTimeMaxMs`; expand: pending
     * adds (maximalIsr - committedIsr). Captures leaderEpoch/zkVersion for the check on completion.
     * A lagging *pending* member (whose expansion outcome may be unknown) also forces a write, even
     * though the proposal then equals committedIsr: it may only leave maximalIsr after a successful
     * CAS (or an adoption), because a write that threw may in fact have put it in the ZK ISR.
     */
    internal fun prepareIsrChange(): IsrProposal? = lock.withLock {
        if (closed || role != Role.LEADER || isrStale) return null
        val now = clock.millis()
        val shrink = maximalIsr.filter { r ->
            r != brokerId && (followers[r]?.let { now - it.lastCaughtUpMs > replicaLagTimeMaxMs } ?: true)
        }.toSet()
        val proposal = maximalIsr - shrink
        if (proposal == committedIsr && shrink.isEmpty()) return null
        IsrProposal(brokerId, leaderEpoch, zkVersion, proposal, shrink, znodeControllerEpoch)
    }

    /** Applies the outcome of the CAS for [proposal], only if no LeaderAndIsr intervened. */
    internal fun completeIsrChange(proposal: IsrProposal, outcome: IsrWriteOutcome) {
        lock.withLock {
            if (closed || role != Role.LEADER || leaderEpoch != proposal.leaderEpoch || zkVersion != proposal.zkVersion) {
                logger.info("b{} {}: ISR change to {} discarded: state changed while in flight", brokerId, tp, proposal.isr.sorted())
                return
            }
            when (outcome) {
                is IsrWriteOutcome.Written -> applyIsrLocked(proposal, outcome.zkVersion, outcome.controllerEpoch, "")
                is IsrWriteOutcome.Conflict -> {
                    val current = outcome.current
                    val mine = current != null && current.value.leader == brokerId && current.value.leaderEpoch == leaderEpoch
                    when {
                        // Our own write already committed (e.g. a replay after ConnectionLoss).
                        mine && current.value.isr.toSet() == proposal.isr ->
                            applyIsrLocked(proposal, current.zkVersion, current.value.controllerEpoch, " (adopted after BadVersion)")
                        // R6: still our leadership and the znode's ISR is covered by maximalIsr (e.g. an
                        // earlier write committed but threw): adopt it and re-propose next round.
                        mine && maximalIsr.containsAll(current.value.isr) -> adoptLocked(current)
                        else -> {
                            // R5, conservative: keep committed and maximal ISR as they are (pending adds
                            // stay counted in HW, so ZK ISR stays a subset of maximalIsr); stop retrying.
                            isrStale = true
                            logger.info(
                                "b{} {}: ISR CAS to {} rejected (fenced), znode now {}; awaiting LeaderAndIsr",
                                brokerId, tp, proposal.isr.sorted(), current
                            )
                        }
                    }
                }
                is IsrWriteOutcome.Failed ->
                    logger.warn("b{} {}: ISR write to {} failed, will retry: {}", brokerId, tp, proposal.isr.sorted(), outcome.error.toString())
            }
        }
    }

    private fun adoptLocked(current: Versioned<PartitionState>) {
        val old = committedIsr
        committedIsr = current.value.isr.toSet()
        zkVersion = current.zkVersion
        znodeControllerEpoch = current.value.controllerEpoch
        logger.info(
            "b{} {}: ISR {} -> {} adopted from znode v{} after BadVersion (maximal ISR {} unchanged)",
            brokerId, tp, old.sorted(), committedIsr.sorted(), zkVersion, maximalIsr.sorted()
        )
        stateChanged.signalAll()
    }

    private fun applyIsrLocked(proposal: IsrProposal, newZkVersion: Int, controllerEpoch: Int, note: String) {
        val old = committedIsr
        committedIsr = proposal.isr
        maximalIsr = maximalIsr - proposal.shrink // shrunk members leave maximal ISR only now
        zkVersion = newZkVersion
        znodeControllerEpoch = controllerEpoch
        val removed = old - proposal.isr
        val added = proposal.isr - old
        if (removed.isNotEmpty()) logger.info("b{} {}: ISR shrink {} -> {} (removed {}){}", brokerId, tp, old.sorted(), proposal.isr.sorted(), removed.sorted(), note)
        if (added.isNotEmpty()) logger.info("b{} {}: ISR expand {} -> {} (added {}){}", brokerId, tp, old.sorted(), proposal.isr.sorted(), added.sorted(), note)
        maybeAdvanceHwLocked()
        stateChanged.signalAll()
    }

    // ---- helpers ----

    /** HW = max(HW, min LEO over maximal ISR), unknown follower LEO = -1; leader's own LEO always counted. */
    private fun maybeAdvanceHwLocked() {
        var min = log.logEndOffset()
        for (r in maximalIsr) if (r != brokerId) min = minOf(min, followers[r]?.leo ?: -1L)
        if (min > highWatermark) {
            highWatermark = min
            stateChanged.signalAll()
        }
    }

    private fun checkEpochLocked(currentLeaderEpoch: Int, what: String): Short? {
        if (currentLeaderEpoch == -1 || currentLeaderEpoch == leaderEpoch) return null
        val code = if (currentLeaderEpoch < leaderEpoch) ErrorCodes.FENCED_LEADER_EPOCH else ErrorCodes.UNKNOWN_LEADER_EPOCH
        val name = if (code == ErrorCodes.FENCED_LEADER_EPOCH) "FENCED_LEADER_EPOCH" else "UNKNOWN_LEADER_EPOCH"
        logger.info("b{} {}: {} rejected {}: epoch {} vs local {}", brokerId, tp, what, name, currentLeaderEpoch, leaderEpoch)
        return code
    }

    private fun epochError(code: Short) = OffsetsForLeaderEpochResponse(code, -1, -1L)

    internal fun logEndOffset(): Long = lock.withLock { log.logEndOffset() }

    internal fun readLocal(from: Long, untilExclusive: Long): List<Record> = lock.withLock {
        val out = mutableListOf<Record>()
        var next = from
        while (next < minOf(untilExclusive, log.logEndOffset())) {
            val batch = log.read(next, 1024 * 1024, untilExclusive)
            if (batch.isEmpty()) break
            out += batch
            next = batch.last().offset + 1
        }
        out
    }

    internal fun snapshot(): PartitionSnapshot = lock.withLock {
        PartitionSnapshot(
            tp, role, leader, leaderEpoch, replicas, committedIsr.sorted(), maximalIsr.sorted(), zkVersion,
            highWatermark, log.logEndOffset(), leaderEpochStartOffset, isrStale,
            followers.mapValues { it.value.leo }.toSortedMap()
        )
    }

    /** Fails pending acks=all waiters (NOT_LEADER) and closes the log. */
    internal fun close() {
        lock.withLock {
            if (closed) return
            closed = true
            role = Role.NONE
            stateChanged.signalAll()
            log.close()
        }
    }

    companion object {
        const val ACKS_LEADER: Short = 1
        const val ACKS_ALL: Short = -1
    }
}
