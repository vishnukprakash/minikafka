package minikafka.testing

import minikafka.log.Record
import minikafka.model.TopicPartition

/**
 * One partition as the replicas hold it: the [leader] (from ZooKeeper), its [highWatermark], the
 * ZooKeeper [isr], and every *running* assigned replica's whole local log ([logs], by broker id).
 */
data class PartitionReplicas(
    val partition: Int,
    val leader: Int,
    val highWatermark: Long,
    val isr: List<Int>,
    val logs: Map<Int, List<Record>>
)

/** Reads every partition of [topic] from the replicas' local logs (test hook `readLocal`), regardless of role. */
fun TestCluster.replicaViews(topic: String): List<PartitionReplicas> = partitionsOf(topic).map { p ->
    val tp = TopicPartition(topic, p)
    val state = checkNotNull(partitionState(topic, p)) { "no state for $tp" }.value
    val hw = if (state.leader >= 0 && isRunning(state.leader)) {
        broker(state.leader).snapshot().partitions.firstOrNull { it.tp == tp }?.highWatermark ?: -1L
    } else -1L
    val logs = replicasOf(topic, p).filter(::isRunning)
        .associateWith { broker(it).replicaManager.readLocal(tp, 0, Long.MAX_VALUE) }
    PartitionReplicas(p, state.leader, hw, state.isr, logs)
}

/**
 * The Jepsen-lite acked-write checker. Given what a [Workload] did and what the replicas hold, it
 * returns every violation of:
 *
 * 1. **No acked loss:** every acknowledged (partition, offset) is below the leader's high watermark
 *    and holds the acknowledged value on every ISR replica.
 * 2. **No fabrication:** every value in any replica's log (or read back by a consumer, [consumed])
 *    was attempted by the workload on that partition.
 * 3. **Duplicates only from unknown outcomes:** in the committed log a value appears at most once
 *    per attempt that may have appended it ([Send.maxOccurrences]): a definitely-failed value never,
 *    an acked value without timed-out/unknown attempts exactly once.
 * 4. **Order:** the first occurrences of each producer's values in the committed log are in send order.
 * 5. **Replicas agree:** every ISR replica holds exactly the leader's records (offset, leader epoch,
 *    key, value) below the high watermark; with [fullyReplicated], every running replica's whole
 *    log equals the leader's.
 */
object AckedWriteChecker {
    fun check(
        history: WorkloadHistory,
        replicas: List<PartitionReplicas>,
        fullyReplicated: Boolean = true,
        consumed: Map<Int, List<String>> = emptyMap()
    ): List<String> {
        val violations = mutableListOf<String>()
        val byPartition = replicas.associateBy { it.partition }
        for (p in (history.partitions + consumed.keys).sorted()) {
            val view = byPartition[p]
            if (view == null) {
                violations += "p$p: no replica view"
                continue
            }
            violations += checkPartition(history.sendsFor(p), view, fullyReplicated, consumed[p].orEmpty())
        }
        return violations
    }

    /** Assertion message: the violations, one per line, plus what the workload did. */
    fun describe(history: WorkloadHistory, violations: List<String>): String =
        "${violations.size} violation(s):\n" + violations.take(50).joinToString("\n") { "  - $it" } +
            (if (violations.size > 50) "\n  ... ${violations.size - 50} more" else "") + "\nworkload: ${history.summary()}"

    private fun checkPartition(sends: List<Send>, view: PartitionReplicas, fullyReplicated: Boolean, consumed: List<String>): List<String> {
        val out = mutableListOf<String>()
        val p = view.partition
        val leaderLog = view.logs[view.leader]
        if (leaderLog == null) return listOf("p$p: leader ${view.leader} is not running; cannot check")
        val hw = view.highWatermark
        val committed = leaderLog.filter { it.offset < hw }
        val attempted = sends.associateBy { it.value }

        // 5. Replicas agree below HW (ISR) / everywhere (fully replicated).
        for (r in view.isr) {
            val log = view.logs[r]
            if (log == null) {
                out += "p$p: ISR replica $r is not running; cannot check"
                continue
            }
            if (r != view.leader) firstDifference(committed, log.filter { it.offset < hw })?.let {
                out += "p$p: ISR replica $r differs from leader ${view.leader} below HW $hw: $it"
            }
        }
        if (fullyReplicated) {
            for ((r, log) in view.logs) {
                if (r != view.leader) firstDifference(leaderLog, log)?.let {
                    out += "p$p: replica $r is not identical to leader ${view.leader} after full replication: $it"
                }
            }
        }

        // 1. Acked writes survive on every ISR replica.
        val isrByOffset = view.isr.mapNotNull { r -> view.logs[r]?.let { log -> r to log.associateBy { it.offset } } }
        for (send in sends) {
            val ack = send.ack ?: continue
            if (ack.offset >= hw) out += "p$p: acked ${ack.value} at offset ${ack.offset} is not below the leader HW $hw"
            for ((r, byOffset) in isrByOffset) {
                val value = byOffset[ack.offset]?.let { String(it.value) }
                if (value != ack.value) out += "p$p: acked ${ack.value} at offset ${ack.offset} LOST on ISR replica $r (holds ${value ?: "nothing"})"
            }
        }

        // 2. No fabricated values, in any replica or in what a consumer read.
        for ((r, log) in view.logs) {
            log.map { String(it.value) }.filter { it !in attempted }.distinct().take(5).forEach {
                out += "p$p: replica $r holds a value never attempted on this partition: $it"
            }
        }
        consumed.filter { it !in attempted }.distinct().take(5).forEach { out += "p$p: consumer read a value never attempted: $it" }

        // 3. Duplicates only for sends with unknown-outcome attempts.
        val counts = committed.groupingBy { String(it.value) }.eachCount()
        for ((value, n) in counts) {
            val send = attempted[value] ?: continue
            if (n > send.maxOccurrences) {
                out += "p$p: $value appears $n times below HW but only ${send.maxOccurrences} attempt(s) could have appended it " +
                    "(${send.outcome}, ${send.attempts} attempts, ${send.indeterminateAttempts} unknown)"
            }
        }

        // 4. First occurrences keep the producer's order.
        val seen = HashSet<String>()
        var lastSeq = -1L
        for (record in committed) {
            val value = String(record.value)
            val send = attempted[value] ?: continue
            if (!seen.add(value)) continue
            if (send.seq <= lastSeq) out += "p$p: first occurrence of $value (seq ${send.seq}) at offset ${record.offset} follows seq $lastSeq"
            lastSeq = maxOf(lastSeq, send.seq)
        }
        return out
    }

    private fun describe(r: Record) = "${r.offset}/e${r.leaderEpoch}/${r.key?.let(::String)}/${String(r.value)}"

    /** Null if equal, else the first differing position (or the length mismatch). */
    private fun firstDifference(expected: List<Record>, actual: List<Record>): String? {
        for (i in 0 until minOf(expected.size, actual.size)) {
            val e = describe(expected[i])
            val a = describe(actual[i])
            if (e != a) return "at index $i expected $e but was $a"
        }
        if (expected.size != actual.size) return "expected ${expected.size} records but was ${actual.size}"
        return null
    }
}
