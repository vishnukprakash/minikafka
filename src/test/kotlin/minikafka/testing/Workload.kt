package minikafka.testing

import minikafka.client.ClientConfig
import minikafka.client.HostPort
import minikafka.client.MiniKafkaClient
import minikafka.proto.ErrorCodes
import org.junit.jupiter.api.Assertions.assertTrue
import java.io.IOException
import java.net.ConnectException
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.LockSupport
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/** A send acknowledged with NONE: [value] was committed at [offset] of [partition]. */
data class Ack(val partition: Int, val seq: Long, val offset: Long, val value: String)

enum class Outcome {
    /** Acknowledged (NONE). */
    ACKED,

    /** Every attempt was rejected before any append (LEADER_NOT_AVAILABLE, NOT_ENOUGH_REPLICAS, ...): never written. */
    FAILED,

    /** Given up (workload stopped) after at least one attempt whose outcome is unknown: may or may not be written. */
    INDETERMINATE
}

/**
 * Everything one producer did for one value. [indeterminateAttempts] counts attempts whose outcome
 * is unknown (timeouts, NOT_ENOUGH_REPLICAS_AFTER_APPEND, NOT_LEADER — it may follow an append —,
 * I/O errors): each may have appended the value once (D20, at-least-once).
 */
data class Send(
    val partition: Int,
    val seq: Long,
    val value: String,
    val outcome: Outcome,
    val attempts: Int,
    val indeterminateAttempts: Int,
    val ack: Ack?
) {
    /** How often [value] may legitimately appear in the log: once per possibly-appending attempt. */
    val maxOccurrences: Int get() = indeterminateAttempts + if (outcome == Outcome.ACKED) 1 else 0
}

/** What a [Workload] did, per partition in send order. */
data class WorkloadHistory(val topic: String, val sends: List<Send>) {
    val acks: List<Ack> get() = sends.mapNotNull { it.ack }
    fun sendsFor(partition: Int): List<Send> = sends.filter { it.partition == partition }.sortedBy { it.seq }
    val partitions: Set<Int> get() = sends.map { it.partition }.toSet()

    fun summary(): String = partitions.sorted().joinToString("; ") { p ->
        val s = sendsFor(p)
        "p$p: ${s.size} sends, ${s.count { it.outcome == Outcome.ACKED }} acked, " +
            "${s.count { it.outcome == Outcome.FAILED }} failed, ${s.count { it.outcome == Outcome.INDETERMINATE }} indeterminate, " +
            "${s.sumOf { it.indeterminateAttempts }} unknown-outcome attempts"
    }
}

/**
 * A Jepsen-style producer workload: one sequential producer thread per partition, each sending
 * `p<p>-<seq>` for seq = 0, 1, 2, ... to its explicit partition with [acks], and recording every
 * outcome. A value is retried (same value) until it is acknowledged or the workload is stopped, so
 * delivery is at-least-once; retries after an unknown outcome may duplicate (D20) and are recorded
 * as such.
 *
 * The client runs with `maxRetries = 0`: the workload does the retrying itself, so it sees every
 * attempt's outcome (the client's own retries would hide an I/O-failed attempt that appended).
 * After any error the metadata is refreshed; if no known broker answers, the client is rebuilt from
 * [bootstrap] (brokers restarted in tests get new ports).
 */
class Workload(
    private val topic: String,
    private val partitions: List<Int>,
    private val acks: Short,
    private val bootstrap: () -> List<HostPort>,
    clientConfig: ClientConfig,
    private val retryBackoffMs: Long = 50,
    private val name: String = "workload"
) : AutoCloseable {
    private val config = clientConfig.copy(maxRetries = 0)
    private val sends = CopyOnWriteArrayList<Send>()
    private val acked = AtomicInteger()
    private val ackedPerPartition = partitions.associateWith { AtomicInteger() }
    private val threads = mutableListOf<Thread>()
    private val errors = CopyOnWriteArrayList<Throwable>()

    @Volatile
    private var stopped = false

    fun start(): Workload {
        check(threads.isEmpty()) { "already started" }
        for (p in partitions) {
            threads += Thread({ produceLoop(p) }, "$name-p$p").apply { isDaemon = true; start() }
        }
        return this
    }

    fun ackCount(): Int = acked.get()

    fun ackCount(partition: Int): Int = ackedPerPartition.getValue(partition).get()

    /** Waits until [n] more acks than now have arrived: on each partition in [on], or in total if [on] is null. */
    fun awaitAcks(n: Int, timeout: Duration = 30.seconds, on: List<Int>? = null) {
        val start = on?.associateWith(::ackCount)
        val total = ackCount()
        eventually(timeout) {
            if (start == null) {
                assertTrue(ackCount() >= total + n) { "$name: ${ackCount() - total} of $n new acks; ${history().summary()}" }
            } else {
                for ((p, c) in start) assertTrue(ackCount(p) >= c + n) { "$name p$p: ${ackCount(p) - c} of $n new acks; ${history().summary()}" }
            }
        }
    }

    /** Stops every producer (the in-flight value is recorded as failed/indeterminate) and waits for them. */
    fun stop(): WorkloadHistory {
        stopped = true
        threads.forEach { it.join(TimeUnit.SECONDS.toMillis(30)) }
        val alive = threads.filter { it.isAlive }.map { it.name }
        check(alive.isEmpty()) { "$name: producers still running after 30s: $alive" }
        check(errors.isEmpty()) { "$name: producer crashed: ${errors.first()}" }
        return history()
    }

    fun history(): WorkloadHistory = WorkloadHistory(topic, sends.toList())

    override fun close() {
        if (!stopped) runCatching { stop() }
    }

    private fun produceLoop(partition: Int) {
        var client = MiniKafkaClient(bootstrap(), config)
        try {
            var seq = 0L
            while (!stopped) {
                val value = "p$partition-$seq"
                var attempts = 0
                var unknown = 0
                var ack: Ack? = null
                while (ack == null && !stopped) {
                    if (attempts > 0) backoff()
                    attempts++
                    val code = try {
                        val r = client.produce(topic, null, value.toByteArray(), acks, partition)
                        if (r.errorCode == ErrorCodes.NONE) ack = Ack(partition, seq, r.offset, value)
                        r.errorCode
                    } catch (_: ConnectException) {
                        ErrorCodes.LEADER_NOT_AVAILABLE // refused before anything was sent: not appended
                    } catch (_: IOException) {
                        null
                    }
                    if (ack != null) break
                    if (code == null || code !in DEFINITELY_NOT_APPENDED) unknown++
                    client = refreshed(client)
                }
                val outcome = when {
                    ack != null -> Outcome.ACKED
                    unknown > 0 -> Outcome.INDETERMINATE
                    else -> Outcome.FAILED
                }
                sends += Send(partition, seq, value, outcome, attempts, unknown, ack)
                if (ack != null) {
                    acked.incrementAndGet()
                    ackedPerPartition.getValue(partition).incrementAndGet()
                }
                seq++
            }
        } catch (t: Throwable) {
            errors += t
        } finally {
            runCatching { client.close() }
        }
    }

    /** Refreshes the metadata; if no broker the client knows answers, rebuilds it on the current bootstrap list. */
    private fun refreshed(client: MiniKafkaClient): MiniKafkaClient = try {
        client.metadata()
        client
    } catch (_: IOException) {
        runCatching { client.close() }
        MiniKafkaClient(bootstrap().ifEmpty { listOf(HostPort("127.0.0.1", 1)) }, config)
    }

    private fun backoff() {
        val deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(retryBackoffMs)
        while (!stopped) {
            val left = deadline - System.nanoTime()
            if (left <= 0) return
            LockSupport.parkNanos(left)
        }
    }

    companion object {
        /** Error codes a broker (or the client) returns only *before* appending: the value was not written by that attempt. */
        val DEFINITELY_NOT_APPENDED = setOf(
            ErrorCodes.LEADER_NOT_AVAILABLE,
            ErrorCodes.NOT_ENOUGH_REPLICAS,
            ErrorCodes.UNKNOWN_TOPIC,
            ErrorCodes.UNKNOWN_PARTITION,
            ErrorCodes.INVALID_REQUIRED_ACKS
        )
    }
}
