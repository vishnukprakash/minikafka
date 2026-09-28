package minikafka.broker

import minikafka.log.Record
import minikafka.model.BrokerInfo
import minikafka.model.TopicPartition
import minikafka.net.Connection
import minikafka.proto.ApiKeys
import minikafka.proto.ErrorCodes
import minikafka.proto.FetchRequest
import minikafka.proto.FetchResponse
import minikafka.proto.OffsetsForLeaderEpochRequest
import minikafka.proto.OffsetsForLeaderEpochResponse
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.io.IOException
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * Broker id -> advertised endpoint. The server backs it with ZooKeeper's `/brokers/ids` (the
 * broker package has no ZooKeeper dependency). [refresh] = bypass any cache: the fetcher asks for
 * it after a failed connection or request, since a restarted leader may have a new port.
 * Returns null if the broker is not registered; may throw (treated like an I/O error).
 */
fun interface BrokerResolver {
    fun resolve(brokerId: Int, refresh: Boolean): BrokerInfo?
}

/** The follower's connection to its leader (TCP in production, a fake in unit tests). Not thread-safe. */
interface LeaderClient : Closeable {
    fun offsetsForLeaderEpoch(request: OffsetsForLeaderEpochRequest): OffsetsForLeaderEpochResponse

    /** FETCH; decoding verifies each record's CRC (a mismatch throws `CorruptRecordException`, an IOException). */
    fun fetch(request: FetchRequest): FetchResponse
}

fun interface LeaderClientFactory {
    fun connect(leader: BrokerInfo): LeaderClient

    companion object {
        /** A [LeaderClient] over one [Connection] (never reused after it throws: the fetcher closes it and reconnects). */
        fun tcp(socketTimeoutMs: Int): LeaderClientFactory = LeaderClientFactory { leader ->
            val connection = Connection(leader.host, leader.port, socketTimeoutMs)
            object : LeaderClient {
                override fun offsetsForLeaderEpoch(request: OffsetsForLeaderEpochRequest) =
                    connection.request(ApiKeys.OFFSETS_FOR_LEADER_EPOCH, request::encode, OffsetsForLeaderEpochResponse::decode)

                override fun fetch(request: FetchRequest) =
                    connection.request(ApiKeys.FETCH, request::encode, FetchResponse::decode)

                override fun close() = connection.close()
            }
        }
    }
}

/**
 * Fetcher timings: [backoffMs] after an empty response (and the poll interval while paused);
 * errors back off exponentially from [backoffMs], doubling up to [maxBackoffMs].
 */
data class FetcherSettings(
    val backoffMs: Long = 50,
    val maxBackoffMs: Long = 1_000,
    val maxBytes: Int = 1 shl 20
) {
    companion object {
        fun from(config: BrokerConfig) =
            FetcherSettings(config.replicaFetchBackoffMs, config.replicaFetchMaxBackoffMs, config.replicaFetchMaxBytes)
    }
}

enum class FetcherBackoff { EMPTY, ERROR, PAUSED }

/**
 * Algorithm 8: replicates one partition from its leader, for exactly one leader epoch. Runs on its
 * own daemon thread `b<id>-fetcher-<topic>-<p>`; [ReplicaFetcherManager] starts a new one on every
 * follower transition (also an epoch bump with the same leader), so a fetcher never outlives its
 * epoch: any sign that the epoch moved on (FENCED_LEADER_EPOCH from the leader, or the local
 * replica no longer being a follower in [leaderEpoch]) ends the thread.
 *
 * Loop: connect (endpoint via [BrokerResolver]) → leader-epoch handshake ([ReplicaManager.truncateFollower],
 * skipped for an empty log) → FETCH(replicaId=me, offset=LEO, currentLeaderEpoch) → append + HW.
 * Empty response ⇒ back off [FetcherSettings.backoffMs]. UNKNOWN_LEADER_EPOCH / NOT_LEADER /
 * I/O error (incl. a corrupt record: the whole response is discarded) ⇒ capped exponential backoff;
 * a failed connection is closed and recreated with a refreshed endpoint. OFFSET_OUT_OF_RANGE or a
 * batch the log rejects ⇒ redo the handshake. While the broker's fetchers are paused (test hook)
 * no request is sent. Never holds a partition lock across I/O: every local step goes through the
 * [ReplicaManager], which locks only around the local operation.
 */
class ReplicaFetcher internal constructor(
    private val replicaManager: ReplicaManager,
    val tp: TopicPartition,
    val leaderId: Int,
    val leaderEpoch: Int,
    private val resolver: BrokerResolver,
    private val clientFactory: LeaderClientFactory,
    private val settings: FetcherSettings,
    private val onBackoff: (FetcherBackoff, Long) -> Unit = { _, _ -> }
) {
    private val logger = LoggerFactory.getLogger(ReplicaFetcher::class.java)
    private val brokerId = replicaManager.config.brokerId
    val threadName = "b$brokerId-fetcher-${tp.topic}-${tp.partition}"
    private val thread = Thread(::run, threadName).apply { isDaemon = true }
    private val waitLock = ReentrantLock()
    private val wakeup = waitLock.newCondition()
    private val clientLock = Any()

    /** Written under [clientLock]; read lock-free by the fetcher thread (shutdown may null and close it). */
    @Volatile
    private var client: LeaderClient? = null

    @Volatile
    private var running = true

    val isAlive: Boolean get() = thread.isAlive

    private enum class Step { CONTINUE, IDLE, ERROR, REHANDSHAKE, STOP }

    /** A non-NONE OFFSETS_FOR_LEADER_EPOCH answer, thrown out of the handshake's leader query (Task 8 contract). */
    private class LeaderError(val code: Short) : RuntimeException("leader answered error $code", null, false, false)

    fun start() = thread.start()

    /**
     * Asks the thread to stop without blocking: wakes any backoff wait, closes the leader
     * connection (unblocking a socket read) and interrupts. Log writes use RandomAccessFile, which
     * an interrupt cannot close. A fetcher that lingers is harmless: all its local effects are
     * fenced by [leaderEpoch].
     */
    fun shutdown() {
        running = false
        waitLock.withLock { wakeup.signalAll() }
        closeClient()
        thread.interrupt()
    }

    /** Joins the thread for at most [timeoutMs]; true if it has exited. */
    fun awaitShutdown(timeoutMs: Long): Boolean {
        if (Thread.currentThread() !== thread) thread.join(timeoutMs)
        return !thread.isAlive
    }

    // Confined to the fetcher thread.
    private var needHandshake = true
    private var refreshEndpoint = false

    private fun run() {
        logger.info("b{} {}: fetcher started (leader {}, epoch {})", brokerId, tp, leaderId, leaderEpoch)
        var errorBackoffMs = settings.backoffMs
        try {
            while (running) {
                if (replicaManager.fetchersPaused()) {
                    backoff(FetcherBackoff.PAUSED, settings.backoffMs)
                    continue
                }
                val step = try {
                    val c = client ?: connect()
                    if (needHandshake) handshake(c) else fetchOnce(c)
                } catch (e: InterruptedException) {
                    Step.STOP
                } catch (e: LeaderError) {
                    if (e.code == ErrorCodes.FENCED_LEADER_EPOCH) {
                        logger.info("b{} {}: handshake fenced by leader {} (epoch {} is stale); awaiting LeaderAndIsr", brokerId, tp, leaderId, leaderEpoch)
                        Step.STOP
                    } else {
                        logger.info("b{} {}: handshake: leader {} answered error {}; backing off", brokerId, tp, leaderId, e.code)
                        if (e.code != ErrorCodes.UNKNOWN_LEADER_EPOCH) dropConnection()
                        Step.ERROR
                    }
                } catch (e: IOException) {
                    if (running) logger.info("b{} {}: request to leader {} failed: {}; reconnecting after backoff", brokerId, tp, leaderId, e.toString())
                    dropConnection()
                    Step.ERROR
                } catch (e: Exception) {
                    if (running) logger.warn("b{} {}: fetcher step failed: {}", brokerId, tp, e.toString())
                    dropConnection()
                    Step.ERROR
                }
                when (step) {
                    Step.CONTINUE -> errorBackoffMs = settings.backoffMs
                    Step.IDLE -> {
                        errorBackoffMs = settings.backoffMs
                        backoff(FetcherBackoff.EMPTY, settings.backoffMs)
                    }
                    Step.ERROR, Step.REHANDSHAKE -> {
                        if (step == Step.REHANDSHAKE) needHandshake = true
                        backoff(FetcherBackoff.ERROR, errorBackoffMs)
                        errorBackoffMs = minOf(errorBackoffMs * 2, settings.maxBackoffMs)
                    }
                    Step.STOP -> return
                }
            }
        } catch (_: InterruptedException) {
            // shutdown() interrupted a backoff wait
        } finally {
            closeClient()
            logger.info("b{} {}: fetcher for epoch {} stopped", brokerId, tp, leaderEpoch)
        }
    }

    private fun connect(): LeaderClient {
        val refresh = refreshEndpoint
        refreshEndpoint = true // until a connection succeeds
        val info = resolver.resolve(leaderId, refresh) ?: throw IOException("leader $leaderId is not registered")
        val c = clientFactory.connect(info)
        synchronized(clientLock) {
            if (!running) {
                c.close()
                throw InterruptedException("fetcher stopped")
            }
            client = c
        }
        refreshEndpoint = false
        return c
    }

    /** Algorithm 8's leader-epoch handshake; the follower's truncation is logged by the replica manager. */
    private fun handshake(c: LeaderClient): Step {
        val outcome = replicaManager.truncateFollower(tp, leaderEpoch) { requestedEpoch ->
            val r = c.offsetsForLeaderEpoch(OffsetsForLeaderEpochRequest(tp.topic, tp.partition, brokerId, leaderEpoch, requestedEpoch))
            if (r.errorCode != ErrorCodes.NONE) throw LeaderError(r.errorCode)
            r.leaderEpoch to r.endOffset
        }
        if (outcome.errorCode != ErrorCodes.NONE) return localRoleChanged(outcome.errorCode)
        // The final round may answer NONE without re-checking our epoch: FETCH fencing covers that.
        needHandshake = false
        return Step.CONTINUE
    }

    private fun fetchOnce(c: LeaderClient): Step {
        val leo = replicaManager.logEndOffset(tp) ?: return localRoleChanged(ErrorCodes.NOT_LEADER_FOR_PARTITION)
        val response = c.fetch(FetchRequest(tp.topic, tp.partition, leo, settings.maxBytes, brokerId, leaderEpoch))
        return when (response.errorCode) {
            ErrorCodes.NONE -> {
                val records = response.records.map { Record(it.offset, it.leaderEpoch, it.timestamp, it.key, it.value) }
                val code = try {
                    replicaManager.appendAsFollower(tp, leaderEpoch, records, response.highWatermark)
                } catch (e: IllegalArgumentException) {
                    logger.info("b{} {}: leader {} batch rejected at LEO {} ({}); redoing handshake", brokerId, tp, leaderId, leo, e.message)
                    return Step.REHANDSHAKE
                }
                when {
                    code != ErrorCodes.NONE -> localRoleChanged(code)
                    records.isEmpty() -> Step.IDLE
                    else -> Step.CONTINUE
                }
            }
            ErrorCodes.FENCED_LEADER_EPOCH -> {
                logger.info("b{} {}: fetch fenced by leader {} (epoch {} is stale); awaiting LeaderAndIsr", brokerId, tp, leaderId, leaderEpoch)
                Step.STOP
            }
            ErrorCodes.OFFSET_OUT_OF_RANGE -> {
                logger.info("b{} {}: leader {} answered OFFSET_OUT_OF_RANGE for {}; redoing handshake", brokerId, tp, leaderId, leo)
                Step.REHANDSHAKE
            }
            ErrorCodes.UNKNOWN_LEADER_EPOCH -> Step.ERROR // the leader has not applied our epoch yet
            else -> {
                // NOT_LEADER / REPLICA_NOT_ASSIGNED / ...: maybe not applied yet, or a stale endpoint.
                logger.info("b{} {}: leader {} answered error {}; backing off", brokerId, tp, leaderId, response.errorCode)
                dropConnection()
                Step.ERROR
            }
        }
    }

    private fun localRoleChanged(code: Short): Step {
        logger.info("b{} {}: no longer a follower in epoch {} (code {}); fetcher exits", brokerId, tp, leaderEpoch, code)
        return Step.STOP
    }

    private fun backoff(reason: FetcherBackoff, ms: Long) {
        onBackoff(reason, ms)
        waitLock.withLock {
            var remaining = TimeUnit.MILLISECONDS.toNanos(ms)
            while (running && remaining > 0) remaining = wakeup.awaitNanos(remaining)
        }
    }

    /** Never reuse a connection after a failure: close it; the next connect refreshes the endpoint. */
    private fun dropConnection() {
        closeClient()
        refreshEndpoint = true
    }

    private fun closeClient() {
        val c = synchronized(clientLock) { client.also { client = null } }
        c?.let { runCatching { it.close() } }
    }
}
