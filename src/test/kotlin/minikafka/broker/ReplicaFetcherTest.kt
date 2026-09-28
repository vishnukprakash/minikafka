package minikafka.broker

import minikafka.io.CorruptRecordException
import minikafka.model.BrokerInfo
import minikafka.proto.ErrorCodes
import minikafka.proto.FetchRequest
import minikafka.proto.FetchResponse
import minikafka.proto.FetchedRecord
import minikafka.proto.OffsetsForLeaderEpochRequest
import minikafka.proto.OffsetsForLeaderEpochResponse
import minikafka.testing.alwaysFor
import minikafka.testing.eventually
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNotSame
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File
import java.io.IOException
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/**
 * The follower fetch loop (algorithm 8) against a fake leader connection that delegates to a real
 * leader [ReplicaManager] (broker 1) and can inject faults per request. The follower is broker 2.
 */
class ReplicaFetcherTest {
    @TempDir
    lateinit var dir: File

    private val leader by lazy { ReplicaHarness(File(dir, "leader"), brokerId = 1) }
    private val follower by lazy { ReplicaHarness(File(dir, "follower"), brokerId = 2) }
    private val fetchers = CopyOnWriteArrayList<ReplicaFetcher>()
    private val backoffs = ConcurrentLinkedQueue<Pair<FetcherBackoff, Long>>()
    private val settings = FetcherSettings(backoffMs = 5, maxBackoffMs = 20, maxBytes = 1 shl 20)

    /** Fault injection: consulted before each FETCH; return a response or throw to replace the real one, null = real. */
    private val fetchFaults = ConcurrentLinkedQueue<(FetchRequest) -> FetchResponse?>()
    private val fetchRequests = CopyOnWriteArrayList<FetchRequest>()
    private val epochRequests = CopyOnWriteArrayList<OffsetsForLeaderEpochRequest>()
    private val connects = AtomicInteger()
    private val closes = AtomicInteger()

    private inner class FakeLeaderClient : LeaderClient {
        override fun offsetsForLeaderEpoch(request: OffsetsForLeaderEpochRequest): OffsetsForLeaderEpochResponse {
            epochRequests += request
            return leader.rm.offsetsForLeaderEpoch(leader.tp, request.replicaId, request.currentLeaderEpoch, request.requestedEpoch)
        }

        override fun fetch(request: FetchRequest): FetchResponse {
            fetchRequests += request
            fetchFaults.poll()?.let { fault -> fault(request)?.let { return it } }
            val r = leader.rm.fetchAsFollower(leader.tp, request.replicaId, request.offset, request.currentLeaderEpoch, request.maxBytes)
            return FetchResponse(r.errorCode, r.highWatermark, r.records.map { FetchedRecord(it.offset, it.leaderEpoch, it.timestamp, it.key, it.value) })
        }

        override fun close() {
            closes.incrementAndGet()
        }
    }

    private val factory = LeaderClientFactory { connects.incrementAndGet(); FakeLeaderClient() }
    private val resolver = BrokerResolver { id, _ -> BrokerInfo(id, "fake", 1) }

    @AfterEach
    fun tearDown() {
        fetchers.forEach { it.shutdown(); it.awaitShutdown(2_000) }
        follower.close()
        leader.close()
    }

    /** Leader 1 / follower 2 at [epoch], replicas [1,2], ISR [1,2]. */
    private fun roles(epoch: Int = 0) {
        assertEquals(ErrorCodes.NONE, leader.leaderAndIsr(1, epoch, listOf(1, 2), listOf(1, 2)))
        assertEquals(ErrorCodes.NONE, follower.leaderAndIsr(1, epoch, listOf(1, 2), listOf(1, 2)))
    }

    private fun startFetcher(epoch: Int = 0, s: FetcherSettings = settings): ReplicaFetcher =
        ReplicaFetcher(follower.rm, follower.tp, 1, epoch, resolver, factory, s) { reason, ms -> backoffs += reason to ms }
            .also { fetchers += it; it.start() }

    @Test
    fun `replicates the leader's records and propagates the high watermark`() {
        roles()
        repeat(3) { leader.produce("m$it") }
        startFetcher()
        eventually(5.seconds) {
            val s = follower.state()
            assertEquals(3L, s.logEndOffset)
            assertEquals(3L, s.highWatermark)
        }
        assertEquals(3L, leader.state().highWatermark)
        assertEquals(listOf("m0", "m1", "m2"), follower.rm.readLocal(follower.tp, 0, 3).map { String(it.value) })
        assertTrue(fetchRequests.all { it.replicaId == 2 && it.currentLeaderEpoch == 0 })
        assertTrue(epochRequests.isEmpty()) { "empty follower log skips the handshake" }
    }

    @Test
    fun `an idle fetcher backs off by the empty-response backoff between fetches`() {
        roles()
        startFetcher()
        eventually(5.seconds) { assertTrue(backoffs.count { it.first == FetcherBackoff.EMPTY } >= 3) }
        assertTrue(backoffs.filter { it.first == FetcherBackoff.EMPTY }.all { it.second == settings.backoffMs })
        assertTrue(backoffs.none { it.first == FetcherBackoff.ERROR })
    }

    @Test
    fun `FENCED_LEADER_EPOCH from the leader stops the fetcher`() {
        roles()
        fetchFaults += { FetchResponse(ErrorCodes.FENCED_LEADER_EPOCH, -1L, emptyList()) }
        val fetcher = startFetcher()
        assertTrue(fetcher.awaitShutdown(5_000)) { "fetcher thread should exit" }
        assertEquals(1, fetchRequests.size)
        assertEquals(1, closes.get())
    }

    @Test
    fun `a fetcher from an older epoch is fenced by the leader and stops`() {
        leader.leaderAndIsr(1, 1, listOf(1, 2), listOf(1, 2))
        follower.leaderAndIsr(1, 0, listOf(1, 2), listOf(1, 2))
        val fetcher = startFetcher(epoch = 0)
        assertTrue(fetcher.awaitShutdown(5_000))
        assertEquals(0L, follower.state().logEndOffset)
    }

    @Test
    fun `a corrupt response is discarded, the connection recreated, and the fetcher backs off`() {
        roles()
        repeat(2) { leader.produce("m$it") }
        fetchFaults += { throw CorruptRecordException("bad crc") }
        startFetcher()
        eventually(5.seconds) { assertEquals(2L, follower.state().logEndOffset) }
        assertEquals(2, connects.get(), "reconnected after the corrupt response")
        assertTrue(closes.get() >= 1)
        assertEquals(FetcherBackoff.ERROR to settings.backoffMs, backoffs.first())
        assertEquals(listOf("m0", "m1"), follower.rm.readLocal(follower.tp, 0, 2).map { String(it.value) })
    }

    @Test
    fun `IO errors back off exponentially up to the cap, then reset after a success`() {
        roles()
        repeat(5) { fetchFaults += { throw IOException("connection reset") } }
        startFetcher()
        eventually(5.seconds) { assertTrue(backoffs.any { it.first == FetcherBackoff.EMPTY }) }
        val errors = backoffs.takeWhile { it.first == FetcherBackoff.ERROR }.map { it.second }
        assertEquals(listOf(5L, 10L, 20L, 20L, 20L), errors)
        assertEquals(6, connects.get(), "a new connection after every IO error")
    }

    @Test
    fun `UNKNOWN_LEADER_EPOCH backs off and retries on the same connection`() {
        roles()
        leader.produce("m0")
        repeat(2) { fetchFaults += { FetchResponse(ErrorCodes.UNKNOWN_LEADER_EPOCH, -1L, emptyList()) } }
        startFetcher()
        eventually(5.seconds) { assertEquals(1L, follower.state().logEndOffset) }
        assertEquals(listOf(5L, 10L), backoffs.take(2).map { it.second })
        assertEquals(1, connects.get())
    }

    @Test
    fun `OFFSET_OUT_OF_RANGE redoes the leader-epoch handshake`() {
        roles()
        repeat(2) { leader.produce("m$it") }
        startFetcher()
        eventually(5.seconds) { assertEquals(2L, follower.state().logEndOffset) }
        assertTrue(epochRequests.isEmpty())
        fetchFaults += { FetchResponse(ErrorCodes.OFFSET_OUT_OF_RANGE, -1L, emptyList()) }
        eventually(5.seconds) { assertEquals(1, epochRequests.size) }
        val q = epochRequests.single()
        assertEquals(0, q.requestedEpoch)
        assertEquals(0, q.currentLeaderEpoch)
        assertEquals(2, q.replicaId)
        leader.produce("m2")
        eventually(5.seconds) { assertEquals(3L, follower.state().logEndOffset) }
    }

    @Test
    fun `repeated OFFSET_OUT_OF_RANGE cycles escalate the error backoff despite successful handshakes`() {
        roles()
        repeat(2) { leader.produce("m$it") }
        startFetcher()
        eventually(5.seconds) { assertEquals(2L, follower.state().logEndOffset) }
        backoffs.clear()
        repeat(4) { fetchFaults += { FetchResponse(ErrorCodes.OFFSET_OUT_OF_RANGE, -1L, emptyList()) } }
        eventually(5.seconds) { assertTrue(epochRequests.size >= 4 && backoffs.any { it.first == FetcherBackoff.EMPTY }) }
        assertEquals(listOf(5L, 10L, 20L, 20L), backoffs.filter { it.first == FetcherBackoff.ERROR }.map { it.second })
        // ...and a FETCH answered NONE resets it.
        fetchFaults += { FetchResponse(ErrorCodes.UNKNOWN_LEADER_EPOCH, -1L, emptyList()) }
        eventually(5.seconds) { assertEquals(5L, backoffs.filter { it.first == FetcherBackoff.ERROR }.getOrNull(4)?.second) }
    }

    @Test
    fun `a local epoch bump fences the append and stops the fetcher even if the leader accepts the old epoch`() {
        roles()
        leader.produce("m0")
        // Before the first FETCH is answered, this replica applies epoch 1; the leader is still at epoch 0.
        fetchFaults += { follower.leaderAndIsr(1, 1, listOf(1, 2), listOf(1, 2)); null }
        val fetcher = startFetcher(epoch = 0)
        assertTrue(fetcher.awaitShutdown(5_000), "fetcher stops on the local fence")
        assertEquals(1, fetchRequests.size)
        assertEquals(0L, follower.state().logEndOffset, "nothing appended")
        assertEquals(1, follower.state().leaderEpoch)
    }

    @Test
    fun `a batch that does not start at the log end offset redoes the handshake`() {
        roles()
        repeat(2) { leader.produce("m$it") }
        startFetcher()
        eventually(5.seconds) { assertEquals(2L, follower.state().logEndOffset) }
        fetchFaults += { FetchResponse(ErrorCodes.NONE, 2L, listOf(FetchedRecord(7L, 0, 1L, null, "bogus".toByteArray()))) }
        eventually(5.seconds) { assertEquals(1, epochRequests.size) }
        assertEquals(2L, follower.state().logEndOffset)
        assertEquals(listOf("m0", "m1"), follower.rm.readLocal(follower.tp, 0, 10).map { String(it.value) })
    }

    @Test
    fun `the handshake truncates a divergent suffix before fetching`() {
        // Follower 2 was leader in epoch 0 and wrote an unreplicated record; broker 1 then led epoch 1.
        follower.leaderAndIsr(2, 0, listOf(2), listOf(1, 2))
        follower.produce("lost")
        follower.leaderAndIsr(1, 1, listOf(1, 2), listOf(1, 2))
        leader.leaderAndIsr(1, 1, listOf(1, 2), listOf(1, 2))
        leader.produce("kept")
        startFetcher(epoch = 1)
        eventually(5.seconds) {
            assertEquals(listOf("kept"), follower.rm.readLocal(follower.tp, 0, 10).map { String(it.value) })
        }
        assertEquals(0, epochRequests.first().requestedEpoch)
        assertEquals(1, epochRequests.first().currentLeaderEpoch)
    }

    @Test
    fun `paused fetchers send no FETCH requests until resumed`() {
        roles()
        follower.rm.pauseFetchers(true)
        startFetcher()
        alwaysFor(300.milliseconds) { assertTrue(fetchRequests.isEmpty()) }
        assertTrue(backoffs.all { it.first == FetcherBackoff.PAUSED })
        leader.produce("m0")
        follower.rm.pauseFetchers(false)
        eventually(5.seconds) { assertEquals(1L, follower.state().logEndOffset) }
    }

    @Test
    fun `shutdown is bounded while a request is blocked on the leader`() {
        roles()
        val blocked = CountDownLatch(1)
        val released = CountDownLatch(1)
        val blockingFactory = LeaderClientFactory {
            object : LeaderClient {
                override fun offsetsForLeaderEpoch(request: OffsetsForLeaderEpochRequest) = throw IOException("unused")
                override fun fetch(request: FetchRequest): FetchResponse {
                    blocked.countDown()
                    // Like a blocking socket read: only close() releases it; an interrupt alone does not.
                    var interrupted = false
                    while (true) {
                        try {
                            released.await()
                            break
                        } catch (_: InterruptedException) {
                            interrupted = true
                        }
                    }
                    if (interrupted) Thread.currentThread().interrupt()
                    throw IOException("socket closed")
                }
                override fun close() = released.countDown()
            }
        }
        val fetcher = ReplicaFetcher(follower.rm, follower.tp, 1, 0, resolver, blockingFactory, settings).also { fetchers += it; it.start() }
        assertTrue(blocked.await(5, TimeUnit.SECONDS))
        val start = System.nanoTime()
        fetcher.shutdown()
        assertTrue(fetcher.awaitShutdown(2_000))
        assertTrue(System.nanoTime() - start < TimeUnit.SECONDS.toNanos(2))
    }

    @Test
    fun `an unresolvable leader is retried with backoff and a refreshed lookup`() {
        roles()
        val lookups = CopyOnWriteArrayList<Boolean>()
        val flaky = BrokerResolver { id, refresh ->
            lookups += refresh
            if (lookups.size < 3) null else BrokerInfo(id, "fake", 1)
        }
        ReplicaFetcher(follower.rm, follower.tp, 1, 0, flaky, factory, settings) { reason, ms -> backoffs += reason to ms }
            .also { fetchers += it; it.start() }
        eventually(5.seconds) { assertTrue(fetchRequests.isNotEmpty()) }
        assertEquals(listOf(false, true, true), lookups.take(3))
        assertEquals(listOf(FetcherBackoff.ERROR, FetcherBackoff.ERROR), backoffs.take(2).map { it.first })
    }

    @Test
    fun `the manager runs one fetcher per followed partition and replaces it on every epoch bump`() {
        roles()
        follower.rm.startReplication(resolver, factory, settings)
        // roles() ran before the manager existed; a new epoch (same leader) starts the first fetcher.
        leader.leaderAndIsr(1, 1, listOf(1, 2), listOf(1, 2))
        follower.leaderAndIsr(1, 1, listOf(1, 2), listOf(1, 2))
        val first = eventually { assertNotNull(follower.rm.fetcherFor(follower.tp)); follower.rm.fetcherFor(follower.tp)!! }
        assertEquals(1, first.leaderEpoch)
        assertEquals("b2-fetcher-t-0", first.threadName)

        leader.leaderAndIsr(1, 2, listOf(1, 2), listOf(1, 2))
        follower.leaderAndIsr(1, 2, listOf(1, 2), listOf(1, 2))
        val second = follower.rm.fetcherFor(follower.tp)!!
        assertNotSame(first, second)
        assertEquals(2, second.leaderEpoch)
        assertTrue(first.awaitShutdown(2_000), "the old fetcher is stopped")
        leader.produce("m0")
        eventually(5.seconds) { assertEquals(1L, follower.state().logEndOffset) }

        // Becoming leader stops fetching.
        follower.leaderAndIsr(2, 3, listOf(2), listOf(1, 2))
        assertNull(follower.rm.fetcherFor(follower.tp))
        assertTrue(second.awaitShutdown(2_000))

        // Offline partition (leader -1): no fetcher.
        follower.leaderAndIsr(-1, 4, listOf(2), listOf(1, 2))
        assertNull(follower.rm.fetcherFor(follower.tp))
    }

    @Test
    fun `stopReplication stops every fetcher and no new ones start afterwards`() {
        follower.rm.startReplication(resolver, factory, settings)
        roles()
        val fetcher = follower.rm.fetcherFor(follower.tp)!!
        follower.rm.stopReplication()
        assertTrue(fetcher.awaitShutdown(2_000))
        follower.leaderAndIsr(1, 1, listOf(1, 2), listOf(1, 2))
        assertNull(follower.rm.fetcherFor(follower.tp))
        assertFalse(fetcher.isAlive)
    }
}
