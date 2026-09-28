package minikafka.broker

import minikafka.log.Record
import minikafka.model.PartitionState
import minikafka.model.TopicPartition
import minikafka.model.Versioned
import minikafka.proto.ErrorCodes
import minikafka.proto.LeaderAndIsrPartition
import minikafka.proto.LeaderAndIsrRequest
import minikafka.proto.OffsetsForLeaderEpochResponse
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.LockSupport

/** LeaderAndIsr fencing, the isr-updater CAS protocol, OffsetsForLeaderEpoch, follower path, stress. */
class ReplicaManagerTest {
    @TempDir
    lateinit var dir: File

    // ---- LeaderAndIsr ----

    @Test
    fun `LeaderAndIsr opens the partition log lazily under dataDir topic-p`() {
        ReplicaHarness(dir).use { h ->
            assertTrue(h.rm.snapshot().partitions.isEmpty())
            h.leaderAndIsr(1, 0, listOf(1), listOf(1))
            assertTrue(File(dir, "t-0").isDirectory)
            val s = h.state()
            assertEquals(Partition.Role.LEADER, s.role)
            assertEquals(1, s.leader)
            assertEquals(0, s.leaderEpoch)
            assertEquals(0, s.zkVersion)
        }
    }

    @Test
    fun `LeaderAndIsr for a partition not assigned to this broker is ignored`() {
        ReplicaHarness(dir).use { h ->
            assertEquals(ErrorCodes.NONE, h.leaderAndIsr(2, 0, listOf(2, 3), listOf(2, 3)))
            assertTrue(h.rm.snapshot().partitions.isEmpty())
            assertFalse(File(dir, "t-0").exists())
        }
    }

    @Test
    fun `LeaderAndIsr with a stale or equal leader epoch is skipped`() {
        ReplicaHarness(dir).use { h ->
            h.leaderAndIsr(1, 2, listOf(1, 2), listOf(1, 2))
            assertEquals(ErrorCodes.NONE, h.leaderAndIsr(2, 1, listOf(2), listOf(1, 2)))
            assertEquals(ErrorCodes.NONE, h.leaderAndIsr(2, 2, listOf(2), listOf(1, 2)))
            val s = h.state()
            assertEquals(Partition.Role.LEADER, s.role)
            assertEquals(2, s.leaderEpoch)
            assertEquals(listOf(1, 2), s.committedIsr)
        }
    }

    @Test
    fun `LeaderAndIsr from an older controller epoch is rejected STALE_CONTROLLER_EPOCH`() {
        ReplicaHarness(dir).use { h ->
            h.rm.seenControllerEpoch = 5
            h.controllerEpoch = 4
            assertEquals(ErrorCodes.STALE_CONTROLLER_EPOCH, h.leaderAndIsr(1, 0, listOf(1), listOf(1)))
            assertTrue(h.rm.snapshot().partitions.isEmpty())
            assertEquals(5, h.rm.seenControllerEpoch)

            h.controllerEpoch = 5
            assertEquals(ErrorCodes.NONE, h.leaderAndIsr(1, 0, listOf(1), listOf(1)))
            h.controllerEpoch = 7
            assertEquals(ErrorCodes.NONE, h.leaderAndIsr(1, 1, listOf(1), listOf(1)))
            assertEquals(7, h.rm.seenControllerEpoch)
            h.controllerEpoch = 6
            assertEquals(ErrorCodes.STALE_CONTROLLER_EPOCH, h.leaderAndIsr(1, 2, listOf(1), listOf(1)))
            assertEquals(1, h.state().leaderEpoch)
        }
    }

    @Test
    fun `LeaderAndIsr for another broker epoch is rejected STALE_BROKER_EPOCH`() {
        ReplicaHarness(dir).use { h ->
            val p = h.partitionRequest(1, 0, listOf(1), listOf(1), 0)
            val request = LeaderAndIsrRequest(0, 1, brokerEpoch = 99L, partitions = listOf(p))
            assertEquals(ErrorCodes.STALE_BROKER_EPOCH, h.rm.applyLeaderAndIsr(request, myBrokerEpoch = 100L))
            assertTrue(h.rm.snapshot().partitions.isEmpty())
            assertEquals(0, h.rm.seenControllerEpoch, "a rejected request does not update the seen epoch")
        }
    }

    @Test
    fun `role listener is told about transitions after the partition lock is released`() {
        ReplicaHarness(dir).use { h ->
            val events = CopyOnWriteArrayList<String>()
            h.rm.roleListener = object : ReplicaRoleListener {
                override fun onBecomeLeader(tp: TopicPartition, leaderEpoch: Int) {
                    // another thread can take the partition lock => it is not held here
                    async { h.consumerFetch(0) }.get(5, TimeUnit.SECONDS)
                    events.add("leader:$tp:$leaderEpoch")
                }

                override fun onBecomeFollower(tp: TopicPartition, leader: Int, leaderEpoch: Int) {
                    async { h.consumerFetch(0) }.get(5, TimeUnit.SECONDS)
                    events.add("follower:$tp:$leader:$leaderEpoch")
                }
            }
            h.leaderAndIsr(1, 0, listOf(1, 2), listOf(1, 2))
            h.leaderAndIsr(2, 1, listOf(1, 2), listOf(1, 2))
            h.leaderAndIsr(2, 2, listOf(1, 2), listOf(1, 2)) // same leader, new epoch => new fetcher
            h.leaderAndIsr(2, 2, listOf(1, 2), listOf(1, 2)) // skipped => no event
            assertEquals(listOf("leader:t-0:0", "follower:t-0:2:1", "follower:t-0:2:2"), events)
        }
    }

    @Test
    fun `becoming leader resets follower LEOs to unknown`() {
        ReplicaHarness(dir).use { h ->
            h.leaderAndIsr(1, 0, listOf(1, 2), listOf(1, 2))
            h.produce("a")
            h.followerFetch(2, 1)
            assertEquals(1L, h.state().followerLeos[2])
            h.leaderAndIsr(1, 1, listOf(1, 2), listOf(1, 2))
            assertEquals(-1L, h.state().followerLeos[2])
            assertEquals(1L, h.state().highWatermark, "HW kept")
        }
    }

    // ---- isr-updater CAS protocol ----

    @Test
    fun `ISR CAS conflict whose stored state equals the proposal is adopted`() {
        ReplicaHarness(dir).use { h ->
            h.leaderAndIsr(1, 0, listOf(1, 2), listOf(1, 2))
            h.store.onWrite = { tp, state, _ ->
                h.store.states[tp] = Versioned(state, 7) // our write committed, then a replay saw BadVersion
                null
            }
            h.clock.advance(h.lagMs + 1)
            h.runIsrUpdater()
            val s = h.state()
            assertEquals(listOf(1), s.committedIsr)
            assertEquals(listOf(1), s.maximalIsr)
            assertEquals(7, s.zkVersion)
            assertFalse(s.isrStale)
        }
    }

    @Test
    fun `ISR CAS conflict with a different stored state marks the partition stale without local change`() {
        ReplicaHarness(dir).use { h ->
            h.leaderAndIsr(1, 0, listOf(1, 2), listOf(1, 2))
            h.store.onWrite = { tp, _, _ ->
                h.store.states[tp] = Versioned(PartitionState(2, 1, listOf(2), 1), 9)
                null
            }
            h.clock.advance(h.lagMs + 1)
            h.runIsrUpdater()
            val s = h.state()
            assertEquals(listOf(1, 2), s.committedIsr)
            assertEquals(listOf(1, 2), s.maximalIsr)
            assertEquals(0, s.zkVersion)
            assertTrue(s.isrStale)

            h.store.writes.clear()
            h.runIsrUpdater()
            assertTrue(h.store.writes.isEmpty(), "a stale partition awaits LeaderAndIsr instead of retrying")

            h.store.onWrite = null
            h.leaderAndIsr(1, 2, listOf(1, 2), listOf(1, 2))
            assertFalse(h.state().isrStale)
        }
    }

    @Test
    fun `a failed expansion stops being retried but stays in the maximal ISR until LeaderAndIsr`() {
        ReplicaHarness(dir).use { h ->
            h.leaderAndIsr(1, 0, listOf(1), listOf(1, 2))
            h.followerFetch(2, 0)
            h.store.onWrite = { tp, _, _ ->
                h.store.states[tp] = Versioned(PartitionState(1, 0, listOf(1, 3), 1), 4)
                null
            }
            h.runIsrUpdater()
            val s = h.state()
            assertTrue(s.isrStale)
            assertEquals(listOf(1), s.committedIsr)
            assertEquals(listOf(1, 2), s.maximalIsr, "conservative: HW keeps waiting for it")
        }
    }

    @Test
    fun `ISR write that throws leaves state unchanged and is retried next round`() {
        ReplicaHarness(dir).use { h ->
            h.leaderAndIsr(1, 0, listOf(1, 2), listOf(1, 2))
            h.store.onWrite = { _, _, _ -> throw java.io.IOException("connection loss") }
            h.clock.advance(h.lagMs + 1)
            h.runIsrUpdater()
            assertEquals(listOf(1, 2), h.state().committedIsr)
            assertFalse(h.state().isrStale)
            h.store.onWrite = null
            h.runIsrUpdater()
            assertEquals(listOf(1), h.state().committedIsr)
        }
    }

    @Test
    fun `a shrink is applied only after CAS success and HW uses the maximal ISR while in flight`() {
        ReplicaHarness(dir).use { h ->
            h.leaderAndIsr(1, 0, listOf(1, 2), listOf(1, 2))
            h.produce("a")
            h.produce("b")
            val seenDuringCas = mutableListOf<PartitionSnapshot>()
            h.store.onWrite = { tp, state, expected ->
                seenDuringCas.add(h.state()) // snapshot takes the partition lock: proves it is not held
                assertEquals(ErrorCodes.NONE, h.produce("c").errorCode, "produce proceeds during the CAS")
                h.store.onWrite = null
                h.store.write(tp, state, expected)
            }
            h.clock.advance(h.lagMs + 1)
            h.runIsrUpdater()
            val during = seenDuringCas.single()
            assertEquals(listOf(1, 2), during.committedIsr)
            assertEquals(listOf(1, 2), during.maximalIsr)
            assertEquals(0L, during.highWatermark, "still waits for the follower being shrunk")
            assertEquals(listOf(1), h.state().committedIsr)
            assertEquals(3L, h.state().highWatermark)
            assertEquals(1, h.state().zkVersion)
        }
    }

    @Test
    fun `an ISR CAS result is discarded if a LeaderAndIsr arrived while it was in flight`() {
        ReplicaHarness(dir).use { h ->
            h.leaderAndIsr(1, 0, listOf(1, 2), listOf(1, 2))
            h.store.onWrite = { tp, state, expected ->
                h.store.onWrite = null
                val v = h.store.write(tp, state, expected)
                h.leaderAndIsr(1, 1, listOf(1, 2), listOf(1, 2)) // controller moved on meanwhile
                v
            }
            h.clock.advance(h.lagMs + 1)
            h.runIsrUpdater()
            val s = h.state()
            assertEquals(1, s.leaderEpoch)
            assertEquals(listOf(1, 2), s.committedIsr)
            assertEquals(2, s.zkVersion)
        }
    }

    @Test
    fun `the background isr-updater thread shrinks on its own`() {
        val clock = minikafka.testing.MutableClock()
        val store = FakeIsrStore()
        val tp = TopicPartition("t", 0)
        ReplicaManager(BrokerConfig(1, "localhost", 0, dir, replicaLagTimeMaxMs = 100), store, clock).use { rm ->
            assertTrue(Thread.getAllStackTraces().keys.any { it.name == "b1-isr-updater" && it.isDaemon })
            val v = store.controllerSet(tp, PartitionState(1, 0, listOf(1, 2), 1))
            val req = LeaderAndIsrRequest(0, 1, 5L, listOf(LeaderAndIsrPartition("t", 0, 1, 0, listOf(1, 2), listOf(1, 2), v)))
            rm.applyLeaderAndIsr(req, 5L)
            clock.advance(101)
            minikafka.testing.eventually {
                assertEquals(listOf(1), rm.snapshot().partitions.single().committedIsr)
            }
        }
    }

    // ---- OffsetsForLeaderEpoch ----

    @Test
    fun `offsetsForLeaderEpoch answers from the leader epoch cache`() {
        ReplicaHarness(dir).use { h ->
            h.leaderAndIsr(1, 0, listOf(1), listOf(1, 2))
            assertEquals(OffsetsForLeaderEpochResponse(ErrorCodes.NONE, -1, 0L), h.rm.offsetsForLeaderEpoch(h.tp, 2, 0, 0), "empty log")
            repeat(3) { h.produce("e0-$it") }
            h.leaderAndIsr(1, 2, listOf(1), listOf(1, 2))
            repeat(2) { h.produce("e2-$it") }

            fun ask(requested: Int) = h.rm.offsetsForLeaderEpoch(h.tp, 2, 2, requested)
            assertEquals(OffsetsForLeaderEpochResponse(ErrorCodes.NONE, 0, 3L), ask(0))
            assertEquals(OffsetsForLeaderEpochResponse(ErrorCodes.NONE, 0, 3L), ask(1))
            assertEquals(OffsetsForLeaderEpochResponse(ErrorCodes.NONE, 2, 5L), ask(2))
            assertEquals(OffsetsForLeaderEpochResponse(ErrorCodes.NONE, 2, 5L), ask(9))
            assertEquals(ErrorCodes.FENCED_LEADER_EPOCH, h.rm.offsetsForLeaderEpoch(h.tp, 2, 1, 0).errorCode)
            assertEquals(ErrorCodes.UNKNOWN_LEADER_EPOCH, h.rm.offsetsForLeaderEpoch(h.tp, 2, 3, 0).errorCode)
            assertEquals(ErrorCodes.REPLICA_NOT_ASSIGNED, h.rm.offsetsForLeaderEpoch(h.tp, 9, 2, 0).errorCode)
            assertEquals(ErrorCodes.NONE, h.rm.offsetsForLeaderEpoch(h.tp, 2, -1, 0).errorCode)
        }
    }

    @Test
    fun `offsetsForLeaderEpoch on a follower or unknown partition is NOT_LEADER`() {
        ReplicaHarness(dir).use { h ->
            assertEquals(ErrorCodes.NOT_LEADER_FOR_PARTITION, h.rm.offsetsForLeaderEpoch(h.tp, 2, 0, 0).errorCode)
            h.leaderAndIsr(2, 0, listOf(1, 2), listOf(1, 2))
            assertEquals(ErrorCodes.NOT_LEADER_FOR_PARTITION, h.rm.offsetsForLeaderEpoch(h.tp, 2, 0, 0).errorCode)
        }
    }

    // ---- follower path (driven by Task 10's fetcher) ----

    @Test
    fun `follower append checks the epoch, appends, and sets HW to min(LEO, leader HW)`() {
        ReplicaHarness(dir).use { h ->
            h.leaderAndIsr(2, 3, listOf(1, 2), listOf(1, 2))
            val records = (0L until 3L).map { Record(it, 3, 0L, null, "v$it".toByteArray()) }
            assertEquals(ErrorCodes.FENCED_LEADER_EPOCH, h.rm.appendAsFollower(h.tp, 2, records, 2L))
            assertEquals(0L, h.state().logEndOffset)
            assertEquals(ErrorCodes.NONE, h.rm.appendAsFollower(h.tp, 3, records, 2L))
            assertEquals(3L, h.state().logEndOffset)
            assertEquals(2L, h.state().highWatermark)
            assertEquals(ErrorCodes.NONE, h.rm.appendAsFollower(h.tp, 3, emptyList(), 10L))
            assertEquals(3L, h.state().highWatermark, "capped at LEO")
        }
    }

    @Test
    fun `follower append on a leader or unknown partition is NOT_LEADER`() {
        ReplicaHarness(dir).use { h ->
            val r = listOf(Record(0, 0, 0L, null, "v".toByteArray()))
            assertEquals(ErrorCodes.NOT_LEADER_FOR_PARTITION, h.rm.appendAsFollower(h.tp, 0, r, 0L))
            h.leaderAndIsr(1, 0, listOf(1), listOf(1, 2))
            assertEquals(ErrorCodes.NOT_LEADER_FOR_PARTITION, h.rm.appendAsFollower(h.tp, 0, r, 0L))
        }
    }

    @Test
    fun `follower truncation caps HW at the new LEO`() {
        ReplicaHarness(dir).use { h ->
            h.leaderAndIsr(2, 0, listOf(1, 2), listOf(1, 2))
            val records = (0L until 4L).map { Record(it, 0, 0L, null, "v$it".toByteArray()) }
            h.rm.appendAsFollower(h.tp, 0, records, 4L)
            assertEquals(ErrorCodes.NONE, h.rm.truncateTo(h.tp, 0, 2L))
            assertEquals(2L, h.state().logEndOffset)
            assertEquals(2L, h.state().highWatermark)
            assertEquals(ErrorCodes.FENCED_LEADER_EPOCH, h.rm.truncateTo(h.tp, 5, 0L))
        }
    }

    @Test
    fun `pauseFetchers is recorded in the snapshot`() {
        ReplicaHarness(dir).use { h ->
            assertFalse(h.rm.snapshot().fetchersPaused)
            h.rm.pauseFetchers(true)
            assertTrue(h.rm.snapshot().fetchersPaused)
            h.rm.pauseFetchers(false)
            assertFalse(h.rm.snapshot().fetchersPaused)
        }
    }

    @Test
    fun `close fails acks=all waiters with NOT_LEADER`() {
        val h = ReplicaHarness(dir, minInsyncReplicas = 2)
        h.leaderAndIsr(1, 0, listOf(1, 2), listOf(1, 2))
        val pending = h.produceAsync("a", timeoutMs = 10_000)
        minikafka.testing.eventually { assertEquals(1L, h.state().logEndOffset) }
        h.close()
        assertEquals(ErrorCodes.NOT_LEADER_FOR_PARTITION, pending.get(5, TimeUnit.SECONDS).errorCode)
        assertEquals(ErrorCodes.NOT_LEADER_FOR_PARTITION, h.produce("b").errorCode)
    }

    // ---- concurrency stress ----

    @Test
    fun `stress - producers, follower fetches, leadership flips, truncation and ISR updates keep invariants`() {
        ReplicaHarness(dir, minInsyncReplicas = 2, lagMs = 1_000).use { h ->
            val replicas = listOf(1, 2, 3)
            val epoch = AtomicInteger(0)
            h.leaderAndIsr(1, 0, replicas, replicas)
            val running = AtomicBoolean(true)
            val failures = ConcurrentLinkedQueue<Throwable>()
            val produced = AtomicInteger()
            val ackedAll = ConcurrentLinkedQueue<Pair<Long, String>>()
            val threads = mutableListOf<Thread>()
            fun spawn(name: String, body: () -> Unit) {
                threads += Thread({
                    try {
                        body()
                    } catch (t: Throwable) {
                        failures.add(t)
                    }
                }, name).apply { isDaemon = true; start() }
            }

            repeat(8) { id ->
                val acks = if (id % 2 == 0) ACKS_ONE else ACKS_ALL
                spawn("producer-$id") {
                    var lastOffsetInEpoch = -1L
                    var lastEpoch = -1
                    repeat(250) { i ->
                        val value = "p$id-$i"
                        val epochBefore = h.state().leaderEpoch
                        val r = h.rm.produce(h.tp, null, value.toByteArray(), acks, timeoutMs = 200)
                        produced.incrementAndGet()
                        val ok = setOf(
                            ErrorCodes.NONE, ErrorCodes.NOT_LEADER_FOR_PARTITION, ErrorCodes.NOT_ENOUGH_REPLICAS,
                            ErrorCodes.NOT_ENOUGH_REPLICAS_AFTER_APPEND, ErrorCodes.REQUEST_TIMED_OUT
                        )
                        check(r.errorCode in ok) { "unexpected error ${r.errorCode}" }
                        if (r.errorCode == ErrorCodes.NONE) {
                            if (epochBefore == lastEpoch) check(r.offset > lastOffsetInEpoch) { "offsets not increasing" }
                            lastEpoch = epochBefore
                            lastOffsetInEpoch = r.offset
                            if (acks == ACKS_ALL) ackedAll.add(r.offset to value)
                        }
                    }
                }
            }
            for (follower in listOf(2, 3)) {
                spawn("follower-$follower") {
                    var leo = 0L
                    var iteration = 0
                    while (running.get()) {
                        // follower 3 stalls periodically => lag shrink, then rejoin (expand)
                        if (follower == 3 && (iteration++ / 20) % 2 == 1) {
                            LockSupport.parkNanos(100_000)
                            continue
                        }
                        val s = h.state()
                        val r = h.rm.fetchAsFollower(h.tp, follower, leo, s.leaderEpoch, 4096)
                        when (r.errorCode) {
                            ErrorCodes.NONE -> leo += r.records.size
                            ErrorCodes.OFFSET_OUT_OF_RANGE -> leo = s.highWatermark
                        }
                        LockSupport.parkNanos(100_000)
                    }
                }
            }
            spawn("flipper") {
                var nextFlipAt = 100
                while (running.get()) {
                    if (produced.get() >= nextFlipAt) {
                        nextFlipAt += 100
                        val e = epoch.incrementAndGet()
                        val leader = if (e % 3 == 2) 2 else 1
                        h.leaderAndIsr(leader, e, replicas, replicas)
                        if (leader != 1) {
                            val s = h.state()
                            // a follower truncates, but never below HW (acked data stays)
                            h.rm.truncateTo(h.tp, e, maxOf(s.highWatermark, s.logEndOffset - 3))
                        }
                    }
                    LockSupport.parkNanos(200_000)
                }
            }
            spawn("isr-updater") {
                while (running.get()) {
                    h.clock.advance(300)
                    h.runIsrUpdater()
                    LockSupport.parkNanos(500_000)
                }
            }
            spawn("observer") {
                var lastEpoch = -1
                var lastHw = -1L
                while (running.get()) {
                    val s = h.state()
                    check(s.highWatermark <= s.logEndOffset) { "HW ${s.highWatermark} > LEO ${s.logEndOffset}" }
                    if (s.leaderEpoch == lastEpoch && s.role == Partition.Role.LEADER) {
                        check(s.highWatermark >= lastHw) { "HW moved backward $lastHw -> ${s.highWatermark}" }
                    }
                    if (s.role == Partition.Role.LEADER) {
                        lastEpoch = s.leaderEpoch
                        lastHw = s.highWatermark
                    } else {
                        lastEpoch = -1
                    }
                }
            }

            val producers = threads.filter { it.name.startsWith("producer") }
            producers.forEach { it.join(60_000) }
            running.set(false)
            threads.forEach { it.join(10_000) }
            threads.forEach { assertFalse(it.isAlive, "${it.name} still running") }
            failures.firstOrNull()?.let { throw AssertionError("stress failure in a worker", it) }

            assertTrue(epoch.get() >= 5, "leadership flipped ${epoch.get()} times")
            assertTrue(h.store.writes.isNotEmpty(), "the isr-updater made ISR changes during the run")
            val offsets = ackedAll.map { it.first }
            assertEquals(offsets.size, offsets.toSet().size, "acks=all offsets are unique")
            // Every acks=all-acknowledged record is still in the log at its offset.
            h.leaderAndIsr(1, epoch.incrementAndGet(), listOf(1), replicas)
            for ((offset, value) in ackedAll) {
                val record = h.consumerFetch(offset).records.firstOrNull()
                assertEquals(value, record?.value?.let { String(it) }, "acked record at $offset")
            }
            assertNull(failures.peek())
        }
    }
}
