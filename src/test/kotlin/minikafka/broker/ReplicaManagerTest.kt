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
import java.util.concurrent.CountDownLatch
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.LockSupport
import kotlin.time.Duration.Companion.milliseconds

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
    fun `a partition whose log fails to open is skipped without blocking the others and is retried later`() {
        ReplicaHarness(dir).use { h ->
            val events = CopyOnWriteArrayList<String>()
            h.rm.roleListener = object : ReplicaRoleListener {
                override fun onBecomeLeader(tp: TopicPartition, leaderEpoch: Int) {
                    events.add("leader:$tp:$leaderEpoch")
                }

                override fun onBecomeFollower(tp: TopicPartition, leader: Int, leaderEpoch: Int) {
                    events.add("follower:$tp:$leader:$leaderEpoch")
                }
            }
            val tps = (0..2).map { TopicPartition("t", it) }
            val obstruction = File(dir, "t-1").apply { writeText("not a directory") }
            fun request(): LeaderAndIsrRequest {
                val parts = tps.map { tp ->
                    val v = h.store.controllerSet(tp, PartitionState(2, 3, listOf(1, 2), h.controllerEpoch))
                    h.partitionRequest(2, 3, listOf(1, 2), listOf(1, 2), v, tp)
                }
                return LeaderAndIsrRequest(0, h.controllerEpoch, h.brokerEpoch, parts)
            }

            assertEquals(ErrorCodes.NONE, h.rm.applyLeaderAndIsr(request(), h.brokerEpoch))
            assertEquals(listOf("follower:t-0:2:3", "follower:t-2:2:3"), events,
                "partitions before and after the failed one get their role transition")
            assertEquals(listOf(tps[0], tps[2]), h.rm.snapshot().partitions.map { it.tp })

            // The controller channel retries the same request after the obstruction is gone:
            // t-1's epoch was never advanced, so only t-1 transitions now.
            events.clear()
            assertTrue(obstruction.delete())
            assertEquals(ErrorCodes.NONE, h.rm.applyLeaderAndIsr(request(), h.brokerEpoch))
            assertEquals(listOf("follower:t-1:2:3"), events)
            assertEquals(3, h.state(tps[1]).leaderEpoch)
            assertTrue(File(dir, "t-1").isDirectory)
        }
    }

    @Test
    fun `acks=all produce timeout is clamped to requestTimeoutMs`() {
        ReplicaHarness(dir, requestTimeoutMs = 200).use { h ->
            h.leaderAndIsr(1, 0, listOf(1, 2), listOf(1, 2)) // follower 2 never fetches => never committed
            val started = System.nanoTime()
            val r = h.produceAsync("a", timeoutMs = Int.MAX_VALUE).get(10, TimeUnit.SECONDS)
            val elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started)
            assertEquals(ErrorCodes.REQUEST_TIMED_OUT, r.errorCode)
            assertTrue(elapsedMs < 5_000, "waited ${elapsedMs}ms, expected about requestTimeoutMs")
        }
    }

    @Test
    fun `a negative produce timeout is treated as zero`() {
        ReplicaHarness(dir, requestTimeoutMs = 10_000).use { h ->
            h.leaderAndIsr(1, 0, listOf(1), listOf(1, 2)) // sole ISR member => committed on append
            assertEquals(ErrorCodes.NONE, h.produce("a", ACKS_ALL, timeoutMs = -1).errorCode)
            h.leaderAndIsr(1, 1, listOf(1, 2), listOf(1, 2))
            val started = System.nanoTime()
            assertEquals(ErrorCodes.REQUEST_TIMED_OUT, h.produce("b", ACKS_ALL, timeoutMs = -5).errorCode)
            assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started) < 2_000)
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
    fun `an ISR write that committed but threw does not leave the partition stuck (R6)`() {
        ReplicaHarness(dir).use { h ->
            h.leaderAndIsr(1, 0, listOf(1, 2, 3), listOf(1, 2, 3))
            h.produce("a")
            h.followerFetch(2, 1) // follower 2 keeps pace, follower 3 never fetches
            h.store.onWrite = { tp, state, expected ->
                h.store.onWrite = null
                h.store.write(tp, state, expected) // P1 = [1, 2] commits in ZK ...
                throw java.io.IOException("connection loss after commit") // ... but the outcome is unknown
            }
            h.clock.advance(h.lagMs + 1)
            h.followerFetch(2, 1)
            h.runIsrUpdater()
            assertEquals(listOf(1, 2), h.store.states[h.tp]!!.value.isr)
            assertEquals(listOf(1, 2, 3), h.state().committedIsr, "outcome unknown: no local change")

            h.produce("b") // follower 2 now starts lagging too
            h.clock.advance(h.lagMs + 1)
            h.runIsrUpdater() // proposes [1] at the old version => BadVersion; znode [1, 2] is ours => adopt
            var s = h.state()
            assertFalse(s.isrStale)
            assertEquals(listOf(1, 2), s.committedIsr)
            assertEquals(h.store.states[h.tp]!!.zkVersion, s.zkVersion)
            assertEquals(listOf(1, 2, 3), s.maximalIsr, "maximal ISR unchanged by the adoption")

            h.runIsrUpdater() // re-proposes as normal
            s = h.state()
            assertEquals(listOf(1), s.committedIsr)
            assertEquals(listOf(1), s.maximalIsr)
            assertEquals(listOf(1), h.store.states[h.tp]!!.value.isr)
            assertEquals(2L, s.highWatermark, "HW advances once the ISR converges")
        }
    }

    @Test
    fun `a pending member that lags before its expansion commits is removed only via a CAS`() {
        ReplicaHarness(dir).use { h ->
            h.leaderAndIsr(1, 0, listOf(1), listOf(1, 2))
            h.followerFetch(2, 0) // joins maximal ISR
            h.store.onWrite = { _, _, _ -> throw java.io.IOException("connection loss") }
            h.runIsrUpdater() // expansion outcome unknown
            h.store.onWrite = null
            h.store.writes.clear()
            h.produce("a")
            assertEquals(0L, h.state().highWatermark, "pending member counted in HW")
            h.clock.advance(h.lagMs + 1)
            h.runIsrUpdater()
            assertEquals(listOf(1), h.store.writes.single().second.isr, "written even though committed ISR is unchanged")
            assertEquals(listOf(1), h.state().maximalIsr)
            assertEquals(1L, h.state().highWatermark)
        }
    }

    @Test
    fun `ISR writes preserve the znode's controller epoch`() {
        ReplicaHarness(dir).use { h ->
            // znode last written by controller epoch 3; LeaderAndIsr resent by a newer controller (5)
            val v = h.store.controllerSet(h.tp, PartitionState(1, 0, listOf(1, 2), 3))
            h.controllerEpoch = 5
            val req = LeaderAndIsrRequest(0, 5, h.brokerEpoch, listOf(h.partitionRequest(1, 0, listOf(1, 2), listOf(1, 2), v)))
            assertEquals(ErrorCodes.NONE, h.rm.applyLeaderAndIsr(req, h.brokerEpoch))
            h.clock.advance(h.lagMs + 1)
            h.runIsrUpdater()
            assertEquals(PartitionState(1, 0, listOf(1), 3), h.store.states[h.tp]!!.value)
            assertEquals(listOf(1), h.state().committedIsr)

            h.followerFetch(2, 0)
            h.runIsrUpdater() // second write reuses the known epoch without another read
            assertEquals(PartitionState(1, 0, listOf(1, 2), 3), h.store.states[h.tp]!!.value)
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

    @Test
    fun `a caught-up follower is written into the ISR by the background updater without waiting for the interval`() {
        val store = FakeIsrStore()
        val tp = TopicPartition("t", 0)
        // Interval = lag / 2 = 300 s: only requestRun() can make the thread act within the test.
        ReplicaManager(BrokerConfig(1, "localhost", 0, dir, 1, replicaLagTimeMaxMs = 600_000), store, minikafka.testing.MutableClock()).use { rm ->
            val v = store.controllerSet(tp, PartitionState(1, 0, listOf(1), 1))
            rm.applyLeaderAndIsr(LeaderAndIsrRequest(0, 1, 100L, listOf(LeaderAndIsrPartition("t", 0, 1, 0, listOf(1), listOf(1, 2), v))), 100L)
            rm.produce(tp, null, "a".toByteArray(), ACKS_ONE, 1_000)
            rm.fetchAsFollower(tp, 2, 1, 0, 1 shl 20)
            minikafka.testing.eventually { assertEquals(listOf(1, 2), store.states[tp]!!.value.isr) }
        }
    }

    @Test
    fun `close stops an idle isr-updater thread promptly`() {
        val rm = ReplicaManager(BrokerConfig(8, "localhost", 0, dir, 1, replicaLagTimeMaxMs = 600_000), FakeIsrStore(), minikafka.testing.MutableClock())
        fun updater() = Thread.getAllStackTraces().keys.filter { it.name == "b8-isr-updater" && it.isAlive }
        try {
            minikafka.testing.eventually { assertEquals(Thread.State.TIMED_WAITING, updater().single().state) }
            val start = System.nanoTime()
            rm.close()
            assertTrue(System.nanoTime() - start < TimeUnit.SECONDS.toNanos(2), "close waits for the 300 s interval")
            assertTrue(updater().isEmpty())
        } finally {
            rm.close() // idempotent; never leak the updater thread on a failed assertion
        }
    }

    /** stopReplication runs before the ZooKeeper client closes: it must not return while an ISR write is in flight. */
    @Test
    fun `close waits for an in-flight ISR write to finish`() {
        val store = FakeIsrStore()
        val clock = minikafka.testing.MutableClock()
        val tp = TopicPartition("t", 0)
        val writing = CountDownLatch(1)
        val release = CountDownLatch(1)
        store.onWrite = { _, _, _ -> writing.countDown(); release.await(); null }
        val rm = ReplicaManager(BrokerConfig(9, "localhost", 0, dir, 1, replicaLagTimeMaxMs = 100), store, clock)
        try {
            val v = store.controllerSet(tp, PartitionState(9, 0, listOf(9, 2), 1))
            rm.applyLeaderAndIsr(LeaderAndIsrRequest(0, 1, 5L, listOf(LeaderAndIsrPartition("t", 0, 9, 0, listOf(9, 2), listOf(9, 2), v))), 5L)
            clock.advance(101)
            assertTrue(writing.await(10, TimeUnit.SECONDS), "the updater thread starts the shrink CAS")
            val closed = async { rm.close() }
            minikafka.testing.alwaysFor(200.milliseconds) { assertFalse(closed.isDone, "close returned with the write in flight") }
            release.countDown()
            closed.get(5, TimeUnit.SECONDS)
            assertTrue(Thread.getAllStackTraces().keys.none { it.name == "b9-isr-updater" && it.isAlive })
        } finally {
            release.countDown()
            rm.close()
        }
    }

    @Test
    fun `a state znode missing at the controller-epoch pre-read marks the partition stale`() {
        ReplicaHarness(dir).use { h ->
            h.leaderAndIsr(1, 0, listOf(1, 2), listOf(1, 2))
            h.store.states.remove(h.tp)
            h.clock.advance(h.lagMs + 1)
            h.runIsrUpdater()
            val s = h.state()
            assertTrue(s.isrStale)
            assertEquals(listOf(1, 2), s.committedIsr)
            assertEquals(0, s.zkVersion)
            assertTrue(h.store.writes.isEmpty(), "no CAS without knowing the znode's controller_epoch")
        }
    }

    /**
     * The controller re-elected this same broker in a newer leader epoch while our ISR CAS of the
     * old epoch was in flight: the conflicting znode names us as leader, but it is not our
     * leadership. Adopting its zkVersion would let the next CAS write the old leader_epoch back
     * over the newer one (a leader-epoch regression in ZooKeeper).
     */
    @Test
    fun `ISR CAS conflict with our own leadership in a newer leader epoch marks the partition stale`() {
        for (storedIsr in listOf(listOf(1), listOf(1, 2))) { // equal to the proposal / covered by maximal ISR
            ReplicaHarness(File(dir, "isr-${storedIsr.size}")).use { h ->
                h.leaderAndIsr(1, 0, listOf(1, 2), listOf(1, 2))
                h.store.onWrite = { tp, state, _ ->
                    h.store.states[tp] = Versioned(state.copy(leaderEpoch = 1, isr = storedIsr), 9)
                    null
                }
                h.clock.advance(h.lagMs + 1)
                h.runIsrUpdater()
                val s = h.state()
                assertTrue(s.isrStale, "stored isr $storedIsr")
                assertEquals(0, s.zkVersion, "stored isr $storedIsr: the newer epoch's zkVersion is not adopted")
                assertEquals(listOf(1, 2), s.committedIsr)
                assertEquals(listOf(1, 2), s.maximalIsr)

                h.store.writes.clear()
                h.runIsrUpdater()
                assertTrue(h.store.writes.isEmpty(), "no further ISR write in the old epoch")

                h.store.onWrite = null
                h.leaderAndIsr(1, 1, listOf(1, 2), listOf(1, 2))
                assertFalse(h.state().isrStale)
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

    @Test
    fun `LeaderAndIsr after close opens no partition`() {
        val h = ReplicaHarness(dir)
        h.close()
        assertEquals(ErrorCodes.NONE, h.leaderAndIsr(1, 0, listOf(1), listOf(1)))
        assertTrue(h.rm.snapshot().partitions.isEmpty())
        assertFalse(File(dir, "t-0").exists())
    }

    @Test
    fun `every entry point answers NOT_LEADER for a partition that is not open here`() {
        ReplicaHarness(dir).use { h ->
            val tp = TopicPartition("nope", 0)
            assertEquals(ErrorCodes.NOT_LEADER_FOR_PARTITION, h.rm.produce(tp, null, byteArrayOf(), ACKS_ONE, 100).errorCode)
            assertEquals(ErrorCodes.NOT_LEADER_FOR_PARTITION, h.rm.fetchAsConsumer(tp, 0, 100).errorCode)
            assertEquals(ErrorCodes.NOT_LEADER_FOR_PARTITION, h.rm.fetchAsFollower(tp, 2, 0, 0, 100).errorCode)
            assertEquals(ErrorCodes.NOT_LEADER_FOR_PARTITION, h.rm.offsetsForLeaderEpoch(tp, 2, 0, 0).errorCode)
            assertEquals(ErrorCodes.NOT_LEADER_FOR_PARTITION, h.rm.appendAsFollower(tp, 0, emptyList(), 0L))
            assertEquals(ErrorCodes.NOT_LEADER_FOR_PARTITION, h.rm.truncateTo(tp, 0, 0L))
            assertEquals(ErrorCodes.NOT_LEADER_FOR_PARTITION, h.rm.truncateFollower(tp, 0) { 0 to 0L }.errorCode)
            assertNull(h.rm.logEndOffset(tp))
        }
    }

    @Test
    fun `truncateTo on the leader, even with its own epoch, is NOT_LEADER and keeps the log`() {
        ReplicaHarness(dir).use { h ->
            h.leaderAndIsr(1, 0, listOf(1), listOf(1, 2))
            h.produce("a"); h.produce("b")
            assertEquals(ErrorCodes.NOT_LEADER_FOR_PARTITION, h.rm.truncateTo(h.tp, 0, 0L))
            assertEquals(2L, h.state().logEndOffset)
        }
    }

    /** The partition lock is reentrant, so only another thread can tell whether it was released. */
    @Test
    fun `partition operations release the partition lock`() {
        ReplicaHarness(dir).use { h ->
            fun leoFromAnotherThread() = async { h.state().logEndOffset }.get(2, TimeUnit.SECONDS)
            h.leaderAndIsr(1, 0, listOf(1, 2), listOf(1, 2))
            h.produce("a")
            h.rm.offsetsForLeaderEpoch(h.tp, 2, 0, 0)
            assertEquals(1L, leoFromAnotherThread())
            h.leaderAndIsr(2, 1, listOf(1, 2), listOf(1, 2))
            h.rm.appendAsFollower(h.tp, 1, listOf(Record(1, 1, 0, null, "b".toByteArray())), 0)
            assertEquals(2L, leoFromAnotherThread())
            h.rm.truncateFollower(h.tp, 1) { 1 to 1L }
            assertEquals(1L, leoFromAnotherThread())
            h.rm.truncateTo(h.tp, 1, 0L)
            assertEquals(0L, leoFromAnotherThread())
        }
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
            // Deterministic ISR change: follower 3 does not fetch and no flips happen until the
            // isr-updater (advancing the MutableClock past the lag) has committed its shrink.
            val firstShrink = CountDownLatch(1)
            val invariantChecks = AtomicInteger()
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
                    if (follower == 3) check(firstShrink.await(30, TimeUnit.SECONDS)) { "no initial shrink" }
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
                check(firstShrink.await(30, TimeUnit.SECONDS)) { "no initial shrink" }
                var nextFlipAt = produced.get() + 100
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
                    if (firstShrink.count > 0 && 3 !in h.state().committedIsr) firstShrink.countDown()
                    LockSupport.parkNanos(500_000)
                }
            }
            spawn("observer") {
                var lastEpoch = -1
                var lastHw = -1L
                while (running.get()) {
                    val s = h.state()
                    check(s.highWatermark <= s.logEndOffset) { "HW ${s.highWatermark} > LEO ${s.logEndOffset}" }
                    // D7: committed ISR ⊆ maximal ISR, and the ZK ISR at the version we hold ⊆ maximal ISR
                    // (a given zkVersion's content never changes, so reading the store after is sound).
                    check(s.maximalIsr.containsAll(s.committedIsr)) { "committed ${s.committedIsr} ⊄ maximal ${s.maximalIsr}" }
                    val zk = h.store.states[h.tp]
                    if (zk != null && zk.zkVersion == s.zkVersion && zk.value.leaderEpoch == s.leaderEpoch) {
                        check(s.maximalIsr.containsAll(zk.value.isr)) { "ZK ISR ${zk.value.isr} ⊄ maximal ${s.maximalIsr}" }
                        check(zk.value.isr == s.committedIsr) { "ZK ISR ${zk.value.isr} != committed ${s.committedIsr} at v${s.zkVersion}" }
                        invariantChecks.incrementAndGet()
                    }
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
            assertEquals(0L, firstShrink.count, "the isr-updater committed a shrink")
            assertTrue(h.store.writes.isNotEmpty())
            assertTrue(invariantChecks.get() > 0, "ZK-vs-local ISR invariant was exercised")
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
