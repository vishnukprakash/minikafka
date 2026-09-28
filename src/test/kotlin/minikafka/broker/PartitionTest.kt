package minikafka.broker

import minikafka.log.Log
import minikafka.model.PartitionState
import minikafka.model.TopicPartition
import minikafka.model.Versioned
import minikafka.proto.ErrorCodes
import minikafka.proto.LeaderAndIsrPartition
import minikafka.testing.MutableClock
import minikafka.testing.eventually
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/** High watermark, ISR (maximal ISR, lag shrink, rejoin), acks and fetch semantics of one partition. */
class PartitionTest {
    @TempDir
    lateinit var dir: File

    // ---- high watermark ----

    @Test
    fun `HW is the min LEO over maximal ISR and unknown follower LEOs (-1) hold it back`() {
        ReplicaHarness(dir).use { h ->
            h.leaderAndIsr(leader = 1, epoch = 0, isr = listOf(1, 2, 3), replicas = listOf(1, 2, 3))
            repeat(5) { h.produce("v$it") }
            assertEquals(0L, h.state().highWatermark, "both followers unknown (-1)")

            h.followerFetch(2, 3)
            assertEquals(0L, h.state().highWatermark, "follower 3 still unknown")
            h.followerFetch(3, 2)
            assertEquals(2L, h.state().highWatermark)
            h.followerFetch(2, 5)
            assertEquals(2L, h.state().highWatermark)
            h.followerFetch(3, 4)
            assertEquals(4L, h.state().highWatermark)
            assertEquals(4L, h.followerFetch(3, 4).highWatermark, "fetch response carries the HW")
        }
    }

    @Test
    fun `HW never moves backward`() {
        ReplicaHarness(dir).use { h ->
            h.leaderAndIsr(1, 0, listOf(1, 2), listOf(1, 2))
            repeat(4) { h.produce("v$it") }
            h.followerFetch(2, 4)
            assertEquals(4L, h.state().highWatermark)
            h.followerFetch(2, 1) // e.g. follower truncated and re-fetches from an earlier offset
            assertEquals(4L, h.state().highWatermark)
        }
    }

    @Test
    fun `ISR of only the leader makes HW equal LEO, with RF=1 and with an out-of-ISR follower`() {
        ReplicaHarness(dir).use { h ->
            h.leaderAndIsr(1, 0, listOf(1), listOf(1))
            h.produce("a")
            h.produce("b")
            assertEquals(2L, h.state().highWatermark)
            assertEquals(2L, h.state().logEndOffset)
        }
        ReplicaHarness(File(dir, "second")).use { h ->
            h.leaderAndIsr(1, 0, listOf(1), listOf(1, 2))
            h.produce("a")
            assertEquals(1L, h.state().highWatermark)
        }
    }

    @Test
    fun `becoming leader with ISR of only itself sets HW to LEO immediately`() {
        ReplicaHarness(dir).use { h ->
            h.leaderAndIsr(1, 0, listOf(1, 2), listOf(1, 2))
            repeat(3) { h.produce("v$it") }
            assertEquals(0L, h.state().highWatermark)
            h.leaderAndIsr(1, 1, listOf(1), listOf(1, 2))
            assertEquals(3L, h.state().highWatermark)
        }
    }

    // ---- ISR shrink ----

    @Test
    fun `a follower that stops fetching is shrunk out of the ISR after replicaLagTimeMaxMs`() {
        ReplicaHarness(dir, lagMs = 1_000).use { h ->
            h.leaderAndIsr(1, 0, listOf(1, 2), listOf(1, 2))
            h.produce("a")
            h.clock.advance(1_000)
            h.runIsrUpdater()
            assertEquals(listOf(1, 2), h.state().committedIsr, "exactly lag ms is not yet lagging")
            assertTrue(h.store.writes.isEmpty())

            h.clock.advance(1)
            h.runIsrUpdater()
            assertEquals(listOf(1), h.state().committedIsr)
            assertEquals(listOf(1), h.state().maximalIsr)
            assertEquals(listOf(1), h.store.states[h.tp]!!.value.isr)
            assertEquals(0, h.store.states[h.tp]!!.value.leaderEpoch, "leader epoch preserved")
            assertEquals(1L, h.state().highWatermark, "HW recomputed after the shrink")
        }
    }

    @Test
    fun `a follower keeping pace under constant appends is never shrunk`() {
        ReplicaHarness(dir, lagMs = 1_000).use { h ->
            h.leaderAndIsr(1, 0, listOf(1, 2), listOf(1, 2))
            var followerLeo = 0L
            repeat(20) {
                h.produce("v$it") // leader LEO always one ahead of the follower's next fetch
                h.clock.advance(600)
                val fetched = h.followerFetch(2, followerLeo)
                assertEquals(ErrorCodes.NONE, fetched.errorCode)
                followerLeo += fetched.records.size
                h.runIsrUpdater()
                assertEquals(listOf(1, 2), h.state().committedIsr, "iteration $it")
            }
            assertTrue(h.store.writes.isEmpty())
        }
    }

    @Test
    fun `a follower that fetches but falls behind is shrunk`() {
        ReplicaHarness(dir, lagMs = 1_000).use { h ->
            h.leaderAndIsr(1, 0, listOf(1, 2), listOf(1, 2))
            repeat(3) {
                h.produce("a$it")
                h.produce("b$it")
                h.clock.advance(600)
                h.followerFetch(2, it.toLong()) // fetches 1 record per round while 2 are produced
                h.runIsrUpdater()
            }
            assertEquals(listOf(1), h.state().committedIsr)
        }
    }

    // ---- ISR expansion ----

    @Test
    fun `a follower rejoins only once it reaches both HW and leaderEpochStartOffset`() {
        ReplicaHarness(dir).use { h ->
            h.leaderAndIsr(1, 0, listOf(1, 2), listOf(1, 2, 3))
            repeat(2) { h.produce("old$it") }
            h.followerFetch(2, 2)
            h.followerFetch(3, 0)
            repeat(3) { h.produce("new$it") } // LEO=5, HW=2
            assertEquals(2L, h.state().highWatermark)

            // New epoch: ISR {1,3} (3 unknown => HW stays 2), leaderEpochStartOffset = 5.
            h.leaderAndIsr(1, 1, listOf(1, 3), listOf(1, 2, 3))
            assertEquals(5L, h.state().leaderEpochStartOffset)
            assertEquals(2L, h.state().highWatermark)

            h.followerFetch(2, 3) // >= HW but < leaderEpochStartOffset
            assertEquals(listOf(1, 3), h.state().maximalIsr)
            h.followerFetch(2, 5)
            assertEquals(listOf(1, 2, 3), h.state().maximalIsr, "joins maximal ISR before the CAS")
            assertEquals(listOf(1, 3), h.state().committedIsr)

            h.runIsrUpdater()
            assertEquals(listOf(1, 2, 3), h.state().committedIsr)
            assertEquals(listOf(1, 2, 3), h.store.states[h.tp]!!.value.isr)
        }
    }

    @Test
    fun `a follower below HW does not rejoin`() {
        ReplicaHarness(dir).use { h ->
            h.leaderAndIsr(1, 0, listOf(1), listOf(1, 2))
            repeat(3) { h.produce("v$it") }
            h.followerFetch(2, 2)
            assertEquals(listOf(1), h.state().maximalIsr)
            h.followerFetch(2, 3)
            assertEquals(listOf(1, 2), h.state().maximalIsr)
        }
    }

    @Test
    fun `an expansion racing a produce does not ack before the joining follower has the record`() {
        ReplicaHarness(dir, minInsyncReplicas = 1).use { h ->
            h.leaderAndIsr(1, 0, listOf(1), listOf(1, 2))
            h.produce("a")
            h.followerFetch(2, 1) // caught up: joins maximal ISR, CAS not yet run
            assertEquals(listOf(1), h.state().committedIsr)

            val pending = h.produceAsync("b", ACKS_ALL)
            eventually { assertEquals(2L, h.state().logEndOffset) }
            assertEquals(1L, h.state().highWatermark, "HW waits for the pending member")
            assertFalse(pending.isDone)

            h.runIsrUpdater() // the expansion commits while the produce is still waiting
            assertEquals(listOf(1, 2), h.state().committedIsr)
            assertFalse(pending.isDone)

            h.followerFetch(2, 2)
            val result = pending.get(5, TimeUnit.SECONDS)
            assertEquals(ErrorCodes.NONE, result.errorCode)
            assertEquals(1L, result.offset)
        }
    }

    @Test
    fun `a follower catching up requests an ISR expansion exactly once`() {
        val expansions = AtomicInteger()
        val partition = Partition(TopicPartition("t", 0), 1, Log(File(dir, "t-0")), MutableClock(), 1, 1_000) {
            expansions.incrementAndGet()
        }
        try {
            partition.applyLeaderAndIsr(LeaderAndIsrPartition("t", 0, 1, 0, listOf(1), listOf(1, 2), 0))
            partition.produce(null, "a".toByteArray(), ACKS_ONE, 1_000)
            partition.fetchAsFollower(2, 1, 0, 1 shl 20)
            assertEquals(1, expansions.get(), "joining the maximal ISR asks the isr-updater to run")
            partition.fetchAsFollower(2, 1, 0, 1 shl 20)
            assertEquals(1, expansions.get(), "already a pending member: no second request")
        } finally {
            partition.close()
        }
    }

    @Test
    fun `a partition whose ISR CAS was fenced proposes no further ISR change`() {
        val clock = MutableClock()
        val partition = Partition(TopicPartition("t", 0), 1, Log(File(dir, "t-0")), clock, 1, 1_000) {}
        try {
            partition.applyLeaderAndIsr(LeaderAndIsrPartition("t", 0, 1, 0, listOf(1, 2), listOf(1, 2), 0))
            clock.advance(1_001)
            val proposal = partition.prepareIsrChange()!!
            partition.completeIsrChange(proposal, Partition.IsrWriteOutcome.Conflict(Versioned(PartitionState(2, 1, listOf(2), 1), 9)))
            assertTrue(partition.snapshot().isrStale)
            assertNull(partition.prepareIsrChange(), "stale: awaits LeaderAndIsr instead of proposing")
        } finally {
            partition.close()
        }
    }

    // ---- acks ----

    @Test
    fun `acks=all completes once every maximal-ISR member has the record`() {
        ReplicaHarness(dir, minInsyncReplicas = 2).use { h ->
            h.leaderAndIsr(1, 0, listOf(1, 2), listOf(1, 2))
            val pending = h.produceAsync("a")
            eventually { assertEquals(1L, h.state().logEndOffset) }
            assertFalse(pending.isDone)
            h.followerFetch(2, 1)
            assertEquals(ProduceResult(ErrorCodes.NONE, 0L), pending.get(5, TimeUnit.SECONDS))
        }
    }

    @Test
    fun `acks=all is woken as soon as the HW passes the record, not at its timeout`() {
        ReplicaHarness(dir, minInsyncReplicas = 2).use { h ->
            h.leaderAndIsr(1, 0, listOf(1, 2), listOf(1, 2))
            val pending = h.produceAsync("a", timeoutMs = 20_000)
            eventually { assertEquals(1L, h.state().logEndOffset) }
            h.followerFetch(2, 1)
            assertEquals(ProduceResult(ErrorCodes.NONE, 0L), pending.get(3, TimeUnit.SECONDS))
        }
    }

    @Test
    fun `acks=1 returns right after the local append`() {
        ReplicaHarness(dir).use { h ->
            h.leaderAndIsr(1, 0, listOf(1, 2), listOf(1, 2))
            assertEquals(ProduceResult(ErrorCodes.NONE, 0L), h.produce("a"))
            assertEquals(ProduceResult(ErrorCodes.NONE, 1L), h.produce("b"))
            assertEquals(0L, h.state().highWatermark)
        }
    }

    @Test
    fun `acks=all with committed ISR below minISR fails NOT_ENOUGH_REPLICAS without appending`() {
        ReplicaHarness(dir, minInsyncReplicas = 2).use { h ->
            h.leaderAndIsr(1, 0, listOf(1), listOf(1, 2))
            assertEquals(ErrorCodes.NOT_ENOUGH_REPLICAS, h.produce("a", ACKS_ALL).errorCode)
            assertEquals(0L, h.state().logEndOffset)
            // pending adds do not count towards minISR (committed ISR only)
            h.followerFetch(2, 0)
            assertEquals(listOf(1, 2), h.state().maximalIsr)
            assertEquals(ErrorCodes.NOT_ENOUGH_REPLICAS, h.produce("a", ACKS_ALL).errorCode)
            assertEquals(ErrorCodes.NONE, h.produce("a", ACKS_ONE).errorCode, "acks=1 unaffected")
        }
    }

    @Test
    fun `acks=all times out with REQUEST_TIMED_OUT`() {
        ReplicaHarness(dir, minInsyncReplicas = 2).use { h ->
            h.leaderAndIsr(1, 0, listOf(1, 2), listOf(1, 2))
            val result = h.produce("a", ACKS_ALL, timeoutMs = 100)
            assertEquals(ErrorCodes.REQUEST_TIMED_OUT, result.errorCode)
            assertEquals(1L, h.state().logEndOffset, "the record was appended; outcome unknown")
        }
    }

    @Test
    fun `acks=all waiter fails NOT_LEADER when leadership is lost while waiting`() {
        ReplicaHarness(dir, minInsyncReplicas = 2).use { h ->
            h.leaderAndIsr(1, 0, listOf(1, 2), listOf(1, 2))
            val pending = h.produceAsync("a", timeoutMs = 10_000)
            eventually { assertEquals(1L, h.state().logEndOffset) }
            h.leaderAndIsr(leader = 2, epoch = 1, isr = listOf(1, 2), replicas = listOf(1, 2))
            assertEquals(ErrorCodes.NOT_LEADER_FOR_PARTITION, pending.get(5, TimeUnit.SECONDS).errorCode)
        }
    }

    @Test
    fun `acks=all waiter fails NOT_LEADER on a new epoch even when staying leader`() {
        ReplicaHarness(dir, minInsyncReplicas = 2).use { h ->
            h.leaderAndIsr(1, 0, listOf(1, 2), listOf(1, 2))
            val pending = h.produceAsync("a", timeoutMs = 10_000)
            eventually { assertEquals(1L, h.state().logEndOffset) }
            h.leaderAndIsr(leader = 1, epoch = 1, isr = listOf(1, 2), replicas = listOf(1, 2))
            assertEquals(ErrorCodes.NOT_LEADER_FOR_PARTITION, pending.get(5, TimeUnit.SECONDS).errorCode)
        }
    }

    @Test
    fun `acks=all acked after ISR shrank below minISR returns NOT_ENOUGH_REPLICAS_AFTER_APPEND`() {
        ReplicaHarness(dir, minInsyncReplicas = 2, lagMs = 1_000).use { h ->
            h.leaderAndIsr(1, 0, listOf(1, 2), listOf(1, 2))
            val pending = h.produceAsync("a", timeoutMs = 10_000)
            eventually { assertEquals(1L, h.state().logEndOffset) }
            h.clock.advance(1_001)
            h.runIsrUpdater() // follower 2 shrunk => HW = LEO, committed ISR {1} < minISR 2
            assertEquals(ErrorCodes.NOT_ENOUGH_REPLICAS_AFTER_APPEND, pending.get(5, TimeUnit.SECONDS).errorCode)
        }
    }

    @Test
    fun `acks other than 1 and -1 is INVALID_REQUIRED_ACKS`() {
        ReplicaHarness(dir).use { h ->
            h.leaderAndIsr(1, 0, listOf(1), listOf(1))
            assertEquals(ErrorCodes.INVALID_REQUIRED_ACKS, h.produce("a", acks = 0).errorCode)
            assertEquals(ErrorCodes.INVALID_REQUIRED_ACKS, h.produce("a", acks = 2).errorCode)
            assertEquals(0L, h.state().logEndOffset)
        }
    }

    @Test
    fun `produce to a follower or unknown partition is NOT_LEADER`() {
        ReplicaHarness(dir).use { h ->
            assertEquals(ErrorCodes.NOT_LEADER_FOR_PARTITION, h.produce("a").errorCode, "partition not opened yet")
            h.leaderAndIsr(leader = 2, epoch = 0, isr = listOf(1, 2), replicas = listOf(1, 2))
            assertEquals(ErrorCodes.NOT_LEADER_FOR_PARTITION, h.produce("a").errorCode)
            assertEquals(ErrorCodes.NOT_LEADER_FOR_PARTITION, h.produce("a", ACKS_ALL).errorCode)
        }
    }

    // ---- consumer fetch ----

    @Test
    fun `consumer fetch returns only records below HW`() {
        ReplicaHarness(dir).use { h ->
            h.leaderAndIsr(1, 0, listOf(1, 2), listOf(1, 2))
            repeat(5) { h.produce("v$it") }
            h.followerFetch(2, 3) // HW = 3, LEO = 5
            val below = h.consumerFetch(0)
            assertEquals(ErrorCodes.NONE, below.errorCode)
            assertEquals(listOf(0L, 1L, 2L), below.records.map { it.offset })
            assertEquals(3L, below.highWatermark)

            for (offset in 3L..5L) {
                val r = h.consumerFetch(offset)
                assertEquals(ErrorCodes.NONE, r.errorCode, "offset $offset in [HW, LEO]")
                assertTrue(r.records.isEmpty())
            }
            assertEquals(ErrorCodes.OFFSET_OUT_OF_RANGE, h.consumerFetch(6).errorCode)
            assertEquals(ErrorCodes.OFFSET_OUT_OF_RANGE, h.consumerFetch(-1).errorCode)
        }
    }

    @Test
    fun `records carry the leader's clock time as their timestamp`() {
        ReplicaHarness(dir).use { h ->
            h.leaderAndIsr(1, 0, listOf(1), listOf(1))
            h.clock.set(42_000L)
            h.produce("a")
            h.clock.set(43_500L)
            h.produce("b")
            assertEquals(listOf(42_000L, 43_500L), h.consumerFetch(0).records.map { it.timestamp })
        }
    }

    @Test
    fun `consumer fetch from a follower or unknown partition is NOT_LEADER`() {
        ReplicaHarness(dir).use { h ->
            assertEquals(ErrorCodes.NOT_LEADER_FOR_PARTITION, h.consumerFetch(0).errorCode)
            h.leaderAndIsr(2, 0, listOf(1, 2), listOf(1, 2))
            assertEquals(ErrorCodes.NOT_LEADER_FOR_PARTITION, h.consumerFetch(0).errorCode)
        }
    }

    // ---- follower fetch validation ----

    @Test
    fun `follower fetch with an older epoch is FENCED and a newer one UNKNOWN, neither recorded`() {
        ReplicaHarness(dir).use { h ->
            h.leaderAndIsr(1, 3, listOf(1, 2), listOf(1, 2))
            h.produce("a")
            assertEquals(ErrorCodes.FENCED_LEADER_EPOCH, h.followerFetch(2, 1, epoch = 2).errorCode)
            assertEquals(ErrorCodes.UNKNOWN_LEADER_EPOCH, h.followerFetch(2, 1, epoch = 4).errorCode)
            assertEquals(-1L, h.state().followerLeos[2])
            assertEquals(0L, h.state().highWatermark)
            assertEquals(ErrorCodes.NONE, h.followerFetch(2, 1, epoch = 3).errorCode)
            assertEquals(1L, h.state().highWatermark)
        }
    }

    @Test
    fun `follower fetch without a leader epoch (-1) is FENCED and not recorded`() {
        ReplicaHarness(dir).use { h ->
            h.leaderAndIsr(1, 3, listOf(1, 2), listOf(1, 2))
            h.produce("a")
            assertEquals(ErrorCodes.FENCED_LEADER_EPOCH, h.followerFetch(2, 1, epoch = -1).errorCode,
                "a replica fetch must name its leader epoch; -1 skips fencing only for consumers")
            assertEquals(-1L, h.state().followerLeos[2])
            assertEquals(0L, h.state().highWatermark)
            assertEquals(listOf(1, 2), h.state().maximalIsr)
        }
    }

    @Test
    fun `follower fetch from a replica not in the assignment is REPLICA_NOT_ASSIGNED`() {
        ReplicaHarness(dir).use { h ->
            h.leaderAndIsr(1, 0, listOf(1), listOf(1, 2))
            h.produce("a")
            assertEquals(ErrorCodes.REPLICA_NOT_ASSIGNED, h.followerFetch(9, 1).errorCode)
            assertEquals(ErrorCodes.REPLICA_NOT_ASSIGNED, h.followerFetch(9, 1, epoch = 7).errorCode, "checked before epoch")
            assertEquals(listOf(1), h.state().maximalIsr)
        }
    }

    @Test
    fun `follower fetch returns records up to LEO, beyond HW`() {
        ReplicaHarness(dir).use { h ->
            h.leaderAndIsr(1, 0, listOf(1, 2), listOf(1, 2))
            repeat(3) { h.produce("v$it") }
            val r = h.followerFetch(2, 0)
            assertEquals(listOf(0L, 1L, 2L), r.records.map { it.offset })
            assertEquals(listOf(0, 0, 0), r.records.map { it.leaderEpoch })
            assertEquals(0L, r.highWatermark)
            assertEquals(ErrorCodes.OFFSET_OUT_OF_RANGE, h.followerFetch(2, 4).errorCode)
        }
    }

    @Test
    fun `follower fetch sent to a non-leader is NOT_LEADER`() {
        ReplicaHarness(dir).use { h ->
            h.leaderAndIsr(2, 0, listOf(1, 2), listOf(1, 2))
            assertEquals(ErrorCodes.NOT_LEADER_FOR_PARTITION, h.followerFetch(2, 0).errorCode)
        }
    }
}
