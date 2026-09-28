package minikafka.broker

import minikafka.log.Log
import minikafka.proto.ErrorCodes
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.io.TempDir
import java.io.File
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue

/**
 * Follower truncation by leader epoch (D8, algorithm 8; KIP-101 + iterative KIP-279), with two
 * real [Log]s: the leader log answers each query through its own LeaderEpochCache.
 */
class FollowerTruncationTest {
    @TempDir
    lateinit var dir: File

    private val opened = mutableListOf<Log>()

    @AfterEach
    fun closeLogs() = opened.forEach { it.close() }

    /**
     * A log in `dir/name` holding, for each (epoch, count) run, `count` records of that epoch. The
     * value is derived from (epoch, offset) only, so two logs agree on a record iff they agree on
     * its epoch (divergent records always differ in epoch at the same offset).
     */
    private fun logOf(name: String, vararg runs: Pair<Int, Int>): Log {
        val log = Log(File(dir, name)).also(opened::add)
        for ((epoch, count) in runs) repeat(count) {
            val offset = log.logEndOffset()
            log.appendAsLeader(offset, null, "e$epoch-o$offset".toByteArray(), epoch)
        }
        return log
    }

    /** (offset, epoch, value) of every record in the log. */
    private fun contents(log: Log): List<Triple<Long, Int, String>> {
        val out = mutableListOf<Triple<Long, Int, String>>()
        var next = 0L
        while (next < log.logEndOffset()) {
            val batch = log.read(next, 1 shl 20)
            check(batch.isNotEmpty())
            batch.forEach { out.add(Triple(it.offset, it.leaderEpoch, String(it.value))) }
            next = batch.last().offset + 1
        }
        return out
    }

    private fun assertPrefix(follower: Log, leader: Log) {
        val f = contents(follower)
        val l = contents(leader)
        assertTrue(f.size <= l.size, "follower longer than leader: $f vs $l")
        assertEquals(l.take(f.size), f, "follower is not a prefix of the leader")
    }

    /** Appends the leader's records beyond the follower's LEO, as the fetcher would. */
    private fun catchUp(follower: Log, leader: Log) {
        while (follower.logEndOffset() < leader.logEndOffset()) {
            follower.appendAsFollower(leader.read(follower.logEndOffset(), 1 shl 20))
        }
    }

    /** Leader side: answers from its LeaderEpochCache (same rules as ReplicaManager.offsetsForLeaderEpoch). */
    private fun queriesTo(leader: Log, asked: MutableList<Int> = mutableListOf()): (Int) -> Pair<Int, Long> = { e ->
        asked.add(e)
        leader.endOffsetForEpoch(e)
    }

    @Test
    fun `KIP-101 scenario 1 - committed records beyond a stale HW are kept`() {
        // Leader (epoch 0) wrote m0, m1; the follower replicated both, and the leader's HW became 2
        // (both committed), but the follower only learned HW=1 before restarting. The leader then
        // started epoch 1 and appended m2.
        val leader = logOf("leader", 0 to 2, 1 to 1)
        val follower = logOf("follower", 0 to 2)
        val staleFollowerHw = 1L

        val result = truncateForLeaderEpoch(follower, queriesTo(leader))

        assertEquals(2L, follower.logEndOffset(), "committed m1 must survive")
        assertTrue(follower.logEndOffset() > staleFollowerHw, "HW-based truncation would have dropped m1")
        assertEquals(TruncationResult(2L, 2L, 0, rounds = 1, handshakeSkipped = false), result)
        assertFalse(result.truncated)
        assertPrefix(follower, leader)
    }

    @Test
    fun `KIP-101 scenario 2 - divergent logs after back-to-back failures converge`() {
        // A led epoch 0 and wrote m0, m1; B replicated only m0. Both crashed; B came back first,
        // became leader in epoch 1 and wrote m1' at offset 1. A now rejoins as a follower.
        val leaderB = logOf("b", 0 to 1, 1 to 1)
        val followerA = logOf("a", 0 to 2)

        val result = truncateForLeaderEpoch(followerA, queriesTo(leaderB))

        assertEquals(TruncationResult(2L, 1L, 0, rounds = 1, handshakeSkipped = false), result)
        assertTrue(result.truncated)
        assertPrefix(followerA, leaderB)
        catchUp(followerA, leaderB)
        assertEquals(contents(leaderB), contents(followerA))
    }

    @Test
    fun `multi-round KIP-279 case - follower 1@0 3@5 4@8 vs leader 1@0 2@4 5@7`() {
        val follower = logOf("follower", 1 to 5, 3 to 3, 4 to 2)
        val leader = logOf("leader", 1 to 4, 2 to 3, 5 to 2)
        assertEquals(listOf(1 to 0L, 3 to 5L, 4 to 8L), follower.epochEntries())
        assertEquals(listOf(1 to 0L, 2 to 4L, 5 to 7L), leader.epochEntries())
        val asked = mutableListOf<Int>()

        val result = truncateForLeaderEpoch(follower, queriesTo(leader, asked))

        // Round 1: E=4 -> leader (2, 7) -> min(7, follower end of epoch<=2 = 5, 10) = 5.
        // Round 2: E=1 -> leader (1, 4) -> min(4, 5, 5) = 4; epochs agree, stop.
        assertEquals(listOf(4, 1), asked)
        assertTrue(result.rounds > 1)
        assertEquals(TruncationResult(10L, 4L, 1, rounds = 2, handshakeSkipped = false), result)
        // Offset 4 (epoch 1 on the follower, epoch 2 on the leader) must not survive.
        assertEquals(4L, follower.logEndOffset())
        assertPrefix(follower, leader)
        catchUp(follower, leader)
        assertEquals(contents(leader), contents(follower))
    }

    @Test
    fun `leader answers with a lower epoch the follower lacks`() {
        // Follower {1:0, 3:3}; leader {1:0, 2:3}. The leader's answer for 3 is epoch 2, which the
        // follower never saw: it truncates to the end of its largest epoch <= 2 (epoch 1 ends at 3).
        val follower = logOf("follower", 1 to 3, 3 to 3)
        val leader = logOf("leader", 1 to 3, 2 to 4)

        val result = truncateForLeaderEpoch(follower, queriesTo(leader))

        assertEquals(3L, follower.logEndOffset())
        assertEquals(1, result.finalEpoch)
        assertPrefix(follower, leader)
        catchUp(follower, leader)
        assertEquals(contents(leader), contents(follower))
    }

    @Test
    fun `leader epoch below the follower's earliest epoch truncates to the follower's earliest start`() {
        // Follower only has epoch 3; the leader's largest epoch <= 3 is 1, which the follower lacks
        // entirely (and has nothing older), so nothing of the follower's log can be trusted.
        val follower = logOf("follower", 3 to 3)
        val leader = logOf("leader", 1 to 4, 5 to 1)

        val result = truncateForLeaderEpoch(follower, queriesTo(leader))

        assertEquals(TruncationResult(3L, 0L, -1, rounds = 1, handshakeSkipped = false), result)
        assertEquals(0L, follower.logEndOffset())
        catchUp(follower, leader)
        assertEquals(contents(leader), contents(follower))
    }

    @Test
    fun `empty leader log truncates the follower to 0`() {
        val follower = logOf("follower", 0 to 2, 1 to 2)
        val leader = logOf("leader")

        val result = truncateForLeaderEpoch(follower, queriesTo(leader))

        assertEquals(TruncationResult(4L, 0L, -1, rounds = 1, handshakeSkipped = false), result)
        assertEquals(0L, follower.logEndOffset())
        assertEquals(emptyList<Pair<Int, Long>>(), follower.epochEntries())
    }

    @Test
    fun `empty follower log skips the handshake`() {
        val follower = logOf("follower")

        val result = truncateForLeaderEpoch(follower, queryLeader = { error("queryLeader must not be called") })

        assertEquals(TruncationResult(0L, 0L, -1, rounds = 0, handshakeSkipped = true), result)
    }

    @Test
    fun `follower identical to the leader is not truncated and takes one round`() {
        val follower = logOf("follower", 0 to 3, 2 to 2)
        val leader = logOf("leader", 0 to 3, 2 to 2)
        val asked = mutableListOf<Int>()

        val result = truncateForLeaderEpoch(follower, queriesTo(leader, asked))

        assertEquals(listOf(2), asked)
        assertEquals(TruncationResult(5L, 5L, 2, rounds = 1, handshakeSkipped = false), result)
        assertEquals(contents(leader), contents(follower))
    }

    @Test
    fun `epoch cache is trimmed after truncation, also when the log is reopened`() {
        val follower = logOf("follower", 1 to 5, 3 to 3, 4 to 2)
        val leader = logOf("leader", 1 to 4, 2 to 3, 5 to 2)

        truncateForLeaderEpoch(follower, queriesTo(leader))

        assertEquals(listOf(1 to 0L), follower.epochEntries())
        assertEquals(1, follower.latestEpoch())
        follower.close()
        opened.remove(follower)
        val reopened = Log(File(dir, "follower")).also(opened::add)
        assertEquals(listOf(1 to 0L), reopened.epochEntries())
        assertEquals(4L, reopened.logEndOffset())
    }

    @Test
    fun `iteration cap throws on a pathological leader`() {
        val follower = logOf("follower", 0 to 2, 1 to 2)
        var calls = 0

        val e = assertThrows<IllegalStateException> {
            // Always claims an epoch above the requested one: epochs never converge.
            truncateForLeaderEpoch(follower, { requested -> calls++; (requested + 1) to Long.MAX_VALUE }, maxRounds = 5)
        }

        assertEquals(5, calls)
        assertTrue(e.message!!.contains("5"), e.message)
        assertEquals(4L, follower.logEndOffset())
    }

    // ---- through ReplicaManager (locking and fencing around the pure unit) ----

    @Test
    fun `ReplicaManager truncates a follower and caps its HW`() {
        ReplicaHarness(File(dir, "b2"), brokerId = 2).use { h ->
            h.leaderAndIsr(leader = 2, epoch = 0, isr = listOf(2), replicas = listOf(1, 2))
            repeat(3) { h.produce("m$it") } // epoch 0, offsets 0..2, HW 3 (ISR = {2})
            h.leaderAndIsr(leader = 1, epoch = 1, isr = listOf(1, 2), replicas = listOf(1, 2))
            val leader = logOf("leader", 0 to 1, 1 to 2) // new leader kept only offset 0 of epoch 0

            val outcome = h.rm.truncateFollower(h.tp, expectedLeaderEpoch = 1, queryLeader = queriesTo(leader))

            assertEquals(ErrorCodes.NONE, outcome.errorCode)
            assertEquals(TruncationResult(3L, 1L, 0, rounds = 1, handshakeSkipped = false), outcome.result)
            assertEquals(1L, h.state().logEndOffset)
            assertEquals(1L, h.state().highWatermark)
        }
    }

    @Test
    fun `ReplicaManager truncation is fenced if the leader epoch moves while the leader is queried`() {
        ReplicaHarness(File(dir, "b2"), brokerId = 2).use { h ->
            h.leaderAndIsr(leader = 2, epoch = 0, isr = listOf(2), replicas = listOf(1, 2))
            repeat(3) { h.produce("m$it") }
            h.leaderAndIsr(leader = 1, epoch = 1, isr = listOf(1, 2), replicas = listOf(1, 2))

            val outcome = h.rm.truncateFollower(h.tp, expectedLeaderEpoch = 1) { _ ->
                // Runs with no partition lock held: a LeaderAndIsr can be applied meanwhile.
                h.leaderAndIsr(leader = 1, epoch = 2, isr = listOf(1, 2), replicas = listOf(1, 2))
                -1 to 0L
            }

            assertEquals(ErrorCodes.FENCED_LEADER_EPOCH, outcome.errorCode)
            assertNull(outcome.result)
            assertEquals(3L, h.state().logEndOffset, "nothing truncated for a stale epoch")
        }
    }

    @Test
    fun `ReplicaManager refuses to truncate a leader or an unknown partition`() {
        ReplicaHarness(File(dir, "b2"), brokerId = 2).use { h ->
            val never: (Int) -> Pair<Int, Long> = { error("queryLeader must not be called") }
            assertEquals(ErrorCodes.NOT_LEADER_FOR_PARTITION, h.rm.truncateFollower(h.tp, 0, never).errorCode)
            h.leaderAndIsr(leader = 2, epoch = 0, isr = listOf(2), replicas = listOf(1, 2))
            h.produce("m0")
            assertEquals(ErrorCodes.NOT_LEADER_FOR_PARTITION, h.rm.truncateFollower(h.tp, 0, never).errorCode)
            h.leaderAndIsr(leader = 1, epoch = 1, isr = listOf(1, 2), replicas = listOf(1, 2))
            assertEquals(ErrorCodes.FENCED_LEADER_EPOCH, h.rm.truncateFollower(h.tp, 0, never).errorCode)
            assertEquals(1L, h.state().logEndOffset)
        }
    }
}
