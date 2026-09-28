package minikafka.testing

import minikafka.log.Record
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

/** The checker must be able to fail: each rule is violated once on synthetic histories and logs. */
class AckedWriteCheckerTest {
    private fun rec(offset: Long, value: String, epoch: Int = 0) = Record(offset, epoch, 0L, null, value.toByteArray())

    private fun acked(seq: Long, offset: Long, unknown: Int = 0) =
        Send(0, seq, "p0-$seq", Outcome.ACKED, 1 + unknown, unknown, Ack(0, seq, offset, "p0-$seq"))

    private fun history(vararg sends: Send) = WorkloadHistory("t", sends.toList())

    private fun view(hw: Long, isr: List<Int>, vararg logs: Pair<Int, List<Record>>) =
        listOf(PartitionReplicas(0, 1, hw, isr, logs.toMap()))

    private val good = listOf(rec(0, "p0-0"), rec(1, "p0-1"))

    @Test
    fun `a clean history has no violations`() {
        val v = AckedWriteChecker.check(history(acked(0, 0), acked(1, 1)), view(2, listOf(1, 2), 1 to good, 2 to good))
        assertEquals(emptyList<String>(), v)
    }

    @Test
    fun `an acked write missing on an ISR replica is lost`() {
        val v = AckedWriteChecker.check(
            history(acked(0, 0), acked(1, 1)), view(2, listOf(1, 2), 1 to good, 2 to good.take(1)), fullyReplicated = false
        )
        assertTrue(v.any { "LOST on ISR replica 2" in it }, "$v")
    }

    @Test
    fun `an acked offset at or above the HW is a violation`() {
        val v = AckedWriteChecker.check(history(acked(0, 0), acked(1, 1)), view(1, listOf(1), 1 to good))
        assertTrue(v.any { "not below the leader HW" in it }, "$v")
    }

    @Test
    fun `a value never attempted is fabricated`() {
        val log = good + rec(2, "p0-99")
        val v = AckedWriteChecker.check(history(acked(0, 0), acked(1, 1)), view(3, listOf(1), 1 to log))
        assertTrue(v.any { "never attempted" in it && "p0-99" in it }, "$v")
        val consumed = AckedWriteChecker.check(history(acked(0, 0), acked(1, 1)), view(2, listOf(1), 1 to good), consumed = mapOf(0 to listOf("zz")))
        assertTrue(consumed.any { "consumer read" in it }, "$consumed")
    }

    @Test
    fun `duplicates are allowed only for sends with unknown-outcome attempts`() {
        val dup = listOf(rec(0, "p0-0"), rec(1, "p0-0"), rec(2, "p0-1"))
        val clean = AckedWriteChecker.check(history(acked(0, 1), acked(1, 2)), view(3, listOf(1), 1 to dup))
        assertTrue(clean.any { "appears 2 times" in it }, "$clean")
        val retried = AckedWriteChecker.check(history(acked(0, 1, unknown = 1), acked(1, 2)), view(3, listOf(1), 1 to dup))
        assertEquals(emptyList<String>(), retried)
        val failed = Send(0, 2, "p0-2", Outcome.FAILED, 3, 0, null)
        val written = AckedWriteChecker.check(history(acked(0, 0), acked(1, 1), failed), view(3, listOf(1), 1 to good + rec(2, "p0-2")))
        assertTrue(written.any { "p0-2 appears 1 times" in it }, "$written")
    }

    @Test
    fun `first occurrences out of send order are a violation`() {
        val swapped = listOf(rec(0, "p0-1"), rec(1, "p0-0"))
        val v = AckedWriteChecker.check(history(acked(0, 1), acked(1, 0)), view(2, listOf(1), 1 to swapped))
        assertTrue(v.any { "follows seq 1" in it }, "$v")
    }

    @Test
    fun `replicas that differ below the HW, or at all once fully replicated, are violations`() {
        val otherEpoch = listOf(rec(0, "p0-0"), rec(1, "p0-1", epoch = 1))
        val below = AckedWriteChecker.check(history(acked(0, 0), acked(1, 1)), view(2, listOf(1, 2), 1 to good, 2 to otherEpoch), fullyReplicated = false)
        assertTrue(below.any { "differs from leader" in it }, "$below")
        val tail = good + rec(2, "p0-2")
        val sends = history(acked(0, 0), acked(1, 1), Send(0, 2, "p0-2", Outcome.INDETERMINATE, 1, 1, null))
        assertEquals(emptyList<String>(), AckedWriteChecker.check(sends, view(2, listOf(1, 2), 1 to tail, 2 to good), fullyReplicated = false))
        val full = AckedWriteChecker.check(sends, view(2, listOf(1, 2), 1 to tail, 2 to good), fullyReplicated = true)
        assertTrue(full.any { "not identical" in it }, "$full")
    }
}
