package minikafka.cluster

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test

class PartitionLeaderElectorTest {

    @Test
    fun `electLeader picks the first live ISR replica in assignment order`() {
        val leader = PartitionLeaderElector.electLeader(
            assignedReplicas = listOf(1, 2, 3),
            isr = listOf(1, 2, 3),
            liveBrokers = setOf(1, 2, 3)
        )
        assertEquals(1, leader)
    }

    @Test
    fun `electLeader skips a dead ISR member`() {
        val leader = PartitionLeaderElector.electLeader(
            assignedReplicas = listOf(1, 2, 3),
            isr = listOf(1, 2, 3),
            liveBrokers = setOf(2, 3)
        )
        assertEquals(2, leader)
    }

    @Test
    fun `electLeader skips a live replica that is not in the ISR`() {
        val leader = PartitionLeaderElector.electLeader(
            assignedReplicas = listOf(1, 2, 3),
            isr = listOf(2, 3),
            liveBrokers = setOf(1, 2, 3)
        )
        assertEquals(2, leader)
    }

    @Test
    fun `electLeader returns null rather than electing an out-of-ISR replica`() {
        val leader = PartitionLeaderElector.electLeader(
            assignedReplicas = listOf(1, 2, 3),
            isr = listOf(2),
            liveBrokers = setOf(1, 3)
        )
        assertNull(leader)
    }

    @Test
    fun `initialState picks the first live assigned replica as leader and isr in assignment order`() {
        val state = PartitionLeaderElector.initialState(
            assignedReplicas = listOf(1, 2, 3),
            liveBrokers = setOf(2, 3)
        )
        assertEquals(2 to listOf(2, 3), state)
    }

    @Test
    fun `initialState is null when every assigned replica is dead`() {
        val state = PartitionLeaderElector.initialState(
            assignedReplicas = listOf(1, 2, 3),
            liveBrokers = setOf(4, 5)
        )
        assertNull(state)
    }
}
