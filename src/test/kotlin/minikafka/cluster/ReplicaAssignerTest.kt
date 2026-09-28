package minikafka.cluster

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test

class ReplicaAssignerTest {

    @Test
    fun `round robin layout for 3 brokers, 6 partitions, rf 2`() {
        val assignment = ReplicaAssigner.assign(brokerIds = listOf(1, 2, 3), numPartitions = 6, replicationFactor = 2)

        assertEquals(
            mapOf(
                0 to listOf(1, 2),
                1 to listOf(2, 3),
                2 to listOf(3, 1),
                3 to listOf(1, 2),
                4 to listOf(2, 3),
                5 to listOf(3, 1)
            ),
            assignment
        )
    }

    @Test
    fun `no partition assigns the same replica twice`() {
        val assignment = ReplicaAssigner.assign(brokerIds = listOf(1, 2, 3, 4, 5), numPartitions = 10, replicationFactor = 3)

        assignment.values.forEach { replicas ->
            assertEquals(replicas.size, replicas.toSet().size, "duplicate replica in $replicas")
        }
    }

    @Test
    fun `leaders spread evenly across brokers`() {
        val brokerIds = listOf(1, 2, 3)
        val numPartitions = 6
        val assignment = ReplicaAssigner.assign(brokerIds, numPartitions, replicationFactor = 2)

        val leaderCounts = assignment.values.map { it.first() }.groupingBy { it }.eachCount()
        brokerIds.forEach { broker ->
            assertEquals(numPartitions / brokerIds.size, leaderCounts[broker], "broker $broker leader count")
        }
    }

    @Test
    fun `replication factor greater than live broker count is invalid`() {
        assertThrows(InvalidReplicationFactorException::class.java) {
            ReplicaAssigner.assign(brokerIds = listOf(1, 2), numPartitions = 1, replicationFactor = 3)
        }
    }

    @Test
    fun `replication factor of 0 is invalid`() {
        assertThrows(InvalidReplicationFactorException::class.java) {
            ReplicaAssigner.assign(brokerIds = listOf(1, 2, 3), numPartitions = 1, replicationFactor = 0)
        }
    }

    @Test
    fun `numPartitions less than 1 throws IllegalArgumentException`() {
        val exception = assertThrows(IllegalArgumentException::class.java) {
            ReplicaAssigner.assign(brokerIds = listOf(1, 2, 3), numPartitions = 0, replicationFactor = 1)
        }
        assertEquals(false, exception is InvalidReplicationFactorException)
    }

    @Test
    fun `same startIndex is deterministic`() {
        val first = ReplicaAssigner.assign(listOf(1, 2, 3), numPartitions = 6, replicationFactor = 2, startIndex = 1)
        val second = ReplicaAssigner.assign(listOf(1, 2, 3), numPartitions = 6, replicationFactor = 2, startIndex = 1)

        assertEquals(first, second)
    }

    @Test
    fun `different startIndex produces a different assignment`() {
        val first = ReplicaAssigner.assign(listOf(1, 2, 3), numPartitions = 6, replicationFactor = 2, startIndex = 0)
        val second = ReplicaAssigner.assign(listOf(1, 2, 3), numPartitions = 6, replicationFactor = 2, startIndex = 1)

        assertNotEquals(first, second)
    }

    @Test
    fun `unsorted broker ids are sorted before assignment`() {
        val fromUnsorted = ReplicaAssigner.assign(brokerIds = listOf(3, 1, 2), numPartitions = 6, replicationFactor = 2)
        val fromSorted = ReplicaAssigner.assign(brokerIds = listOf(1, 2, 3), numPartitions = 6, replicationFactor = 2)

        assertEquals(fromSorted, fromUnsorted)
    }
}
