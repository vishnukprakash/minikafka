package minikafka.broker

import minikafka.proto.ErrorCodes
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File

class BrokerTest {
    @Test
    fun `creates a topic and rejects a duplicate create`(@TempDir tempDir: File) {
        val broker = Broker(tempDir)
        assertEquals(ErrorCodes.NONE, broker.createTopic("t", 3))
        assertEquals(ErrorCodes.TOPIC_ALREADY_EXISTS, broker.createTopic("t", 3))
    }

    @Test
    fun `produce with a key always routes to the same partition`(@TempDir tempDir: File) {
        val broker = Broker(tempDir)
        broker.createTopic("t", 4)
        val key = "user-123".toByteArray()
        val (_, partitionA, _) = broker.produce("t", key, "v1".toByteArray())
        val (_, partitionB, _) = broker.produce("t", key, "v2".toByteArray())
        assertEquals(partitionA, partitionB)
    }

    @Test
    fun `produce without a key round-robins across partitions`(@TempDir tempDir: File) {
        val broker = Broker(tempDir)
        broker.createTopic("t", 3)
        val partitions = (0 until 3).map { broker.produce("t", null, "v".toByteArray()).second }
        assertEquals(setOf(0, 1, 2), partitions.toSet())
    }

    @Test
    fun `fetch returns records produced earlier`(@TempDir tempDir: File) {
        val broker = Broker(tempDir)
        broker.createTopic("t", 1)
        broker.produce("t", null, "a".toByteArray())
        broker.produce("t", null, "b".toByteArray())

        val (errorCode, records) = broker.fetch("t", 0, 0L, 1024)
        assertEquals(ErrorCodes.NONE, errorCode)
        assertEquals(2, records.size)
    }

    @Test
    fun `fetch on an unknown topic returns an error code`(@TempDir tempDir: File) {
        val broker = Broker(tempDir)
        val (errorCode, records) = broker.fetch("missing", 0, 0L, 1024)
        assertEquals(ErrorCodes.UNKNOWN_TOPIC, errorCode)
        assertEquals(0, records.size)
    }

    @Test
    fun `commits and fetches a consumer offset`(@TempDir tempDir: File) {
        val broker = Broker(tempDir)
        broker.createTopic("t", 1)
        broker.commitOffset("g", "t", 0, 5L)
        val (errorCode, offset) = broker.fetchOffset("g", "t", 0)
        assertEquals(ErrorCodes.NONE, errorCode)
        assertEquals(5L, offset)
    }

    @Test
    fun `recovers topic registry and data on restart with hyphenated topic name`(@TempDir tempDir: File) {
        // First broker: create topic and produce message
        val broker1 = Broker(tempDir)
        assertEquals(ErrorCodes.NONE, broker1.createTopic("my-topic", 2))
        val (_, partition, offset) = broker1.produce("my-topic", null, "test-message".toByteArray())
        assertEquals(0L, offset)

        // Second broker: simulates restart, should recover topic and data
        val broker2 = Broker(tempDir)

        // Verify topic metadata is recovered
        val topics = broker2.listTopics()
        assertEquals(1, topics.size)
        val metadata = topics.first()
        assertEquals("my-topic", metadata.name)
        assertEquals(2, metadata.numPartitions)

        // Verify data is recovered
        val (errorCode, records) = broker2.fetch("my-topic", partition, 0L, 1024)
        assertEquals(ErrorCodes.NONE, errorCode)
        assertEquals(1, records.size)
        assertEquals("test-message".toByteArray().contentToString(), records[0].value.contentToString())
    }
}
