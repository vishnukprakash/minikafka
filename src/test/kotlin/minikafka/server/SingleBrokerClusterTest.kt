package minikafka.server

import minikafka.client.MiniKafkaClient
import minikafka.model.TopicPartition
import minikafka.net.Connection
import minikafka.proto.ApiKeys
import minikafka.proto.FetchRequest
import minikafka.proto.FetchResponse
import minikafka.proto.ProduceRequest
import minikafka.proto.ProduceResponse
import minikafka.proto.ErrorCodes
import minikafka.testing.TestCluster
import minikafka.testing.TestClusterExtension
import minikafka.testing.eventually
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.RegisterExtension
import java.io.IOException

/** A real one-broker cluster (RF=1) over ZooKeeper and TCP: the old single-broker behaviour, now cluster-backed. */
@Tag("zk")
class SingleBrokerClusterTest {
    @JvmField
    @RegisterExtension
    val clusterExtension = TestClusterExtension { TestCluster(size = 1) }
    private val cluster get() = clusterExtension.cluster

    @Test
    fun `the broker elects itself controller and leads every partition of a new topic`() {
        val client = cluster.client()
        assertEquals(1, cluster.awaitController())
        assertEquals(1, cluster.controllerEpoch())
        assertEquals(ErrorCodes.NONE, client.createTopic("demo", 3))

        for (p in 0 until 3) {
            assertEquals(1, cluster.awaitLeader("demo", p))
            val state = cluster.partitionState("demo", p)!!.value
            assertEquals(0, state.leaderEpoch)
            assertEquals(listOf(1), state.isr)
            assertEquals(1, state.controllerEpoch)
        }
        val metadata = client.metadata()
        assertEquals(1, metadata.controllerId)
        assertEquals(listOf(1), metadata.brokers.map { it.id })
        assertEquals(cluster.broker(1).port(), metadata.brokers.single().port) // the actual bound port
        val topic = metadata.topic("demo")!!
        assertEquals(3, topic.numPartitions)
        assertTrue(topic.partitions.all { it.leader == 1 && it.leaderEpoch == 0 && it.replicas == listOf(1) && it.isr == listOf(1) })
        assertEquals(1, cluster.broker(1).snapshot().seenControllerEpoch)
    }

    @Test
    fun `produces and fetches keyed messages end to end`() {
        val client = cluster.client()
        assertEquals(ErrorCodes.NONE, client.createTopic("orders", 2))
        (0 until 2).forEach { cluster.awaitLeader("orders", it) }

        val response1 = client.produce("orders", "user-1".toByteArray(), "first".toByteArray())
        val response2 = client.produce("orders", "user-1".toByteArray(), "second".toByteArray())
        assertEquals(ErrorCodes.NONE, response1.errorCode)
        assertEquals(ErrorCodes.NONE, response2.errorCode)
        assertEquals(response1.partition, response2.partition)

        val fetched = client.fetch("orders", response1.partition, 0L, 1024 * 1024)
        assertEquals(ErrorCodes.NONE, fetched.errorCode)
        assertEquals(2L, fetched.highWatermark)
        assertEquals(2, fetched.records.size)
        assertEquals("first", String(fetched.records[0].value))
        assertEquals("second", String(fetched.records[1].value))
        assertEquals(0, fetched.records[0].leaderEpoch)

        val topics = client.metadata().topics
        assertEquals(1, topics.size)
        assertEquals("orders", topics[0].name)
    }

    @Test
    fun `acks 1 and acks all both succeed and unsupported acks are rejected`() {
        val client = cluster.client()
        client.createTopic("t", 1)
        cluster.awaitLeader("t", 0)
        assertEquals(ErrorCodes.NONE, client.produce("t", null, "a".toByteArray(), acks = MiniKafkaClient.ACKS_LEADER).errorCode)
        assertEquals(ErrorCodes.NONE, client.produce("t", null, "b".toByteArray(), acks = MiniKafkaClient.ACKS_ALL).errorCode)
        assertEquals(ErrorCodes.INVALID_REQUIRED_ACKS, client.produce("t", null, "c".toByteArray(), acks = 0).errorCode)
        val explicit = client.produce("t", null, "d".toByteArray(), partition = 0)
        assertEquals(0, explicit.partition)
        assertEquals(2L, explicit.offset)
        assertEquals(3L, client.fetch("t", 0, 0L).highWatermark)
    }

    @Test
    fun `commits and fetches a consumer offset end to end`() {
        val client = cluster.client()
        client.createTopic("orders", 1)
        cluster.awaitLeader("orders", 0)
        client.produce("orders", null, "a".toByteArray())
        client.produce("orders", null, "b".toByteArray())

        assertEquals(-1L, client.fetchOffset("group-1", "orders", 0))
        assertEquals(ErrorCodes.NONE, client.commitOffset("group-1", "orders", 0, 1L))
        assertEquals(1L, client.fetchOffset("group-1", "orders", 0))
    }

    @Test
    fun `consumer offsets live in ZooKeeper and survive a broker restart`() {
        val client = cluster.client()
        client.createTopic("orders", 1)
        assertEquals(ErrorCodes.NONE, client.commitOffset("group-1", "orders", 0, 7L))
        assertEquals(7L, cluster.admin().fetchOffset("group-1", TopicPartition("orders", 0)))
        assertEquals(false, java.io.File(cluster.dataDir(1), "offsets.log").exists())

        cluster.restartBroker(1)
        cluster.client().use { assertEquals(7L, it.fetchOffset("group-1", "orders", 0)) }
    }

    @Test
    fun `unknown topic is reported immediately without retrying`() {
        // A long backoff: any retry would blow the time bound below.
        val client = cluster.client(cluster.clientConfig().copy(retryBackoffMs = 5_000))
        val started = System.nanoTime()
        assertEquals(ErrorCodes.UNKNOWN_TOPIC, client.fetch("missing", 0, 0L, 1024).errorCode)
        assertEquals(ErrorCodes.UNKNOWN_TOPIC, client.produce("missing", null, "v".toByteArray()).errorCode)
        assertEquals(ErrorCodes.UNKNOWN_TOPIC, client.produce("missing", null, "v".toByteArray(), partition = 0).errorCode)
        assertTrue(System.nanoTime() - started < 3_000_000_000L, "UNKNOWN_TOPIC must not be retried")
    }

    @Test
    fun `broker answers UNKNOWN_TOPIC and UNKNOWN_PARTITION for partitions it does not know`() {
        cluster.client().createTopic("known", 1)
        cluster.awaitLeader("known", 0)
        // Raw requests: the client would catch these from metadata before asking the broker.
        Connection("127.0.0.1", cluster.broker(1).port()).use { conn ->
            fun produce(topic: String, p: Int) = conn.request(ApiKeys.PRODUCE, {
                ProduceRequest(topic, p, null, "v".toByteArray(), -1, 1000).encode(it)
            }) { ProduceResponse.decode(it) }.errorCode
            fun fetch(topic: String, p: Int) = conn.request(ApiKeys.FETCH, { FetchRequest(topic, p, 0L, 1024).encode(it) }) {
                FetchResponse.decode(it)
            }.errorCode
            assertEquals(ErrorCodes.UNKNOWN_TOPIC, produce("missing", 0))
            assertEquals(ErrorCodes.UNKNOWN_TOPIC, fetch("missing", 0))
            assertEquals(ErrorCodes.UNKNOWN_PARTITION, produce("known", 5))
            assertEquals(ErrorCodes.UNKNOWN_PARTITION, fetch("known", 5))
            assertEquals(ErrorCodes.NONE, produce("known", 0))
            // a follower fetch from a broker that is not a replica
            val follower = conn.request(ApiKeys.FETCH, { FetchRequest("known", 0, 0L, 1024, replicaId = 7, currentLeaderEpoch = 0).encode(it) }) {
                FetchResponse.decode(it)
            }
            assertEquals(ErrorCodes.REPLICA_NOT_ASSIGNED, follower.errorCode)
        }
    }

    @Test
    fun `an unknown api key closes the connection`() {
        Connection("127.0.0.1", cluster.broker(1).port(), readTimeoutMs = 5_000).use { conn ->
            assertThrows(IOException::class.java) { conn.request(99.toShort(), {}) { it.readShort() } }
        }
        cluster.client().use { assertEquals(1, it.metadata().controllerId) } // the broker keeps serving
    }

    @Test
    fun `duplicate topic create is rejected and keeps the original assignment`() {
        val client = cluster.client()
        assertEquals(ErrorCodes.NONE, client.createTopic("t", 2))
        assertEquals(ErrorCodes.TOPIC_ALREADY_EXISTS, client.createTopic("t", 5))
        assertEquals(2, cluster.partitionsOf("t").size)
        assertEquals(2, client.metadata().topic("t")!!.numPartitions)
    }

    @Test
    fun `replication factor above the live broker count is rejected`() {
        val client = cluster.client()
        assertEquals(ErrorCodes.INVALID_REPLICATION_FACTOR, client.createTopic("t", 1, replicationFactor = 2))
        assertEquals(ErrorCodes.INVALID_REPLICATION_FACTOR, client.createTopic("t", 1, replicationFactor = 0))
        assertNull(cluster.admin().readAssignment("t"))
        assertEquals(ErrorCodes.NONE, client.createTopic("t", 1, replicationFactor = 1))
    }

    @Test
    fun `restart retains data for hyphenated topic 'my-topic'`() {
        val client = cluster.client()
        assertEquals(ErrorCodes.NONE, client.createTopic("my-topic", 2))
        (0 until 2).forEach { cluster.awaitLeader("my-topic", it) }
        val produced = client.produce("my-topic", null, "test-message".toByteArray())
        assertEquals(ErrorCodes.NONE, produced.errorCode)
        assertEquals(0L, produced.offset)
        val epochBefore = cluster.controllerEpoch()

        cluster.restartBroker(1)
        assertEquals(1, cluster.awaitController())
        assertEquals(epochBefore + 1, cluster.controllerEpoch())
        (0 until 2).forEach { cluster.awaitLeader("my-topic", it) }

        cluster.client().use { restarted ->
            val topics = restarted.metadata().topics
            assertEquals(listOf("my-topic"), topics.map { it.name })
            assertEquals(2, topics.single().numPartitions)
            val fetched = restarted.fetch("my-topic", produced.partition, 0L, 1024)
            assertEquals(ErrorCodes.NONE, fetched.errorCode)
            assertEquals(1, fetched.records.size)
            assertEquals("test-message", String(fetched.records[0].value))
            // and the log keeps growing from where it was
            assertEquals(1L, restarted.produce("my-topic", null, "next".toByteArray(), partition = produced.partition).offset)
        }
    }

    @Test
    fun `consume-style fetch stops at the high watermark`() {
        val client = cluster.client()
        client.createTopic("t", 1)
        cluster.awaitLeader("t", 0)
        repeat(5) { client.produce("t", null, "m$it".toByteArray(), partition = 0) }
        var offset = 0L
        val seen = mutableListOf<String>()
        while (true) {
            val response = client.fetch("t", 0, offset, maxBytes = 64) // small: several rounds
            assertEquals(ErrorCodes.NONE, response.errorCode)
            response.records.forEach { seen += String(it.value); offset = it.offset + 1 }
            if (response.records.isEmpty() || offset >= response.highWatermark) break
        }
        assertEquals((0 until 5).map { "m$it" }, seen)
        assertEquals(ErrorCodes.OFFSET_OUT_OF_RANGE, client.fetch("t", 0, 6L).errorCode)
        assertEquals(0, client.fetch("t", 0, 5L).records.size)
    }

    @Test
    fun `the data dir is locked while in use and bound to its broker id`() {
        val error = assertThrows(IllegalStateException::class.java) { Server(cluster.config(1)).start() }
        assertTrue(error.message!!.contains("locked"), error.message)

        cluster.stopBroker(1)
        val wrongId = cluster.config(1).copy(brokerId = 2)
        val mismatch = assertThrows(IllegalStateException::class.java) { Server(wrongId).start() }
        assertTrue(mismatch.message!!.contains("belongs to broker 1"), mismatch.message)

        cluster.startBroker(1) // the failed attempts released everything
        eventually { assertEquals(setOf(1), cluster.admin().liveBrokers().keys) }
        assertNotEquals(-1L, cluster.broker(1).brokerEpoch)
    }
}
