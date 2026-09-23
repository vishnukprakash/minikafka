package minikafka.integration

import minikafka.client.MiniKafkaClient
import minikafka.server.Server
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File

class BrokerIntegrationTest {
    private lateinit var server: Server

    @BeforeEach
    fun startServer(@TempDir tempDir: File) {
        server = Server(port = 0, dataDir = tempDir)
        server.start()
    }

    @AfterEach
    fun stopServer() {
        server.stop()
    }

    @Test
    fun `produces and fetches keyed messages end to end`() {
        MiniKafkaClient("localhost", server.port()).use { client ->
            assertEquals(0.toShort(), client.createTopic("orders", 2))

            val response1 = client.produce("orders", "user-1".toByteArray(), "first".toByteArray())
            val response2 = client.produce("orders", "user-1".toByteArray(), "second".toByteArray())
            assertEquals(response1.partition, response2.partition)

            val fetched = client.fetch("orders", response1.partition, 0L, 1024 * 1024)
            assertEquals(2, fetched.records.size)
            assertEquals("first", String(fetched.records[0].value))
            assertEquals("second", String(fetched.records[1].value))

            val topics = client.metadata()
            assertEquals(1, topics.size)
            assertEquals("orders", topics[0].name)
        }
    }

    @Test
    fun `commits and fetches a consumer offset end to end`() {
        MiniKafkaClient("localhost", server.port()).use { client ->
            client.createTopic("orders", 1)
            client.produce("orders", null, "a".toByteArray())
            client.produce("orders", null, "b".toByteArray())

            assertEquals(-1L, client.fetchOffset("group-1", "orders", 0))
            assertEquals(0.toShort(), client.commitOffset("group-1", "orders", 0, 1L))
            assertEquals(1L, client.fetchOffset("group-1", "orders", 0))
        }
    }

    @Test
    fun `returns an error code for an unknown topic`() {
        MiniKafkaClient("localhost", server.port()).use { client ->
            val fetched = client.fetch("missing", 0, 0L, 1024)
            assertNotEquals(0.toShort(), fetched.errorCode)
        }
    }
}
