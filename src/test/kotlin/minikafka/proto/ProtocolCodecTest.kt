package minikafka.proto

import minikafka.io.CorruptRecordException
import minikafka.log.Record
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream

class ProtocolCodecTest {
    private fun <T> roundTrip(encode: (DataOutputStream) -> Unit, decode: (DataInputStream) -> T): T {
        val buffer = ByteArrayOutputStream()
        encode(DataOutputStream(buffer))
        return decode(DataInputStream(ByteArrayInputStream(buffer.toByteArray())))
    }

    @Test
    fun `round trips CreateTopicRequest`() {
        val decoded = roundTrip({ CreateTopicRequest("t", 3, 2).encode(it) }, { CreateTopicRequest.decode(it) })
        assertEquals(CreateTopicRequest("t", 3, 2), decoded)
    }

    @Test
    fun `round trips MetadataResponse`() {
        val topics = listOf(
            TopicMetadata(
                "t1",
                listOf(
                    PartitionMetadata(0, 1, 3, listOf(1, 2, 3), listOf(1, 2)),
                    PartitionMetadata(1, -1, 5, listOf(2, 3, 1), listOf(2))
                )
            ),
            TopicMetadata("t2", listOf(PartitionMetadata(0, -1, -1, listOf(3), emptyList())))
        )
        val brokers = listOf(BrokerMetadata(1, "localhost", 9092), BrokerMetadata(2, "host-b", 9093))
        val response = MetadataResponse(2, brokers, topics)
        val decoded = roundTrip({ response.encode(it) }, { MetadataResponse.decode(it) })
        assertEquals(response, decoded)
        assertEquals(2, decoded.topic("t1")!!.numPartitions)
        assertEquals(1, decoded.topic("t2")!!.numPartitions)
        assertEquals("host-b", decoded.broker(2)!!.host)

        val empty = MetadataResponse(-1, emptyList(), emptyList())
        assertEquals(empty, roundTrip({ empty.encode(it) }, { MetadataResponse.decode(it) }))
    }

    @Test
    fun `round trips ProduceRequest and ProduceResponse`() {
        val req = roundTrip(
            { ProduceRequest("t", 3, "k".toByteArray(), "v".toByteArray(), -1, 5000).encode(it) },
            { ProduceRequest.decode(it) }
        )
        assertEquals("t", req.topic)
        assertEquals(3, req.partition)
        assertEquals((-1).toShort(), req.acks)
        assertEquals(5000, req.timeoutMs)
        assertEquals("k", String(req.key!!))
        assertEquals("v", String(req.value))

        val reqNullKey = roundTrip(
            { ProduceRequest("t", 0, null, "v".toByteArray(), 1, 30_000).encode(it) },
            { ProduceRequest.decode(it) }
        )
        assertEquals("t", reqNullKey.topic)
        assertEquals(1.toShort(), reqNullKey.acks)
        assertNull(reqNullKey.key)
        assertEquals("v", String(reqNullKey.value))

        val resp = roundTrip(
            { ProduceResponse(ErrorCodes.NONE, 2, 10L).encode(it) },
            { ProduceResponse.decode(it) }
        )
        assertEquals(ProduceResponse(ErrorCodes.NONE, 2, 10L), resp)
    }

    @Test
    fun `round trips FetchRequest and FetchResponse`() {
        val req = roundTrip(
            { FetchRequest("t", 1, 5L, 2048, replicaId = 2, currentLeaderEpoch = 7).encode(it) },
            { FetchRequest.decode(it) }
        )
        assertEquals(FetchRequest("t", 1, 5L, 2048, 2, 7), req)

        val consumerReq = roundTrip({ FetchRequest("t", 0, 0L, 10).encode(it) }, { FetchRequest.decode(it) })
        assertEquals(-1, consumerReq.replicaId)
        assertEquals(-1, consumerReq.currentLeaderEpoch)

        val records = listOf(FetchedRecord(0L, 4, 1000L, null, "v".toByteArray()))
        val resp = roundTrip(
            { FetchResponse(ErrorCodes.NONE, 12L, records).encode(it) },
            { FetchResponse.decode(it) }
        )
        assertEquals(12L, resp.highWatermark)
        assertEquals(1, resp.records.size)
        assertEquals(4, resp.records[0].leaderEpoch)
        assertEquals(1000L, resp.records[0].timestamp)
        assertNull(resp.records[0].key)
        assertEquals("v", String(resp.records[0].value))
        assertEquals(records[0].crc, resp.records[0].crc)

        val recordsWithKey = listOf(FetchedRecord(0L, 0, 1000L, "k".toByteArray(), "v".toByteArray()))
        val respWithKey = roundTrip(
            { FetchResponse(ErrorCodes.NONE, 1L, recordsWithKey).encode(it) },
            { FetchResponse.decode(it) }
        )
        assertEquals(1, respWithKey.records.size)
        assertEquals("k", String(respWithKey.records[0].key!!))
        assertEquals("v", String(respWithKey.records[0].value))
    }

    @Test
    fun `fetched record crc matches the on-disk record crc`() {
        val record = Record(9L, 3, 1234L, "k".toByteArray(), "value".toByteArray())
        val onDisk = ByteArrayOutputStream().also { record.writeTo(DataOutputStream(it)) }.toByteArray()
        val storedCrc = DataInputStream(ByteArrayInputStream(onDisk)).run { readLong(); readInt() }
        assertEquals(storedCrc, FetchedRecord(9L, 3, 1234L, "k".toByteArray(), "value".toByteArray()).crc)
    }

    @Test
    fun `decoding a fetched record whose crc does not match throws CorruptRecordException`() {
        val bytes = ByteArrayOutputStream().also {
            FetchResponse(ErrorCodes.NONE, 1L, listOf(FetchedRecord(0L, 1, 5L, null, "hello".toByteArray()))).encode(DataOutputStream(it))
        }.toByteArray()
        bytes[bytes.size - 1] = (bytes[bytes.size - 1] + 1).toByte() // flip the last value byte
        assertThrows(CorruptRecordException::class.java) {
            FetchResponse.decode(DataInputStream(ByteArrayInputStream(bytes)))
        }

        val wrongCrc = FetchedRecord(0L, 1, 5L, null, "hello".toByteArray(), crc = 42)
        val encoded = ByteArrayOutputStream().also { wrongCrc.encode(DataOutputStream(it)) }.toByteArray()
        assertThrows(CorruptRecordException::class.java) {
            FetchedRecord.decode(DataInputStream(ByteArrayInputStream(encoded)))
        }
    }

    @Test
    fun `round trips OffsetCommitRequest and OffsetFetchResponse`() {
        val commitReq = roundTrip(
            { OffsetCommitRequest("g", "t", 0, 7L).encode(it) },
            { OffsetCommitRequest.decode(it) }
        )
        assertEquals(OffsetCommitRequest("g", "t", 0, 7L), commitReq)

        val fetchResp = roundTrip(
            { OffsetFetchResponse(ErrorCodes.NONE, 7L).encode(it) },
            { OffsetFetchResponse.decode(it) }
        )
        assertEquals(OffsetFetchResponse(ErrorCodes.NONE, 7L), fetchResp)
    }

    @Test
    fun `round trips MetadataRequest`() {
        val decoded = roundTrip(
            { MetadataRequest().encode(it) },
            { MetadataRequest.decode(it) }
        )
        assertNotNull(decoded)
    }

    @Test
    fun `round trips OffsetFetchRequest`() {
        val decoded = roundTrip(
            { OffsetFetchRequest("g", "t", 0).encode(it) },
            { OffsetFetchRequest.decode(it) }
        )
        assertEquals(OffsetFetchRequest("g", "t", 0), decoded)
    }

    @Test
    fun `round trips CreateTopicResponse`() {
        val decoded = roundTrip(
            { CreateTopicResponse(ErrorCodes.NONE).encode(it) },
            { CreateTopicResponse.decode(it) }
        )
        assertEquals(CreateTopicResponse(ErrorCodes.NONE), decoded)
    }

    @Test
    fun `round trips OffsetCommitResponse`() {
        val decoded = roundTrip(
            { OffsetCommitResponse(ErrorCodes.NONE).encode(it) },
            { OffsetCommitResponse.decode(it) }
        )
        assertEquals(OffsetCommitResponse(ErrorCodes.NONE), decoded)
    }

    @Test
    fun `ProduceRequest keeps an empty key and an empty value distinct from a null key`() {
        val decoded = roundTrip(
            { ProduceRequest("t", 0, ByteArray(0), ByteArray(0), 1, 1000).encode(it) },
            { ProduceRequest.decode(it) }
        )
        assertNotNull(decoded.key)
        assertEquals(0, decoded.key!!.size)
        assertEquals(0, decoded.value.size)
    }
}
