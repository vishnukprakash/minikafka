package minikafka.proto

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.assertEquals
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
        val decoded = roundTrip({ CreateTopicRequest("t", 3).encode(it) }, { CreateTopicRequest.decode(it) })
        assertEquals(CreateTopicRequest("t", 3), decoded)
    }

    @Test
    fun `round trips MetadataResponse`() {
        val topics = listOf(TopicMetadata("t1", 2), TopicMetadata("t2", 1))
        val decoded = roundTrip({ MetadataResponse(topics).encode(it) }, { MetadataResponse.decode(it) })
        assertEquals(topics, decoded.topics)
    }

    @Test
    fun `round trips ProduceRequest and ProduceResponse`() {
        val req = roundTrip(
            { ProduceRequest("t", "k".toByteArray(), "v".toByteArray()).encode(it) },
            { ProduceRequest.decode(it) }
        )
        assertEquals("t", req.topic)
        assertEquals("k", String(req.key!!))
        assertEquals("v", String(req.value))

        val resp = roundTrip(
            { ProduceResponse(ErrorCodes.NONE, 2, 10L).encode(it) },
            { ProduceResponse.decode(it) }
        )
        assertEquals(ProduceResponse(ErrorCodes.NONE, 2, 10L), resp)
    }

    @Test
    fun `round trips FetchRequest and FetchResponse`() {
        val req = roundTrip(
            { FetchRequest("t", 1, 5L, 2048).encode(it) },
            { FetchRequest.decode(it) }
        )
        assertEquals(FetchRequest("t", 1, 5L, 2048), req)

        val records = listOf(FetchedRecord(0L, 1000L, null, "v".toByteArray()))
        val resp = roundTrip(
            { FetchResponse(ErrorCodes.NONE, records).encode(it) },
            { FetchResponse.decode(it) }
        )
        assertEquals(1, resp.records.size)
        assertEquals("v", String(resp.records[0].value))
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
}
