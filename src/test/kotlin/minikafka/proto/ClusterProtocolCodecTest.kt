package minikafka.proto

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream

class ClusterProtocolCodecTest {
    private fun <T> roundTrip(encode: (DataOutputStream) -> Unit, decode: (DataInputStream) -> T): T {
        val buffer = ByteArrayOutputStream()
        encode(DataOutputStream(buffer))
        return decode(DataInputStream(ByteArrayInputStream(buffer.toByteArray())))
    }

    @Test
    fun `round trips LeaderAndIsrPartition`() {
        val partition = LeaderAndIsrPartition(
            topic = "t",
            partition = 1,
            leader = 2,
            leaderEpoch = 3,
            isr = listOf(1, 2, 3),
            replicas = listOf(1, 2, 3),
            zkVersion = 4
        )
        val decoded = roundTrip({ partition.encode(it) }, { LeaderAndIsrPartition.decode(it) })
        assertEquals(partition, decoded)
    }

    @Test
    fun `round trips LeaderAndIsrPartition with empty isr and replicas`() {
        val partition = LeaderAndIsrPartition(
            topic = "t",
            partition = 0,
            leader = -1,
            leaderEpoch = 0,
            isr = emptyList(),
            replicas = emptyList(),
            zkVersion = 0
        )
        val decoded = roundTrip({ partition.encode(it) }, { LeaderAndIsrPartition.decode(it) })
        assertEquals(partition, decoded)
    }

    @Test
    fun `round trips LeaderAndIsrRequest with no partitions`() {
        val request = LeaderAndIsrRequest(controllerId = 1, controllerEpoch = 5, brokerEpoch = 100L, partitions = emptyList())
        val decoded = roundTrip({ request.encode(it) }, { LeaderAndIsrRequest.decode(it) })
        assertEquals(request, decoded)
    }

    @Test
    fun `round trips LeaderAndIsrRequest with many partitions`() {
        val partitions = (0 until 20).map { p ->
            LeaderAndIsrPartition(
                topic = "t$p",
                partition = p,
                leader = p % 3,
                leaderEpoch = p,
                isr = listOf(0, 1, 2),
                replicas = listOf(0, 1, 2),
                zkVersion = p
            )
        }
        val request = LeaderAndIsrRequest(controllerId = 2, controllerEpoch = 7, brokerEpoch = 999L, partitions = partitions)
        val decoded = roundTrip({ request.encode(it) }, { LeaderAndIsrRequest.decode(it) })
        assertEquals(request, decoded)
    }

    @Test
    fun `round trips LeaderAndIsrResponse`() {
        val decoded = roundTrip(
            { LeaderAndIsrResponse(ErrorCodes.NONE).encode(it) },
            { LeaderAndIsrResponse.decode(it) }
        )
        assertEquals(LeaderAndIsrResponse(ErrorCodes.NONE), decoded)

        val errorDecoded = roundTrip(
            { LeaderAndIsrResponse(ErrorCodes.STALE_CONTROLLER_EPOCH).encode(it) },
            { LeaderAndIsrResponse.decode(it) }
        )
        assertEquals(LeaderAndIsrResponse(ErrorCodes.STALE_CONTROLLER_EPOCH), errorDecoded)
    }

    @Test
    fun `round trips OffsetsForLeaderEpochRequest`() {
        val request = OffsetsForLeaderEpochRequest(
            topic = "t",
            partition = 1,
            replicaId = 2,
            currentLeaderEpoch = 3,
            requestedEpoch = 4
        )
        val decoded = roundTrip({ request.encode(it) }, { OffsetsForLeaderEpochRequest.decode(it) })
        assertEquals(request, decoded)
    }

    @Test
    fun `round trips OffsetsForLeaderEpochResponse`() {
        val response = OffsetsForLeaderEpochResponse(errorCode = ErrorCodes.NONE, leaderEpoch = 3, endOffset = 42L)
        val decoded = roundTrip({ response.encode(it) }, { OffsetsForLeaderEpochResponse.decode(it) })
        assertEquals(response, decoded)
    }

    @Test
    fun `every error code is distinct`() {
        val codes = listOf(
            ErrorCodes.NONE,
            ErrorCodes.UNKNOWN_TOPIC,
            ErrorCodes.UNKNOWN_PARTITION,
            ErrorCodes.OFFSET_OUT_OF_RANGE,
            ErrorCodes.TOPIC_ALREADY_EXISTS,
            ErrorCodes.NOT_LEADER_FOR_PARTITION,
            ErrorCodes.LEADER_NOT_AVAILABLE,
            ErrorCodes.NOT_ENOUGH_REPLICAS,
            ErrorCodes.NOT_ENOUGH_REPLICAS_AFTER_APPEND,
            ErrorCodes.REQUEST_TIMED_OUT,
            ErrorCodes.STALE_CONTROLLER_EPOCH,
            ErrorCodes.STALE_BROKER_EPOCH,
            ErrorCodes.FENCED_LEADER_EPOCH,
            ErrorCodes.UNKNOWN_LEADER_EPOCH,
            ErrorCodes.INVALID_REPLICATION_FACTOR,
            ErrorCodes.INVALID_REQUIRED_ACKS,
            ErrorCodes.REPLICA_NOT_ASSIGNED
        )

        assertEquals(codes.size, codes.toSet().size, "expected all error codes to be distinct")
        assertEquals((0..16).map { it.toShort() }, codes.sorted(), "expected error codes numbered consecutively 0..16")
    }

    @Test
    fun `every api key is distinct`() {
        val keys = listOf(
            ApiKeys.CREATE_TOPIC,
            ApiKeys.METADATA,
            ApiKeys.PRODUCE,
            ApiKeys.FETCH,
            ApiKeys.OFFSET_COMMIT,
            ApiKeys.OFFSET_FETCH,
            ApiKeys.LEADER_AND_ISR,
            ApiKeys.OFFSETS_FOR_LEADER_EPOCH
        )
        assertEquals(keys.size, keys.toSet().size, "expected all api keys to be distinct")
    }
}
