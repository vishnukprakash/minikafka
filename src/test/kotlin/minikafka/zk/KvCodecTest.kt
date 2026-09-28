package minikafka.zk

import minikafka.model.BrokerInfo
import minikafka.model.PartitionState
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class KvCodecTest {

    @Test
    fun `encodes sorted k=v lines so bytes are stable regardless of map order`() {
        val a = KvCodec.encode(linkedMapOf("port" to "9092", "host" to "localhost"))
        val b = KvCodec.encode(linkedMapOf("host" to "localhost", "port" to "9092"))
        assertArrayEquals(a, b)
        assertEquals("host=localhost\nport=9092\n", String(a, Charsets.UTF_8))
    }

    @Test
    fun `round trips arbitrary maps including empty values and values containing equals`() {
        val map = mapOf("a" to "", "b" to "x=y", "c" to "1,2,3")
        assertEquals(map, KvCodec.decode(KvCodec.encode(map)))
        assertEquals(emptyMap<String, String>(), KvCodec.decode(KvCodec.encode(emptyMap())))
    }

    @Test
    fun `rejects keys or values that would break the line format`() {
        assertThrows<IllegalArgumentException> { KvCodec.encode(mapOf("a=b" to "1")) }
        assertThrows<IllegalArgumentException> { KvCodec.encode(mapOf("a" to "1\n2")) }
        assertThrows<IllegalArgumentException> { KvCodec.decode("no-equals-sign\n".toByteArray()) }
    }

    @Test
    fun `partition state round trips with stable bytes and allows an empty isr`() {
        val state = PartitionState(leader = 2, leaderEpoch = 5, isr = listOf(2, 3, 1), controllerEpoch = 7)
        val bytes = KvCodec.encodePartitionState(state)
        assertEquals(
            "controller_epoch=7\nisr=2,3,1\nleader=2\nleader_epoch=5\n",
            String(bytes, Charsets.UTF_8)
        )
        assertEquals(state, KvCodec.decodePartitionState(bytes))
        assertArrayEquals(bytes, KvCodec.encodePartitionState(state.copy()))

        val offline = PartitionState(leader = -1, leaderEpoch = 6, isr = emptyList(), controllerEpoch = 7)
        assertEquals(offline, KvCodec.decodePartitionState(KvCodec.encodePartitionState(offline)))
    }

    @Test
    fun `broker info round trips (id comes from the znode name)`() {
        val info = BrokerInfo(id = 3, host = "10.0.0.3", port = 9093)
        val bytes = KvCodec.encodeBrokerInfo(info)
        assertEquals("host=10.0.0.3\nport=9093\n", String(bytes, Charsets.UTF_8))
        assertEquals(info, KvCodec.decodeBrokerInfo(3, bytes))
    }

    @Test
    fun `assignment round trips with stable bytes`() {
        val assignment = mapOf(1 to listOf(2, 3, 1), 0 to listOf(1, 2, 3))
        val bytes = KvCodec.encodeAssignment(assignment)
        assertEquals("0=1,2,3\n1=2,3,1\n", String(bytes, Charsets.UTF_8))
        assertEquals(assignment, KvCodec.decodeAssignment(bytes))
        assertArrayEquals(bytes, KvCodec.encodeAssignment(linkedMapOf(0 to listOf(1, 2, 3), 1 to listOf(2, 3, 1))))
    }

    @Test
    fun `rejects empty keys and znodes missing a required key`() {
        assertThrows<IllegalArgumentException> { KvCodec.encode(mapOf("" to "x")) }
        assertThrows<IllegalArgumentException> { KvCodec.decode("=x\n".toByteArray()) }
        assertThrows<IllegalArgumentException> { KvCodec.decodeBrokerInfo(1, "port=9\n".toByteArray()) }
        assertThrows<IllegalArgumentException> {
            KvCodec.decodePartitionState("leader=1\nleader_epoch=0\ncontroller_epoch=1\n".toByteArray())
        }
    }
}
