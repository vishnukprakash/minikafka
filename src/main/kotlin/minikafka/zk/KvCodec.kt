package minikafka.zk

import minikafka.model.BrokerInfo
import minikafka.model.PartitionState

/**
 * Znode payload codec (D21): UTF-8 `key=value\n` lines sorted by key, so the same map always
 * encodes to the same bytes and payloads stay readable in `zkCli`. (Not `java.util.Properties`,
 * whose date comment makes bytes unstable.) Keys may not contain `=` or newlines; values may not
 * contain newlines (a value may contain `=`: only the first `=` on a line separates).
 */
object KvCodec {

    fun encode(map: Map<String, String>): ByteArray {
        val sb = StringBuilder()
        for ((k, v) in map.toSortedMap()) {
            require(k.isNotEmpty() && '=' !in k && '\n' !in k) { "invalid key: '$k'" }
            require('\n' !in v) { "invalid value for key '$k': contains a newline" }
            sb.append(k).append('=').append(v).append('\n')
        }
        return sb.toString().toByteArray(Charsets.UTF_8)
    }

    fun decode(bytes: ByteArray): Map<String, String> =
        String(bytes, Charsets.UTF_8).lineSequence().filter { it.isNotEmpty() }.associate { line ->
            val i = line.indexOf('=')
            require(i > 0) { "malformed line: '$line'" }
            line.substring(0, i) to line.substring(i + 1)
        }

    // --- PartitionState: keys leader, leader_epoch, isr (comma list, may be empty), controller_epoch

    fun encodePartitionState(state: PartitionState): ByteArray = encode(
        mapOf(
            "leader" to state.leader.toString(),
            "leader_epoch" to state.leaderEpoch.toString(),
            "isr" to encodeIntList(state.isr),
            "controller_epoch" to state.controllerEpoch.toString()
        )
    )

    fun decodePartitionState(bytes: ByteArray): PartitionState {
        val m = decode(bytes)
        return PartitionState(
            leader = m.int("leader"),
            leaderEpoch = m.int("leader_epoch"),
            isr = decodeIntList(m.required("isr")),
            controllerEpoch = m.int("controller_epoch")
        )
    }

    // --- BrokerInfo: keys host, port (the id is the znode name)

    fun encodeBrokerInfo(info: BrokerInfo): ByteArray =
        encode(mapOf("host" to info.host, "port" to info.port.toString()))

    fun decodeBrokerInfo(id: Int, bytes: ByteArray): BrokerInfo {
        val m = decode(bytes)
        return BrokerInfo(id, m.required("host"), m.int("port"))
    }

    // --- Assignment: keys are partition numbers, values comma lists of replica broker ids

    fun encodeAssignment(assignment: Map<Int, List<Int>>): ByteArray =
        encode(assignment.entries.associate { (p, replicas) -> p.toString() to encodeIntList(replicas) })

    fun decodeAssignment(bytes: ByteArray): Map<Int, List<Int>> =
        decode(bytes).entries.associate { (p, replicas) ->
            (p.toIntOrNull() ?: throw IllegalArgumentException("bad partition key '$p'")) to decodeIntList(replicas)
        }

    private fun encodeIntList(ids: List<Int>): String = ids.joinToString(",")

    private fun decodeIntList(s: String): List<Int> =
        if (s.isEmpty()) emptyList() else s.split(",").map { it.trim().toInt() }

    private fun Map<String, String>.required(key: String): String =
        this[key] ?: throw IllegalArgumentException("missing key '$key' in $this")

    private fun Map<String, String>.int(key: String): Int =
        required(key).toIntOrNull() ?: throw IllegalArgumentException("key '$key' is not an int in $this")
}
