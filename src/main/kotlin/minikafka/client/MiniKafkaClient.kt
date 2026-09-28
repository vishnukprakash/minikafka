package minikafka.client

import minikafka.net.Connection
import minikafka.proto.ApiKeys
import minikafka.proto.CreateTopicRequest
import minikafka.proto.CreateTopicResponse
import minikafka.proto.ErrorCodes
import minikafka.proto.FetchRequest
import minikafka.proto.FetchResponse
import minikafka.proto.MetadataRequest
import minikafka.proto.MetadataResponse
import minikafka.proto.OffsetCommitRequest
import minikafka.proto.OffsetCommitResponse
import minikafka.proto.OffsetFetchRequest
import minikafka.proto.OffsetFetchResponse
import minikafka.proto.ProduceRequest
import minikafka.proto.ProduceResponse
import java.io.Closeable
import java.io.IOException

data class HostPort(val host: String, val port: Int) {
    override fun toString(): String = "$host:$port"

    companion object {
        /** `host:port` */
        fun parse(s: String): HostPort {
            val i = s.lastIndexOf(':')
            require(i > 0 && i < s.length - 1) { "expected host:port, got '$s'" }
            return HostPort(s.substring(0, i), s.substring(i + 1).toInt())
        }

        /** `host:port,host:port,...` */
        fun parseList(s: String): List<HostPort> = s.split(',').map { it.trim() }.filter { it.isNotEmpty() }.map(::parse)
    }
}

/**
 * Client timeouts and retry budget. Timeout ordering: [socketTimeoutMs] > [produceTimeoutMs] >
 * the brokers' replica lag time. Retries: up to [maxRetries] after the first attempt, [retryBackoffMs] apart.
 */
data class ClientConfig(
    val socketTimeoutMs: Int = 40_000,
    val produceTimeoutMs: Int = 30_000,
    val maxRetries: Int = 10,
    val retryBackoffMs: Long = 200
)

/**
 * Routing client (algorithm 13). Knows the cluster from a [bootstrap] list, caches METADATA, keeps
 * one [Connection] per broker address, chooses partitions client-side ([Partitioner], D13) and
 * sends produce/fetch to the partition's leader.
 *
 * NOT_LEADER_FOR_PARTITION, LEADER_NOT_AVAILABLE, FENCED_LEADER_EPOCH and I/O errors (including a
 * CRC mismatch on a fetched record) refresh metadata and retry, at most [ClientConfig.maxRetries]
 * times; the last error is then returned (or the last I/O exception thrown). UNKNOWN_TOPIC is
 * returned immediately. A connection whose request threw is closed and never reused. Delivery is
 * at-least-once: a retried produce whose first attempt succeeded is written twice (D20).
 *
 * Methods are synchronized: safe to share, but calls are serialised (use one client per producer
 * thread for parallelism).
 */
class MiniKafkaClient(bootstrap: List<HostPort>, private val config: ClientConfig = ClientConfig()) : Closeable {
    constructor(host: String, port: Int) : this(listOf(HostPort(host, port)))

    private val bootstrap: List<HostPort> = bootstrap.toList().also { require(it.isNotEmpty()) { "empty bootstrap list" } }
    private val partitioner = Partitioner()
    private val connections = HashMap<HostPort, Connection>()
    private var cached: MetadataResponse? = null

    /** Fresh cluster metadata (also refreshes the cache). */
    @Synchronized
    fun metadata(): MetadataResponse = withAnyBroker(config.maxRetries) { fetchMetadata(it) }

    /** Creation is asynchronous: partition leaders become available shortly after NONE is returned. */
    @Synchronized
    fun createTopic(topic: String, numPartitions: Int, replicationFactor: Int = 1): Short =
        withAnyBroker(config.maxRetries) { conn ->
            conn.request(ApiKeys.CREATE_TOPIC, { CreateTopicRequest(topic, numPartitions, replicationFactor).encode(it) }) {
                CreateTopicResponse.decode(it).errorCode
            }
        }.also { cached = null }

    /**
     * @param acks [ACKS_ALL] (-1, default) or [ACKS_LEADER] (1).
     * @param partition explicit partition, or null to let the [Partitioner] choose (key hash / round-robin).
     */
    @Synchronized
    fun produce(
        topic: String,
        key: ByteArray?,
        value: ByteArray,
        acks: Short = ACKS_ALL,
        partition: Int? = null
    ): ProduceResponse {
        val p = partition ?: run {
            val md = topicMetadata(topic, retries = config.maxRetries) ?: return ProduceResponse(ErrorCodes.UNKNOWN_TOPIC, -1, -1L)
            partitioner.partition(topic, key, md.numPartitions)
        }
        return withLeader(topic, p, { ProduceResponse(it, p, -1L) }, { it.errorCode }) { conn ->
            conn.request(ApiKeys.PRODUCE, { ProduceRequest(topic, p, key, value, acks, config.produceTimeoutMs).encode(it) }) {
                ProduceResponse.decode(it)
            }
        }
    }

    /** Consumer fetch from the leader: records below the high watermark, plus the high watermark itself. */
    @Synchronized
    fun fetch(topic: String, partition: Int, offset: Long, maxBytes: Int = 1024 * 1024): FetchResponse =
        withLeader(topic, partition, { FetchResponse(it, -1L, emptyList()) }, { it.errorCode }) { conn ->
            conn.request(ApiKeys.FETCH, { FetchRequest(topic, partition, offset, maxBytes).encode(it) }) {
                FetchResponse.decode(it)
            }
        }

    @Synchronized
    fun commitOffset(group: String, topic: String, partition: Int, offset: Long): Short =
        withAnyBroker(config.maxRetries) { conn ->
            conn.request(ApiKeys.OFFSET_COMMIT, { OffsetCommitRequest(group, topic, partition, offset).encode(it) }) {
                OffsetCommitResponse.decode(it).errorCode
            }
        }

    /** The committed offset, or -1 if the group has none for this partition. */
    @Synchronized
    fun fetchOffset(group: String, topic: String, partition: Int): Long =
        withAnyBroker(config.maxRetries) { conn ->
            conn.request(ApiKeys.OFFSET_FETCH, { OffsetFetchRequest(group, topic, partition).encode(it) }) {
                OffsetFetchResponse.decode(it).offset
            }
        }

    @Synchronized
    override fun close() {
        connections.values.forEach { runCatching { it.close() } }
        connections.clear()
    }

    // ------------------------------------------------------------------ routing

    /**
     * Sends [call] to the leader of [topic]-[partition], refreshing metadata and retrying on
     * retriable errors. [error] builds a response for client-side errors; [code] extracts a
     * response's error code. Returns the last outcome once the budget is spent.
     */
    private fun <T> withLeader(topic: String, partition: Int, error: (Short) -> T, code: (T) -> Short, call: (Connection) -> T): T {
        var last: Result<T>? = null
        for (attempt in 0..config.maxRetries) {
            if (attempt > 0) {
                cached = null // refresh metadata before retrying
                backoff()
            }
            val outcome: Result<T> = try {
                val md = topicMetadata(topic) ?: return error(ErrorCodes.UNKNOWN_TOPIC)
                val pm = md.partition(partition) ?: return error(ErrorCodes.UNKNOWN_PARTITION)
                val leader = cached?.broker(pm.leader)
                if (pm.leader < 0 || leader == null) {
                    Result.success(error(ErrorCodes.LEADER_NOT_AVAILABLE))
                } else {
                    Result.success(onConnection(HostPort(leader.host, leader.port), call))
                }
            } catch (e: IOException) {
                Result.failure(e)
            }
            last = outcome
            val result = outcome.getOrNull() ?: continue
            if (code(result) !in RETRIABLE) return result
        }
        return last!!.getOrThrow()
    }

    /**
     * Metadata of [topic]: from the cache, else (or if absent there) from a fresh METADATA, trying
     * every known broker up to 1 + [retries] times; null = unknown topic.
     */
    private fun topicMetadata(topic: String, retries: Int = 0) =
        cached?.topic(topic) ?: withAnyBroker(retries) { fetchMetadata(it) }.topic(topic)

    private fun fetchMetadata(conn: Connection): MetadataResponse =
        conn.request(ApiKeys.METADATA, { MetadataRequest().encode(it) }) { MetadataResponse.decode(it) }
            .also { cached = it }

    /**
     * Runs [call] against the first reachable broker (known brokers first, then the bootstrap list);
     * a full pass that reaches none is retried up to [retries] more times, then the last I/O error is thrown.
     */
    private fun <T> withAnyBroker(retries: Int, call: (Connection) -> T): T {
        var lastError: IOException? = null
        for (attempt in 0..retries) {
            if (attempt > 0) backoff()
            val candidates = (cached?.brokers?.map { HostPort(it.host, it.port) } ?: emptyList()) + bootstrap
            for (address in candidates.distinct()) {
                try {
                    return onConnection(address, call)
                } catch (e: IOException) {
                    lastError = e
                }
            }
        }
        throw lastError ?: IOException("no broker reachable")
    }

    /** Runs [call] on the (possibly new) connection to [address]; on an I/O error closes and forgets it (never reused). */
    private fun <T> onConnection(address: HostPort, call: (Connection) -> T): T {
        val conn = connections.getOrPut(address) { Connection(address.host, address.port, config.socketTimeoutMs) }
        try {
            return call(conn)
        } catch (e: IOException) {
            connections.remove(address)
            runCatching { conn.close() }
            throw e
        }
    }

    private fun backoff() {
        try {
            Thread.sleep(config.retryBackoffMs)
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
            throw IOException("interrupted while backing off", e)
        }
    }

    companion object {
        const val ACKS_ALL: Short = -1
        const val ACKS_LEADER: Short = 1

        private val RETRIABLE = setOf(
            ErrorCodes.NOT_LEADER_FOR_PARTITION,
            ErrorCodes.LEADER_NOT_AVAILABLE,
            ErrorCodes.FENCED_LEADER_EPOCH
        )
    }
}
