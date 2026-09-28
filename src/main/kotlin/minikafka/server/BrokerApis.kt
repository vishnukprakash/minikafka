package minikafka.server

import minikafka.broker.ReplicaManager
import minikafka.cluster.InvalidReplicationFactorException
import minikafka.cluster.ReplicaAssigner
import minikafka.model.TopicPartition
import minikafka.proto.BrokerMetadata
import minikafka.proto.CreateTopicRequest
import minikafka.proto.CreateTopicResponse
import minikafka.proto.ErrorCodes
import minikafka.proto.FetchRequest
import minikafka.proto.FetchResponse
import minikafka.proto.FetchedRecord
import minikafka.proto.LeaderAndIsrRequest
import minikafka.proto.LeaderAndIsrResponse
import minikafka.proto.MetadataResponse
import minikafka.proto.OffsetCommitRequest
import minikafka.proto.OffsetCommitResponse
import minikafka.proto.OffsetFetchRequest
import minikafka.proto.OffsetFetchResponse
import minikafka.proto.OffsetsForLeaderEpochRequest
import minikafka.proto.OffsetsForLeaderEpochResponse
import minikafka.proto.PartitionMetadata
import minikafka.proto.ProduceRequest
import minikafka.proto.ProduceResponse
import minikafka.proto.TopicMetadata
import minikafka.zk.ZkStore
import org.slf4j.LoggerFactory

/**
 * One handler per API key; [ConnectionHandler] does the framing. Produce, fetch, LeaderAndIsr and
 * OffsetsForLeaderEpoch go to the [ReplicaManager]; topic creation, metadata (D6) and consumer
 * offsets (D14) read or write ZooKeeper directly, so any broker can serve them. A ZooKeeper failure
 * surfaces as an exception, which makes the handler drop the connection (the client retries).
 *
 * @param brokerEpoch this broker's current registration czxid (changes after a session expiry).
 */
internal class BrokerApis(
    private val brokerId: Int,
    private val zk: ZkStore,
    private val replicaManager: ReplicaManager,
    private val brokerEpoch: () -> Long
) {
    private val log = LoggerFactory.getLogger(BrokerApis::class.java)

    /**
     * Algorithm 5 (the broker half): validate, assign replicas over the live brokers, and create
     * the assignment znode — the topic's commit point. The controller creates the state znodes.
     * An illegal name answers UNKNOWN_TOPIC and a partition count < 1 UNKNOWN_PARTITION (the wire
     * protocol has no dedicated codes for these).
     */
    fun createTopic(req: CreateTopicRequest): CreateTopicResponse {
        if (!isLegalTopicName(req.topic)) return CreateTopicResponse(ErrorCodes.UNKNOWN_TOPIC)
        if (req.numPartitions < 1) return CreateTopicResponse(ErrorCodes.UNKNOWN_PARTITION)
        val live = zk.liveBrokers().keys.toList()
        val assignment = try {
            ReplicaAssigner.assign(live, req.numPartitions, req.replicationFactor, Math.floorMod(req.topic.hashCode(), maxOf(1, live.size)))
        } catch (e: InvalidReplicationFactorException) {
            log.info("b{}: create topic {} rejected: {}", brokerId, req.topic, e.message)
            return CreateTopicResponse(ErrorCodes.INVALID_REPLICATION_FACTOR)
        }
        // NodeExists ⇒ TOPIC_ALREADY_EXISTS; a Curator replay of our own create reports it too (documented).
        if (!zk.createTopicAssignment(req.topic, assignment)) return CreateTopicResponse(ErrorCodes.TOPIC_ALREADY_EXISTS)
        log.info("b{}: created topic {} with assignment {}", brokerId, req.topic, assignment.toSortedMap())
        return CreateTopicResponse(ErrorCodes.NONE)
    }

    /** D6: read straight from ZooKeeper; a partition without a state znode reports leader -1, epoch -1. */
    fun metadata(): MetadataResponse {
        val controllerId = zk.currentController() ?: -1
        val brokers = zk.liveBrokers().values.map { (info, _) -> BrokerMetadata(info.id, info.host, info.port) }
        val topics = zk.allTopics().mapNotNull { topic ->
            val assignment = zk.readAssignment(topic) ?: return@mapNotNull null
            TopicMetadata(topic, assignment.keys.sorted().map { p ->
                val state = zk.readPartitionState(TopicPartition(topic, p))?.value
                PartitionMetadata(p, state?.leader ?: -1, state?.leaderEpoch ?: -1, assignment.getValue(p), state?.isr ?: emptyList())
            })
        }
        return MetadataResponse(controllerId, brokers, topics)
    }

    fun produce(req: ProduceRequest): ProduceResponse {
        val tp = TopicPartition(req.topic, req.partition)
        val result = replicaManager.produce(tp, req.key, req.value, req.acks, req.timeoutMs)
        return ProduceResponse(refineNotLeader(tp, result.errorCode), req.partition, result.offset)
    }

    /** replicaId -1 ⇒ consumer fetch (algorithm 12; currentLeaderEpoch is not checked), else follower fetch (algorithm 9). */
    fun fetch(req: FetchRequest): FetchResponse {
        val tp = TopicPartition(req.topic, req.partition)
        val result = if (req.replicaId == FetchRequest.CONSUMER_REPLICA_ID) {
            replicaManager.fetchAsConsumer(tp, req.offset, req.maxBytes)
        } else {
            replicaManager.fetchAsFollower(tp, req.replicaId, req.offset, req.currentLeaderEpoch, req.maxBytes)
        }
        // Records read from the log had their stored CRC verified, so recomputing it yields the stored value (R4).
        val records = result.records.map { FetchedRecord(it.offset, it.leaderEpoch, it.timestamp, it.key, it.value) }
        return FetchResponse(refineNotLeader(tp, result.errorCode), result.highWatermark, records)
    }

    fun offsetCommit(req: OffsetCommitRequest): OffsetCommitResponse {
        zk.commitOffset(req.group, TopicPartition(req.topic, req.partition), req.offset)
        return OffsetCommitResponse(ErrorCodes.NONE)
    }

    fun offsetFetch(req: OffsetFetchRequest): OffsetFetchResponse =
        OffsetFetchResponse(ErrorCodes.NONE, zk.fetchOffset(req.group, TopicPartition(req.topic, req.partition)))

    fun leaderAndIsr(req: LeaderAndIsrRequest): LeaderAndIsrResponse =
        LeaderAndIsrResponse(replicaManager.applyLeaderAndIsr(req, brokerEpoch()))

    fun offsetsForLeaderEpoch(req: OffsetsForLeaderEpochRequest): OffsetsForLeaderEpochResponse =
        replicaManager.offsetsForLeaderEpoch(
            TopicPartition(req.topic, req.partition), req.replicaId, req.currentLeaderEpoch, req.requestedEpoch
        )

    /**
     * The replica manager answers NOT_LEADER for any partition it has not opened; tell the client
     * UNKNOWN_TOPIC / UNKNOWN_PARTITION instead when ZooKeeper says so (not retried by clients).
     * If ZooKeeper is unreachable the NOT_LEADER stands (the client refreshes metadata and retries).
     */
    private fun refineNotLeader(tp: TopicPartition, code: Short): Short {
        if (code != ErrorCodes.NOT_LEADER_FOR_PARTITION) return code
        val assignment = try {
            zk.readAssignment(tp.topic)
        } catch (e: Exception) {
            return code
        } ?: return ErrorCodes.UNKNOWN_TOPIC
        return if (tp.partition in assignment) code else ErrorCodes.UNKNOWN_PARTITION
    }

    private fun isLegalTopicName(name: String): Boolean =
        name.isNotEmpty() && name.length <= 249 && name != "." && name != ".." &&
            name.all { it.isLetterOrDigit() && it.code < 128 || it == '.' || it == '_' || it == '-' }
}
