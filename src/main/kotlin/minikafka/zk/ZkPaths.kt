package minikafka.zk

import minikafka.model.TopicPartition

/**
 * Every znode path minikafka uses (relative to the optional chroot/namespace):
 *
 * ```
 * /brokers/ids/<id>                          EPHEMERAL  host=..,port=..    czxid = broker epoch
 * /brokers/topics/<t>                        PERSISTENT 0=1,2,3 / 1=2,3,1  assignment; topic commit point
 * /brokers/topics/<t>/partitions/<p>/state   PERSISTENT leader, leader_epoch, isr, controller_epoch
 * /controller                                EPHEMERAL  brokerid=<id>
 * /controller_epoch                          PERSISTENT <int>              never deleted
 * /consumers/<g>/offsets/<t>/<p>             PERSISTENT <offset>
 * ```
 */
object ZkPaths {
    const val BROKER_IDS = "/brokers/ids"
    const val BROKER_TOPICS = "/brokers/topics"
    const val CONTROLLER = "/controller"
    const val CONTROLLER_EPOCH = "/controller_epoch"
    const val CONSUMERS = "/consumers"

    fun brokerId(id: Int): String = "$BROKER_IDS/$id"

    fun topic(topic: String): String = "$BROKER_TOPICS/$topic"

    fun partitions(topic: String): String = "${topic(topic)}/partitions"

    fun partition(tp: TopicPartition): String = "${partitions(tp.topic)}/${tp.partition}"

    fun partitionState(tp: TopicPartition): String = "${partition(tp)}/state"

    fun consumerOffset(group: String, tp: TopicPartition): String =
        "$CONSUMERS/$group/offsets/${tp.topic}/${tp.partition}"
}
