package minikafka.proto

object ApiKeys {
    const val CREATE_TOPIC: Short = 1
    const val METADATA: Short = 2
    const val PRODUCE: Short = 3
    const val FETCH: Short = 4
    const val OFFSET_COMMIT: Short = 5
    const val OFFSET_FETCH: Short = 6
    const val LEADER_AND_ISR: Short = 7
    const val OFFSETS_FOR_LEADER_EPOCH: Short = 8
}

object ErrorCodes {
    const val NONE: Short = 0
    const val UNKNOWN_TOPIC: Short = 1
    const val UNKNOWN_PARTITION: Short = 2
    const val OFFSET_OUT_OF_RANGE: Short = 3
    const val TOPIC_ALREADY_EXISTS: Short = 4
    const val NOT_LEADER_FOR_PARTITION: Short = 5
    const val LEADER_NOT_AVAILABLE: Short = 6
    const val NOT_ENOUGH_REPLICAS: Short = 7
    const val NOT_ENOUGH_REPLICAS_AFTER_APPEND: Short = 8
    const val REQUEST_TIMED_OUT: Short = 9
    const val STALE_CONTROLLER_EPOCH: Short = 10
    const val STALE_BROKER_EPOCH: Short = 11
    const val FENCED_LEADER_EPOCH: Short = 12
    const val UNKNOWN_LEADER_EPOCH: Short = 13
    const val INVALID_REPLICATION_FACTOR: Short = 14
    const val INVALID_REQUIRED_ACKS: Short = 15
    const val REPLICA_NOT_ASSIGNED: Short = 16
}
