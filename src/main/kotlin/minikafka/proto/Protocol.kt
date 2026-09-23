package minikafka.proto

object ApiKeys {
    const val CREATE_TOPIC: Short = 1
    const val METADATA: Short = 2
    const val PRODUCE: Short = 3
    const val FETCH: Short = 4
    const val OFFSET_COMMIT: Short = 5
    const val OFFSET_FETCH: Short = 6
}

object ErrorCodes {
    const val NONE: Short = 0
    const val UNKNOWN_TOPIC: Short = 1
    const val UNKNOWN_PARTITION: Short = 2
    const val OFFSET_OUT_OF_RANGE: Short = 3
    const val TOPIC_ALREADY_EXISTS: Short = 4
}
