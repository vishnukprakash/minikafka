package minikafka.model

data class TopicPartition(val topic: String, val partition: Int) {
    override fun toString(): String = "$topic-$partition"
}
