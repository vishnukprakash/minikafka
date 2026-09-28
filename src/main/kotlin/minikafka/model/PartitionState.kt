package minikafka.model

data class PartitionState(
    val leader: Int,
    val leaderEpoch: Int,
    val isr: List<Int>,
    val controllerEpoch: Int
)
