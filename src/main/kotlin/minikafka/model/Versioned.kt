package minikafka.model

data class Versioned<T>(val value: T, val zkVersion: Int)
