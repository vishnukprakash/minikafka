package minikafka.server

import minikafka.broker.IsrStore
import minikafka.model.PartitionState
import minikafka.model.TopicPartition
import minikafka.model.Versioned
import minikafka.zk.ZkStore

/** The leader's ISR CAS (algorithm 10) over ZooKeeper; replay-safe per [ZkStore.casPartitionState] (D16). */
internal class ZkIsrStore(private val zk: ZkStore) : IsrStore {
    override fun write(tp: TopicPartition, state: PartitionState, expectedZkVersion: Int): Int? =
        zk.casPartitionState(tp, state, expectedZkVersion)

    override fun read(tp: TopicPartition): Versioned<PartitionState>? = zk.readPartitionState(tp)
}
