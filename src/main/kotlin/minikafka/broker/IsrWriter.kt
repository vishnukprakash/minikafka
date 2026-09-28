package minikafka.broker

import minikafka.model.PartitionState
import minikafka.model.TopicPartition
import minikafka.model.Versioned

/**
 * The leader's only window onto ZooKeeper (the broker package never depends on `zk`, D7):
 * compare-and-set of a partition's state znode, plus a plain read used by algorithm 10's
 * BadVersion branch. Task 9 implements it in `server` over `ZkStore.casPartitionState` /
 * `ZkStore.readPartitionState`.
 *
 * It is one interface with two functions (rather than the `fun interface IsrWriter` plus a
 * separate reader) because algorithm 10 always needs both, from the same store.
 */
interface IsrStore {
    /**
     * Writes [state] iff the znode's version is still [expectedZkVersion]; returns the new
     * zkVersion, or null on a version conflict (or missing znode). Implementations may recognise
     * a Curator replay of our own committed write (D16) and return its version instead of null.
     * May throw on I/O failure (outcome unknown); the caller then retries on a later round.
     */
    fun write(tp: TopicPartition, state: PartitionState, expectedZkVersion: Int): Int?

    /** Current state znode with its zkVersion, or null if absent. May throw on I/O failure. */
    fun read(tp: TopicPartition): Versioned<PartitionState>?
}
