package minikafka.cluster

/**
 * Leader election over an already-known assignment (algorithms 5, 6, 10). Pure decision logic:
 * never reads or writes ZooKeeper, never talks to a broker. Unclean election is disabled (D11):
 * both functions only ever return a replica that is live, never one merely assigned.
 */
object PartitionLeaderElector {

    /**
     * Failover / reconciliation choice (algorithms 3, 6, 10): the first replica, in assignment
     * order, that is both live and in [isr]. Returns null when no such replica exists — the
     * caller then sets `leader = -1` and preserves the ISR rather than electing an out-of-ISR
     * replica (D11).
     */
    fun electLeader(assignedReplicas: List<Int>, isr: List<Int>, liveBrokers: Set<Int>): Int? {
        val isrSet = isr.toSet()
        return assignedReplicas.firstOrNull { it in liveBrokers && it in isrSet }
    }

    /**
     * Initial state at topic creation (algorithm 5): leader = the first live assigned replica,
     * isr = every live assigned replica, in assignment order. Returns null when no assigned
     * replica is live — the caller leaves partition state absent until one registers.
     */
    fun initialState(assignedReplicas: List<Int>, liveBrokers: Set<Int>): Pair<Int, List<Int>>? {
        val liveAssigned = assignedReplicas.filter { it in liveBrokers }
        if (liveAssigned.isEmpty()) return null
        return liveAssigned.first() to liveAssigned
    }
}
