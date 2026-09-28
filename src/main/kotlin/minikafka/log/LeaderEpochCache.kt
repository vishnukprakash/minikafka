package minikafka.log

/**
 * In-memory map of leader epoch -> first offset written in that epoch, kept in ascending order of
 * both epoch and start offset (KIP-101). Rebuilt by [Log] from the records' own `leaderEpoch`
 * fields on every open (see D9: there is no checkpoint file), and kept current by every append and
 * truncation. Not thread-safe on its own beyond its own monitor; [Log] guards it with its lock.
 */
class LeaderEpochCache {
    private val epochs = mutableListOf<Pair<Int, Long>>() // (epoch, startOffset), ascending

    /** Records that [epoch] starts at [startOffset]; ignored unless [epoch] is newer than the latest. */
    @Synchronized
    fun assign(epoch: Int, startOffset: Long) {
        val latest = epochs.lastOrNull()
        if (latest != null && epoch <= latest.first) return
        require(latest == null || startOffset >= latest.second) {
            "epoch $epoch start offset $startOffset precedes epoch ${latest!!.first} start ${latest.second}"
        }
        epochs.add(epoch to startOffset)
    }

    /** The newest epoch in the cache, or -1 if it is empty. */
    @Synchronized
    fun latestEpoch(): Int = epochs.lastOrNull()?.first ?: -1

    /**
     * Leader-side answer to OFFSETS_FOR_LEADER_EPOCH (shared-context algorithm 8):
     * - empty cache => (-1, [logEndOffset])
     * - [requestedEpoch] below the earliest epoch => (requestedEpoch, earliest start offset)
     * - otherwise => (largest epoch <= requestedEpoch, start offset of the entry after it, or
     *   [logEndOffset] if it is the latest epoch).
     */
    @Synchronized
    fun endOffsetFor(requestedEpoch: Int, logEndOffset: Long): Pair<Int, Long> {
        if (epochs.isEmpty()) return -1 to logEndOffset
        val earliest = epochs.first()
        if (requestedEpoch < earliest.first) return requestedEpoch to earliest.second
        val idx = epochs.indexOfLast { it.first <= requestedEpoch }
        val epoch = epochs[idx].first
        val end = if (idx == epochs.lastIndex) logEndOffset else epochs[idx + 1].second
        return epoch to end
    }

    /** Removes every entry whose start offset is >= [offset] (the log was truncated to [offset]). */
    @Synchronized
    fun truncateFromEnd(offset: Long) {
        epochs.removeAll { it.second >= offset }
    }

    @Synchronized
    fun entries(): List<Pair<Int, Long>> = epochs.toList()
}
