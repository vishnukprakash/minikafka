package minikafka.broker

import minikafka.log.Log

/**
 * What [truncateForLeaderEpoch] did: the log end offset before ([fromOffset]) and after
 * ([toOffset]), the follower's latest epoch afterwards ([finalEpoch], -1 if the log ended up
 * empty), and how many OFFSETS_FOR_LEADER_EPOCH queries it took ([rounds]; 0 when the handshake
 * was skipped because the log was empty).
 */
data class TruncationResult(
    val fromOffset: Long,
    val toOffset: Long,
    val finalEpoch: Int,
    val rounds: Int,
    val handshakeSkipped: Boolean
) {
    val truncated: Boolean get() = toOffset < fromOffset
}

/** Outcome of [ReplicaManager.truncateFollower]: [result] is null unless [errorCode] is NONE. */
data class FollowerTruncationOutcome(val errorCode: Short, val result: TruncationResult?)

const val DEFAULT_MAX_TRUNCATION_ROUNDS = 64

/**
 * Follower-side leader-epoch handshake (D8, shared-context algorithm 8; KIP-101 with the iterative
 * KIP-279 fix). Never truncates to the high watermark.
 *
 * - Empty log: nothing can diverge, so the handshake is skipped and [queryLeader] is not called.
 * - Otherwise, each round sends E = own latest epoch; the leader answers (E', end') per
 *   [minikafka.log.LeaderEpochCache.endOffsetFor]; the log is truncated to
 *   `min(end', own.endOffsetFor(E').offset, LEO)` (just `min(end', LEO)` when E' == -1, i.e. the
 *   leader's log is empty). It stops when E' == E (the logs agree up to the new LEO), when
 *   E' == -1, or when the log became empty. Otherwise it repeats with the new, strictly lower,
 *   own latest epoch (after the truncation our latest epoch is <= E' < E), so a well-behaved
 *   leader always terminates; more than [maxRounds] rounds means a misbehaving leader and throws
 *   [IllegalStateException] (whatever was already truncated stays truncated, which is safe:
 *   the follower re-fetches it).
 *
 * [queryLeader] may do network I/O and may throw; the exception propagates. [truncate] performs
 * the actual truncation (default: [Log.truncateTo]); [ReplicaManager.truncateFollower] substitutes
 * a version that re-checks the partition's role and epoch under its lock. The caller must ensure
 * nothing else appends to [log] meanwhile (only the partition's single fetcher appends to a
 * follower log).
 */
fun truncateForLeaderEpoch(
    log: Log,
    queryLeader: (requestedEpoch: Int) -> Pair<Int, Long>,
    truncate: (offset: Long) -> Unit = log::truncateTo,
    maxRounds: Int = DEFAULT_MAX_TRUNCATION_ROUNDS
): TruncationResult {
    val from = log.logEndOffset()
    if (from == 0L) return TruncationResult(0L, 0L, -1, rounds = 0, handshakeSkipped = true)

    var rounds = 0
    while (true) {
        val epoch = log.latestEpoch()
        if (epoch == -1) break // truncated to empty: nothing left that could diverge
        check(rounds < maxRounds) {
            "leader-epoch truncation did not converge after $maxRounds rounds (latest epoch $epoch, leo ${log.logEndOffset()})"
        }
        rounds++
        val (leaderEpoch, leaderEnd) = queryLeader(epoch)
        check(leaderEnd >= 0) { "leader answered epoch $leaderEpoch with negative end offset $leaderEnd" }
        val leo = log.logEndOffset()
        val target = if (leaderEpoch == -1) minOf(leaderEnd, leo)
        else minOf(leaderEnd, log.endOffsetForEpoch(leaderEpoch).second, leo)
        if (target < leo) truncate(target)
        if (leaderEpoch == epoch || leaderEpoch == -1) break
    }
    return TruncationResult(from, log.logEndOffset(), log.latestEpoch(), rounds, handshakeSkipped = false)
}
