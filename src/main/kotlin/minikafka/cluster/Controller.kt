package minikafka.cluster

import minikafka.model.BrokerInfo
import minikafka.model.PartitionState
import minikafka.model.TopicPartition
import minikafka.model.Versioned
import minikafka.proto.LeaderAndIsrPartition
import minikafka.zk.ConnectionStateKind
import minikafka.zk.FencedWriteResult
import minikafka.zk.ZkPaths
import minikafka.zk.ZkStore
import org.slf4j.LoggerFactory
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit

/**
 * The cluster controller (algorithms 3–6). Every broker runs one; at most one is *active* — the
 * one whose election multi created `/controller` and bumped `/controller_epoch` (D2).
 *
 * All work happens on a single event thread (`b<id>-controller`) that drains a FIFO queue of
 * [ControllerEvent]s, so controller state needs no locks. Only the active controller watches
 * `/brokers/ids` and `/brokers/topics`; every other broker watches `/controller`.
 *
 * Every ZooKeeper write the controller makes is epoch-fenced (D3, `fencedCreatePartitionState` /
 * `fencedSetPartitionState`); a fenced rejection makes it resign. Every write of a partition state
 * bumps `leader_epoch` (except the initial create, epoch 0). LeaderAndIsr is sent only for states
 * that are known to be in ZooKeeper — ZK first, RPC second (D5) — through [channel].
 *
 * A failed ZooKeeper operation (ConnectionLoss etc., outcome unknown) is retried by re-reading the
 * whole state ([ControllerEvent.Reconcile]) after [retryBackoffMs]; every step is idempotent.
 *
 * @param onSessionReconnected run on the event thread after a reconnect, before re-electing: the
 *   server re-registers `/brokers/ids/<id>` there (it may block while a stale registration expires).
 */
class Controller(
    private val brokerId: Int,
    private val zk: ZkStore,
    private val channel: ControllerChannel,
    private val onSessionReconnected: () -> Unit = {},
    private val retryBackoffMs: Long = 1_000
) : AutoCloseable {
    private val log = LoggerFactory.getLogger(Controller::class.java)
    private val queue = LinkedBlockingQueue<ControllerEvent>()
    private val thread = Thread(::run, "b$brokerId-controller").apply { isDaemon = true }

    @Volatile
    private var running = false

    // ---- event-thread confined state ----
    private var active = false
    private var epoch = -1
    private var epochZkVersion = -1
    private var sessionAtElection = 0L
    /** Tags the cluster watches of one controller term (bumped on every (re)load and resignation). */
    private var term = 0L
    /** Tags the `/controller` watch armed after losing an election. */
    private var controllerWatchGeneration = 0L
    private var liveBrokers: Map<Int, Pair<BrokerInfo, Long>> = emptyMap()
    private val assignments = HashMap<String, Map<Int, List<Int>>>()
    private val states = HashMap<TopicPartition, Versioned<PartitionState>>()
    private var retryAtNanos: Long? = null
    private var retryEvent: ControllerEvent = ControllerEvent.Elect

    /** True while this broker is the active controller (for tests / diagnostics). */
    @Volatile
    var isActive: Boolean = false
        private set

    /** The epoch this broker won, or -1 while it is not the controller. */
    @Volatile
    var activeEpoch: Int = -1
        private set

    fun start() {
        running = true
        zk.onConnectionStateChanged { state ->
            when (state) {
                ConnectionStateKind.LOST -> enqueue(ControllerEvent.SessionLost)
                ConnectionStateKind.RECONNECTED -> enqueue(ControllerEvent.SessionReconnected)
                else -> {}
            }
        }
        thread.start()
    }

    fun enqueue(event: ControllerEvent) {
        queue.add(event)
    }

    override fun close() {
        running = false
        queue.add(ControllerEvent.Shutdown)
        thread.join(2_000)
        if (thread.isAlive) {
            thread.interrupt()
            thread.join(2_000)
        }
        channel.close()
    }

    // ------------------------------------------------------------------ event loop

    private fun run() {
        while (running) {
            val event = try {
                nextEvent()
            } catch (_: InterruptedException) {
                break
            } ?: continue
            if (event == ControllerEvent.Shutdown) break
            try {
                handle(event)
            } catch (e: InterruptedException) {
                break
            } catch (e: Exception) {
                if (!running) break
                retryEvent = retryAfterFailure(event, pendingRetry = if (retryAtNanos != null) retryEvent else null, active = active)
                retryAtNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(retryBackoffMs)
                log.warn("b{}: controller event {} failed ({}); retrying with {} in {}ms", brokerId, event, e.toString(), retryEvent, retryBackoffMs)
            }
        }
        log.debug("b{}: controller event thread stopped", brokerId)
    }

    internal companion object {
        /**
         * The single retry to schedule after [failed] threw. A pending [ControllerEvent.SessionReconnected]
         * retry is never downgraded: it re-registers the broker in its new session (and then elects),
         * which a plain Elect/Reconcile would never do, so losing it would leave the broker unregistered.
         */
        fun retryAfterFailure(failed: ControllerEvent, pendingRetry: ControllerEvent?, active: Boolean): ControllerEvent = when {
            failed == ControllerEvent.SessionReconnected || pendingRetry == ControllerEvent.SessionReconnected ->
                ControllerEvent.SessionReconnected
            active -> ControllerEvent.Reconcile
            else -> ControllerEvent.Elect
        }
    }

    private fun nextEvent(): ControllerEvent? {
        val retryAt = retryAtNanos ?: return queue.take()
        val event = queue.poll(maxOf(0L, retryAt - System.nanoTime()), TimeUnit.NANOSECONDS)
        if (event != null) return event
        retryAtNanos = null
        return retryEvent
    }

    private fun handle(event: ControllerEvent) {
        when (event) {
            ControllerEvent.Elect -> elect()
            is ControllerEvent.ControllerChanged -> if (event.generation == controllerWatchGeneration && !active) elect()
            is ControllerEvent.BrokersChanged -> if (active && event.term == term) onBrokersChanged()
            is ControllerEvent.TopicsChanged -> if (active && event.term == term) onTopicsChanged()
            ControllerEvent.Reconcile -> if (active) load() else elect()
            ControllerEvent.SessionLost -> resign("ZooKeeper session lost", reelect = false)
            ControllerEvent.SessionReconnected -> {
                if (active && zk.sessionId() != sessionAtElection) resign("ZooKeeper session changed", reelect = false)
                onSessionReconnected()
                elect()
            }
            ControllerEvent.Shutdown -> {}
        }
    }

    // ------------------------------------------------------------------ election (algorithm 3)

    private fun elect() {
        if (active) return
        zk.ensureControllerEpochNode()
        val won = zk.electController(brokerId)
        if (won == null) {
            val generation = ++controllerWatchGeneration
            val current = zk.watchNode(ZkPaths.CONTROLLER) { enqueue(ControllerEvent.ControllerChanged(generation)) }
            if (current == null) {
                enqueue(ControllerEvent.Elect) // the winner already went away: try again now
            } else {
                log.debug("b{}: not the controller (current: {})", brokerId, zk.currentController())
            }
            return
        }
        active = true
        epoch = won.epoch
        epochZkVersion = won.epochZkVersion
        sessionAtElection = zk.sessionId()
        isActive = true
        activeEpoch = epoch
        log.info("b{}: elected controller with epoch {}", brokerId, epoch)
        load()
    }

    /** Stops acting as controller. [reelect] = compete again right away (after being fenced). */
    private fun resign(reason: String, reelect: Boolean) {
        if (!active) return
        log.info("b{}: resigned as controller (epoch {}): {}", brokerId, epoch, reason)
        active = false
        isActive = false
        activeEpoch = -1
        term++
        epoch = -1
        epochZkVersion = -1
        liveBrokers = emptyMap()
        assignments.clear()
        states.clear()
        channel.removeAll()
        if (reelect) enqueue(ControllerEvent.Elect)
    }

    /**
     * Arms the cluster watches (each read+watch in one call), reloads brokers, assignments and
     * states, then runs the full reconcile: create missing state znodes (half-created topics),
     * elect leaders for partitions whose leader is dead or absent (incl. offline partitions whose
     * ISR member returned), and send the full state to every live broker.
     */
    private fun load() {
        val t = ++term
        zk.watchChildren(ZkPaths.BROKER_IDS) { enqueue(ControllerEvent.BrokersChanged(t)) }
        liveBrokers = zk.liveBrokers()
        syncSenders()
        val topics = zk.watchChildren(ZkPaths.BROKER_TOPICS) { enqueue(ControllerEvent.TopicsChanged(t)) }
        assignments.clear()
        states.clear()
        for (topic in topics) zk.readAssignment(topic)?.let { assignments[topic] = it }
        for (tp in allPartitions()) zk.readPartitionState(tp)?.let { states[tp] = it }
        log.info("b{}: controller epoch {} loaded: live brokers {}, {} topics", brokerId, epoch, liveBrokers.keys, assignments.size)

        for (tp in allPartitions()) {
            val state = states[tp]
            when {
                state == null -> createInitialState(tp)
                state.value.leader !in liveBrokers -> electLeader(tp) // covers leader = -1 (offline)
            }
            if (!active) return // fenced mid-way
        }
        liveBrokers.keys.forEach(::sendFullState)
    }

    // ------------------------------------------------------------------ algorithm 5 (controller half)

    private fun onTopicsChanged() {
        val t = term
        val topics = zk.watchChildren(ZkPaths.BROKER_TOPICS) { enqueue(ControllerEvent.TopicsChanged(t)) }
        val changed = mutableListOf<TopicPartition>()
        for (topic in topics.filter { it !in assignments }) {
            val assignment = zk.readAssignment(topic) ?: continue
            assignments[topic] = assignment
            log.info("b{}: controller: new topic {} with assignment {}", brokerId, topic, assignment.toSortedMap())
            for (p in assignment.keys.sorted()) {
                val tp = TopicPartition(topic, p)
                val existing = zk.readPartitionState(tp)
                if (existing != null) {
                    states[tp] = existing
                    changed += tp
                } else if (createInitialState(tp)) {
                    changed += tp
                }
                if (!active) return
            }
        }
        sendChanged(changed, skip = emptySet())
    }

    /**
     * Creates the partition's state znode (leader = first live assigned replica, leader_epoch 0,
     * isr = live assigned replicas); none live ⇒ leaves it absent. True if a state now exists.
     */
    private fun createInitialState(tp: TopicPartition): Boolean {
        val replicas = replicasOf(tp)
        val initial = PartitionLeaderElector.initialState(replicas, liveBrokers.keys)
        if (initial == null) {
            log.info("b{}: controller: {} has no live replica among {}; leaving its state absent", brokerId, tp, replicas)
            return false
        }
        val state = PartitionState(initial.first, 0, initial.second, epoch)
        return when (val result = zk.fencedCreatePartitionState(tp, state, epochZkVersion)) {
            is FencedWriteResult.Ok -> {
                states[tp] = Versioned(state, result.zkVersion)
                log.info("b{}: controller: created {} state leader={} leader_epoch=0 isr={}", brokerId, tp, state.leader, state.isr)
                true
            }
            FencedWriteResult.ControllerFenced -> {
                resign("fenced while creating $tp state", reelect = true)
                false
            }
            FencedWriteResult.AlreadyExists, FencedWriteResult.StateConflict -> {
                val current = zk.readPartitionState(tp)
                if (current != null) states[tp] = current
                current != null
            }
        }
    }

    // ------------------------------------------------------------------ algorithm 6

    private fun onBrokersChanged() {
        val t = term
        zk.watchChildren(ZkPaths.BROKER_IDS) { enqueue(ControllerEvent.BrokersChanged(t)) }
        val old = liveBrokers
        val now = zk.liveBrokers()
        val dead = old.keys - now.keys
        val bounced = old.keys.filter { it in now && now.getValue(it).second != old.getValue(it).second }.toSet()
        val added = now.keys - old.keys
        liveBrokers = now
        log.info("b{}: controller: live brokers {} (dead {}, bounced {}, new {})", brokerId, now.keys, dead, bounced, added)
        syncSenders()

        // Leaders change ISRs by CAS without telling the controller: refresh the cached states so
        // that what we send below is what ZooKeeper holds now (ZK first, RPC second).
        for (tp in allPartitions()) zk.readPartitionState(tp)?.let { states[tp] = it }

        val gone = dead + bounced
        val changed = mutableListOf<TopicPartition>()
        for (tp in allPartitions()) {
            val state = states[tp]
            val didChange = when {
                state == null -> createInitialState(tp)
                state.value.leader in gone || state.value.leader !in now -> electLeader(tp)
                else -> false
            }
            if (!active) return
            if (didChange) changed += tp
        }
        val fresh = added + bounced
        sendChanged(changed, skip = fresh)
        fresh.forEach(::sendFullState)
    }

    /**
     * Re-reads the state and elects: new leader = first assigned replica that is live and in ISR,
     * written as `leader, leader_epoch+1, isr ∩ live`; none ⇒ `leader=-1, leader_epoch+1, isr
     * unchanged` (D11, no unclean election) unless already offline. True if a new state was written.
     */
    private fun electLeader(tp: TopicPartition): Boolean {
        repeat(3) {
            val current = zk.readPartitionState(tp) ?: return createInitialState(tp)
            states[tp] = current
            val cur = current.value
            val live = liveBrokers.keys
            val newLeader = PartitionLeaderElector.electLeader(replicasOf(tp), cur.isr, live)
            val target = when {
                newLeader != null -> PartitionState(newLeader, cur.leaderEpoch + 1, cur.isr.filter { it in live }, epoch)
                cur.leader != -1 -> PartitionState(-1, cur.leaderEpoch + 1, cur.isr, epoch)
                else -> return false // still offline: nothing to write
            }
            when (val result = zk.fencedSetPartitionState(tp, target, current.zkVersion, epochZkVersion)) {
                is FencedWriteResult.Ok -> {
                    states[tp] = Versioned(target, result.zkVersion)
                    log.info(
                        "b{}: controller: {} leader {} -> {} (leader_epoch {}, isr {})",
                        brokerId, tp, cur.leader, target.leader, target.leaderEpoch, target.isr
                    )
                    return true
                }
                FencedWriteResult.ControllerFenced -> {
                    resign("fenced while electing a leader for $tp", reelect = true)
                    return false
                }
                FencedWriteResult.StateConflict, FencedWriteResult.AlreadyExists -> {} // re-read and recompute
            }
        }
        log.warn("b{}: controller: gave up electing a leader for {} after repeated conflicts", brokerId, tp)
        return false
    }

    // ------------------------------------------------------------------ LeaderAndIsr

    /** LeaderAndIsr for [changed] partitions to each of their live replicas (except [skip]). */
    private fun sendChanged(changed: List<TopicPartition>, skip: Set<Int>) {
        if (changed.isEmpty()) return
        val perBroker = HashMap<Int, MutableList<LeaderAndIsrPartition>>()
        for (tp in changed) {
            val entry = leaderAndIsrEntry(tp) ?: continue
            for (r in entry.replicas) if (r in liveBrokers && r !in skip) perBroker.getOrPut(r) { mutableListOf() } += entry
        }
        perBroker.forEach { (b, partitions) -> channel.send(b, epoch, partitions) }
    }

    /** The full state of every partition [target] replicates (sent even if empty: it raises the seen controller epoch). */
    private fun sendFullState(target: Int) {
        val partitions = allPartitions().filter { target in replicasOf(it) }.mapNotNull(::leaderAndIsrEntry)
        channel.send(target, epoch, partitions)
    }

    private fun leaderAndIsrEntry(tp: TopicPartition): LeaderAndIsrPartition? {
        val state = states[tp] ?: return null
        val s = state.value
        return LeaderAndIsrPartition(tp.topic, tp.partition, s.leader, s.leaderEpoch, s.isr, replicasOf(tp), state.zkVersion)
    }

    // ------------------------------------------------------------------ helpers

    private fun syncSenders() {
        for (id in channel.brokerEpochs().keys) {
            val live = liveBrokers[id]
            if (live == null || channel.brokerEpochs()[id] != live.second) channel.removeBroker(id)
        }
        for ((_, entry) in liveBrokers) channel.addBroker(entry.first, entry.second)
    }

    private fun replicasOf(tp: TopicPartition): List<Int> = assignments[tp.topic]?.get(tp.partition) ?: emptyList()

    private fun allPartitions(): List<TopicPartition> =
        assignments.toSortedMap().flatMap { (topic, a) -> a.keys.sorted().map { TopicPartition(topic, it) } }
}
