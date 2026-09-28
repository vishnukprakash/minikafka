package minikafka.cluster

/**
 * Everything the controller reacts to, processed one at a time on its single event thread
 * (`b<id>-controller`). ZooKeeper watch callbacks and session listeners only *enqueue* these.
 *
 * Watch-driven events carry the generation that armed the watch: ZooKeeper watches are one-shot
 * and are only re-armed by the handler of the event they produced, but a resignation/re-election
 * (or a reconnect that kept the session, so old watches survive) can leave an older watch armed.
 * An event whose generation is no longer current is ignored, so a stale watch can never cause
 * duplicate handling.
 */
sealed interface ControllerEvent {
    /** Try to become controller (algorithm 3); on losing, watch `/controller`. */
    data object Elect : ControllerEvent

    /** `/controller` changed or disappeared (armed while not controller). */
    data class ControllerChanged(val generation: Long) : ControllerEvent

    /** Children of `/brokers/ids` changed (armed during controller term [term]). */
    data class BrokersChanged(val term: Long) : ControllerEvent

    /** Children of `/brokers/topics` changed (armed during controller term [term]). */
    data class TopicsChanged(val term: Long) : ControllerEvent

    /** Re-read the whole cluster state and re-send it (retry after a failed ZooKeeper operation). */
    data object Reconcile : ControllerEvent

    /** The ZooKeeper session expired (D17: the controller resigns; the broker keeps serving). */
    data object SessionLost : ControllerEvent

    /** Reconnected to ZooKeeper, possibly with a new session: re-register the broker, then [Elect]. */
    data object SessionReconnected : ControllerEvent

    data object Shutdown : ControllerEvent
}
