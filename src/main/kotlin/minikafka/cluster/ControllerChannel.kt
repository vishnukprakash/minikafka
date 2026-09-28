package minikafka.cluster

import minikafka.model.BrokerInfo
import minikafka.net.Connection
import minikafka.proto.ApiKeys
import minikafka.proto.ErrorCodes
import minikafka.proto.LeaderAndIsrPartition
import minikafka.proto.LeaderAndIsrRequest
import minikafka.proto.LeaderAndIsrResponse
import org.slf4j.LoggerFactory
import java.io.IOException
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.LinkedBlockingQueue

/**
 * The controller's push channel to brokers (D5): one FIFO sender thread per broker
 * (`b<me>-controller-send-<target>`), which delivers each queued LEADER_AND_ISR in order and retries
 * it (reconnecting, with [retryBackoffMs] between attempts) until the broker answers. The
 * controller talks to itself this way too — no in-process shortcut.
 *
 * A sender is bound to one incarnation of its broker ([brokerEpoch], the czxid of its
 * registration, D4): every request it sends carries that epoch. When the broker leaves or bounces
 * the controller calls [removeBroker], which discards the queue (the new incarnation gets the full
 * state through a fresh sender). Thread-safe.
 */
class ControllerChannel(
    private val myBrokerId: Int,
    private val socketTimeoutMs: Int = 30_000,
    private val retryBackoffMs: Long = 100
) : AutoCloseable {
    private val log = LoggerFactory.getLogger(ControllerChannel::class.java)
    private val senders = ConcurrentHashMap<Int, Sender>()

    /** Starts a sender for [info] at [brokerEpoch]; replaces (discarding the queue of) one for an older incarnation. */
    fun addBroker(info: BrokerInfo, brokerEpoch: Long) {
        val existing = senders[info.id]
        if (existing != null && existing.brokerEpoch == brokerEpoch && existing.info == info) return
        existing?.stop()
        senders[info.id] = Sender(info, brokerEpoch).also { it.start() }
    }

    /** Stops the broker's sender and discards whatever it had not delivered yet. */
    fun removeBroker(brokerId: Int) {
        senders.remove(brokerId)?.stop()
    }

    fun removeAll() {
        senders.keys.toList().forEach(::removeBroker)
    }

    /** Broker id -> the broker epoch its sender is bound to. */
    fun brokerEpochs(): Map<Int, Long> = senders.mapValues { it.value.brokerEpoch }

    /** Queues a LeaderAndIsr for [brokerId]; false if there is no sender for it (not live). */
    fun send(brokerId: Int, controllerEpoch: Int, partitions: List<LeaderAndIsrPartition>): Boolean {
        val sender = senders[brokerId] ?: return false
        sender.queue.add(LeaderAndIsrRequest(myBrokerId, controllerEpoch, sender.brokerEpoch, partitions))
        return true
    }

    override fun close() = removeAll()

    private companion object {
        const val MAX_STALE_BACKOFF_MS = 1_000L
    }

    private inner class Sender(val info: BrokerInfo, val brokerEpoch: Long) {
        val queue = LinkedBlockingQueue<LeaderAndIsrRequest>()
        @Volatile private var stopped = false
        @Volatile private var connection: Connection? = null
        private val thread = Thread(::run, "b$myBrokerId-controller-send-${info.id}").apply { isDaemon = true }

        fun start() = thread.start()

        fun stop() {
            stopped = true
            queue.clear()
            thread.interrupt()
            closeConnection() // unblocks a pending socket read
        }

        private fun run() {
            try {
                while (!stopped) deliver(queue.take())
            } catch (_: InterruptedException) {
            } finally {
                closeConnection()
            }
        }

        /**
         * Sends [request] until the broker answers. I/O errors reconnect and retry. A
         * STALE_BROKER_EPOCH answer is also retried (R8): our epoch comes from the broker's new
         * registration, which the controller can see before that broker has recorded its own
         * epoch (its registerBroker call has not returned yet). A genuinely stale sender is
         * stopped by [removeBroker] when the controller sees the bounce, which ends the retries.
         */
        private fun deliver(request: LeaderAndIsrRequest) {
            var staleBackoffMs = retryBackoffMs
            while (!stopped) {
                try {
                    val conn = connection ?: Connection(info.host, info.port, socketTimeoutMs).also { connection = it }
                    val response = conn.request(ApiKeys.LEADER_AND_ISR, { request.encode(it) }) { LeaderAndIsrResponse.decode(it) }
                    when (response.errorCode) {
                        ErrorCodes.NONE -> {
                            log.debug("b{}: LeaderAndIsr ({} partitions, controller epoch {}) delivered to broker {}", myBrokerId, request.partitions.size, request.controllerEpoch, info.id)
                            return
                        }
                        ErrorCodes.STALE_BROKER_EPOCH -> {
                            log.info(
                                "b{}: broker {} does not know broker epoch {} yet (STALE_BROKER_EPOCH); retrying in {}ms",
                                myBrokerId, info.id, request.brokerEpoch, staleBackoffMs
                            )
                            Thread.sleep(staleBackoffMs)
                            staleBackoffMs = minOf(staleBackoffMs * 2, MAX_STALE_BACKOFF_MS)
                        }
                        else -> {
                            log.info(
                                "b{}: LeaderAndIsr (controller epoch {}, broker epoch {}) rejected by broker {} with error {}",
                                myBrokerId, request.controllerEpoch, request.brokerEpoch, info.id, response.errorCode
                            )
                            return
                        }
                    }
                } catch (e: IOException) {
                    // The stream may be desynchronised: never reuse this connection.
                    closeConnection()
                    if (stopped) return
                    log.debug("b{}: LeaderAndIsr to broker {} failed ({}); retrying", myBrokerId, info.id, e.toString())
                    Thread.sleep(retryBackoffMs)
                }
            }
        }

        private fun closeConnection() {
            connection?.let { runCatching { it.close() } }
            connection = null
        }
    }
}
