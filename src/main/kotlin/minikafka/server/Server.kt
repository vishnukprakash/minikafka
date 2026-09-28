package minikafka.server

import minikafka.broker.BrokerSnapshot
import minikafka.broker.ReplicaManager
import minikafka.cluster.Controller
import minikafka.cluster.ControllerChannel
import minikafka.cluster.ControllerEvent
import minikafka.model.BrokerInfo
import minikafka.zk.ZkStore
import org.slf4j.LoggerFactory
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.net.Socket
import java.net.SocketException
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

/**
 * One broker: the composition root wiring the replica manager, ZooKeeper store, controller and
 * TCP server together. Startup follows algorithm 1 and [stop] algorithm 2.
 *
 * Threads (all daemons): `b<id>-acceptor`, `b<id>-handler-<n>` (one per connection),
 * `b<id>-controller`, `b<id>-controller-send-<target>`, `b<id>-isr-updater`,
 * `b<id>-fetcher-<topic>-<p>` (one per followed partition).
 */
class Server(val config: ServerConfig) {
    private val log = LoggerFactory.getLogger(Server::class.java)
    val brokerId: Int get() = config.brokerId

    private var dataDirLock: DataDirLock? = null
    private var zk: ZkStore? = null
    private var replicas: ReplicaManager? = null
    private var controllerOrNull: Controller? = null
    private var serverSocket: ServerSocket? = null
    private val connections = ConcurrentHashMap.newKeySet<Socket>()
    private val handlerIds = AtomicInteger()
    private var started = false
    private var stopped = false

    @Volatile
    private var running = false

    /** czxid of this broker's `/brokers/ids/<id>` (D4); -1 until registered. */
    @Volatile
    var brokerEpoch: Long = -1L
        private set

    internal val zkStore: ZkStore get() = checkNotNull(zk) { "server not started" }
    internal val replicaManager: ReplicaManager get() = checkNotNull(replicas) { "server not started" }
    internal val controller: Controller get() = checkNotNull(controllerOrNull) { "server not started" }

    /** Algorithm 1. On any failure everything started so far is stopped again and the error rethrown. */
    @Synchronized
    fun start() {
        check(!started) { "server already started" }
        check(!stopped) { "server already stopped" }
        started = true
        try {
            dataDirLock = DataDirLock.acquire(config.dataDir, brokerId)
            val store = ZkStore(config.zkConnect, config.zkSessionTimeoutMs, connectionTimeoutMs = config.zkSessionTimeoutMs).also { zk = it }
            store.start()
            val seenControllerEpoch = store.controllerEpoch().value
            val rm = ReplicaManager(config.brokerConfig(), ZkIsrStore(store)).also { replicas = it }
            rm.seenControllerEpoch = seenControllerEpoch
            rm.startReplication(ZkBrokerResolver(store)) // fetchers start on LeaderAndIsr
            val apis = BrokerApis(brokerId, store, rm) { brokerEpoch }

            val socket = ServerSocket().also { serverSocket = it }
            socket.reuseAddress = true
            socket.bind(InetSocketAddress(config.port))
            running = true
            Thread({ acceptLoop(socket, apis) }, "b$brokerId-acceptor").apply { isDaemon = true }.start()

            register()
            val controller = Controller(brokerId, store, ControllerChannel(brokerId, config.controllerSocketTimeoutMs), ::register)
            controllerOrNull = controller
            controller.start()
            controller.enqueue(ControllerEvent.Elect)
            log.info(
                "b{}: started on {}:{} (data dir {}, broker epoch {}, seen controller epoch {})",
                brokerId, config.advertisedHost, socket.localPort, config.dataDir.absolutePath, brokerEpoch, seenControllerEpoch
            )
        } catch (e: Exception) {
            stop()
            throw e
        }
    }

    /** The actually bound TCP port. */
    fun port(): Int = checkNotNull(serverSocket) { "server not started" }.localPort

    fun isController(): Boolean = controllerOrNull?.isActive == true

    fun snapshot(): BrokerSnapshot = replicaManager.snapshot()

    /**
     * Algorithm 2: stop fetchers and the isr-updater → stop the controller and close the ZooKeeper client
     * (the ephemeral registration disappears at once ⇒ fast failover) → close the listening socket
     * and every connection → close the logs → release the data dir. Idempotent.
     */
    @Synchronized
    fun stop() {
        if (stopped) return
        stopped = true
        running = false
        replicas?.let { runCatching { it.stopReplication() } } // 1. fetchers, then the isr-updater
        controllerOrNull?.let { runCatching { it.close() } }
        zk?.let { runCatching { it.close() } }
        serverSocket?.let { runCatching { it.close() } }
        connections.forEach { runCatching { it.close() } }
        replicas?.let { runCatching { it.close() } }
        dataDirLock?.let { runCatching { it.close() } }
        log.info("b{}: stopped", brokerId)
    }

    /** (Re-)registers `/brokers/ids/<id>` with the bound port; idempotent within a session. */
    private fun register() {
        val port = port()
        brokerEpoch = zkStore.registerBroker(BrokerInfo(brokerId, config.advertisedHost, port))
    }

    private fun acceptLoop(socket: ServerSocket, apis: BrokerApis) {
        while (running) {
            val client = try {
                socket.accept()
            } catch (e: SocketException) {
                if (running) log.warn("b{}: accept failed", brokerId, e)
                break
            }
            connections.add(client)
            if (!running) runCatching { client.close() }
            Thread({
                try {
                    ConnectionHandler(client, apis, brokerId).handle()
                } finally {
                    connections.remove(client)
                }
            }, "b$brokerId-handler-${handlerIds.incrementAndGet()}").apply { isDaemon = true }.start()
        }
    }
}
