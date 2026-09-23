package minikafka.server

import minikafka.broker.Broker
import java.io.File
import java.net.ServerSocket
import java.net.SocketException

class Server(private val port: Int, private val dataDir: File) {
    private val broker = Broker(dataDir)
    private lateinit var serverSocket: ServerSocket
    @Volatile private var running = false

    fun start() {
        serverSocket = ServerSocket(port)
        running = true
        Thread {
            while (running) {
                try {
                    val socket = serverSocket.accept()
                    Thread { ConnectionHandler(socket, broker).handle() }.apply { isDaemon = true }.start()
                } catch (e: SocketException) {
                    if (running) throw e
                }
            }
        }.apply { isDaemon = true }.start()
    }

    fun port(): Int = serverSocket.localPort

    fun stop() {
        running = false
        serverSocket.close()
    }
}
