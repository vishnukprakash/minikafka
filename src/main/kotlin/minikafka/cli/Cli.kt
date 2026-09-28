package minikafka.cli

import minikafka.client.HostPort
import minikafka.client.MiniKafkaClient
import minikafka.proto.ErrorCodes
import minikafka.server.Server
import minikafka.server.ServerConfig
import org.apache.zookeeper.server.ZooKeeperServerMain
import java.io.File
import java.io.IOException
import org.apache.zookeeper.server.ServerConfig as ZooKeeperServerConfig

fun main(args: Array<String>) {
    if (args.isEmpty()) printUsageAndExit()
    try {
        when (args[0]) {
            "server" -> runServer(args.drop(1))
            "zk" -> runZooKeeper(args.drop(1))
            "topics" -> runTopics(args.drop(1))
            "produce" -> runProduce(args.drop(1))
            "consume" -> runConsume(args.drop(1))
            else -> printUsageAndExit()
        }
    } catch (e: IOException) {
        System.err.println("error: could not complete the request (${e.message ?: e.javaClass.simpleName}); are the brokers reachable?")
        kotlin.system.exitProcess(1)
    }
}

private fun printUsageAndExit(): Nothing {
    System.err.println(
        """
        Usage:
          minikafka zk [--port <port>] [--data-dir <dir>]
          minikafka server --broker-id <id> --zk <host:port[/chroot]> [--port <port>] [--data-dir <dir>]
                           [--advertised-host <host>] [--min-insync <n>]
          minikafka topics create --topic <name> --partitions <n> [--replication-factor <n>] [<bootstrap>]
          minikafka topics list [<bootstrap>]
          minikafka topics describe [--topic <name>] [<bootstrap>]
          minikafka produce --topic <name> [--key <key>] [--acks all|1] [<bootstrap>] <value>
          minikafka consume --topic <name> --partition <n> [--from-beginning] [--group <group>] [<bootstrap>]

        <bootstrap> is --bootstrap <host:port,host:port,...> or --host <host> --port <port>
        (default localhost:9092).
        """.trimIndent()
    )
    kotlin.system.exitProcess(1)
}

private fun flag(args: List<String>, name: String): String? {
    val index = args.indexOf(name)
    return if (index >= 0 && index + 1 < args.size) args[index + 1] else null
}

private fun intFlag(args: List<String>, name: String, default: Int? = null): Int {
    val raw = flag(args, name) ?: return default ?: printUsageAndExit()
    return raw.toIntOrNull() ?: run {
        System.err.println("$name must be an integer, got '$raw'")
        printUsageAndExit()
    }
}

private fun bootstrap(args: List<String>): List<HostPort> {
    flag(args, "--bootstrap")?.let { return HostPort.parseList(it) }
    val host = flag(args, "--host") ?: "localhost"
    val port = intFlag(args, "--port", 9092)
    return listOf(HostPort(host, port))
}

/** A single-node ZooKeeper for local clusters (`ZooKeeperServerMain`, admin server disabled). */
private fun runZooKeeper(args: List<String>) {
    val port = intFlag(args, "--port", 2181)
    val dataDir = File(flag(args, "--data-dir") ?: "./zk-data").absoluteFile
    dataDir.mkdirs()
    System.setProperty("zookeeper.admin.enableServer", "false")
    val zkConfig = ZooKeeperServerConfig().apply { parse(arrayOf(port.toString(), dataDir.path)) }
    println("ZooKeeper listening on port $port, data dir ${dataDir.path}")
    ZooKeeperServerMain().runFromConfig(zkConfig) // blocks until shut down
}

private fun runServer(args: List<String>) {
    val brokerId = intFlag(args, "--broker-id")
    val zk = flag(args, "--zk") ?: printUsageAndExit()
    val dataDir = File(flag(args, "--data-dir") ?: "./data")
    val config = ServerConfig(
        brokerId = brokerId,
        zkConnect = zk,
        dataDir = dataDir,
        port = intFlag(args, "--port", 9092),
        advertisedHost = flag(args, "--advertised-host") ?: "localhost",
        minInsyncReplicas = intFlag(args, "--min-insync", 1)
    )
    val server = Server(config)
    try {
        server.start()
    } catch (e: Exception) {
        System.err.println("error starting broker $brokerId: ${e.message}")
        kotlin.system.exitProcess(1)
    }
    // Graceful stop (algorithm 2) on SIGTERM / Ctrl-C: closing the ZooKeeper session removes the
    // registration at once, so the controller fails over this broker's partitions immediately.
    Runtime.getRuntime().addShutdownHook(Thread({ server.stop() }, "b$brokerId-shutdown"))
    println("minikafka server listening on port ${server.port()}, data dir ${dataDir.absolutePath}")
    Thread.currentThread().join()
}

private fun runTopics(args: List<String>) {
    MiniKafkaClient(bootstrap(args)).use { client ->
        when (args.getOrNull(0)) {
            "create" -> {
                val topic = flag(args, "--topic") ?: printUsageAndExit()
                val partitions = intFlag(args, "--partitions", 1)
                val replicationFactor = intFlag(args, "--replication-factor", 1)
                val errorCode = client.createTopic(topic, partitions, replicationFactor)
                if (errorCode == ErrorCodes.NONE) println("created topic $topic with $partitions partitions")
                else println("error creating topic: code $errorCode")
            }
            "list" -> client.metadata().topics.forEach { println("${it.name}\t${it.numPartitions} partitions") }
            "describe" -> describe(client, flag(args, "--topic"))
            else -> printUsageAndExit()
        }
    }
}

/** One line per partition; OFFLINE = no leader, UNDER-REPLICATED = ISR smaller than the replica set. */
private fun describe(client: MiniKafkaClient, only: String?) {
    val metadata = client.metadata()
    val brokers = metadata.brokers.joinToString(",") { "${it.id}@${it.host}:${it.port}" }
    println("controller ${metadata.controllerId}\tbrokers $brokers")
    val topics = metadata.topics.filter { only == null || it.name == only }
    if (only != null && topics.isEmpty()) {
        System.err.println("unknown topic $only")
        kotlin.system.exitProcess(1)
    }
    for (t in topics) {
        for (p in t.partitions) {
            val marks = buildList {
                if (p.leader < 0) add("OFFLINE")
                if (p.isr.size < p.replicas.size) add("UNDER-REPLICATED")
            }
            println(
                "${t.name}\tpartition ${p.partition}\tleader ${p.leader}\tepoch ${p.leaderEpoch}" +
                    "\treplicas ${p.replicas.joinToString(",")}\tisr ${p.isr.joinToString(",")}" +
                    marks.joinToString("") { "\t$it" }
            )
        }
    }
}

private val PRODUCE_VALUE_FLAGS = setOf("--topic", "--key", "--acks", "--bootstrap", "--host", "--port")

private fun runProduce(args: List<String>) {
    val topic = flag(args, "--topic") ?: printUsageAndExit()
    val key = flag(args, "--key")
    val acks: Short = when (val a = flag(args, "--acks") ?: "all") {
        "all", "-1" -> MiniKafkaClient.ACKS_ALL
        "1" -> MiniKafkaClient.ACKS_LEADER
        else -> {
            System.err.println("--acks must be 'all' or '1', got '$a'")
            printUsageAndExit()
        }
    }
    // The value is the last argument, and must not be a flag or a flag's value.
    val value = args.lastOrNull()?.takeIf { !it.startsWith("--") && args.getOrNull(args.size - 2) !in PRODUCE_VALUE_FLAGS }
        ?: printUsageAndExit()
    MiniKafkaClient(bootstrap(args)).use { client ->
        val response = client.produce(topic, key?.toByteArray(), value.toByteArray(), acks)
        if (response.errorCode != ErrorCodes.NONE) {
            System.err.println("error producing: code ${response.errorCode}")
            kotlin.system.exitProcess(1)
        }
        println("produced to partition ${response.partition} at offset ${response.offset}")
    }
}

/** Reads from the leader until the high watermark returned by the fetch is reached, then exits (no tailing). */
private fun runConsume(args: List<String>) {
    val topic = flag(args, "--topic") ?: printUsageAndExit()
    val partition = intFlag(args, "--partition", 0)
    val group = flag(args, "--group")
    val fromBeginning = args.contains("--from-beginning")
    MiniKafkaClient(bootstrap(args)).use { client ->
        var offset = when {
            fromBeginning -> 0L
            group != null -> client.fetchOffset(group, topic, partition).let { if (it < 0) 0L else it }
            else -> 0L
        }
        while (true) {
            val response = client.fetch(topic, partition, offset, 1024 * 1024)
            if (response.errorCode != ErrorCodes.NONE) {
                System.err.println("error fetching: code ${response.errorCode}")
                kotlin.system.exitProcess(1)
            }
            for (record in response.records) {
                val key = record.key?.toString(Charsets.UTF_8)
                println("offset=${record.offset} key=$key value=${String(record.value, Charsets.UTF_8)}")
                offset = record.offset + 1
            }
            if (group != null && response.records.isNotEmpty()) client.commitOffset(group, topic, partition, offset)
            if (response.records.isEmpty() || offset >= response.highWatermark) break
        }
    }
}
