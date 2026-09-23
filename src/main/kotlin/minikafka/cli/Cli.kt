package minikafka.cli

import minikafka.client.MiniKafkaClient
import minikafka.server.Server
import java.io.File

fun main(args: Array<String>) {
    if (args.isEmpty()) printUsageAndExit()
    when (args[0]) {
        "server" -> runServer(args.drop(1))
        "topics" -> runTopics(args.drop(1))
        "produce" -> runProduce(args.drop(1))
        "consume" -> runConsume(args.drop(1))
        else -> printUsageAndExit()
    }
}

private fun printUsageAndExit(): Nothing {
    System.err.println(
        """
        Usage:
          minikafka server --port <port> [--data-dir <dir>]
          minikafka topics create --topic <name> --partitions <n> [--host <host>] [--port <port>]
          minikafka topics list [--host <host>] [--port <port>]
          minikafka produce --topic <name> [--key <key>] [--host <host>] [--port <port>] <value>
          minikafka consume --topic <name> --partition <n> [--from-beginning] [--group <group>] [--host <host>] [--port <port>]
        """.trimIndent()
    )
    kotlin.system.exitProcess(1)
}

private fun flag(args: List<String>, name: String): String? {
    val index = args.indexOf(name)
    return if (index >= 0 && index + 1 < args.size) args[index + 1] else null
}

private fun runServer(args: List<String>) {
    val port = (flag(args, "--port") ?: "9092").toInt()
    val dataDir = File(flag(args, "--data-dir") ?: "./data")
    val server = Server(port, dataDir)
    server.start()
    println("minikafka server listening on port $port, data dir ${dataDir.absolutePath}")
    Thread.currentThread().join()
}

private fun runTopics(args: List<String>) {
    val host = flag(args, "--host") ?: "localhost"
    val port = (flag(args, "--port") ?: "9092").toInt()
    MiniKafkaClient(host, port).use { client ->
        when (args.getOrNull(0)) {
            "create" -> {
                val topic = flag(args, "--topic") ?: printUsageAndExit()
                val partitions = (flag(args, "--partitions") ?: "1").toInt()
                val errorCode = client.createTopic(topic, partitions)
                if (errorCode.toInt() == 0) println("created topic $topic with $partitions partitions")
                else println("error creating topic: code $errorCode")
            }
            "list" -> client.metadata().forEach { println("${it.name}\t${it.numPartitions} partitions") }
            else -> printUsageAndExit()
        }
    }
}

private fun runProduce(args: List<String>) {
    val host = flag(args, "--host") ?: "localhost"
    val port = (flag(args, "--port") ?: "9092").toInt()
    val topic = flag(args, "--topic") ?: printUsageAndExit()
    val key = flag(args, "--key")
    val value = args.last()
    MiniKafkaClient(host, port).use { client ->
        val response = client.produce(topic, key?.toByteArray(), value.toByteArray())
        println("produced to partition ${response.partition} at offset ${response.offset}")
    }
}

private fun runConsume(args: List<String>) {
    val host = flag(args, "--host") ?: "localhost"
    val port = (flag(args, "--port") ?: "9092").toInt()
    val topic = flag(args, "--topic") ?: printUsageAndExit()
    val partition = (flag(args, "--partition") ?: "0").toInt()
    val group = flag(args, "--group")
    MiniKafkaClient(host, port).use { client ->
        var offset = if (group != null) {
            client.fetchOffset(group, topic, partition).let { if (it < 0) 0L else it }
        } else {
            0L
        }
        while (true) {
            val response = client.fetch(topic, partition, offset, 1024 * 1024)
            if (response.records.isEmpty()) break
            for (record in response.records) {
                val key = record.key?.toString(Charsets.UTF_8)
                println("offset=${record.offset} key=$key value=${String(record.value, Charsets.UTF_8)}")
                offset = record.offset + 1
            }
            if (group != null) client.commitOffset(group, topic, partition, offset)
        }
    }
}
