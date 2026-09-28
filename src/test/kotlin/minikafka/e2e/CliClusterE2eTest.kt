package minikafka.e2e

import minikafka.model.TopicPartition
import minikafka.testing.EmbeddedZk
import minikafka.testing.eventually
import minikafka.zk.ZkStore
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import java.io.File
import java.nio.file.Files
import java.util.concurrent.TimeUnit
import kotlin.time.Duration.Companion.seconds

/**
 * The installed distribution (`installDist`, `build/install/minikafka`, system property
 * `minikafka.home`) as real processes: 3 `minikafka server` JVMs against an embedded ZooKeeper in
 * the test JVM, topics/produce/consume through CLI subprocesses, and a SIGKILL of the partition
 * leader (`destroyForcibly`, no shutdown hook: failover waits for its 6s session to expire).
 * Every subprocess's stdout/stderr goes to a file, printed if the test fails.
 */
@Tag("slow")
@Timeout(value = 240, unit = TimeUnit.SECONDS)
class CliClusterE2eTest {
    private val home = File(checkNotNull(System.getProperty("minikafka.home")) { "run via ./gradlew e2eTest (sets minikafka.home)" })
    private val script = File(home, "bin/minikafka")
    private val work: File = Files.createTempDirectory("minikafka-cli-e2e").toFile()
    private val brokers = mutableMapOf<Int, Process>()
    private val logs = mutableListOf<File>()
    private var cliRuns = 0

    private data class CliResult(val exit: Int, val out: String, val err: String)

    private fun process(name: String, vararg args: String): Pair<Process, File> {
        check(script.canExecute()) { "missing $script: run installDist" }
        val out = File(work, "$name.out").also { logs += it }
        val err = File(work, "$name.err").also { logs += it }
        val pb = ProcessBuilder(listOf(script.absolutePath) + args).redirectOutput(out).redirectError(err).directory(work)
        pb.environment()["JAVA_HOME"] = System.getProperty("java.home")
        pb.environment()["JAVA_OPTS"] = "-Dzookeeper.sasl.client=false -Dzookeeper.admin.enableServer=false"
        return pb.start() to out
    }

    private fun cli(vararg args: String): CliResult {
        val name = "cli-${++cliRuns}-${args.take(2).joinToString("-")}"
        val (p, out) = process(name, *args)
        check(p.waitFor(60, TimeUnit.SECONDS)) { p.destroyForcibly(); "$name did not finish in 60s" }
        return CliResult(p.exitValue(), out.readText(), File(work, "$name.err").readText())
    }

    private fun startBroker(id: Int, zkConnect: String): Int {
        val (p, out) = process("broker-$id", "server", "--broker-id", "$id", "--zk", zkConnect, "--port", "0",
            "--data-dir", File(work, "data-$id").absolutePath, "--advertised-host", "127.0.0.1", "--min-insync", "2")
        brokers[id] = p
        return eventually(30.seconds) {
            assertTrue(p.isAlive, "broker $id exited with ${runCatching { p.exitValue() }.getOrNull()}")
            val line = out.readLines().firstOrNull { it.startsWith("minikafka server listening on port ") }
            checkNotNull(line) { "broker $id not listening yet" }.substringAfter("port ").substringBefore(',').trim().toInt()
        }
    }

    @Test
    fun `a 3-process cluster survives a SIGKILL of the leader and consume returns everything produced`() {
        val zk = EmbeddedZk()
        val admin = ZkStore(zk.connectString + "/cli", 10_000)
        try {
            val zkConnect = zk.connectString + "/cli"
            val ports = (1..3).associateWith { startBroker(it, zkConnect) }
            admin.start()
            eventually(30.seconds) {
                assertEquals(setOf(1, 2, 3), admin.liveBrokers().keys)
                assertTrue(admin.currentController() != null)
            }
            val all = ports.values.joinToString(",") { "127.0.0.1:$it" }

            val created = cli("topics", "create", "--topic", "demo", "--partitions", "1", "--replication-factor", "3", "--bootstrap", all)
            assertEquals(0, created.exit, created.err)
            assertTrue("created topic demo" in created.out, created.out)
            val tp = TopicPartition("demo", 0)
            eventually(30.seconds) {
                val s = checkNotNull(admin.readPartitionState(tp)) { "no state yet" }.value
                assertTrue(s.leader > 0 && s.isr.toSet() == setOf(1, 2, 3)) { "not fully in sync yet: $s" }
            }

            val produced = mutableListOf<String>()
            fun produce(value: String, bootstrap: String) {
                eventually(30.seconds) { // a failed run may have appended: a retry may duplicate (D20)
                    val r = cli("produce", "--topic", "demo", "--bootstrap", bootstrap, value)
                    assertEquals(0, r.exit, "produce $value: ${r.out} ${r.err}")
                    assertTrue(r.out.startsWith("produced to partition 0 at offset "), r.out)
                }
                produced += value
            }
            (0 until 4).forEach { produce("before-$it", all) }

            val leader = admin.readPartitionState(tp)!!.value.leader
            val killed = brokers.getValue(leader)
            val killedAt = System.nanoTime()
            killed.destroyForcibly() // SIGKILL: no shutdown hook, the ZooKeeper session must expire
            assertTrue(killed.waitFor(30, TimeUnit.SECONDS))
            eventually(30.seconds) {
                val s = admin.readPartitionState(tp)!!.value
                assertTrue(s.leader > 0 && s.leader != leader) { "leader not moved off the killed $leader yet: $s" }
            }
            println("SIGKILLed leader $leader; new leader elected after ${(System.nanoTime() - killedAt) / 1_000_000}ms")
            val survivors = ports.filterKeys { it != leader }.values.joinToString(",") { "127.0.0.1:$it" }
            (0 until 4).forEach { produce("after-$it", survivors) }

            val described = cli("topics", "describe", "--topic", "demo", "--bootstrap", survivors)
            assertEquals(0, described.exit, described.err)
            println(described.out)

            eventually(30.seconds) {
                val consumed = cli("consume", "--topic", "demo", "--partition", "0", "--from-beginning", "--bootstrap", survivors)
                assertEquals(0, consumed.exit, consumed.err)
                val values = consumed.out.lines().filter { it.startsWith("offset=") }.map { it.substringAfter(" value=") }
                assertTrue(values.all { it in produced }, "only produced values: $values")
                assertEquals(produced, values.distinct(), "everything produced, in order (duplicates only from retries)")
            }
        } catch (t: Throwable) {
            System.err.println("===== CliClusterE2eTest failed; subprocess output (work dir $work) =====")
            for (f in logs.filter { it.exists() }) {
                System.err.println("----- ${f.name} -----")
                System.err.println(f.readText().takeLast(20_000))
            }
            throw t
        } finally {
            brokers.values.forEach { it.destroyForcibly() }
            brokers.values.forEach { it.waitFor(10, TimeUnit.SECONDS) }
            runCatching { admin.close() }
            runCatching { zk.close() }
            work.deleteRecursively()
        }
    }
}
