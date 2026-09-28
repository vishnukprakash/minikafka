package minikafka.e2e

import minikafka.testing.TestCluster
import minikafka.testing.TestClusterExtension
import minikafka.testing.alwaysFor
import minikafka.testing.eventually
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.extension.RegisterExtension
import java.util.concurrent.TimeUnit
import kotlin.random.Random
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/**
 * A seeded nemesis: for ~30s it picks a random fault (crash, graceful restart, isolation from
 * ZooKeeper, session expiry, paused fetchers) against a random broker, holds it for a random time,
 * heals it and waits until every partition is fully in sync again — so at most one broker is ever
 * faulty and minISR=2 stays satisfiable — while two acks=all producers write. Throughout, the
 * controller epoch and every leader epoch must never go backwards. At the end: acked-write checker
 * with zero violations and identical replicas.
 *
 * The seed is printed; replay a run with `./gradlew e2eTest --tests '*NemesisE2eTest' -Dnemesis.seed=<seed>`.
 */
@Tag("e2e")
@Timeout(value = 300, unit = TimeUnit.SECONDS)
class NemesisE2eTest {
    @JvmField
    @RegisterExtension
    val clusterExtension = TestClusterExtension { TestCluster(size = 3, minInsyncReplicas = 2) }
    private val cluster get() = clusterExtension.cluster

    private enum class Fault { CRASH, RESTART, ISOLATE, EXPIRE, PAUSE }

    @Test
    fun `random single-broker faults for 30s lose no acknowledged write`() {
        val seed = System.getProperty("nemesis.seed")?.toLong() ?: Random.nextLong()
        println("NEMESIS SEED = $seed  (replay: -Dnemesis.seed=$seed)")
        System.err.println("NEMESIS SEED = $seed")
        val rng = Random(seed)
        val ops = mutableListOf<String>()
        val context = { "nemesis seed $seed; ops:\n  " + ops.joinToString("\n  ") + "\n" }

        val topic = "nem"
        val partitions = listOf(0, 1)
        cluster.createReplicatedTopic(topic, partitions.size)
        val workload = cluster.workload(topic, partitions).start() // two sequential acks=all producers
        workload.awaitAcks(10, on = partitions)

        var maxControllerEpoch = cluster.controllerEpoch()
        val maxLeaderEpoch = partitions.associateWith { cluster.state(topic, it).leaderEpoch }.toMutableMap()
        val initialControllerEpoch = maxControllerEpoch
        val initialLeaderEpochs = maxLeaderEpoch.toMap()
        val acksBeforeFaults = workload.ackCount()
        val monotonic = {
            val ce = cluster.controllerEpoch()
            assertTrue(ce >= maxControllerEpoch) { context() + "controller epoch went back: $ce < $maxControllerEpoch" }
            maxControllerEpoch = ce
            for (p in partitions) {
                val le = cluster.state(topic, p).leaderEpoch
                assertTrue(le >= maxLeaderEpoch.getValue(p)) { context() + "leader epoch of $topic-$p went back: $le < ${maxLeaderEpoch[p]}" }
                maxLeaderEpoch[p] = le
            }
        }

        val start = System.nanoTime()
        val elapsed = { (System.nanoTime() - start) / 1_000_000 }
        while (elapsed() < 30_000) {
            val victim = cluster.brokerIds.random(rng)
            val fault = Fault.entries.random(rng)
            val hold = (200 + rng.nextInt(3800)).milliseconds // sometimes longer than the 3s session
            ops += "t=${elapsed()}ms $fault b$victim hold ${hold.inWholeMilliseconds}ms (controller ${cluster.controllerId()}, acks ${workload.ackCount()})"
            when (fault) {
                Fault.CRASH -> cluster.crashBroker(victim)
                Fault.RESTART -> cluster.restartBroker(victim)
                Fault.ISOLATE -> cluster.isolateFromZk(victim)
                Fault.EXPIRE -> cluster.expireSession(victim)
                Fault.PAUSE -> cluster.pauseFetchers(victim)
            }
            alwaysFor(hold, pollInterval = 50.milliseconds) { monotonic() }
            when (fault) {
                Fault.CRASH -> cluster.restartBroker(victim)
                Fault.ISOLATE -> cluster.healZk(victim)
                Fault.PAUSE -> cluster.pauseFetchers(victim, paused = false)
                Fault.RESTART, Fault.EXPIRE -> {}
            }
            // Recovery before the next fault: every partition led by a running broker that applied
            // it, with the full replica set back in the ISR (so the next fault is again a single one).
            eventually(40.seconds) {
                monotonic()
                // Every broker registered under its current session (so the next EXPIRE targets a live session).
                val live = cluster.admin().liveBrokers()
                for (id in cluster.brokerIds) {
                    assertEquals(cluster.broker(id).brokerEpoch, live[id]?.second, context() + "broker $id not registered under its current session")
                }
                for (p in partitions) {
                    cluster.awaitLeader(topic, p, timeout = kotlin.time.Duration.ZERO)
                    assertEquals(cluster.replicasOf(topic, p).toSet(), cluster.state(topic, p).isr.toSet(), context() + "ISR of $topic-$p not restored")
                }
            }
            ops[ops.lastIndex] += " -> recovered at t=${elapsed()}ms"
        }
        val acksDuringFaults = workload.ackCount() - acksBeforeFaults
        // The fault phase must have exercised something: enough faults, progress under them, and
        // at least one leadership or controller change.
        assertTrue(ops.size >= 5, context() + "only ${ops.size} faults injected")
        // Floor derived from the run rather than a fixed rate (slow CI): at least one ack per
        // fault-recovery cycle on average, and never fewer than 20 in total.
        val minAcks = maxOf(20, ops.size)
        assertTrue(acksDuringFaults >= minAcks, context() + "only $acksDuringFaults acks during the fault phase (need >= $minAcks)")
        val finalLeaderEpochs = partitions.associateWith { cluster.state(topic, it).leaderEpoch }
        assertTrue(cluster.controllerEpoch() > initialControllerEpoch || finalLeaderEpochs != initialLeaderEpochs) {
            context() + "no controller or leader change: controller epoch $initialControllerEpoch, leader epochs $initialLeaderEpochs -> $finalLeaderEpochs"
        }
        println("faults ${ops.size}, acks during faults $acksDuringFaults, controller epoch $initialControllerEpoch -> ${cluster.controllerEpoch()}, " +
            "leader epochs $initialLeaderEpochs -> $finalLeaderEpochs")
        workload.awaitAcks(10, on = partitions)
        println(context())
        cluster.stopAndCheck(workload, topic, context())
        eventually(20.seconds) {
            val controllers = cluster.runningBrokers().filter { cluster.broker(it).isController() }
            assertEquals(listOf(cluster.controllerId()), controllers, context() + "exactly one controller")
        }
    }
}
