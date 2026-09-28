package minikafka.testing

import org.junit.jupiter.api.extension.AfterEachCallback
import org.junit.jupiter.api.extension.BeforeEachCallback
import org.junit.jupiter.api.extension.ExtensionContext
import org.junit.jupiter.api.extension.TestExecutionExceptionHandler

/**
 * Gives each test a fresh, started [TestCluster] and tears it down afterwards. When a test fails
 * it prints [TestCluster.dumpDiagnostics] (ZooKeeper tree + every broker's snapshot) to stderr
 * *before* teardown, while the evidence still exists.
 *
 * ```
 * @JvmField @RegisterExtension
 * val clusterExtension = TestClusterExtension { TestCluster(size = 3) }
 * private val cluster get() = clusterExtension.cluster
 * ```
 */
class TestClusterExtension(private val factory: () -> TestCluster) :
    BeforeEachCallback, AfterEachCallback, TestExecutionExceptionHandler {

    private var current: TestCluster? = null
    private var dumped = false

    val cluster: TestCluster get() = checkNotNull(current) { "no cluster: used outside a test?" }

    override fun beforeEach(context: ExtensionContext) {
        dumped = false
        val cluster = factory()
        current = cluster
        try {
            cluster.start()
        } catch (e: Throwable) {
            dump(context)
            throw e
        }
    }

    override fun handleTestExecutionException(context: ExtensionContext, throwable: Throwable) {
        dump(context)
        throw throwable
    }

    override fun afterEach(context: ExtensionContext) {
        if (context.executionException.isPresent) dump(context)
        current?.close()
        current = null
    }

    private fun dump(context: ExtensionContext) {
        if (dumped) return
        dumped = true
        val cluster = current ?: return
        System.err.println("Test '${context.displayName}' failed; cluster state before teardown:")
        System.err.println(runCatching { cluster.dumpDiagnostics() }.getOrElse { "(diagnostics failed: $it)" })
    }
}
