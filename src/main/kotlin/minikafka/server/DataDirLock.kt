package minikafka.server

import java.io.File
import java.nio.channels.FileChannel
import java.nio.channels.FileLock
import java.nio.channels.OverlappingFileLockException
import java.nio.file.StandardOpenOption

/**
 * Exclusive ownership of a broker data directory: an OS file lock on `<dataDir>/.lock`
 * (`FileChannel.tryLock`) plus the `<dataDir>/broker.id` file, written on first start and checked
 * on every later start, so two brokers can never share a directory and a directory is never
 * reused under a different broker id.
 */
internal class DataDirLock private constructor(private val channel: FileChannel, private val lock: FileLock) : AutoCloseable {

    override fun close() {
        runCatching { lock.release() }
        runCatching { channel.close() }
    }

    companion object {
        fun acquire(dataDir: File, brokerId: Int): DataDirLock {
            dataDir.mkdirs()
            check(dataDir.isDirectory) { "data dir ${dataDir.absolutePath} is not a directory" }
            val channel = FileChannel.open(
                File(dataDir, ".lock").toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE
            )
            val lock = try {
                channel.tryLock()
            } catch (_: OverlappingFileLockException) {
                null // held by another broker in this JVM
            }
            if (lock == null) {
                channel.close()
                throw IllegalStateException("data dir ${dataDir.absolutePath} is locked by another running broker")
            }
            val held = DataDirLock(channel, lock)
            try {
                val idFile = File(dataDir, "broker.id")
                if (idFile.exists()) {
                    val existing = idFile.readText().trim().toIntOrNull()
                    check(existing == brokerId) {
                        "data dir ${dataDir.absolutePath} belongs to broker $existing, refusing to start as broker $brokerId"
                    }
                } else {
                    idFile.writeText("$brokerId\n")
                }
            } catch (e: Exception) {
                held.close()
                throw e
            }
            return held
        }
    }
}
