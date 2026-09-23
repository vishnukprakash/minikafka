package minikafka.broker

import minikafka.io.readNullableString
import minikafka.io.writeNullableString
import java.io.BufferedInputStream
import java.io.DataInputStream
import java.io.EOFException
import java.io.File
import java.io.FileInputStream
import java.io.RandomAccessFile
import java.util.concurrent.ConcurrentHashMap

private data class OffsetKey(val group: String, val topic: String, val partition: Int)

class OffsetStore(private val file: File) {
    private val offsets = ConcurrentHashMap<OffsetKey, Long>()

    init {
        if (file.exists()) {
            DataInputStream(BufferedInputStream(FileInputStream(file))).use { input ->
                while (true) {
                    try {
                        val group = input.readNullableString()!!
                        val topic = input.readNullableString()!!
                        val partition = input.readInt()
                        val offset = input.readLong()
                        offsets[OffsetKey(group, topic, partition)] = offset
                    } catch (e: EOFException) {
                        break
                    }
                }
            }
        }
    }

    @Synchronized
    fun commit(group: String, topic: String, partition: Int, offset: Long) {
        offsets[OffsetKey(group, topic, partition)] = offset
        RandomAccessFile(file, "rw").use { raf ->
            raf.seek(raf.length())
            raf.writeNullableString(group)
            raf.writeNullableString(topic)
            raf.writeInt(partition)
            raf.writeLong(offset)
        }
    }

    fun fetch(group: String, topic: String, partition: Int): Long =
        offsets[OffsetKey(group, topic, partition)] ?: -1L
}
