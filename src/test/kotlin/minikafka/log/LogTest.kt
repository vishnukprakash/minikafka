package minikafka.log

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.io.TempDir
import java.io.File
import java.io.RandomAccessFile

class LogTest {
    @Test
    fun `appends records and assigns sequential offsets`(@TempDir tempDir: File) {
        val log = Log(File(tempDir, "t-0"))
        val offset0 = log.append(1000L, null, "a".toByteArray())
        val offset1 = log.append(1001L, null, "b".toByteArray())
        assertEquals(0L, offset0)
        assertEquals(1L, offset1)
        log.close()
    }

    @Test
    fun `reads back records appended earlier`(@TempDir tempDir: File) {
        val log = Log(File(tempDir, "t-0"))
        log.append(1000L, "k".toByteArray(), "a".toByteArray())
        log.append(1001L, "k".toByteArray(), "b".toByteArray())

        val records = log.read(0L, maxBytes = 1024)
        assertEquals(2, records.size)
        assertEquals("a", String(records[0].value))
        assertEquals("b", String(records[1].value))
        log.close()
    }

    @Test
    fun `rolls to a new segment once the active one exceeds the size threshold`(@TempDir tempDir: File) {
        val dir = File(tempDir, "t-0")
        val log = Log(dir, segmentMaxBytes = 40L)
        repeat(5) { i -> log.append(1000L + i, null, "value$i".toByteArray()) }
        log.close()

        val segmentFiles = dir.listFiles { f -> f.name.endsWith(".log") }!!
        assertTrue(segmentFiles.size > 1, "expected more than one segment file, found ${segmentFiles.size}")
    }

    @Test
    fun `reads across segment roll boundaries and exercises the sparse index`(@TempDir tempDir: File) {
        val dir = File(tempDir, "t-0")
        val log = Log(dir, segmentMaxBytes = 200L, indexIntervalBytes = 64)
        val count = 50
        for (i in 0 until count) {
            log.append(1000L + i, null, "value-%02d".format(i).toByteArray())
        }
        log.close()

        val segmentFiles = dir.listFiles { f -> f.name.endsWith(".log") }!!
        assertTrue(segmentFiles.size > 1, "expected multiple segment files, found ${segmentFiles.size}")

        // At least one segment's index file must have more than one entry (8 bytes each) for
        // OffsetIndex.lookup's binary search to actually be exercised rather than always
        // hitting the empty-index "no entries" case.
        val indexFiles = dir.listFiles { f -> f.name.endsWith(".index") }!!
        assertTrue(
            indexFiles.any { it.length() >= 16 },
            "expected at least one index file with multiple entries"
        )

        val reopened = Log(dir, segmentMaxBytes = 200L, indexIntervalBytes = 64)

        // Read starting partway through the middle of the data (not offset 0), which lands
        // partway into a later segment, past at least one roll boundary. A single read()
        // call only ever returns records from the segment it lands in (mirroring how a
        // real consumer loop re-fetches with the next offset), so drain the rest with
        // successive reads and verify every record comes back, in order.
        val midOffset = 30L
        val drained = mutableListOf<Record>()
        var nextOffset = midOffset
        var reads = 0
        while (nextOffset < count) {
            val batch = reopened.read(nextOffset, maxBytes = 1024 * 1024)
            assertTrue(batch.isNotEmpty(), "expected at least one record from offset $nextOffset")
            drained.addAll(batch)
            nextOffset = batch.last().offset + 1
            reads++
            assertTrue(reads <= count, "too many read iterations, possible infinite loop")
        }
        assertTrue(reads > 1, "expected draining from offset $midOffset to require reads spanning multiple segments")

        assertEquals(count - midOffset.toInt(), drained.size)
        drained.forEachIndexed { idx, record ->
            val expectedOffset = midOffset + idx
            assertEquals(expectedOffset, record.offset)
            assertEquals("value-%02d".format(expectedOffset.toInt()), String(record.value))
        }
        reopened.close()
    }

    @Test
    fun `survives reopening and keeps reading from where it left off`(@TempDir tempDir: File) {
        val dir = File(tempDir, "t-0")
        val log = Log(dir)
        log.append(1000L, null, "a".toByteArray())
        log.close()

        val reopened = Log(dir)
        val offset = reopened.append(1001L, null, "b".toByteArray())
        assertEquals(1L, offset)
        val records = reopened.read(0L, maxBytes = 1024)
        assertEquals(2, records.size)
        reopened.close()
    }

    // ---- Task 3: truncation, follower append, epochs, robust recovery ----

    private fun logFiles(dir: File) = dir.listFiles { f -> f.name.endsWith(".log") }!!.sortedBy { it.name }
    private fun indexFiles(dir: File) = dir.listFiles { f -> f.name.endsWith(".index") }!!.sortedBy { it.name }
    private fun values(records: List<Record>) = records.map { String(it.value) }

    /** Drains the whole log with successive single-segment reads. */
    private fun readAll(log: Log, from: Long = 0L): List<Record> {
        val out = mutableListOf<Record>()
        var next = from
        while (next < log.logEndOffset()) {
            val batch = log.read(next, maxBytes = 1024 * 1024)
            assertTrue(batch.isNotEmpty(), "expected records from offset $next")
            out.addAll(batch)
            next = batch.last().offset + 1
        }
        return out
    }

    @Test
    fun `truncateTo within a segment removes later records and reuses their offsets`(@TempDir tempDir: File) {
        val dir = File(tempDir, "t-0")
        val log = Log(dir)
        repeat(5) { i -> log.append(1000L + i, null, "v$i".toByteArray()) }

        log.truncateTo(3L)
        assertEquals(3L, log.logEndOffset())
        assertEquals(listOf("v0", "v1", "v2"), values(log.read(0L, 1024)))
        assertEquals(emptyList<Record>(), log.read(3L, 1024))

        assertEquals(3L, log.append(2000L, null, "new3".toByteArray()))
        assertEquals(listOf("v0", "v1", "v2", "new3"), values(log.read(0L, 1024)))
        log.close()

        val reopened = Log(dir)
        assertEquals(4L, reopened.logEndOffset())
        assertEquals(listOf("v0", "v1", "v2", "new3"), values(reopened.read(0L, 1024)))
        reopened.close()
    }

    @Test
    fun `truncateTo across segments deletes the later segment files`(@TempDir tempDir: File) {
        val dir = File(tempDir, "t-0")
        val log = Log(dir, segmentMaxBytes = 100L) // "vN" records are 34 bytes: 3 per segment
        repeat(10) { i -> log.append(1000L + i, null, "v$i".toByteArray()) }
        val segmentsBefore = logFiles(dir).size
        assertTrue(segmentsBefore >= 3, "expected >= 3 segments, found $segmentsBefore")

        log.truncateTo(4L)
        assertEquals(4L, log.logEndOffset())
        assertEquals(listOf("%020d.log".format(0L), "%020d.log".format(3L)), logFiles(dir).map { it.name })
        assertEquals(listOf("%020d.index".format(0L), "%020d.index".format(3L)), indexFiles(dir).map { it.name })
        assertEquals(listOf("v0", "v1", "v2", "v3"), values(readAll(log)))

        assertEquals(4L, log.append(2000L, null, "new4".toByteArray()))
        log.close()

        val reopened = Log(dir, segmentMaxBytes = 100L)
        assertEquals(listOf("v0", "v1", "v2", "v3", "new4"), values(readAll(reopened)))
        reopened.close()
    }

    @Test
    fun `truncateTo exactly at a segment base deletes that segment and keeps appending`(@TempDir tempDir: File) {
        val dir = File(tempDir, "t-0")
        val log = Log(dir, segmentMaxBytes = 100L)
        repeat(7) { i -> log.append(1000L + i, null, "v$i".toByteArray()) } // segments at 0, 3, 6
        log.truncateTo(3L)
        assertEquals(listOf("%020d.log".format(0L)), logFiles(dir).map { it.name })
        assertEquals(3L, log.logEndOffset())
        assertEquals(3L, log.append(2000L, null, "new3".toByteArray()))
        assertEquals(listOf("v0", "v1", "v2", "new3"), values(readAll(log)))
        log.close()
    }

    @Test
    fun `truncateTo zero leaves a single empty segment`(@TempDir tempDir: File) {
        val dir = File(tempDir, "t-0")
        val log = Log(dir, segmentMaxBytes = 100L)
        repeat(7) { i -> log.appendAsLeader(1000L + i, null, "v$i".toByteArray(), leaderEpoch = 2) }
        log.truncateTo(0L)
        assertEquals(0L, log.logEndOffset())
        assertEquals(listOf("%020d.log".format(0L)), logFiles(dir).map { it.name })
        assertEquals(0L, logFiles(dir).single().length())
        assertEquals(-1, log.latestEpoch())
        assertEquals(0L, log.append(2000L, null, "fresh".toByteArray()))
        log.close()
    }

    @Test
    fun `truncateTo trims the index so lookups never point past the end`(@TempDir tempDir: File) {
        val dir = File(tempDir, "t-0")
        val log = Log(dir, indexIntervalBytes = 1) // every record is indexed
        repeat(20) { i -> log.append(1000L + i, null, "v$i".toByteArray()) }
        log.truncateTo(10L)
        assertEquals(10L * 8, indexFiles(dir).single().length())

        // Re-append records of a different size: a stale index entry would now point into the
        // middle of a record and corrupt the read.
        for (i in 10 until 20) log.append(3000L + i, "key".toByteArray(), "longer-value-$i".toByteArray())
        val records = log.read(15L, 1024 * 1024)
        assertEquals(15L, records.first().offset)
        assertEquals((15 until 20).map { "longer-value-$it" }, values(records))
        log.close()

        val reopened = Log(dir, indexIntervalBytes = 1)
        assertEquals((15 until 20).map { "longer-value-$it" }, values(reopened.read(15L, 1024 * 1024)))
        reopened.close()
    }

    @Test
    fun `truncateTo at or beyond the log end offset is a no-op`(@TempDir tempDir: File) {
        val dir = File(tempDir, "t-0")
        val log = Log(dir)
        repeat(3) { i -> log.appendAsLeader(1000L + i, null, "v$i".toByteArray(), leaderEpoch = 1) }
        val sizeBefore = logFiles(dir).single().length()
        log.truncateTo(3L)
        log.truncateTo(100L)
        assertEquals(3L, log.logEndOffset())
        assertEquals(sizeBefore, logFiles(dir).single().length())
        assertEquals(listOf(1 to 0L), log.epochEntries())
        log.close()
    }

    @Test
    fun `truncateTo trims the leader epoch cache`(@TempDir tempDir: File) {
        val log = Log(File(tempDir, "t-0"))
        log.appendAsLeader(1L, null, "a".toByteArray(), leaderEpoch = 0)
        log.appendAsLeader(2L, null, "b".toByteArray(), leaderEpoch = 1)
        log.appendAsLeader(3L, null, "c".toByteArray(), leaderEpoch = 3)
        log.truncateTo(2L)
        assertEquals(listOf(0 to 0L, 1 to 1L), log.epochEntries())
        assertEquals(1, log.latestEpoch())
        assertEquals(0 to 1L, log.endOffsetForEpoch(0))
        assertEquals(1 to 2L, log.endOffsetForEpoch(1))
        log.close()
    }

    @Test
    fun `appendAsFollower keeps the leader's offsets and epochs`(@TempDir tempDir: File) {
        val dir = File(tempDir, "t-0")
        val log = Log(dir, segmentMaxBytes = 100L)
        val incoming = (0 until 7).map { i ->
            Record(i.toLong(), if (i < 4) 2 else 5, 5000L + i, "k$i".toByteArray(), "v$i".toByteArray())
        }
        log.appendAsFollower(incoming.subList(0, 3))
        log.appendAsFollower(incoming.subList(3, 7))
        assertEquals(7L, log.logEndOffset())
        assertEquals(listOf(2 to 0L, 5 to 4L), log.epochEntries())

        val stored = readAll(log)
        assertEquals(incoming.map { it.offset }, stored.map { it.offset })
        assertEquals(incoming.map { it.leaderEpoch }, stored.map { it.leaderEpoch })
        assertEquals(incoming.map { it.timestamp }, stored.map { it.timestamp })
        assertEquals(incoming.map { String(it.key!!) }, stored.map { String(it.key!!) })
        assertEquals(values(incoming), values(stored))
        log.close()
    }

    @Test
    fun `appendAsFollower rejects a wrong first offset or a gap and writes nothing`(@TempDir tempDir: File) {
        val log = Log(File(tempDir, "t-0"))
        log.append(1L, null, "a".toByteArray())
        fun rec(o: Long) = Record(o, 0, 1L, null, "x$o".toByteArray())

        assertThrows<IllegalArgumentException> { log.appendAsFollower(listOf(rec(2L))) }
        assertThrows<IllegalArgumentException> { log.appendAsFollower(listOf(rec(0L))) }
        assertThrows<IllegalArgumentException> { log.appendAsFollower(listOf(rec(1L), rec(3L))) }
        assertThrows<IllegalArgumentException> { log.appendAsFollower(listOf(rec(1L), rec(1L))) }
        assertEquals(1L, log.logEndOffset())

        log.appendAsFollower(emptyList())
        log.appendAsFollower(listOf(rec(1L), rec(2L)))
        assertEquals(3L, log.logEndOffset())
        log.close()
    }

    @Test
    fun `epochs survive reopen and the cache is rebuilt identically`(@TempDir tempDir: File) {
        val dir = File(tempDir, "t-0")
        val log = Log(dir, segmentMaxBytes = 100L)
        val epochs = listOf(0, 0, 0, 1, 1, 3, 3, 3, 3, 4, 7)
        epochs.forEachIndexed { i, e -> log.appendAsLeader(1000L + i, null, "v$i".toByteArray(), e) }
        val before = log.epochEntries()
        assertEquals(listOf(0 to 0L, 1 to 3L, 3 to 5L, 4 to 9L, 7 to 10L), before)
        log.close()

        val reopened = Log(dir, segmentMaxBytes = 100L)
        assertEquals(before, reopened.epochEntries())
        assertEquals(7, reopened.latestEpoch())
        assertEquals(3 to 9L, reopened.endOffsetForEpoch(3))
        assertEquals(7 to 11L, reopened.endOffsetForEpoch(7))
        reopened.close()
    }

    @Test
    fun `recovers from a torn tail whose length field is huge`(@TempDir tempDir: File) {
        val dir = File(tempDir, "t-0")
        val log = Log(dir)
        repeat(3) { i -> log.append(1000L + i, null, "v$i".toByteArray()) }
        log.close()
        val logFile = logFiles(dir).single()
        val goodLength = logFile.length()
        RandomAccessFile(logFile, "rw").use { raf ->
            raf.seek(raf.length())
            raf.writeLong(3L)            // offset
            raf.writeInt(0x12345678)     // crc
            raf.writeInt(0)              // leaderEpoch
            raf.writeLong(1003L)         // timestamp
            raf.writeInt(-1)             // null key
            raf.writeInt(Int.MAX_VALUE)  // absurd value length
            raf.write(ByteArray(16) { 7 })
        }

        val reopened = Log(dir)
        assertEquals(3L, reopened.logEndOffset())
        assertEquals(goodLength, logFile.length())
        assertEquals(listOf("v0", "v1", "v2"), values(reopened.read(0L, 1024)))
        assertEquals(3L, reopened.append(2000L, null, "v3".toByteArray()))
        reopened.close()
    }

    @Test
    fun `recovers from a torn tail whose key length is huge or negative`(@TempDir tempDir: File) {
        for (keyLength in listOf(Int.MAX_VALUE - 8, -7)) {
            val dir = File(tempDir, "t-$keyLength")
            val log = Log(dir)
            repeat(2) { i -> log.append(1000L + i, null, "v$i".toByteArray()) }
            log.close()
            val logFile = logFiles(dir).single()
            val goodLength = logFile.length()
            RandomAccessFile(logFile, "rw").use { raf ->
                raf.seek(raf.length())
                raf.writeLong(2L); raf.writeInt(0); raf.writeInt(0); raf.writeLong(1L)
                raf.writeInt(keyLength)
                raf.writeInt(-5) // negative value length
                raf.write(ByteArray(8))
            }
            val reopened = Log(dir)
            assertEquals(2L, reopened.logEndOffset(), "keyLength=$keyLength")
            assertEquals(goodLength, logFile.length(), "keyLength=$keyLength")
            reopened.close()
        }
    }

    @Test
    fun `a bad CRC in the middle of the tail truncates the log there`(@TempDir tempDir: File) {
        val dir = File(tempDir, "t-0")
        val log = Log(dir)
        repeat(5) { i -> log.append(1000L + i, null, "v$i".toByteArray()) }
        log.close()
        val recordSize = 34L // 32 bytes of framing + 2-byte value, null key
        val logFile = logFiles(dir).single()
        RandomAccessFile(logFile, "rw").use { raf ->
            val lastValueByte = 3 * recordSize + recordSize - 1 // inside record 3's value
            raf.seek(lastValueByte)
            raf.write('X'.code)
        }

        val reopened = Log(dir)
        assertEquals(3L, reopened.logEndOffset())
        assertEquals(3 * recordSize, logFile.length())
        assertEquals(listOf("v0", "v1", "v2"), values(reopened.read(0L, 1024)))
        assertEquals(3L, reopened.append(2000L, null, "new3".toByteArray()))
        reopened.close()
    }

    @Test
    fun `corruption in an earlier segment drops every later segment`(@TempDir tempDir: File) {
        val dir = File(tempDir, "t-0")
        val log = Log(dir, segmentMaxBytes = 100L)
        repeat(9) { i -> log.append(1000L + i, null, "v$i".toByteArray()) } // segments 0, 3, 6
        log.close()
        RandomAccessFile(File(dir, "%020d.log".format(3L)), "rw").use { raf ->
            raf.seek(34L + 33L) // last value byte of offset 4
            raf.write('X'.code)
        }

        val reopened = Log(dir, segmentMaxBytes = 100L)
        assertEquals(4L, reopened.logEndOffset())
        assertFalse(File(dir, "%020d.log".format(6L)).exists())
        assertEquals(listOf("v0", "v1", "v2", "v3"), values(readAll(reopened)))
        assertEquals(4L, reopened.append(2000L, null, "new4".toByteArray()))
        reopened.close()
    }

    @Test
    fun `index is rebuilt on reopen when deleted or corrupted`(@TempDir tempDir: File) {
        val dir = File(tempDir, "t-0")
        val log = Log(dir, indexIntervalBytes = 64)
        repeat(30) { i -> log.append(1000L + i, null, "value-%02d".format(i).toByteArray()) }
        log.close()
        val indexFile = indexFiles(dir).single()
        val originalIndexLength = indexFile.length()
        assertTrue(originalIndexLength >= 16)

        indexFile.delete()
        val afterDelete = Log(dir, indexIntervalBytes = 64)
        assertEquals(originalIndexLength, indexFile.length())
        assertEquals("value-17", String(afterDelete.read(17L, 1024).first().value))
        afterDelete.close()

        // Garbage entries pointing into the middle of records / past the end of the file.
        RandomAccessFile(indexFile, "rw").use { raf ->
            raf.setLength(0)
            raf.writeInt(5); raf.writeInt(3)
            raf.writeInt(10); raf.writeInt(999_999)
            raf.writeInt(20); raf.writeInt(-4)
        }
        val afterCorrupt = Log(dir, indexIntervalBytes = 64)
        assertEquals(originalIndexLength, indexFile.length())
        for (o in listOf(0L, 5L, 12L, 21L, 29L)) {
            val first = afterCorrupt.read(o, 1024).first()
            assertEquals(o, first.offset)
            assertEquals("value-%02d".format(o.toInt()), String(first.value))
        }
        afterCorrupt.close()
    }

    @Test
    fun `read with maxOffsetExclusive never returns records at or beyond the cap`(@TempDir tempDir: File) {
        val log = Log(File(tempDir, "t-0"))
        repeat(10) { i -> log.append(1000L + i, null, "v$i".toByteArray()) }
        assertEquals(listOf(2L, 3L, 4L), log.read(2L, 1024, maxOffsetExclusive = 5L).map { it.offset })
        assertEquals(emptyList<Record>(), log.read(5L, 1024, maxOffsetExclusive = 5L))
        assertEquals(emptyList<Record>(), log.read(7L, 1024, maxOffsetExclusive = 5L))
        assertEquals((2L until 10L).toList(), log.read(2L, 1024, maxOffsetExclusive = 100L).map { it.offset })
        // maxBytes still applies on top of the cap
        assertEquals(listOf(2L), log.read(2L, 1, maxOffsetExclusive = 5L).map { it.offset })
        log.close()
    }
}
