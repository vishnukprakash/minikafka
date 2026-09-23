# minikafka Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a working, single-broker miniature of Apache Kafka in Kotlin — topics/partitions, a disk-backed append-only log with sparse offset index, a custom TCP binary wire protocol, and a CLI to drive it.

**Architecture:** A single JVM process layers a thread-per-connection TCP server over an in-process `Broker`, which routes requests to one `Log` per topic-partition (segmented, index-assisted append-only files on disk). A `MiniKafkaClient` speaks the same wire protocol and backs both the CLI and the integration tests.

**Tech Stack:** Kotlin 2.0.21 (JVM), Gradle 8.10.2 (wrapper already cached locally), JUnit 5.10.3. No third-party runtime dependencies — only the JDK standard library (`java.io`, `java.net`).

**Spec:** `docs/superpowers/specs/2026-09-23-minikafka-design.md`

## Global Constraints

- All wire-protocol and on-disk integers are big-endian (Java's `DataOutput`/`DataInput` default) — never use little-endian encoding anywhere.
- No replication, no leader election, no ZooKeeper/KRaft, no consumer-group rebalancing, no log compaction — single broker only, per the spec's non-goals.
- Segment roll threshold defaults to 10MB (`segmentMaxBytes`), sparse index interval defaults to 4KB (`indexIntervalBytes`) — both configurable per `Log`/`LogSegment` instance, never hardcoded elsewhere.
- Every request/response body type lives in package `minikafka.proto`; every type must expose `encode(out: DataOutput)` and a companion `decode(input: DataInput): T` — no ad-hoc serialization elsewhere in the codebase.
- The primitive nullable string/bytes encoding helpers live in the neutral `minikafka.io` package (not `minikafka.proto`), since both the wire protocol and the on-disk record/offset-store formats need them. Storage types (`Record`, `LogSegment`, `Log`) must never import from `minikafka.proto` — keep the log layer independent of the wire format, per the spec's layering.
- Background threads (server accept loop, per-connection handler threads) must be created as daemon threads so tests and the JVM can exit cleanly.

---

## File Structure

```
minikafka/
  build.gradle.kts
  settings.gradle.kts
  .gitignore
  gradlew, gradlew.bat, gradle/wrapper/{gradle-wrapper.jar,gradle-wrapper.properties}
  README.md
  src/main/kotlin/minikafka/
    io/Encoding.kt           - primitive nullable string/bytes read+write helpers (shared by log and proto)
    proto/Framing.kt         - 4-byte length-prefixed request/response frame helpers
    proto/Protocol.kt        - ApiKeys, ErrorCodes constants
    proto/Requests.kt        - request body types (encode/decode)
    proto/Responses.kt       - response body types (encode/decode)
    log/Record.kt            - single record encode/decode + size
    log/OffsetIndex.kt       - sparse offset -> byte position index
    log/LogSegment.kt        - one segment file: append/read/recover
    log/Log.kt               - multi-segment log for one partition: append/read/roll
    broker/OffsetStore.kt    - consumer offset commit/fetch, disk-backed
    broker/Broker.kt         - topic registry, partitioning, produce/fetch/offset dispatch
    server/Server.kt         - TCP accept loop
    server/ConnectionHandler.kt - per-connection request loop
    client/MiniKafkaClient.kt - TCP client used by CLI and tests
    cli/Cli.kt                - `minikafka` subcommands: server, topics, produce, consume
  src/test/kotlin/minikafka/
    io/EncodingTest.kt
    proto/FramingTest.kt
    proto/ProtocolCodecTest.kt
    log/RecordTest.kt
    log/OffsetIndexTest.kt
    log/LogSegmentTest.kt
    log/LogTest.kt
    broker/OffsetStoreTest.kt
    broker/BrokerTest.kt
    integration/BrokerIntegrationTest.kt
```

---

### Task 1: Project scaffolding

**Files:**
- Create: `build.gradle.kts`
- Create: `settings.gradle.kts`
- Create: `.gitignore`
- Create: `gradlew`, `gradlew.bat`, `gradle/wrapper/gradle-wrapper.jar`, `gradle/wrapper/gradle-wrapper.properties` (copied from the sibling `ds-patterns-workshop` project, which already has a working Gradle 8.10.2 wrapper cached locally)

**Interfaces:**
- Produces: a buildable, empty Gradle Kotlin project named `minikafka` that later tasks add sources to.

- [ ] **Step 1: Copy the Gradle wrapper from the sibling project**

```bash
mkdir -p /Users/vishnuprakash/Documents/distributed-systems/minikafka/gradle/wrapper
cp /Users/vishnuprakash/Documents/distributed-systems/ds-patterns-workshop/gradlew \
   /Users/vishnuprakash/Documents/distributed-systems/minikafka/gradlew
cp /Users/vishnuprakash/Documents/distributed-systems/ds-patterns-workshop/gradlew.bat \
   /Users/vishnuprakash/Documents/distributed-systems/minikafka/gradlew.bat
cp /Users/vishnuprakash/Documents/distributed-systems/ds-patterns-workshop/gradle/wrapper/gradle-wrapper.jar \
   /Users/vishnuprakash/Documents/distributed-systems/minikafka/gradle/wrapper/gradle-wrapper.jar
cp /Users/vishnuprakash/Documents/distributed-systems/ds-patterns-workshop/gradle/wrapper/gradle-wrapper.properties \
   /Users/vishnuprakash/Documents/distributed-systems/minikafka/gradle/wrapper/gradle-wrapper.properties
chmod +x /Users/vishnuprakash/Documents/distributed-systems/minikafka/gradlew
```

- [ ] **Step 2: Write `settings.gradle.kts`**

```kotlin
rootProject.name = "minikafka"
```

- [ ] **Step 3: Write `build.gradle.kts`**

```kotlin
plugins {
    kotlin("jvm") version "2.0.21"
    application
}

group = "minikafka"
version = "0.1.0"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(platform("org.junit:junit-bom:5.10.3"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

application {
    mainClass.set("minikafka.cli.CliKt")
}

tasks.test {
    useJUnitPlatform()
}
```

- [ ] **Step 4: Write `.gitignore`**

```
.gradle/
build/
.idea/
*.iml
data/
```

- [ ] **Step 5: Verify the empty project builds**

Run: `./gradlew build`
Expected: `BUILD SUCCESSFUL` (no sources yet, so this just validates plugin resolution and wrapper setup — this is the scaffolding task's substitute for a failing test, since there's no code yet to test).

- [ ] **Step 6: Commit**

```bash
cd /Users/vishnuprakash/Documents/distributed-systems/minikafka
git add build.gradle.kts settings.gradle.kts .gitignore gradlew gradlew.bat gradle/
git commit -m "Scaffold minikafka Gradle/Kotlin project"
```

---

### Task 2: Binary encoding primitives

**Files:**
- Create: `src/main/kotlin/minikafka/io/Encoding.kt`
- Test: `src/test/kotlin/minikafka/io/EncodingTest.kt`

**Interfaces:**
- Produces: `DataOutput.writeNullableString(String?)`, `DataInput.readNullableString(): String?`, `DataOutput.writeNullableBytesAsInt32(ByteArray?)`, `DataInput.readNullableBytesAsInt32(): ByteArray?` — extension functions on `java.io.DataOutput`/`java.io.DataInput` so they work uniformly over network streams, in-memory buffers, and `RandomAccessFile`.

- [ ] **Step 1: Write the failing test**

```kotlin
package minikafka.io

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream

class EncodingTest {
    @Test
    fun `round trips a non-null string`() {
        val buffer = ByteArrayOutputStream()
        DataOutputStream(buffer).writeNullableString("hello")
        val input = DataInputStream(ByteArrayInputStream(buffer.toByteArray()))
        assertEquals("hello", input.readNullableString())
    }

    @Test
    fun `round trips a null string`() {
        val buffer = ByteArrayOutputStream()
        DataOutputStream(buffer).writeNullableString(null)
        val input = DataInputStream(ByteArrayInputStream(buffer.toByteArray()))
        assertNull(input.readNullableString())
    }

    @Test
    fun `round trips non-null bytes`() {
        val bytes = byteArrayOf(1, 2, 3, 4)
        val buffer = ByteArrayOutputStream()
        DataOutputStream(buffer).writeNullableBytesAsInt32(bytes)
        val input = DataInputStream(ByteArrayInputStream(buffer.toByteArray()))
        assertArrayEquals(bytes, input.readNullableBytesAsInt32())
    }

    @Test
    fun `round trips null bytes`() {
        val buffer = ByteArrayOutputStream()
        DataOutputStream(buffer).writeNullableBytesAsInt32(null)
        val input = DataInputStream(ByteArrayInputStream(buffer.toByteArray()))
        assertNull(input.readNullableBytesAsInt32())
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew test --tests "minikafka.io.EncodingTest"`
Expected: compilation failure — `writeNullableString`/`readNullableString`/`writeNullableBytesAsInt32`/`readNullableBytesAsInt32` are unresolved references.

- [ ] **Step 3: Write the implementation**

```kotlin
package minikafka.io

import java.io.DataInput
import java.io.DataOutput

fun DataOutput.writeNullableString(value: String?) {
    if (value == null) {
        writeShort(-1)
    } else {
        val bytes = value.toByteArray(Charsets.UTF_8)
        writeShort(bytes.size)
        write(bytes)
    }
}

fun DataInput.readNullableString(): String? {
    val length = readShort().toInt()
    if (length < 0) return null
    val bytes = ByteArray(length)
    readFully(bytes)
    return String(bytes, Charsets.UTF_8)
}

fun DataOutput.writeNullableBytesAsInt32(value: ByteArray?) {
    if (value == null) {
        writeInt(-1)
    } else {
        writeInt(value.size)
        write(value)
    }
}

fun DataInput.readNullableBytesAsInt32(): ByteArray? {
    val length = readInt()
    if (length < 0) return null
    val bytes = ByteArray(length)
    readFully(bytes)
    return bytes
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `./gradlew test --tests "minikafka.io.EncodingTest"`
Expected: PASS (4 tests)

- [ ] **Step 5: Commit**

```bash
git add src/main/kotlin/minikafka/io/Encoding.kt src/test/kotlin/minikafka/io/EncodingTest.kt
git commit -m "Add binary encoding primitives for nullable strings and byte arrays"
```

---

### Task 3: Record format

**Files:**
- Create: `src/main/kotlin/minikafka/log/Record.kt`
- Test: `src/test/kotlin/minikafka/log/RecordTest.kt`

**Interfaces:**
- Consumes: `DataOutput.writeNullableBytesAsInt32`, `DataInput.readNullableBytesAsInt32` from `minikafka.io` (Task 2).
- Produces: `data class Record(offset: Long, timestamp: Long, key: ByteArray?, value: ByteArray)` with `writeTo(out: DataOutput)`, companion `readFrom(input: DataInput): Record`, and `sizeInBytes(): Int`. On-disk/wire layout: `offset:8 | timestamp:8 | keyLen:4 (-1 if null) | key bytes | valueLen:4 | value bytes`.

- [ ] **Step 1: Write the failing test**

```kotlin
package minikafka.log

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream

class RecordTest {
    @Test
    fun `round trips a record with a key`() {
        val record = Record(offset = 5L, timestamp = 1000L, key = "k".toByteArray(), value = "hello".toByteArray())
        val buffer = ByteArrayOutputStream()
        record.writeTo(DataOutputStream(buffer))
        val decoded = Record.readFrom(DataInputStream(ByteArrayInputStream(buffer.toByteArray())))
        assertEquals(record.offset, decoded.offset)
        assertEquals(record.timestamp, decoded.timestamp)
        assertArrayEquals(record.key, decoded.key)
        assertArrayEquals(record.value, decoded.value)
    }

    @Test
    fun `round trips a record without a key`() {
        val record = Record(offset = 0L, timestamp = 2000L, key = null, value = "world".toByteArray())
        val buffer = ByteArrayOutputStream()
        record.writeTo(DataOutputStream(buffer))
        val decoded = Record.readFrom(DataInputStream(ByteArrayInputStream(buffer.toByteArray())))
        assertNull(decoded.key)
        assertArrayEquals(record.value, decoded.value)
    }

    @Test
    fun `sizeInBytes matches the encoded length`() {
        val record = Record(offset = 0L, timestamp = 0L, key = "k".toByteArray(), value = "value".toByteArray())
        val buffer = ByteArrayOutputStream()
        record.writeTo(DataOutputStream(buffer))
        assertEquals(buffer.size(), record.sizeInBytes())
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew test --tests "minikafka.log.RecordTest"`
Expected: compilation failure — `Record` is unresolved.

- [ ] **Step 3: Write the implementation**

```kotlin
package minikafka.log

import minikafka.io.readNullableBytesAsInt32
import minikafka.io.writeNullableBytesAsInt32
import java.io.DataInput
import java.io.DataOutput

data class Record(
    val offset: Long,
    val timestamp: Long,
    val key: ByteArray?,
    val value: ByteArray
) {
    fun writeTo(out: DataOutput) {
        out.writeLong(offset)
        out.writeLong(timestamp)
        out.writeNullableBytesAsInt32(key)
        out.writeInt(value.size)
        out.write(value)
    }

    fun sizeInBytes(): Int = 8 + 8 + 4 + (key?.size ?: 0) + 4 + value.size

    companion object {
        fun readFrom(input: DataInput): Record {
            val offset = input.readLong()
            val timestamp = input.readLong()
            val key = input.readNullableBytesAsInt32()
            val valueLength = input.readInt()
            val value = ByteArray(valueLength)
            input.readFully(value)
            return Record(offset, timestamp, key, value)
        }
    }
}
```

Note for later tasks: `Record` is a `data class` with `ByteArray` fields, so its generated `equals`/`hashCode` compare arrays by reference, not content. Never assert `record1 == record2` in tests — compare `.offset`, `.timestamp`, and use `assertArrayEquals` for `.key`/`.value` instead.

- [ ] **Step 4: Run test to verify it passes**

Run: `./gradlew test --tests "minikafka.log.RecordTest"`
Expected: PASS (3 tests)

- [ ] **Step 5: Commit**

```bash
git add src/main/kotlin/minikafka/log/Record.kt src/test/kotlin/minikafka/log/RecordTest.kt
git commit -m "Add Record binary format for the append-only log"
```

---

### Task 4: Sparse offset index

**Files:**
- Create: `src/main/kotlin/minikafka/log/OffsetIndex.kt`
- Test: `src/test/kotlin/minikafka/log/OffsetIndexTest.kt`

**Interfaces:**
- Produces: `class OffsetIndex(file: File, baseOffset: Long)` with `append(offset: Long, position: Int)` and `lookup(targetOffset: Long): Int` (returns the byte position of the nearest indexed entry at or before `targetOffset`, or `0` if there are no entries yet or the target is before the first entry).

- [ ] **Step 1: Write the failing test**

```kotlin
package minikafka.log

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.io.TempDir
import java.io.File

class OffsetIndexTest {
    @Test
    fun `lookup returns 0 when no entries exist`(@TempDir tempDir: File) {
        val index = OffsetIndex(File(tempDir, "00000000000000000000.index"), baseOffset = 0L)
        assertEquals(0, index.lookup(100L))
    }

    @Test
    fun `lookup returns the nearest position at or before the target offset`(@TempDir tempDir: File) {
        val index = OffsetIndex(File(tempDir, "00000000000000000000.index"), baseOffset = 0L)
        index.append(offset = 10L, position = 100)
        index.append(offset = 20L, position = 250)
        index.append(offset = 30L, position = 400)

        assertEquals(0, index.lookup(5L))
        assertEquals(100, index.lookup(15L))
        assertEquals(250, index.lookup(25L))
        assertEquals(400, index.lookup(1000L))
    }

    @Test
    fun `reloads entries from an existing index file`(@TempDir tempDir: File) {
        val file = File(tempDir, "00000000000000000000.index")
        val first = OffsetIndex(file, baseOffset = 0L)
        first.append(offset = 10L, position = 100)

        val reloaded = OffsetIndex(file, baseOffset = 0L)
        assertEquals(100, reloaded.lookup(50L))
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew test --tests "minikafka.log.OffsetIndexTest"`
Expected: compilation failure — `OffsetIndex` is unresolved.

- [ ] **Step 3: Write the implementation**

```kotlin
package minikafka.log

import java.io.File
import java.io.RandomAccessFile

class OffsetIndex(private val file: File, private val baseOffset: Long) {
    private val entries = mutableListOf<Pair<Int, Int>>() // relativeOffset to filePosition, in ascending order

    init {
        if (file.exists() && file.length() > 0) {
            RandomAccessFile(file, "r").use { raf ->
                val count = (raf.length() / ENTRY_SIZE).toInt()
                for (i in 0 until count) {
                    raf.seek(i.toLong() * ENTRY_SIZE)
                    val relativeOffset = raf.readInt()
                    val position = raf.readInt()
                    entries.add(relativeOffset to position)
                }
            }
        }
    }

    @Synchronized
    fun append(offset: Long, position: Int) {
        val relativeOffset = (offset - baseOffset).toInt()
        RandomAccessFile(file, "rw").use { raf ->
            raf.seek(raf.length())
            raf.writeInt(relativeOffset)
            raf.writeInt(position)
        }
        entries.add(relativeOffset to position)
    }

    @Synchronized
    fun lookup(targetOffset: Long): Int {
        val relativeTarget = (targetOffset - baseOffset).toInt()
        var low = 0
        var high = entries.size - 1
        var result = 0
        while (low <= high) {
            val mid = (low + high) / 2
            val (relativeOffset, position) = entries[mid]
            if (relativeOffset <= relativeTarget) {
                result = position
                low = mid + 1
            } else {
                high = mid - 1
            }
        }
        return result
    }

    companion object {
        private const val ENTRY_SIZE = 8
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `./gradlew test --tests "minikafka.log.OffsetIndexTest"`
Expected: PASS (3 tests)

- [ ] **Step 5: Commit**

```bash
git add src/main/kotlin/minikafka/log/OffsetIndex.kt src/test/kotlin/minikafka/log/OffsetIndexTest.kt
git commit -m "Add sparse offset index for fast partition reads"
```

---

### Task 5: Log segment

**Files:**
- Create: `src/main/kotlin/minikafka/log/LogSegment.kt`
- Test: `src/test/kotlin/minikafka/log/LogSegmentTest.kt`

**Interfaces:**
- Consumes: `Record` (Task 3), `OffsetIndex` (Task 4).
- Produces: `class LogSegment(dir: File, baseOffset: Long, indexIntervalBytes: Int = 4096)` with `append(record: Record)`, `read(offset: Long, maxBytes: Int): List<Record>`, `val nextOffset: Long`, `val sizeInBytes: Long`, `close()`. On reopen, replays the `.log` file to recompute `nextOffset`/`sizeInBytes` and truncates any partial trailing write left by an unclean shutdown.

- [ ] **Step 1: Write the failing test**

```kotlin
package minikafka.log

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.io.TempDir
import java.io.File
import java.io.RandomAccessFile

class LogSegmentTest {
    @Test
    fun `appends and reads back records in order`(@TempDir tempDir: File) {
        val segment = LogSegment(tempDir, baseOffset = 0L)
        segment.append(Record(0L, 1000L, null, "a".toByteArray()))
        segment.append(Record(1L, 1001L, null, "b".toByteArray()))
        segment.append(Record(2L, 1002L, null, "c".toByteArray()))

        val records = segment.read(0L, maxBytes = 1024)
        assertEquals(3, records.size)
        assertEquals("a", String(records[0].value))
        assertEquals("b", String(records[1].value))
        assertEquals("c", String(records[2].value))
        segment.close()
    }

    @Test
    fun `reads starting from a middle offset`(@TempDir tempDir: File) {
        val segment = LogSegment(tempDir, baseOffset = 0L)
        for (i in 0 until 5) {
            segment.append(Record(i.toLong(), 1000L + i, null, "v$i".toByteArray()))
        }
        val records = segment.read(3L, maxBytes = 1024)
        assertEquals(2, records.size)
        assertEquals("v3", String(records[0].value))
        assertEquals("v4", String(records[1].value))
        segment.close()
    }

    @Test
    fun `recovers by truncating a partial trailing write after an unclean shutdown`(@TempDir tempDir: File) {
        val segment = LogSegment(tempDir, baseOffset = 0L)
        segment.append(Record(0L, 1000L, null, "a".toByteArray()))
        segment.append(Record(1L, 1001L, null, "b".toByteArray()))
        segment.close()

        // Simulate a crash mid-write: a complete offset field followed by an
        // incomplete timestamp field (4 of the 8 bytes it needs).
        val logFile = File(tempDir, "%020d.log".format(0L))
        RandomAccessFile(logFile, "rw").use { raf ->
            raf.seek(raf.length())
            raf.writeLong(2L)
            raf.writeInt(42)
        }

        val recovered = LogSegment(tempDir, baseOffset = 0L)
        assertEquals(2L, recovered.nextOffset)
        val records = recovered.read(0L, maxBytes = 1024)
        assertEquals(2, records.size)
        recovered.close()
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew test --tests "minikafka.log.LogSegmentTest"`
Expected: compilation failure — `LogSegment` is unresolved.

- [ ] **Step 3: Write the implementation**

```kotlin
package minikafka.log

import java.io.EOFException
import java.io.File
import java.io.RandomAccessFile

class LogSegment(
    dir: File,
    val baseOffset: Long,
    private val indexIntervalBytes: Int = 4096
) {
    private val logFile = File(dir, "%020d.log".format(baseOffset))
    private val indexFile = File(dir, "%020d.index".format(baseOffset))
    private val index = OffsetIndex(indexFile, baseOffset)
    private val raf = RandomAccessFile(logFile, "rw")

    var nextOffset: Long = baseOffset
        private set
    var sizeInBytes: Long = 0L
        private set
    private var bytesSinceLastIndex: Int = 0

    init {
        val length = raf.length()
        raf.seek(0)
        var pos = 0L
        while (pos < length) {
            val recordStart = pos
            try {
                val record = Record.readFrom(raf)
                pos = raf.filePointer
                nextOffset = record.offset + 1
            } catch (e: EOFException) {
                raf.setLength(recordStart)
                pos = recordStart
                break
            }
        }
        sizeInBytes = pos
        raf.seek(pos)
    }

    @Synchronized
    fun append(record: Record) {
        require(record.offset == nextOffset) {
            "out-of-order append: expected offset $nextOffset, got ${record.offset}"
        }
        val position = sizeInBytes
        raf.seek(position)
        record.writeTo(raf)
        val recordSize = record.sizeInBytes()
        sizeInBytes += recordSize
        bytesSinceLastIndex += recordSize
        if (bytesSinceLastIndex >= indexIntervalBytes) {
            index.append(record.offset, position.toInt())
            bytesSinceLastIndex = 0
        }
        nextOffset = record.offset + 1
    }

    @Synchronized
    fun read(offset: Long, maxBytes: Int): List<Record> {
        if (offset < baseOffset || offset >= nextOffset) return emptyList()
        val startPosition = index.lookup(offset)
        raf.seek(startPosition.toLong())
        val records = mutableListOf<Record>()
        var bytesRead = 0
        while (raf.filePointer < sizeInBytes && bytesRead < maxBytes) {
            val record = Record.readFrom(raf)
            if (record.offset >= offset) {
                records.add(record)
                bytesRead += record.sizeInBytes()
            }
        }
        return records
    }

    fun close() {
        raf.close()
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `./gradlew test --tests "minikafka.log.LogSegmentTest"`
Expected: PASS (3 tests)

- [ ] **Step 5: Commit**

```bash
git add src/main/kotlin/minikafka/log/LogSegment.kt src/test/kotlin/minikafka/log/LogSegmentTest.kt
git commit -m "Add LogSegment with index-assisted reads and crash recovery"
```

---

### Task 6: Multi-segment log

**Files:**
- Create: `src/main/kotlin/minikafka/log/Log.kt`
- Test: `src/test/kotlin/minikafka/log/LogTest.kt`

**Interfaces:**
- Consumes: `LogSegment` (Task 5).
- Produces: `class Log(dir: File, segmentMaxBytes: Long = 10 * 1024 * 1024, indexIntervalBytes: Int = 4096)` with `append(timestamp: Long, key: ByteArray?, value: ByteArray): Long` (returns the assigned offset), `read(offset: Long, maxBytes: Int): List<Record>`, `logEndOffset(): Long`, `close()`. Rebuilds its segment list from `dir` on construction, so a `Log` survives process restarts.

- [ ] **Step 1: Write the failing test**

```kotlin
package minikafka.log

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.io.TempDir
import java.io.File

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
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew test --tests "minikafka.log.LogTest"`
Expected: compilation failure — `Log` is unresolved.

- [ ] **Step 3: Write the implementation**

```kotlin
package minikafka.log

import java.io.File

class Log(
    private val dir: File,
    private val segmentMaxBytes: Long = 10 * 1024 * 1024,
    private val indexIntervalBytes: Int = 4096
) {
    private val segments = mutableListOf<LogSegment>()

    init {
        dir.mkdirs()
        val baseOffsets = dir.listFiles { f -> f.name.endsWith(".log") }
            ?.map { it.name.removeSuffix(".log").toLong() }
            ?.sorted()
            ?: emptyList()
        if (baseOffsets.isEmpty()) {
            segments.add(LogSegment(dir, 0L, indexIntervalBytes))
        } else {
            baseOffsets.forEach { base -> segments.add(LogSegment(dir, base, indexIntervalBytes)) }
        }
    }

    private fun activeSegment(): LogSegment = segments.last()

    @Synchronized
    fun append(timestamp: Long, key: ByteArray?, value: ByteArray): Long {
        var active = activeSegment()
        if (active.sizeInBytes >= segmentMaxBytes) {
            active = LogSegment(dir, active.nextOffset, indexIntervalBytes)
            segments.add(active)
        }
        val offset = active.nextOffset
        active.append(Record(offset, timestamp, key, value))
        return offset
    }

    @Synchronized
    fun read(offset: Long, maxBytes: Int): List<Record> {
        val segment = segments.lastOrNull { it.baseOffset <= offset } ?: return emptyList()
        return segment.read(offset, maxBytes)
    }

    fun logEndOffset(): Long = activeSegment().nextOffset

    fun close() {
        segments.forEach { it.close() }
    }
}
```

Note: reads do not stitch across a segment boundary — a `read` only returns records from the single segment containing the requested offset. This is a deliberate simplification; a consumer that reaches the end of a segment's records simply issues its next `fetch` at the next offset, which will resolve into the following segment.

- [ ] **Step 4: Run test to verify it passes**

Run: `./gradlew test --tests "minikafka.log.LogTest"`
Expected: PASS (4 tests)

- [ ] **Step 5: Commit**

```bash
git add src/main/kotlin/minikafka/log/Log.kt src/test/kotlin/minikafka/log/LogTest.kt
git commit -m "Add multi-segment Log with rolling and persistence across restarts"
```

---

### Task 7: Wire protocol

**Files:**
- Create: `src/main/kotlin/minikafka/proto/Protocol.kt`
- Create: `src/main/kotlin/minikafka/proto/Framing.kt`
- Create: `src/main/kotlin/minikafka/proto/Requests.kt`
- Create: `src/main/kotlin/minikafka/proto/Responses.kt`
- Test: `src/test/kotlin/minikafka/proto/FramingTest.kt`
- Test: `src/test/kotlin/minikafka/proto/ProtocolCodecTest.kt`

**Interfaces:**
- Consumes: `DataOutput.writeNullableString`/`DataInput.readNullableString`, `DataOutput.writeNullableBytesAsInt32`/`DataInput.readNullableBytesAsInt32` from `minikafka.io` (Task 2).
- Produces:
  - `object ApiKeys` (`CREATE_TOPIC`, `METADATA`, `PRODUCE`, `FETCH`, `OFFSET_COMMIT`, `OFFSET_FETCH`: all `Short`) and `object ErrorCodes` (`NONE`, `UNKNOWN_TOPIC`, `UNKNOWN_PARTITION`, `OFFSET_OUT_OF_RANGE`, `TOPIC_ALREADY_EXISTS`: all `Short`).
  - `data class FrameHeader(apiKey: Short, correlationId: Int)`, `fun writeFrame(out: DataOutputStream, apiKey: Short, correlationId: Int, body: (DataOutput) -> Unit)`, `fun readFrameHeader(input: DataInputStream): Pair<FrameHeader, DataInputStream>`, `fun writeResponseFrame(out: DataOutputStream, correlationId: Int, body: (DataOutput) -> Unit)`, `fun readResponseFrame(input: DataInputStream): Pair<Int, DataInputStream>`.
  - Request types: `CreateTopicRequest`, `MetadataRequest`, `ProduceRequest`, `FetchRequest`, `OffsetCommitRequest`, `OffsetFetchRequest` — each with `encode(out: DataOutput)` and companion `decode(input: DataInput)`.
  - Response types: `CreateTopicResponse`, `TopicMetadata`, `MetadataResponse`, `ProduceResponse`, `FetchedRecord`, `FetchResponse`, `OffsetCommitResponse`, `OffsetFetchResponse` — same `encode`/`decode` shape.

- [ ] **Step 1: Write the failing tests**

`src/test/kotlin/minikafka/proto/FramingTest.kt`:

```kotlin
package minikafka.proto

import minikafka.io.readNullableString
import minikafka.io.writeNullableString
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.assertEquals
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream

class FramingTest {
    @Test
    fun `round trips a request frame`() {
        val buffer = ByteArrayOutputStream()
        writeFrame(DataOutputStream(buffer), apiKey = 3, correlationId = 7) { out ->
            out.writeNullableString("hello")
        }
        val (header, body) = readFrameHeader(DataInputStream(ByteArrayInputStream(buffer.toByteArray())))
        assertEquals(3.toShort(), header.apiKey)
        assertEquals(7, header.correlationId)
        assertEquals("hello", body.readNullableString())
    }

    @Test
    fun `round trips a response frame`() {
        val buffer = ByteArrayOutputStream()
        writeResponseFrame(DataOutputStream(buffer), correlationId = 9) { out ->
            out.writeShort(0)
        }
        val (correlationId, body) = readResponseFrame(DataInputStream(ByteArrayInputStream(buffer.toByteArray())))
        assertEquals(9, correlationId)
        assertEquals(0, body.readShort().toInt())
    }
}
```

`src/test/kotlin/minikafka/proto/ProtocolCodecTest.kt`:

```kotlin
package minikafka.proto

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.assertEquals
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream

class ProtocolCodecTest {
    private fun <T> roundTrip(encode: (DataOutputStream) -> Unit, decode: (DataInputStream) -> T): T {
        val buffer = ByteArrayOutputStream()
        encode(DataOutputStream(buffer))
        return decode(DataInputStream(ByteArrayInputStream(buffer.toByteArray())))
    }

    @Test
    fun `round trips CreateTopicRequest`() {
        val decoded = roundTrip({ CreateTopicRequest("t", 3).encode(it) }, { CreateTopicRequest.decode(it) })
        assertEquals(CreateTopicRequest("t", 3), decoded)
    }

    @Test
    fun `round trips MetadataResponse`() {
        val topics = listOf(TopicMetadata("t1", 2), TopicMetadata("t2", 1))
        val decoded = roundTrip({ MetadataResponse(topics).encode(it) }, { MetadataResponse.decode(it) })
        assertEquals(topics, decoded.topics)
    }

    @Test
    fun `round trips ProduceRequest and ProduceResponse`() {
        val req = roundTrip(
            { ProduceRequest("t", "k".toByteArray(), "v".toByteArray()).encode(it) },
            { ProduceRequest.decode(it) }
        )
        assertEquals("t", req.topic)
        assertEquals("k", String(req.key!!))
        assertEquals("v", String(req.value))

        val resp = roundTrip(
            { ProduceResponse(ErrorCodes.NONE, 2, 10L).encode(it) },
            { ProduceResponse.decode(it) }
        )
        assertEquals(ProduceResponse(ErrorCodes.NONE, 2, 10L), resp)
    }

    @Test
    fun `round trips FetchRequest and FetchResponse`() {
        val req = roundTrip(
            { FetchRequest("t", 1, 5L, 2048).encode(it) },
            { FetchRequest.decode(it) }
        )
        assertEquals(FetchRequest("t", 1, 5L, 2048), req)

        val records = listOf(FetchedRecord(0L, 1000L, null, "v".toByteArray()))
        val resp = roundTrip(
            { FetchResponse(ErrorCodes.NONE, records).encode(it) },
            { FetchResponse.decode(it) }
        )
        assertEquals(1, resp.records.size)
        assertEquals("v", String(resp.records[0].value))
    }

    @Test
    fun `round trips OffsetCommitRequest and OffsetFetchResponse`() {
        val commitReq = roundTrip(
            { OffsetCommitRequest("g", "t", 0, 7L).encode(it) },
            { OffsetCommitRequest.decode(it) }
        )
        assertEquals(OffsetCommitRequest("g", "t", 0, 7L), commitReq)

        val fetchResp = roundTrip(
            { OffsetFetchResponse(ErrorCodes.NONE, 7L).encode(it) },
            { OffsetFetchResponse.decode(it) }
        )
        assertEquals(OffsetFetchResponse(ErrorCodes.NONE, 7L), fetchResp)
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `./gradlew test --tests "minikafka.proto.FramingTest" --tests "minikafka.proto.ProtocolCodecTest"`
Expected: compilation failure — none of the referenced types exist yet.

- [ ] **Step 3: Write `Protocol.kt`**

```kotlin
package minikafka.proto

object ApiKeys {
    const val CREATE_TOPIC: Short = 1
    const val METADATA: Short = 2
    const val PRODUCE: Short = 3
    const val FETCH: Short = 4
    const val OFFSET_COMMIT: Short = 5
    const val OFFSET_FETCH: Short = 6
}

object ErrorCodes {
    const val NONE: Short = 0
    const val UNKNOWN_TOPIC: Short = 1
    const val UNKNOWN_PARTITION: Short = 2
    const val OFFSET_OUT_OF_RANGE: Short = 3
    const val TOPIC_ALREADY_EXISTS: Short = 4
}
```

- [ ] **Step 4: Write `Framing.kt`**

```kotlin
package minikafka.proto

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInput
import java.io.DataInputStream
import java.io.DataOutput
import java.io.DataOutputStream

data class FrameHeader(val apiKey: Short, val correlationId: Int)

fun writeFrame(out: DataOutputStream, apiKey: Short, correlationId: Int, body: (DataOutput) -> Unit) {
    val buffer = ByteArrayOutputStream()
    val bodyOut = DataOutputStream(buffer)
    bodyOut.writeShort(apiKey.toInt())
    bodyOut.writeInt(correlationId)
    body(bodyOut)
    bodyOut.flush()
    val bytes = buffer.toByteArray()
    out.writeInt(bytes.size)
    out.write(bytes)
    out.flush()
}

fun readFrameHeader(input: DataInputStream): Pair<FrameHeader, DataInputStream> {
    val size = input.readInt()
    val bytes = ByteArray(size)
    input.readFully(bytes)
    val bodyIn = DataInputStream(ByteArrayInputStream(bytes))
    val apiKey = bodyIn.readShort()
    val correlationId = bodyIn.readInt()
    return FrameHeader(apiKey, correlationId) to bodyIn
}

fun writeResponseFrame(out: DataOutputStream, correlationId: Int, body: (DataOutput) -> Unit) {
    val buffer = ByteArrayOutputStream()
    val bodyOut = DataOutputStream(buffer)
    bodyOut.writeInt(correlationId)
    body(bodyOut)
    bodyOut.flush()
    val bytes = buffer.toByteArray()
    out.writeInt(bytes.size)
    out.write(bytes)
    out.flush()
}

fun readResponseFrame(input: DataInputStream): Pair<Int, DataInputStream> {
    val size = input.readInt()
    val bytes = ByteArray(size)
    input.readFully(bytes)
    val bodyIn = DataInputStream(ByteArrayInputStream(bytes))
    val correlationId = bodyIn.readInt()
    return correlationId to bodyIn
}
```

- [ ] **Step 5: Write `Requests.kt`**

```kotlin
package minikafka.proto

import minikafka.io.readNullableBytesAsInt32
import minikafka.io.readNullableString
import minikafka.io.writeNullableBytesAsInt32
import minikafka.io.writeNullableString
import java.io.DataInput
import java.io.DataOutput

data class CreateTopicRequest(val topic: String, val numPartitions: Int) {
    fun encode(out: DataOutput) {
        out.writeNullableString(topic)
        out.writeInt(numPartitions)
    }
    companion object {
        fun decode(input: DataInput): CreateTopicRequest {
            val topic = input.readNullableString()!!
            val numPartitions = input.readInt()
            return CreateTopicRequest(topic, numPartitions)
        }
    }
}

class MetadataRequest {
    fun encode(out: DataOutput) {}
    companion object {
        fun decode(input: DataInput): MetadataRequest = MetadataRequest()
    }
}

data class ProduceRequest(val topic: String, val key: ByteArray?, val value: ByteArray) {
    fun encode(out: DataOutput) {
        out.writeNullableString(topic)
        out.writeNullableBytesAsInt32(key)
        out.writeInt(value.size)
        out.write(value)
    }
    companion object {
        fun decode(input: DataInput): ProduceRequest {
            val topic = input.readNullableString()!!
            val key = input.readNullableBytesAsInt32()
            val valueLength = input.readInt()
            val value = ByteArray(valueLength)
            input.readFully(value)
            return ProduceRequest(topic, key, value)
        }
    }
}

data class FetchRequest(val topic: String, val partition: Int, val offset: Long, val maxBytes: Int) {
    fun encode(out: DataOutput) {
        out.writeNullableString(topic)
        out.writeInt(partition)
        out.writeLong(offset)
        out.writeInt(maxBytes)
    }
    companion object {
        fun decode(input: DataInput): FetchRequest {
            val topic = input.readNullableString()!!
            val partition = input.readInt()
            val offset = input.readLong()
            val maxBytes = input.readInt()
            return FetchRequest(topic, partition, offset, maxBytes)
        }
    }
}

data class OffsetCommitRequest(val group: String, val topic: String, val partition: Int, val offset: Long) {
    fun encode(out: DataOutput) {
        out.writeNullableString(group)
        out.writeNullableString(topic)
        out.writeInt(partition)
        out.writeLong(offset)
    }
    companion object {
        fun decode(input: DataInput): OffsetCommitRequest {
            val group = input.readNullableString()!!
            val topic = input.readNullableString()!!
            val partition = input.readInt()
            val offset = input.readLong()
            return OffsetCommitRequest(group, topic, partition, offset)
        }
    }
}

data class OffsetFetchRequest(val group: String, val topic: String, val partition: Int) {
    fun encode(out: DataOutput) {
        out.writeNullableString(group)
        out.writeNullableString(topic)
        out.writeInt(partition)
    }
    companion object {
        fun decode(input: DataInput): OffsetFetchRequest {
            val group = input.readNullableString()!!
            val topic = input.readNullableString()!!
            val partition = input.readInt()
            return OffsetFetchRequest(group, topic, partition)
        }
    }
}
```

- [ ] **Step 6: Write `Responses.kt`**

```kotlin
package minikafka.proto

import minikafka.io.readNullableBytesAsInt32
import minikafka.io.readNullableString
import minikafka.io.writeNullableBytesAsInt32
import minikafka.io.writeNullableString
import java.io.DataInput
import java.io.DataOutput

data class CreateTopicResponse(val errorCode: Short) {
    fun encode(out: DataOutput) { out.writeShort(errorCode.toInt()) }
    companion object {
        fun decode(input: DataInput): CreateTopicResponse = CreateTopicResponse(input.readShort())
    }
}

data class TopicMetadata(val name: String, val numPartitions: Int) {
    fun encode(out: DataOutput) {
        out.writeNullableString(name)
        out.writeInt(numPartitions)
    }
    companion object {
        fun decode(input: DataInput): TopicMetadata {
            val name = input.readNullableString()!!
            val numPartitions = input.readInt()
            return TopicMetadata(name, numPartitions)
        }
    }
}

data class MetadataResponse(val topics: List<TopicMetadata>) {
    fun encode(out: DataOutput) {
        out.writeInt(topics.size)
        topics.forEach { it.encode(out) }
    }
    companion object {
        fun decode(input: DataInput): MetadataResponse {
            val count = input.readInt()
            val topics = (0 until count).map { TopicMetadata.decode(input) }
            return MetadataResponse(topics)
        }
    }
}

data class ProduceResponse(val errorCode: Short, val partition: Int, val offset: Long) {
    fun encode(out: DataOutput) {
        out.writeShort(errorCode.toInt())
        out.writeInt(partition)
        out.writeLong(offset)
    }
    companion object {
        fun decode(input: DataInput): ProduceResponse {
            val errorCode = input.readShort()
            val partition = input.readInt()
            val offset = input.readLong()
            return ProduceResponse(errorCode, partition, offset)
        }
    }
}

data class FetchedRecord(val offset: Long, val timestamp: Long, val key: ByteArray?, val value: ByteArray) {
    fun encode(out: DataOutput) {
        out.writeLong(offset)
        out.writeLong(timestamp)
        out.writeNullableBytesAsInt32(key)
        out.writeInt(value.size)
        out.write(value)
    }
    companion object {
        fun decode(input: DataInput): FetchedRecord {
            val offset = input.readLong()
            val timestamp = input.readLong()
            val key = input.readNullableBytesAsInt32()
            val valueLength = input.readInt()
            val value = ByteArray(valueLength)
            input.readFully(value)
            return FetchedRecord(offset, timestamp, key, value)
        }
    }
}

data class FetchResponse(val errorCode: Short, val records: List<FetchedRecord>) {
    fun encode(out: DataOutput) {
        out.writeShort(errorCode.toInt())
        out.writeInt(records.size)
        records.forEach { it.encode(out) }
    }
    companion object {
        fun decode(input: DataInput): FetchResponse {
            val errorCode = input.readShort()
            val count = input.readInt()
            val records = (0 until count).map { FetchedRecord.decode(input) }
            return FetchResponse(errorCode, records)
        }
    }
}

data class OffsetCommitResponse(val errorCode: Short) {
    fun encode(out: DataOutput) { out.writeShort(errorCode.toInt()) }
    companion object {
        fun decode(input: DataInput): OffsetCommitResponse = OffsetCommitResponse(input.readShort())
    }
}

data class OffsetFetchResponse(val errorCode: Short, val offset: Long) {
    fun encode(out: DataOutput) {
        out.writeShort(errorCode.toInt())
        out.writeLong(offset)
    }
    companion object {
        fun decode(input: DataInput): OffsetFetchResponse {
            val errorCode = input.readShort()
            val offset = input.readLong()
            return OffsetFetchResponse(errorCode, offset)
        }
    }
}
```

Note for later tasks: `ProduceRequest` and `FetchedRecord`/`FetchResponse` contain `ByteArray` fields, so — same as `Record` in Task 3 — never assert whole-object `==` equality on them in tests; compare the non-array fields and use `assertArrayEquals` for the array fields.

- [ ] **Step 7: Run tests to verify they pass**

Run: `./gradlew test --tests "minikafka.proto.FramingTest" --tests "minikafka.proto.ProtocolCodecTest"`
Expected: PASS (7 tests)

- [ ] **Step 8: Commit**

```bash
git add src/main/kotlin/minikafka/proto/Protocol.kt src/main/kotlin/minikafka/proto/Framing.kt \
        src/main/kotlin/minikafka/proto/Requests.kt src/main/kotlin/minikafka/proto/Responses.kt \
        src/test/kotlin/minikafka/proto/FramingTest.kt src/test/kotlin/minikafka/proto/ProtocolCodecTest.kt
git commit -m "Add minikafka wire protocol: framing, API keys, request/response codecs"
```

---

### Task 8: Consumer offset store

**Files:**
- Create: `src/main/kotlin/minikafka/broker/OffsetStore.kt`
- Test: `src/test/kotlin/minikafka/broker/OffsetStoreTest.kt`

**Interfaces:**
- Consumes: `DataOutput.writeNullableString`/`DataInput.readNullableString` from `minikafka.io` (Task 2).
- Produces: `class OffsetStore(file: File)` with `commit(group: String, topic: String, partition: Int, offset: Long)` and `fetch(group: String, topic: String, partition: Int): Long` (returns `-1L` if nothing has been committed). Appends every commit to `file` and replays it on construction to rebuild in-memory state.

- [ ] **Step 1: Write the failing test**

```kotlin
package minikafka.broker

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.io.TempDir
import java.io.File

class OffsetStoreTest {
    @Test
    fun `fetch returns -1 when nothing has been committed`(@TempDir tempDir: File) {
        val store = OffsetStore(File(tempDir, "offsets.log"))
        assertEquals(-1L, store.fetch("g", "t", 0))
    }

    @Test
    fun `commit then fetch returns the committed offset`(@TempDir tempDir: File) {
        val store = OffsetStore(File(tempDir, "offsets.log"))
        store.commit("g", "t", 0, 42L)
        assertEquals(42L, store.fetch("g", "t", 0))
    }

    @Test
    fun `reloads committed offsets from disk`(@TempDir tempDir: File) {
        val file = File(tempDir, "offsets.log")
        val store = OffsetStore(file)
        store.commit("g", "t", 0, 42L)

        val reloaded = OffsetStore(file)
        assertEquals(42L, reloaded.fetch("g", "t", 0))
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew test --tests "minikafka.broker.OffsetStoreTest"`
Expected: compilation failure — `OffsetStore` is unresolved.

- [ ] **Step 3: Write the implementation**

```kotlin
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `./gradlew test --tests "minikafka.broker.OffsetStoreTest"`
Expected: PASS (3 tests)

- [ ] **Step 5: Commit**

```bash
git add src/main/kotlin/minikafka/broker/OffsetStore.kt src/test/kotlin/minikafka/broker/OffsetStoreTest.kt
git commit -m "Add disk-backed consumer offset store"
```

---

### Task 9: Broker

**Files:**
- Create: `src/main/kotlin/minikafka/broker/Broker.kt`
- Test: `src/test/kotlin/minikafka/broker/BrokerTest.kt`

**Interfaces:**
- Consumes: `Log` (Task 6), `OffsetStore` (Task 8), `ErrorCodes`/`TopicMetadata` (Task 7).
- Produces: `class Broker(dataDir: File)` with `createTopic(topic: String, numPartitions: Int): Short`, `listTopics(): List<TopicMetadata>`, `produce(topic: String, key: ByteArray?, value: ByteArray): Triple<Short, Int, Long>` (errorCode, partition, offset), `fetch(topic: String, partition: Int, offset: Long, maxBytes: Int): Pair<Short, List<Record>>`, `commitOffset(group: String, topic: String, partition: Int, offset: Long): Short`, `fetchOffset(group: String, topic: String, partition: Int): Pair<Short, Long>`. Rebuilds its topic/partition registry from `dataDir` on construction.

- [ ] **Step 1: Write the failing test**

```kotlin
package minikafka.broker

import minikafka.proto.ErrorCodes
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File

class BrokerTest {
    @Test
    fun `creates a topic and rejects a duplicate create`(@TempDir tempDir: File) {
        val broker = Broker(tempDir)
        assertEquals(ErrorCodes.NONE, broker.createTopic("t", 3))
        assertEquals(ErrorCodes.TOPIC_ALREADY_EXISTS, broker.createTopic("t", 3))
    }

    @Test
    fun `produce with a key always routes to the same partition`(@TempDir tempDir: File) {
        val broker = Broker(tempDir)
        broker.createTopic("t", 4)
        val key = "user-123".toByteArray()
        val (_, partitionA, _) = broker.produce("t", key, "v1".toByteArray())
        val (_, partitionB, _) = broker.produce("t", key, "v2".toByteArray())
        assertEquals(partitionA, partitionB)
    }

    @Test
    fun `produce without a key round-robins across partitions`(@TempDir tempDir: File) {
        val broker = Broker(tempDir)
        broker.createTopic("t", 3)
        val partitions = (0 until 3).map { broker.produce("t", null, "v".toByteArray()).second }
        assertEquals(setOf(0, 1, 2), partitions.toSet())
    }

    @Test
    fun `fetch returns records produced earlier`(@TempDir tempDir: File) {
        val broker = Broker(tempDir)
        broker.createTopic("t", 1)
        broker.produce("t", null, "a".toByteArray())
        broker.produce("t", null, "b".toByteArray())

        val (errorCode, records) = broker.fetch("t", 0, 0L, 1024)
        assertEquals(ErrorCodes.NONE, errorCode)
        assertEquals(2, records.size)
    }

    @Test
    fun `fetch on an unknown topic returns an error code`(@TempDir tempDir: File) {
        val broker = Broker(tempDir)
        val (errorCode, records) = broker.fetch("missing", 0, 0L, 1024)
        assertEquals(ErrorCodes.UNKNOWN_TOPIC, errorCode)
        assertEquals(0, records.size)
    }

    @Test
    fun `commits and fetches a consumer offset`(@TempDir tempDir: File) {
        val broker = Broker(tempDir)
        broker.createTopic("t", 1)
        broker.commitOffset("g", "t", 0, 5L)
        val (errorCode, offset) = broker.fetchOffset("g", "t", 0)
        assertEquals(ErrorCodes.NONE, errorCode)
        assertEquals(5L, offset)
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew test --tests "minikafka.broker.BrokerTest"`
Expected: compilation failure — `Broker` is unresolved.

- [ ] **Step 3: Write the implementation**

```kotlin
package minikafka.broker

import minikafka.log.Log
import minikafka.log.Record
import minikafka.proto.ErrorCodes
import minikafka.proto.TopicMetadata
import java.io.File
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

class Broker(private val dataDir: File) {
    private data class TopicState(val numPartitions: Int, val logs: List<Log>)

    private val topics = ConcurrentHashMap<String, TopicState>()
    private val roundRobinCounters = ConcurrentHashMap<String, AtomicInteger>()
    private val offsetStore = OffsetStore(File(dataDir, "offsets.log"))

    init {
        dataDir.mkdirs()
        val partitionCounts = mutableMapOf<String, Int>()
        dataDir.listFiles { f -> f.isDirectory }?.forEach { partitionDir ->
            val separatorIndex = partitionDir.name.lastIndexOf('-')
            if (separatorIndex > 0) {
                val topic = partitionDir.name.substring(0, separatorIndex)
                val partition = partitionDir.name.substring(separatorIndex + 1).toIntOrNull()
                if (partition != null) {
                    partitionCounts[topic] = maxOf(partitionCounts.getOrDefault(topic, 0), partition + 1)
                }
            }
        }
        partitionCounts.forEach { (topic, numPartitions) ->
            val logs = (0 until numPartitions).map { p -> Log(File(dataDir, "$topic-$p")) }
            topics[topic] = TopicState(numPartitions, logs)
        }
    }

    fun createTopic(topic: String, numPartitions: Int): Short {
        if (topics.containsKey(topic)) return ErrorCodes.TOPIC_ALREADY_EXISTS
        val logs = (0 until numPartitions).map { p -> Log(File(dataDir, "$topic-$p")) }
        topics[topic] = TopicState(numPartitions, logs)
        return ErrorCodes.NONE
    }

    fun listTopics(): List<TopicMetadata> =
        topics.map { (name, state) -> TopicMetadata(name, state.numPartitions) }

    fun produce(topic: String, key: ByteArray?, value: ByteArray): Triple<Short, Int, Long> {
        val state = topics[topic] ?: return Triple(ErrorCodes.UNKNOWN_TOPIC, -1, -1L)
        val partition = choosePartition(topic, key, state.numPartitions)
        val offset = state.logs[partition].append(System.currentTimeMillis(), key, value)
        return Triple(ErrorCodes.NONE, partition, offset)
    }

    fun fetch(topic: String, partition: Int, offset: Long, maxBytes: Int): Pair<Short, List<Record>> {
        val state = topics[topic] ?: return ErrorCodes.UNKNOWN_TOPIC to emptyList()
        if (partition < 0 || partition >= state.numPartitions) return ErrorCodes.UNKNOWN_PARTITION to emptyList()
        return ErrorCodes.NONE to state.logs[partition].read(offset, maxBytes)
    }

    fun commitOffset(group: String, topic: String, partition: Int, offset: Long): Short {
        offsetStore.commit(group, topic, partition, offset)
        return ErrorCodes.NONE
    }

    fun fetchOffset(group: String, topic: String, partition: Int): Pair<Short, Long> =
        ErrorCodes.NONE to offsetStore.fetch(group, topic, partition)

    private fun choosePartition(topic: String, key: ByteArray?, numPartitions: Int): Int {
        if (key != null) {
            return Math.floorMod(key.contentHashCode(), numPartitions)
        }
        val counter = roundRobinCounters.computeIfAbsent(topic) { AtomicInteger(0) }
        return Math.floorMod(counter.getAndIncrement(), numPartitions)
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `./gradlew test --tests "minikafka.broker.BrokerTest"`
Expected: PASS (5 tests)

- [ ] **Step 5: Commit**

```bash
git add src/main/kotlin/minikafka/broker/Broker.kt src/test/kotlin/minikafka/broker/BrokerTest.kt
git commit -m "Add Broker: topic registry, key/round-robin partitioning, produce/fetch/offset dispatch"
```

---

### Task 10: TCP server

**Files:**
- Create: `src/main/kotlin/minikafka/server/Server.kt`
- Create: `src/main/kotlin/minikafka/server/ConnectionHandler.kt`

**Interfaces:**
- Consumes: `Broker` (Task 9), all `proto` request/response/framing types (Task 7).
- Produces: `class Server(port: Int, dataDir: File)` with `start()`, `stop()`, `port(): Int`. No dedicated unit test here — this task is verified end-to-end by the Task 12 integration test, which is the natural place to exercise real sockets.

- [ ] **Step 1: Write `ConnectionHandler.kt`**

```kotlin
package minikafka.server

import minikafka.broker.Broker
import minikafka.proto.ApiKeys
import minikafka.proto.CreateTopicRequest
import minikafka.proto.CreateTopicResponse
import minikafka.proto.FetchRequest
import minikafka.proto.FetchResponse
import minikafka.proto.FetchedRecord
import minikafka.proto.MetadataRequest
import minikafka.proto.MetadataResponse
import minikafka.proto.OffsetCommitRequest
import minikafka.proto.OffsetCommitResponse
import minikafka.proto.OffsetFetchRequest
import minikafka.proto.OffsetFetchResponse
import minikafka.proto.ProduceRequest
import minikafka.proto.ProduceResponse
import minikafka.proto.readFrameHeader
import minikafka.proto.writeResponseFrame
import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.EOFException
import java.io.IOException
import java.net.Socket

class ConnectionHandler(private val socket: Socket, private val broker: Broker) {
    fun handle() {
        val input = DataInputStream(BufferedInputStream(socket.getInputStream()))
        val output = DataOutputStream(BufferedOutputStream(socket.getOutputStream()))
        try {
            while (true) {
                val (header, body) = readFrameHeader(input)
                when (header.apiKey) {
                    ApiKeys.CREATE_TOPIC -> {
                        val request = CreateTopicRequest.decode(body)
                        val errorCode = broker.createTopic(request.topic, request.numPartitions)
                        writeResponseFrame(output, header.correlationId) { out ->
                            CreateTopicResponse(errorCode).encode(out)
                        }
                    }
                    ApiKeys.METADATA -> {
                        MetadataRequest.decode(body)
                        val topics = broker.listTopics()
                        writeResponseFrame(output, header.correlationId) { out ->
                            MetadataResponse(topics).encode(out)
                        }
                    }
                    ApiKeys.PRODUCE -> {
                        val request = ProduceRequest.decode(body)
                        val (errorCode, partition, offset) = broker.produce(request.topic, request.key, request.value)
                        writeResponseFrame(output, header.correlationId) { out ->
                            ProduceResponse(errorCode, partition, offset).encode(out)
                        }
                    }
                    ApiKeys.FETCH -> {
                        val request = FetchRequest.decode(body)
                        val (errorCode, records) = broker.fetch(request.topic, request.partition, request.offset, request.maxBytes)
                        val fetched = records.map { FetchedRecord(it.offset, it.timestamp, it.key, it.value) }
                        writeResponseFrame(output, header.correlationId) { out ->
                            FetchResponse(errorCode, fetched).encode(out)
                        }
                    }
                    ApiKeys.OFFSET_COMMIT -> {
                        val request = OffsetCommitRequest.decode(body)
                        val errorCode = broker.commitOffset(request.group, request.topic, request.partition, request.offset)
                        writeResponseFrame(output, header.correlationId) { out ->
                            OffsetCommitResponse(errorCode).encode(out)
                        }
                    }
                    ApiKeys.OFFSET_FETCH -> {
                        val request = OffsetFetchRequest.decode(body)
                        val (errorCode, offset) = broker.fetchOffset(request.group, request.topic, request.partition)
                        writeResponseFrame(output, header.correlationId) { out ->
                            OffsetFetchResponse(errorCode, offset).encode(out)
                        }
                    }
                    else -> throw IOException("unknown apiKey: ${header.apiKey}")
                }
            }
        } catch (e: EOFException) {
            // client closed the connection
        } catch (e: IOException) {
            // connection error; drop silently for a mini broker
        } finally {
            socket.close()
        }
    }
}
```

- [ ] **Step 2: Write `Server.kt`**

```kotlin
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
```

- [ ] **Step 3: Verify the project still compiles**

Run: `./gradlew compileKotlin`
Expected: `BUILD SUCCESSFUL`. (Full behavioral verification happens in Task 12's integration test, once `MiniKafkaClient` exists to drive this server.)

- [ ] **Step 4: Commit**

```bash
git add src/main/kotlin/minikafka/server/Server.kt src/main/kotlin/minikafka/server/ConnectionHandler.kt
git commit -m "Add TCP server and per-connection request dispatch"
```

---

### Task 11: Client library

**Files:**
- Create: `src/main/kotlin/minikafka/client/MiniKafkaClient.kt`

**Interfaces:**
- Consumes: all `proto` request/response/framing types (Task 7).
- Produces: `class MiniKafkaClient(host: String, port: Int) : Closeable` with `createTopic(topic: String, numPartitions: Int): Short`, `metadata(): List<TopicMetadata>`, `produce(topic: String, key: ByteArray?, value: ByteArray): ProduceResponse`, `fetch(topic: String, partition: Int, offset: Long, maxBytes: Int = 1024 * 1024): FetchResponse`, `commitOffset(group: String, topic: String, partition: Int, offset: Long): Short`, `fetchOffset(group: String, topic: String, partition: Int): Long`, `close()`.

- [ ] **Step 1: Write the implementation**

```kotlin
package minikafka.client

import minikafka.proto.ApiKeys
import minikafka.proto.CreateTopicRequest
import minikafka.proto.CreateTopicResponse
import minikafka.proto.FetchRequest
import minikafka.proto.FetchResponse
import minikafka.proto.MetadataRequest
import minikafka.proto.MetadataResponse
import minikafka.proto.OffsetCommitRequest
import minikafka.proto.OffsetCommitResponse
import minikafka.proto.OffsetFetchRequest
import minikafka.proto.OffsetFetchResponse
import minikafka.proto.ProduceRequest
import minikafka.proto.ProduceResponse
import minikafka.proto.TopicMetadata
import minikafka.proto.readResponseFrame
import minikafka.proto.writeFrame
import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.Closeable
import java.io.DataInput
import java.io.DataInputStream
import java.io.DataOutput
import java.io.DataOutputStream
import java.net.Socket
import java.util.concurrent.atomic.AtomicInteger

class MiniKafkaClient(host: String, port: Int) : Closeable {
    private val socket = Socket(host, port)
    private val input = DataInputStream(BufferedInputStream(socket.getInputStream()))
    private val output = DataOutputStream(BufferedOutputStream(socket.getOutputStream()))
    private val correlationIds = AtomicInteger(0)

    private fun <T> request(apiKey: Short, encodeBody: (DataOutput) -> Unit, decodeResponse: (DataInput) -> T): T {
        val correlationId = correlationIds.getAndIncrement()
        writeFrame(output, apiKey, correlationId, encodeBody)
        val (responseCorrelationId, body) = readResponseFrame(input)
        check(responseCorrelationId == correlationId) {
            "correlation id mismatch: expected $correlationId, got $responseCorrelationId"
        }
        return decodeResponse(body)
    }

    fun createTopic(topic: String, numPartitions: Int): Short =
        request(ApiKeys.CREATE_TOPIC, { CreateTopicRequest(topic, numPartitions).encode(it) }) {
            CreateTopicResponse.decode(it).errorCode
        }

    fun metadata(): List<TopicMetadata> =
        request(ApiKeys.METADATA, { MetadataRequest().encode(it) }) {
            MetadataResponse.decode(it).topics
        }

    fun produce(topic: String, key: ByteArray?, value: ByteArray): ProduceResponse =
        request(ApiKeys.PRODUCE, { ProduceRequest(topic, key, value).encode(it) }) {
            ProduceResponse.decode(it)
        }

    fun fetch(topic: String, partition: Int, offset: Long, maxBytes: Int = 1024 * 1024): FetchResponse =
        request(ApiKeys.FETCH, { FetchRequest(topic, partition, offset, maxBytes).encode(it) }) {
            FetchResponse.decode(it)
        }

    fun commitOffset(group: String, topic: String, partition: Int, offset: Long): Short =
        request(ApiKeys.OFFSET_COMMIT, { OffsetCommitRequest(group, topic, partition, offset).encode(it) }) {
            OffsetCommitResponse.decode(it).errorCode
        }

    fun fetchOffset(group: String, topic: String, partition: Int): Long =
        request(ApiKeys.OFFSET_FETCH, { OffsetFetchRequest(group, topic, partition).encode(it) }) {
            OffsetFetchResponse.decode(it).offset
        }

    override fun close() {
        socket.close()
    }
}
```

- [ ] **Step 2: Verify the project compiles**

Run: `./gradlew compileKotlin`
Expected: `BUILD SUCCESSFUL`

- [ ] **Step 3: Commit**

```bash
git add src/main/kotlin/minikafka/client/MiniKafkaClient.kt
git commit -m "Add MiniKafkaClient TCP client library"
```

---

### Task 12: End-to-end integration test

**Files:**
- Test: `src/test/kotlin/minikafka/integration/BrokerIntegrationTest.kt`

**Interfaces:**
- Consumes: `Server` (Task 10), `MiniKafkaClient` (Task 11).
- Produces: nothing new — this is the task that proves the whole stack (network, protocol, broker, log, offsets) works together over a real TCP loopback connection.

- [ ] **Step 1: Write the test**

```kotlin
package minikafka.integration

import minikafka.client.MiniKafkaClient
import minikafka.server.Server
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File

class BrokerIntegrationTest {
    private lateinit var server: Server

    @BeforeEach
    fun startServer(@TempDir tempDir: File) {
        server = Server(port = 0, dataDir = tempDir)
        server.start()
    }

    @AfterEach
    fun stopServer() {
        server.stop()
    }

    @Test
    fun `produces and fetches keyed messages end to end`() {
        MiniKafkaClient("localhost", server.port()).use { client ->
            assertEquals(0.toShort(), client.createTopic("orders", 2))

            val response1 = client.produce("orders", "user-1".toByteArray(), "first".toByteArray())
            val response2 = client.produce("orders", "user-1".toByteArray(), "second".toByteArray())
            assertEquals(response1.partition, response2.partition)

            val fetched = client.fetch("orders", response1.partition, 0L, 1024 * 1024)
            assertEquals(2, fetched.records.size)
            assertEquals("first", String(fetched.records[0].value))
            assertEquals("second", String(fetched.records[1].value))

            val topics = client.metadata()
            assertEquals(1, topics.size)
            assertEquals("orders", topics[0].name)
        }
    }

    @Test
    fun `commits and fetches a consumer offset end to end`() {
        MiniKafkaClient("localhost", server.port()).use { client ->
            client.createTopic("orders", 1)
            client.produce("orders", null, "a".toByteArray())
            client.produce("orders", null, "b".toByteArray())

            assertEquals(-1L, client.fetchOffset("group-1", "orders", 0))
            assertEquals(0.toShort(), client.commitOffset("group-1", "orders", 0, 1L))
            assertEquals(1L, client.fetchOffset("group-1", "orders", 0))
        }
    }

    @Test
    fun `returns an error code for an unknown topic`() {
        MiniKafkaClient("localhost", server.port()).use { client ->
            val fetched = client.fetch("missing", 0, 0L, 1024)
            assertNotEquals(0.toShort(), fetched.errorCode)
        }
    }
}
```

- [ ] **Step 2: Run the test**

Run: `./gradlew test --tests "minikafka.integration.BrokerIntegrationTest"`
Expected: PASS (3 tests). If any test hangs, it's almost certainly a socket read blocking because a response was never written — check that every `ApiKeys` branch in `ConnectionHandler` writes exactly one response frame.

- [ ] **Step 3: Run the full test suite**

Run: `./gradlew test`
Expected: PASS, all tests across every task so far.

- [ ] **Step 4: Commit**

```bash
git add src/test/kotlin/minikafka/integration/BrokerIntegrationTest.kt
git commit -m "Add end-to-end integration test driving the broker over a real TCP connection"
```

---

### Task 13: CLI

**Files:**
- Create: `src/main/kotlin/minikafka/cli/Cli.kt`

**Interfaces:**
- Consumes: `Server` (Task 10), `MiniKafkaClient` (Task 11).
- Produces: `fun main(args: Array<String>)` — the `application` plugin's entry point (`minikafka.cli.CliKt`, matching `build.gradle.kts`'s `mainClass`), dispatching `server`, `topics create|list`, `produce`, `consume` subcommands.

- [ ] **Step 1: Write the implementation**

```kotlin
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
```

- [ ] **Step 2: Verify the project builds**

Run: `./gradlew build`
Expected: `BUILD SUCCESSFUL`, all prior tests still pass.

- [ ] **Step 3: Manual smoke test**

Run these in separate terminals (or background the server process):

```bash
./gradlew run --args="server --port 9092 --data-dir ./data" &
sleep 3
./gradlew run --args="topics create --topic demo --partitions 2" -q
./gradlew run --args="produce --topic demo --key user-1 hello" -q
./gradlew run --args="consume --topic demo --partition 0 --from-beginning" -q
kill %1
```

Expected: the `topics create` call prints a confirmation, `produce` prints the assigned partition/offset, and `consume` prints the `hello` record before exiting (fetch loop breaks once it catches up to the log end).

- [ ] **Step 4: Commit**

```bash
git add src/main/kotlin/minikafka/cli/Cli.kt
git commit -m "Add minikafka CLI: server, topics, produce, consume subcommands"
```

---

### Task 14: README

**Files:**
- Create: `README.md`

**Interfaces:**
- Consumes: nothing (documentation only).
- Produces: `README.md` describing what minikafka is, how to build/run it, and the CLI usage — the reference a future reader (or you, in six months) starts from.

- [ ] **Step 1: Write `README.md`**

```markdown
# minikafka

A miniature, single-broker Apache Kafka clone in Kotlin: topics and
partitions, an append-only segmented log with a sparse offset index,
a custom TCP binary wire protocol, and a small CLI — built to be
simple, clean, and working end-to-end.

See `docs/superpowers/specs/2026-09-23-minikafka-design.md` for the
full design (wire protocol, storage format, and what's deliberately
out of scope).

## Build and test

```bash
./gradlew build
```

## Run the broker

```bash
./gradlew run --args="server --port 9092 --data-dir ./data"
```

## CLI

```bash
./gradlew run --args="topics create --topic demo --partitions 2"
./gradlew run --args="topics list"
./gradlew run --args="produce --topic demo --key user-1 hello"
./gradlew run --args="consume --topic demo --partition 0 --from-beginning"
./gradlew run --args="consume --topic demo --partition 0 --group my-group"
```

All commands accept `--host`/`--port` to target a broker other than
`localhost:9092`.
```

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "Add README with build/run/CLI instructions"
```

---

## Final verification

- [ ] **Run the full suite one more time**

Run: `./gradlew build`
Expected: `BUILD SUCCESSFUL`, every test across all 14 tasks passing.
