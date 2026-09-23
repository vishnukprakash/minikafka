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

Note: `consume` fetches until it catches up to the log's current end offset
and then exits — it does not "tail" the log waiting for new messages to
arrive, unlike a typical `kafka-console-consumer`.

All commands accept `--host`/`--port` to target a broker other than
`localhost:9092`.
