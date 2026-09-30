# minikafka

A miniature Apache Kafka clone in Kotlin: topics and partitions, an append-only segmented log with
a sparse offset index, a custom TCP binary wire protocol, and — as of the 2026-09-28 ADR — a
replicated, multi-broker cluster coordinated by ZooKeeper (controller election, leader/ISR
replication, failover). Built to be simple, clean, and working end-to-end; not a production
broker.

See `PRD.md` for goals/non-goals and `DESIGN.md` for the full design (architecture, ZooKeeper
znode layout, wire protocol, on-disk format, core algorithms, guarantees, known limitations). The
ADR that added clustering is `docs/superpowers/plans/2026-09-28-multi-broker-zookeeper.md`.

## Build and test

```bash
./gradlew build            # build + unit/integration tests + e2e/nemesis tests
./gradlew test              # unit/integration tests only (fast)
./gradlew test -PexcludeTags=zk   # ...and skip tests that need a local ZooKeeper
./gradlew e2eTest           # end-to-end + chaos ("nemesis") tests against an installed dist
./gradlew e2eTest -Dnemesis.seed=<seed>   # replay a specific nemesis run (seed printed on every run)
```

## Quick start: a 3-broker cluster

`scripts/local-cluster.sh` scripts a local cluster: one embedded ZooKeeper (port 2181) plus
brokers 1–3 (ports 9092–9094), data/logs/pids under `./.cluster/` (gitignored). It builds
`build/install/minikafka` via `./gradlew installDist` the first time it's needed.

```bash
./scripts/local-cluster.sh start
```
```
starting zk on port 2181
starting broker 1 on port 9092
starting broker 2 on port 9093
starting broker 3 on port 9094
cluster up. logs under .../.cluster/logs, data under .../.cluster.
zk       running (pid ..., port 2181)
broker 1 running (pid ..., port 9092)
broker 2 running (pid ..., port 9093)
broker 3 running (pid ..., port 9094)
```

Create a fully-replicated topic and produce with `acks=all` (the only configuration where an
`acks=all` write survives *any* single broker failure — see the guarantees table in `DESIGN.md`):

```bash
BIN=build/install/minikafka/bin/minikafka
"$BIN" topics create --topic demo --partitions 2 --replication-factor 3 \
    --bootstrap localhost:9092,localhost:9093,localhost:9094

for i in 1 2 3 4 5; do
  "$BIN" produce --topic demo --key "user-$i" --acks all \
      --bootstrap localhost:9092,localhost:9093,localhost:9094 "message-$i"
done
```

`topics create`/`produce`/`consume` all accept `--bootstrap host:port,host:port,...` (or
`--host`/`--port` for a single broker, default `localhost:9092`) — the client discovers the rest of
the cluster from any one broker's `METADATA` response and routes each request to the partition's
current leader.

## Quick start with Docker

`docker-compose.yml` runs ZooKeeper (the official `zookeeper:3.9` image) and three minikafka
brokers built from the `Dockerfile` (RF 3, `min.insync.replicas` 2). The brokers advertise their
service names (`broker1..3`), so clients run inside the compose network via the `cli` service:

```bash
docker compose up -d --build --wait           # build the image, start zookeeper + 3 brokers, wait until healthy
docker compose run --rm cli topics create --topic orders --partitions 3 --replication-factor 3
docker compose run --rm cli topics describe --topic orders
docker compose run --rm cli produce --topic orders --key order-1 --acks all '{"orderId":1}'
docker compose run --rm cli consume --topic orders --partition 0 --from-beginning
docker compose stop broker1                   # simulate a broker failure, then `topics describe` again
docker compose down -v                        # stop everything and delete all data
```

The `cli` service adds `--bootstrap broker1:9092,broker2:9092,broker3:9092` automatically. The
image build is tuned for small Docker VMs (for example Colima's 2 GiB default): only `broker1`
builds the image, and Gradle runs with bounded heaps and an in-process Kotlin compiler.

## CLI

```bash
minikafka zk [--port <port>] [--data-dir <dir>]
minikafka server --broker-id <id> --zk <host:port[/chroot]> [--port <port>] [--data-dir <dir>]
                 [--advertised-host <host>] [--min-insync <n>]
minikafka topics create --topic <name> --partitions <n> [--replication-factor <n>] [<bootstrap>]
minikafka topics list [<bootstrap>]
minikafka topics describe [--topic <name>] [<bootstrap>]
minikafka produce --topic <name> [--key <key>] [--acks all|1] [<bootstrap>] <value>
minikafka consume --topic <name> --partition <n> [--from-beginning] [--group <group>] [<bootstrap>]
```

`<bootstrap>` is `--bootstrap <host:port,host:port,...>` or `--host <host> --port <port>` (default
`localhost:9092`). `--min-insync` must be set the same on every broker in a cluster (nothing
enforces this — a mismatch is a real footgun).

`consume` stops at the partition's **high watermark** — the offset up to which every in-sync
replica has replicated, i.e. what's safe to consider acknowledged — not the log's raw end, and (as
before) it doesn't tail the log waiting for new messages.

`topics describe` prints the controller, every broker, and per-partition leader/epoch/replicas/ISR,
marking a partition `OFFLINE` (no leader) or `UNDER-REPLICATED` (ISR smaller than the replica set):

```
$ ./scripts/local-cluster.sh describe demo
controller 1	brokers 1@localhost:9092,2@localhost:9093,3@localhost:9094
demo	partition 0	leader 2	epoch 0	replicas 2,3,1	isr 2,3,1
demo	partition 1	leader 3	epoch 0	replicas 3,1,2	isr 3,1,2
```

## Failure demos

`scripts/local-cluster.sh kill <id>` sends `SIGKILL` to one broker (simulating a hard crash, not a
graceful shutdown). Here's the actual transcript of killing partition 0's leader (broker 2) while 5
messages sat in the log across both partitions:

```
$ ./scripts/local-cluster.sh kill 2
SIGKILL broker 2 (pid 86464)

$ ./scripts/local-cluster.sh describe demo    # a few seconds later
controller 1	brokers 1@localhost:9092,3@localhost:9094
demo	partition 0	leader 3	epoch 1	replicas 2,3,1	isr 3,1	UNDER-REPLICATED
demo	partition 1	leader 3	epoch 0	replicas 3,1,2	isr 3,1,2
```

Partition 0's leader failed over from broker 2 to broker 3 (`leader_epoch` bumped 0→1) — no
`--min-insync`/`acks=all` write was lost:

```
$ "$BIN" consume --topic demo --partition 0 --from-beginning --bootstrap localhost:9092,localhost:9093,localhost:9094
offset=0 key=user-1 value=message-1
offset=1 key=user-3 value=message-3
offset=2 key=user-5 value=message-5
```

Killing the **controller** (broker 1) shows the ZooKeeper-session-expiry path: the controller doesn't
move until ZooKeeper's session timeout (a few seconds) expires and deletes the dead broker's
ephemeral `/controller` and `/brokers/ids/1` registrations:

```
$ ./scripts/local-cluster.sh kill 1
$ ./scripts/local-cluster.sh describe demo    # too soon: still shows the dead controller/broker
controller 1	brokers 1@localhost:9092,3@localhost:9094
...
$ ./scripts/local-cluster.sh describe demo    # ~10s later
controller 3	brokers 3@localhost:9094
demo	partition 0	leader 3	epoch 1	replicas 2,3,1	isr 3
demo	partition 1	leader 3	epoch 0	replicas 3,1,2	isr 3
```

Broker 3 (the only survivor) became controller and sole ISR member for both partitions; data was
still fully readable with only one broker up. Restarting the dead brokers rejoins them and heals the
ISR back to all three:

```
$ ./scripts/local-cluster.sh restart 1
$ ./scripts/local-cluster.sh restart 2
$ ./scripts/local-cluster.sh describe demo    # a few seconds later
controller 3	brokers 1@localhost:9092,2@localhost:9093,3@localhost:9094
demo	partition 0	leader 3	epoch 1	replicas 2,3,1	isr 1,2,3
demo	partition 1	leader 3	epoch 0	replicas 3,1,2	isr 1,2,3
```

`scripts/local-cluster.sh stop` shuts everything down gracefully (`SIGTERM`, brokers first so their
shutdown hook drops the ZooKeeper registration and the controller fails them over immediately, then
ZooKeeper last).

## Guarantees (summary — full table and caveats in `DESIGN.md`)

| acks | min.insync.replicas | Survives | Can still lose acked writes when |
|---|---|---|---|
| 1 | any | — | leader dies before any follower fetches the record |
| all | 1 | leader death, if ≥1 follower was in the ISR | ISR shrank to just the leader, then the leader is lost |
| all | 2 (with RF=3) | any single broker failure | ≥2 simultaneous host/power losses (no fsync) |

Unclean leader election is always disabled: if every in-sync replica for a partition dies, that
partition goes offline (`LEADER_NOT_AVAILABLE`) rather than electing a leader that might be missing
acknowledged data.

All commands accept `--host`/`--port` (or `--bootstrap`) to target brokers other than the default
`localhost:9092`.
