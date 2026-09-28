#!/usr/bin/env bash
# Scripted local minikafka cluster: one embedded ZooKeeper + 3 brokers.
#
# Usage:
#   scripts/local-cluster.sh start                # build (if needed), start zk + brokers 1-3
#   scripts/local-cluster.sh stop                 # graceful shutdown, all processes
#   scripts/local-cluster.sh status               # what's running, and on which port
#   scripts/local-cluster.sh kill <id>             # SIGKILL one broker (simulate a crash)
#   scripts/local-cluster.sh restart <id>          # SIGKILL (if running) then start that broker fresh
#   scripts/local-cluster.sh describe [topic]      # `topics describe`, bootstrapping off any live broker
#
# Data/logs/pids live under ./.cluster/ (gitignored). Ports: zk=2181, brokers=9092..9094 (ids 1..3).

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="$ROOT/build/install/minikafka/bin/minikafka"
CLUSTER_DIR="$ROOT/.cluster"
PID_DIR="$CLUSTER_DIR/pids"
LOG_DIR="$CLUSTER_DIR/logs"

ZK_PORT=2181
BROKER_IDS=(1 2 3)
BROKER_PORTS=(9092 9093 9094)
BOOTSTRAP="localhost:9092,localhost:9093,localhost:9094"
MIN_INSYNC=2

broker_port() {
    local id=$1
    for i in "${!BROKER_IDS[@]}"; do
        if [ "${BROKER_IDS[$i]}" = "$id" ]; then
            echo "${BROKER_PORTS[$i]}"
            return 0
        fi
    done
    echo "unknown broker id: $id" >&2
    exit 1
}

pid_file() { echo "$PID_DIR/$1.pid"; }

is_running() {
    local name=$1
    local f pid
    f=$(pid_file "$name")
    [ -f "$f" ] || return 1
    pid=$(cat "$f")
    # Confirm the pid is still alive AND still a minikafka process before we trust it — a stale
    # pid file (process died, pid later reused by something unrelated) must never cause us to
    # signal a stranger process.
    if kill -0 "$pid" 2>/dev/null && ps -p "$pid" -o command= 2>/dev/null | grep -q minikafka; then
        return 0
    fi
    rm -f "$f"
    return 1
}

pid_of() { cat "$(pid_file "$1")" 2>/dev/null || true; }

wait_for_port() {
    local port=$1
    local tries=0
    until (exec 3<>"/dev/tcp/localhost/$port") 2>/dev/null; do
        exec 3>&- 2>/dev/null || true
        tries=$((tries + 1))
        if [ "$tries" -ge 100 ]; then
            echo "timed out waiting for port $port" >&2
            return 1
        fi
        sleep 0.2
    done
    exec 3>&- 2>/dev/null || true
}

ensure_dist() {
    if [ ! -x "$BIN" ]; then
        echo "no installed distribution found at $BIN — running ./gradlew installDist"
        (cd "$ROOT" && ./gradlew installDist)
    fi
}

start_zk() {
    mkdir -p "$CLUSTER_DIR/zk-data" "$PID_DIR" "$LOG_DIR"
    if is_running zk; then
        echo "zk already running (pid $(pid_of zk))"
        return 0
    fi
    echo "starting zk on port $ZK_PORT"
    nohup "$BIN" zk --port "$ZK_PORT" --data-dir "$CLUSTER_DIR/zk-data" \
        > "$LOG_DIR/zk.log" 2>&1 &
    echo $! > "$(pid_file zk)"
    wait_for_port "$ZK_PORT"
}

start_broker() {
    local id=$1
    local port
    port=$(broker_port "$id")
    if is_running "b$id"; then
        echo "broker $id already running (pid $(pid_of "b$id"))"
        return 0
    fi
    mkdir -p "$CLUSTER_DIR/data$id" "$PID_DIR" "$LOG_DIR"
    echo "starting broker $id on port $port"
    nohup "$BIN" server --broker-id "$id" --zk "localhost:$ZK_PORT" --port "$port" \
        --data-dir "$CLUSTER_DIR/data$id" --min-insync "$MIN_INSYNC" \
        > "$LOG_DIR/b$id.log" 2>&1 &
    echo $! > "$(pid_file "b$id")"
    wait_for_port "$port"
}

cmd_start() {
    ensure_dist
    start_zk
    for id in "${BROKER_IDS[@]}"; do
        start_broker "$id"
    done
    echo "cluster up. logs under $LOG_DIR, data under $CLUSTER_DIR."
    cmd_status
}

stop_one() {
    local name=$1
    if is_running "$name"; then
        local pid
        pid=$(pid_of "$name")
        echo "stopping $name (pid $pid, SIGTERM)"
        kill "$pid" 2>/dev/null || true
        local tries=0
        while kill -0 "$pid" 2>/dev/null; do
            tries=$((tries + 1))
            if [ "$tries" -ge 100 ]; then
                echo "$name did not stop in time, sending SIGKILL"
                kill -9 "$pid" 2>/dev/null || true
                break
            fi
            sleep 0.1
        done
    fi
    rm -f "$(pid_file "$name")"
}

cmd_stop() {
    # Brokers first (graceful: shutdown hook drops the ZK registration, so the controller fails
    # them over immediately), then zk last.
    for id in "${BROKER_IDS[@]}"; do
        stop_one "b$id"
    done
    stop_one zk
    echo "cluster stopped."
}

cmd_status() {
    if is_running zk; then
        echo "zk       running (pid $(pid_of zk), port $ZK_PORT)"
    else
        echo "zk       stopped"
    fi
    for id in "${BROKER_IDS[@]}"; do
        local port
        port=$(broker_port "$id")
        if is_running "b$id"; then
            echo "broker $id running (pid $(pid_of "b$id"), port $port)"
        else
            echo "broker $id stopped"
        fi
    done
}

cmd_kill() {
    local id=${1:?"usage: $0 kill <broker-id>"}
    broker_port "$id" > /dev/null # validates id
    if ! is_running "b$id"; then
        echo "broker $id is not running"
        return 0
    fi
    local pid
    pid=$(pid_of "b$id")
    echo "SIGKILL broker $id (pid $pid)"
    kill -9 "$pid" 2>/dev/null || true
    rm -f "$(pid_file "b$id")"
}

cmd_restart() {
    local id=${1:?"usage: $0 restart <broker-id>"}
    broker_port "$id" > /dev/null # validates id
    if is_running "b$id"; then
        cmd_kill "$id"
    fi
    ensure_dist
    start_broker "$id"
}

cmd_describe() {
    local topic=${1:-}
    ensure_dist
    if [ -n "$topic" ]; then
        "$BIN" topics describe --topic "$topic" --bootstrap "$BOOTSTRAP"
    else
        "$BIN" topics describe --bootstrap "$BOOTSTRAP"
    fi
}

case "${1:-}" in
    start) cmd_start ;;
    stop) cmd_stop ;;
    status) cmd_status ;;
    kill) cmd_kill "${2:-}" ;;
    restart) cmd_restart "${2:-}" ;;
    describe) cmd_describe "${2:-}" ;;
    *)
        echo "usage: $0 start|stop|status|kill <id>|restart <id>|describe [topic]" >&2
        exit 1
        ;;
esac
