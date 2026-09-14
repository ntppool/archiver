#!/bin/bash

# Runs the ClickHouse integration tests (build tag "integration") against a
# throwaway server that matches production: same ClickHouse version, a
# Replicated database, and the files in schema/ applied verbatim.
#
# usage: scripts/test-clickhouse-integration.sh [run|start|stop]
#   run    start a fresh server, run the tests, remove the server (default)
#   start  start a fresh server and leave it running
#   stop   remove the server

set -euo pipefail

IMAGE="altinity/clickhouse-server:25.8.16.10001.altinitystable"
CONTAINER="archiver-clickhouse-test"
PORT="19000"
DATABASE="ntppool"
DSN="clickhouse://127.0.0.1:${PORT}/${DATABASE}"

cd "$(dirname "$0")/.."

client() {
    docker exec -i "$CONTAINER" clickhouse-client "$@"
}

stop() {
    docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
}

start() {
    stop
    docker run -d --name "$CONTAINER" \
        -p "127.0.0.1:${PORT}:9000" \
        -e CLICKHOUSE_SKIP_USER_SETUP=1 \
        --ulimit nofile=262144:262144 \
        -v "$PWD/scripts/clickhouse-test/config.d/scores-test.xml:/etc/clickhouse-server/config.d/scores-test.xml:ro" \
        "$IMAGE" >/dev/null

    # Wait for Keeper as well as the server; the Replicated database needs it.
    for i in $(seq 1 60); do
        if client --query "SELECT count() FROM system.zookeeper WHERE path = '/'" >/dev/null 2>&1; then
            break
        fi
        if [ "$i" = 60 ]; then
            echo "ClickHouse did not become ready" >&2
            docker logs --tail 50 "$CONTAINER" >&2
            exit 1
        fi
        sleep 1
    done

    client --query "CREATE DATABASE ${DATABASE} ENGINE = Replicated('/clickhouse/databases/${DATABASE}', '{shard}', '{replica}')"
    client --multiquery <"schema/${DATABASE}/log_scores_shard4.sql"
    client --multiquery <"schema/${DATABASE}/log_scores.sql"
}

case "${1:-run}" in
run)
    start
    trap stop EXIT
    export TEST_CLICKHOUSE_DSN="$DSN"
    go test -tags=integration -count=1 -v -run Integration ./storage/clickhouse/
    ;;
start)
    start
    echo "ClickHouse running; TEST_CLICKHOUSE_DSN=${DSN}"
    ;;
stop)
    stop
    ;;
*)
    echo "usage: $0 [run|start|stop]" >&2
    exit 1
    ;;
esac
