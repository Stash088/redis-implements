#!/bin/sh
# Compares our server with a real Redis, command by command.
#
# It starts both servers on free ports, runs the scenarios of testdata/compat
# against them and reports the differences. The exit code is non-zero when a
# scenario differs.
#
# Usage: scripts/compat-check.sh [port-ours] [port-redis]

set -e

OURS_PORT=${1:-7799}
REDIS_PORT=${2:-7798}
DIR=$(mktemp -d)
trap 'rm -rf "$DIR"' EXIT

if ! command -v redis-server >/dev/null 2>&1; then
	echo "compat-check: redis-server is not installed" >&2
	exit 2
fi

echo "starting our server on $OURS_PORT and redis-server on $REDIS_PORT"
go build -o "$DIR/redis-implements" ./cmd/redis-server
"$DIR/redis-implements" --port "$OURS_PORT" --dir "$DIR" >"$DIR/ours.log" 2>&1 &
OURS_PID=$!
redis-server --port "$REDIS_PORT" --dir "$DIR" --save '' --daemonize yes
sleep 1

cleanup() {
	kill "$OURS_PID" 2>/dev/null || true
	redis-cli -p "$REDIS_PORT" shutdown nosave >/dev/null 2>&1 || true
}
trap 'cleanup; rm -rf "$DIR"' EXIT

go run ./cmd/compat-check --ours "127.0.0.1:$OURS_PORT" --redis "127.0.0.1:$REDIS_PORT" \
	--scenarios testdata/compat "$@"
