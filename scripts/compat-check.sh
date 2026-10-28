#!/bin/sh
# Compares our server with a real Redis, command by command.
#
# It starts both servers on free ports, runs the scenarios of testdata/compat
# against them and reports the differences. The exit code is non-zero when a
# scenario differs.
#
# Both models of the input and output are checked: the goroutine model (a
# goroutine per connection) and the event loop (--io-mode=loop). Each of them
# is compared with the real Redis, so the models cannot drift apart.
#
# Usage: scripts/compat-check.sh [port-ours] [port-redis]

set -e

OURS_PORT=${1:-7799}
REDIS_PORT=${2:-7798}
LOOP_PORT=${3:-7797}
DIR=$(mktemp -d)
trap 'rm -rf "$DIR"' EXIT

if ! command -v redis-server >/dev/null 2>&1; then
	echo "compat-check: redis-server is not installed" >&2
	exit 2
fi

echo "starting our server on $OURS_PORT, the event loop on $LOOP_PORT and redis-server on $REDIS_PORT"
go build -o "$DIR/redis-implements" ./cmd/redis-server
"$DIR/redis-implements" --port "$OURS_PORT" --dir "$DIR" --io-mode goroutine >"$DIR/ours.log" 2>&1 &
OURS_PID=$!
"$DIR/redis-implements" --port "$LOOP_PORT" --dir "$DIR" --io-mode loop >"$DIR/loop.log" 2>&1 &
LOOP_PID=$!
redis-server --port "$REDIS_PORT" --dir "$DIR" --save '' --daemonize yes
sleep 1

cleanup() {
	kill "$OURS_PID" "$LOOP_PID" 2>/dev/null || true
	redis-cli -p "$REDIS_PORT" shutdown nosave >/dev/null 2>&1 || true
}
trap 'cleanup; rm -rf "$DIR"' EXIT

status=0

echo
echo "=== the goroutine model (--io-mode=goroutine) ==="
go run ./cmd/compat-check --ours "127.0.0.1:$OURS_PORT" --redis "127.0.0.1:$REDIS_PORT" \
	--scenarios testdata/compat "$@" || status=1

echo
echo "=== the event loop (--io-mode=loop) ==="
go run ./cmd/compat-check --ours "127.0.0.1:$LOOP_PORT" --redis "127.0.0.1:$REDIS_PORT" \
	--scenarios testdata/compat "$@" || status=1

exit $status
