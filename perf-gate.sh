#!/usr/bin/env bash
# PHILOSOPHY §0 / §9.6.5 performance floor gate.
#
# Runs zerobench and wrk head-to-head against the bundled stub server
# for a fixed configuration; asserts the ratio zerobench/wrk ≥ floor.
# Exits non-zero on regression. Intended for CI.
#
# Env overrides:
#   DURATION   — bench duration (default: 10s)
#   CONNS      — connections (default: 50)
#   THREADS    — client threads (default: 1)
#   RUNS       — repetitions (default: 3, median used)
#   FLOOR      — required zerobench/wrk ratio (default: 1.20)
#   STUB_PORT  — stub bind port (default: 18080)

set -euo pipefail

DURATION="${DURATION:-10s}"
CONNS="${CONNS:-50}"
THREADS="${THREADS:-1}"
RUNS="${RUNS:-3}"
FLOOR="${FLOOR:-1.20}"
STUB_PORT="${STUB_PORT:-18080}"
URL="http://127.0.0.1:${STUB_PORT}/"

STUB=./target/release/zerobench-stub
BENCH=./target/release/zerobench

if ! command -v wrk >/dev/null 2>&1; then
    echo "wrk not found in PATH — install wrk (apt/brew) to run the gate" >&2
    exit 2
fi

if [[ ! -x "$STUB" || ! -x "$BENCH" ]]; then
    echo "Building release binaries..." >&2
    cargo build --release --features mio-h1,mio-h2,sse,ws,tui >&2
    cargo build --release -p zerobench-stub >&2
fi

echo "[gate] starting stub on :${STUB_PORT}"
"$STUB" --port "$STUB_PORT" --workers 1 &
STUB_PID=$!
cleanup() { kill "$STUB_PID" 2>/dev/null || true; wait "$STUB_PID" 2>/dev/null || true; }
trap cleanup EXIT
sleep 0.5

# Collect RUNS samples from each tool; take the median per tool.
median() {
    local -a arr=("$@")
    IFS=$'\n' sorted=($(sort -n <<<"${arr[*]}"))
    unset IFS
    echo "${sorted[$((${#sorted[@]} / 2))]}"
}

ZB_SAMPLES=()
WRK_SAMPLES=()

for i in $(seq 1 "$RUNS"); do
    # Extract the throughput from zerobench's terminal output. We
    # accept either "req/s" (HTTP) or "ops/s" (mixed-protocol); the
    # number preceding that token is the achieved rate. Commas in
    # the formatted number are stripped.
    sample=$("$BENCH" "$URL" -d "$DURATION" -c "$CONNS" -t "$THREADS" \
        --no-calibrate --no-archive 2>&1 | \
        awk '/(req|ops)\/s/ {
            for (j=1; j<=NF; j++) if ($j ~ /^(req|ops)\/s$/) {
                gsub(/[,]/, "", $(j-1));
                # Guard: only print if the previous field is numeric.
                if ($(j-1) ~ /^[0-9]+(\.[0-9]+)?$/) {
                    print $(j-1);
                    exit;
                }
            }
        }')
    ZB_SAMPLES+=("${sample:-0}")
    echo "[gate] zerobench run $i: ${sample:-0} ops/s"
done

for i in $(seq 1 "$RUNS"); do
    sample=$(wrk -t "$THREADS" -c "$CONNS" -d "$DURATION" "$URL" 2>&1 \
        | awk '/Requests\/sec:/ {print $2}')
    WRK_SAMPLES+=("${sample:-0}")
    echo "[gate] wrk       run $i: ${sample:-0} req/s"
done

ZB_MED=$(median "${ZB_SAMPLES[@]}")
WRK_MED=$(median "${WRK_SAMPLES[@]}")
echo "[gate] zerobench median: $ZB_MED req/s"
echo "[gate] wrk       median: $WRK_MED req/s"

RATIO=$(awk -v z="$ZB_MED" -v w="$WRK_MED" 'BEGIN { printf "%.3f", z / w }')
echo "[gate] ratio: ${RATIO}× (floor ${FLOOR}×)"

if awk -v r="$RATIO" -v f="$FLOOR" 'BEGIN { exit (r >= f) ? 0 : 1 }'; then
    echo "[gate] PASS — zerobench is ${RATIO}× wrk (≥ ${FLOOR}×)"
    exit 0
else
    echo "[gate] FAIL — zerobench is only ${RATIO}× wrk (floor ${FLOOR}×)" >&2
    exit 1
fi
