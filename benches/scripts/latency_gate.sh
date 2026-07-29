#!/bin/sh
# latency_gate.sh — extract p50/p99 produce latency from producer_kafka_bench
# logs and compare the medians against benches/latency-thresholds.toml
# (issue #52 B4).
#
# Usage:
#   benches/scripts/latency_gate.sh <bench-log> [<bench-log> ...]
#
# Each log is the stdout of one `producer_kafka_bench` invocation. The script
# collects every per-run summary line — the only line carrying percentiles,
# e.g.:
#
#   200000 records sent, 3225806.451613 records/sec (30.76 MB/sec), 15.49 ms \
#   avg latency, 23.00 ms max latency, 16 ms 50th, 21 ms 95th, 22 ms 99th, \
#   23 ms 99.9th. (produce_requests=77, ...)
#
# (interim window lines share the prefix but have no percentile fields, so
# matching on " ms 50th," selects summaries only), takes the median of each
# metric across all summaries, and exits non-zero when a median exceeds its
# threshold. The min-max spread across runs is printed as the A/A noise
# indicator; a spread comparable to the threshold margin means the threshold
# cannot distinguish signal from noise (see "A/B Discipline" in
# benches/README.md).
#
# Environment:
#   KACRAB_LATENCY_THRESHOLDS — thresholds file (default
#                               benches/latency-thresholds.toml relative to
#                               the repo root this script lives in)

set -eu

if [ $# -lt 1 ]; then
    echo "usage: $0 <bench-log> [<bench-log> ...]" >&2
    exit 2
fi

script_dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
thresholds=${KACRAB_LATENCY_THRESHOLDS:-"${script_dir}/../latency-thresholds.toml"}

if [ ! -f "$thresholds" ]; then
    echo "latency-gate: thresholds file not found: $thresholds" >&2
    exit 2
fi

# The thresholds file is a deliberately flat single-table TOML; each key is
# unique in the file, so a line-anchored sed extraction is exact.
toml_int() {
    val=$(sed -n "s/^${1} *= *\([0-9][0-9]*\).*/\1/p" "$thresholds")
    if [ -z "$val" ]; then
        echo "latency-gate: key '${1}' missing from $thresholds" >&2
        exit 2
    fi
    printf '%s\n' "$val"
}

p50_max=$(toml_int p50_ms_max)
p99_max=$(toml_int p99_ms_max)

# Collect one "<p50> <p99>" pair per summary line across all logs.
pairs=$(sed -n \
    's/.*, \([0-9][0-9]*\) ms 50th, [0-9][0-9]* ms 95th, \([0-9][0-9]*\) ms 99th,.*/\1 \2/p' \
    "$@")

if [ -z "$pairs" ]; then
    echo "latency-gate: no bench summary lines (\"... ms 50th, ...\") found in: $*" >&2
    echo "latency-gate: the bench output format may have changed — fix the sed pattern above" >&2
    exit 2
fi

runs=$(printf '%s\n' "$pairs" | wc -l | tr -d ' ')

median_of() {
    printf '%s\n' "$pairs" | awk -v col="$1" '{print $col}' | sort -n |
        awk '{ v[NR] = $1 } END { print v[int((NR + 1) / 2)] }'
}

spread_of() {
    printf '%s\n' "$pairs" | awk -v col="$1" '
        NR == 1 { min = $col; max = $col }
        { if ($col < min) min = $col; if ($col > max) max = $col }
        END { printf "%d-%d", min, max }'
}

p50=$(median_of 1)
p99=$(median_of 2)

report="latency-gate: ${runs} run(s) | p50 median=${p50}ms (spread $(spread_of 1)ms, max ${p50_max}ms) | p99 median=${p99}ms (spread $(spread_of 2)ms, max ${p99_max}ms)"
echo "$report"
if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
    echo "$report" >>"$GITHUB_STEP_SUMMARY"
fi

status=0
if [ "$p50" -gt "$p50_max" ]; then
    echo "latency-gate: FAIL p50 ${p50}ms > ${p50_max}ms" >&2
    status=1
fi
if [ "$p99" -gt "$p99_max" ]; then
    echo "latency-gate: FAIL p99 ${p99}ms > ${p99_max}ms" >&2
    status=1
fi

if [ "$status" -eq 0 ]; then
    echo "latency-gate: PASS"
fi
exit "$status"
