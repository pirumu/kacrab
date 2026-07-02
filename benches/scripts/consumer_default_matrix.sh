#!/usr/bin/env bash
# Java consumer baseline matching benches/src/bin/consumer_kafka_bench.rs:
# kafka-consumer-perf-test.sh, 5 runs per scenario, a fresh group per run so
# every run reads the topic from the earliest offset. Topics must be prefilled
# (KACRAB_BENCH_PREFILL=1 on the Rust bench, or kafka-producer-perf-test.sh).
set -euo pipefail

BOOTSTRAP="${KACRAB_BOOTSTRAP:-127.0.0.1:9092}"
SMALL_TOPIC="${KACRAB_BENCH_TOPIC:-kacrab-bench}"
LARGE_TOPIC="${KACRAB_BENCH_TOPIC:-kacrab-bench-10k}"
KAFKA_BIN="${KAFKA_BIN:-$HOME/.local/share/kacrab-kafka/current/bin}"
KAFKA_ROOT="${KAFKA_ROOT:-$(cd "$KAFKA_BIN/../.." && pwd)}"
KAFKA_CONSUMER_PERF="${KAFKA_CONSUMER_PERF:-$KAFKA_BIN/kafka-consumer-perf-test.sh}"
RUNS="${KACRAB_BENCH_RUNS:-5}"

if [[ ! -x "$KAFKA_CONSUMER_PERF" ]]; then
  echo "missing kafka-consumer-perf-test.sh at $KAFKA_CONSUMER_PERF" >&2
  exit 1
fi

KAFKA_LIB_DIR="$KAFKA_ROOT/libs"
if [[ ! -d "$KAFKA_LIB_DIR" ]]; then
  KAFKA_LIB_DIR="$(cd "$KAFKA_BIN/.." && pwd)/libs"
fi
if ! ls "$KAFKA_LIB_DIR"/kafka-clients-*.jar >/dev/null 2>&1; then
  echo "missing Kafka client jars under $KAFKA_LIB_DIR" >&2
  exit 1
fi

SNAPSHOT_TMP="$(mktemp -d "${TMPDIR:-/tmp}/kacrab-java-consumer-config.XXXXXX")"
cleanup() {
  rm -rf "$SNAPSHOT_TMP"
}
trap cleanup EXIT

# Print the effective Java consumer config for the same props the perf tool
# sets internally, so Rust and Java runs record comparable snapshots.
cat >"$SNAPSHOT_TMP/ConsumerConfigSnapshot.java" <<'JAVA'
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;

public final class ConsumerConfigSnapshot {
    private static final String[] KEYS = new String[] {
        "bootstrap.servers",
        "client.id",
        "group.id",
        "group.protocol",
        "auto.offset.reset",
        "partition.assignment.strategy",
        "isolation.level",
        "fetch.min.bytes",
        "fetch.max.bytes",
        "fetch.max.wait.ms",
        "max.partition.fetch.bytes",
        "max.poll.records",
        "check.crcs",
        "enable.auto.commit",
        "receive.buffer.bytes"
    };

    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", args[0]);
        props.setProperty("group.id", args[1]);
        props.setProperty("client.id", "perf-consumer-client");
        props.setProperty("receive.buffer.bytes", String.valueOf(2 * 1024 * 1024));
        props.setProperty("max.partition.fetch.bytes", String.valueOf(1024 * 1024));
        props.setProperty("auto.offset.reset", "earliest");
        props.setProperty("check.crcs", "false");
        props.setProperty(
            "key.deserializer",
            "org.apache.kafka.common.serialization.ByteArrayDeserializer"
        );
        props.setProperty(
            "value.deserializer",
            "org.apache.kafka.common.serialization.ByteArrayDeserializer"
        );

        ConsumerConfig config = new ConsumerConfig(props);
        Map<String, ?> values = config.values();
        StringBuilder out = new StringBuilder("effective java consumer config: ");
        for (int i = 0; i < KEYS.length; i++) {
            if (i > 0) {
                out.append(", ");
            }
            String key = KEYS[i];
            out.append(key).append("=").append(values.get(key));
        }
        System.out.println(out);
    }
}
JAVA

javac -cp "$KAFKA_LIB_DIR/*" "$SNAPSHOT_TMP/ConsumerConfigSnapshot.java"

print_config_snapshot() {
  local group_id="$1"
  java -cp "$SNAPSHOT_TMP:$KAFKA_LIB_DIR/*" ConsumerConfigSnapshot "$BOOTSTRAP" "$group_id"
}

# The perf line is the tool's CSV row:
# start.time, end.time, data.consumed.in.MB, MB.sec, data.consumed.in.nMsg,
# nMsg.sec, rebalance.time.ms, fetch.time.ms, fetch.MB.sec, fetch.nMsg.sec
parse_java_perf_line() {
  python3 - "$1" <<'PY'
import sys

line = sys.argv[1]
fields = [field.strip() for field in line.split(",")]
if len(fields) != 10:
    raise SystemExit(f"could not parse consumer perf line: {line}")
# MB.sec, nMsg.sec, fetch.MB.sec, fetch.nMsg.sec, rebalance.time.ms
print(fields[3], fields[5], fields[8], fields[9], fields[6])
PY
}

run_java_scenario() {
  local label="$1"
  local topic="$2"
  local records="$3"
  local pretty="$4"
  local sum_mbps="0"
  local sum_rps="0"
  local sum_fetch_mbps="0"
  local sum_fetch_rps="0"
  local sum_rebalance_ms="0"

  echo
  echo "===== java default: $pretty ====="

  for run in $(seq 1 "$RUNS"); do
    local group_id="perf-consumer-java-$$-${label}-run${run}-${RANDOM}"
    echo "java run $run/$RUNS: topic=$topic, num_records=$records, group=$group_id"
    print_config_snapshot "$group_id"

    local output
    output="$("$KAFKA_CONSUMER_PERF" \
      --bootstrap-server "$BOOTSTRAP" \
      --topic "$topic" \
      --num-records "$records" \
      --group "$group_id" \
      --hide-header 2>&1)"

    local perf_line
    perf_line="$(printf '%s\n' "$output" | grep -E '^[0-9]{4}-[0-9]{2}-[0-9]{2} ' | tail -n 1)"
    if [[ -z "$perf_line" ]]; then
      printf '%s\n' "$output"
      echo "missing Java consumer performance summary for $label run $run" >&2
      exit 1
    fi
    printf '%s\n' "$perf_line"

    local parsed
    parsed="$(parse_java_perf_line "$perf_line")"
    read -r mbps rps fetch_mbps fetch_rps rebalance_ms <<<"$parsed"
    sum_mbps="$(awk -v a="$sum_mbps" -v b="$mbps" 'BEGIN { printf "%.12f", a + b }')"
    sum_rps="$(awk -v a="$sum_rps" -v b="$rps" 'BEGIN { printf "%.12f", a + b }')"
    sum_fetch_mbps="$(awk -v a="$sum_fetch_mbps" -v b="$fetch_mbps" 'BEGIN { printf "%.12f", a + b }')"
    sum_fetch_rps="$(awk -v a="$sum_fetch_rps" -v b="$fetch_rps" 'BEGIN { printf "%.12f", a + b }')"
    sum_rebalance_ms="$(awk -v a="$sum_rebalance_ms" -v b="$rebalance_ms" 'BEGIN { printf "%.12f", a + b }')"
  done

  awk \
    -v pretty="$pretty" \
    -v runs="$RUNS" \
    -v sum_rps="$sum_rps" \
    -v sum_mbps="$sum_mbps" \
    -v sum_fetch_rps="$sum_fetch_rps" \
    -v sum_fetch_mbps="$sum_fetch_mbps" \
    -v sum_rebalance_ms="$sum_rebalance_ms" \
    'BEGIN {
      printf "%s: %.0f records/s, %.3f MB/s, fetch %.0f records/s, %.3f MB/s, rebalance_avg=%.0f ms (average over %d runs)\n",
        pretty, sum_rps / runs, sum_mbps / runs, sum_fetch_rps / runs,
        sum_fetch_mbps / runs, sum_rebalance_ms / runs, runs
    }'
}

# KACRAB_BENCH_MESSAGES bounds the record count, mirroring the Rust harness.
SMALL_RECORDS="${KACRAB_BENCH_MESSAGES:-5000000}"
LARGE_RECORDS="${KACRAB_BENCH_MESSAGES:-100000}"

if [[ -z "${KACRAB_ONLY_10KIB:-}" ]]; then
  run_java_scenario "10b" "$SMALL_TOPIC" "$SMALL_RECORDS" "5,000,000 records x 10 bytes"
fi
if [[ -z "${KACRAB_ONLY_10B:-}" ]]; then
  run_java_scenario "10kib" "$LARGE_TOPIC" "$LARGE_RECORDS" "100,000 records x 10 KiB"
fi
