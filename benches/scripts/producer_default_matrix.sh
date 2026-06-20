#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP="${KACRAB_BOOTSTRAP:-127.0.0.1:9092}"
TOPIC="${KACRAB_BENCH_TOPIC:-kacrab-bench}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
KAFKA_BIN="${KAFKA_BIN:-$HOME/.local/share/kacrab-kafka/current/bin}"
KAFKA_ROOT="${KAFKA_ROOT:-$(cd "$KAFKA_BIN/../.." && pwd)}"
KAFKA_PRODUCER_PERF="${KAFKA_PRODUCER_PERF:-$KAFKA_BIN/kafka-producer-perf-test.sh}"
RUNS=5

if [[ ! -x "$KAFKA_PRODUCER_PERF" ]]; then
  echo "missing kafka-producer-perf-test.sh at $KAFKA_PRODUCER_PERF" >&2
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

SNAPSHOT_TMP="$(mktemp -d "${TMPDIR:-/tmp}/kacrab-java-config.XXXXXX")"
cleanup() {
  rm -rf "$SNAPSHOT_TMP"
}
trap cleanup EXIT

cat >"$SNAPSHOT_TMP/ProducerConfigSnapshot.java" <<'JAVA'
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.producer.ProducerConfig;

public final class ProducerConfigSnapshot {
    private static final String[] KEYS = new String[] {
        "bootstrap.servers",
        "client.id",
        "acks",
        "enable.idempotence",
        "retries",
        "max.in.flight.requests.per.connection",
        "batch.size",
        "linger.ms",
        "buffer.memory",
        "compression.type",
        "delivery.timeout.ms",
        "request.timeout.ms",
        "max.block.ms",
        "max.request.size",
        "send.buffer.bytes",
        "receive.buffer.bytes",
        "metadata.max.age.ms",
        "partitioner.adaptive.partitioning.enable",
        "partitioner.availability.timeout.ms",
        "enable.metrics.push"
    };

    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", args[0]);
        props.setProperty("client.id", args[1]);
        props.setProperty(
            "key.serializer",
            "org.apache.kafka.common.serialization.ByteArraySerializer"
        );
        props.setProperty(
            "value.serializer",
            "org.apache.kafka.common.serialization.ByteArraySerializer"
        );

        ProducerConfig config = new ProducerConfig(props);
        Map<String, ?> values = config.values();
        StringBuilder out = new StringBuilder("effective java producer config: ");
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

javac -cp "$KAFKA_LIB_DIR/*" "$SNAPSHOT_TMP/ProducerConfigSnapshot.java"

print_config_snapshot() {
  local client_id="$1"
  java -cp "$SNAPSHOT_TMP:$KAFKA_LIB_DIR/*" ProducerConfigSnapshot "$BOOTSTRAP" "$client_id"
}

parse_java_perf_line() {
  python3 - "$1" <<'PY'
import re
import sys

line = sys.argv[1]
match = re.search(
    r"records sent,\s+([0-9.]+)\s+records/sec\s+\(([0-9.]+)\s+MB/sec\)",
    line,
)
if not match:
    raise SystemExit(f"could not parse producer perf line: {line}")
print(match.group(1), match.group(2))
PY
}

format_java_counter_line() {
  python3 "$SCRIPT_DIR/producer_counter_metrics.py" format "$1"
}

format_java_average_counter_line() {
  python3 "$SCRIPT_DIR/producer_counter_metrics.py" average "$@"
}

run_java_scenario() {
  local label="$1"
  local records="$2"
  local size="$3"
  local pretty="$4"
  local sum_rps="0"
  local sum_mbps="0"
  local counter_files=()

  echo
  echo "===== java default: $pretty ====="

  for run in $(seq 1 "$RUNS"); do
    local client_id="java-default-${label}-run${run}"
    echo "java run $run/$RUNS: records=$records, record_size=$size, throughput=-1, client.id=$client_id"
    print_config_snapshot "$client_id"

    local output
    output="$("$KAFKA_PRODUCER_PERF" \
      --bootstrap-server "$BOOTSTRAP" \
      --topic "$TOPIC" \
      --num-records "$records" \
      --record-size "$size" \
      --throughput -1 \
      --command-property "client.id=$client_id" \
      --print-metrics 2>&1)"

    local perf_line
    perf_line="$(printf '%s\n' "$output" | grep 'records sent,' | tail -n 1)"
    if [[ -z "$perf_line" ]]; then
      printf '%s\n' "$output"
      echo "missing Java producer performance summary for $label run $run" >&2
      exit 1
    fi
    printf '%s\n' "$perf_line"

    local output_file="$SNAPSHOT_TMP/java-${label}-run${run}.log"
    printf '%s\n' "$output" >"$output_file"
    counter_files+=("$output_file")
    format_java_counter_line "$output_file"

    local parsed
    parsed="$(parse_java_perf_line "$perf_line")"
    local rps="${parsed%% *}"
    local mbps="${parsed##* }"
    sum_rps="$(awk -v a="$sum_rps" -v b="$rps" 'BEGIN { printf "%.12f", a + b }')"
    sum_mbps="$(awk -v a="$sum_mbps" -v b="$mbps" 'BEGIN { printf "%.12f", a + b }')"
  done

  awk \
    -v pretty="$pretty" \
    -v runs="$RUNS" \
    -v sum_rps="$sum_rps" \
    -v sum_mbps="$sum_mbps" \
    'BEGIN {
      printf "%s: %.0f messages/s, %.3f MB/s (average over %d runs)\n",
        pretty, sum_rps / runs, sum_mbps / runs, runs
    }'
  format_java_average_counter_line "${counter_files[@]}"
}

run_java_scenario "10b" 5000000 10 "5,000,000 messages x 10 bytes"
run_java_scenario "10kib" 100000 10240 "100,000 messages x 10 KiB"
