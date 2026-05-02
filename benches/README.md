# benches (`kacrab-benches`)

Internal benchmark suite for the kacrab workspace. **Not published.**

> ⚠️ Empty scaffold. Benchmarks land in Phase 8 (Performance) per the
> [workspace ROADMAP](../ROADMAP.md).

## Planned suites

Per ROADMAP §Phase 8:

- **Throughput** — N messages of size S to a topic with P partitions.
  Records msgs/sec and bytes/sec. Compared against `librdkafka` (via
  the `rdkafka` crate) on identical hardware.
- **Latency** — sustained load, captures p50/p99/p999.
- **Memory** — long-running soak, verifies steady-state allocation.
- **Codec micro-benchmarks** — record batch v2 encode/decode, varint
  fast paths, CRC32C throughput.

Throughput target: within 2× `librdkafka`. Realistic per ROADMAP. Hitting
parity is the longer-term goal.

## Running

```bash
cargo bench -p kacrab-benches              # all benches (once wired up)
cargo bench -p kacrab-benches -- throughput
```

## License

MIT — see [LICENSE](../LICENSE).
