# Tuning the producer

Producer tuning is a triangle: **durability**, **throughput**, **latency** —
pick the corner that matters and move deliberately toward it. The good news
from [the benchmarks](../benchmarks.md) is that the safe corner is no longer
the slow corner: at the *default* `acks=all` + idempotence config, kacrab
outruns the Java client's own perf tool. You do not need to trade safety for
speed; you tune within safety.

## The defaults are the durability recipe

```text
acks=all                    # leader + in-sync replicas must confirm
enable.idempotence=true     # exactly-once, in-order per partition
retries=<effectively ∞>     # bounded by delivery.timeout.ms instead
```

These are Kafka's defaults and kacrab keeps them. Resist the classic
anti-patterns:

- **Don't set `retries=0` to avoid duplicates.** That trades duplicates for
  *data loss* on any transient error. Idempotence already removes retry
  duplicates — the broker deduplicates by sequence number
  ([how](../producer/idempotency.md)) — so retries are safe.
- **Don't turn off idempotence "for performance".** Its wire cost is a few
  bytes per batch; every benchmark in this book ran with it on. Turning it
  off also forfeits ordering under retry (below).
- **Complete the recipe broker-side** with `min.insync.replicas=2` on
  replication-factor-3 topics; `acks=all` with `min.insync.replicas=1` only
  promises one disk.

> **Ordering depends on idempotence, not just `max.in.flight`**
>
> With `max.in.flight.requests.per.connection > 1` and idempotence *off*, a
> retried batch can land behind a newer one — silent reordering. With
> idempotence *on*, sequence numbers pin the order and up to 5 in-flight
> requests stay strictly ordered per partition. Keep idempotence on and you
> keep both pipeline depth and order; the mechanism is the
> [idempotency chapter](../producer/idempotency.md)'s whole story.

## Chasing throughput

Throughput comes from **batching** — fewer, fuller requests. In descending
order of effect:

1. **`linger.ms`** (default 0): the number-one lever. `5`–`20` ms lets the
   accumulator fill batches under moderate traffic; at very high send rates
   batches fill before the linger expires and the added latency is ~zero.
   The [pipeline chapter](../producer/pipeline.md) shows where the wait sits.
2. **`compression.type`**: `zstd` for the best ratio (needs the `zstd`
   feature and a C toolchain), `lz4` for the cheapest CPU. Compression
   multiplies effective network and *broker disk* throughput — it is often
   the biggest win on 1 KiB+ payloads. Codec levels via
   `compression.{zstd,gzip,lz4}.level`; note `compression.lz4.level` needs
   the `lz4-hc` feature to have any effect
   ([why](../compression.md)).
3. **`batch.size`** (default 16 KiB): raise to 64–256 KiB when records are
   large or throughput is high. A batch is *up to* this size, never a wait
   for it — oversizing wastes only memory, undersizing caps batching. With
   10 KiB records a 16 KiB batch holds one record; per-broker request
   coalescing is what saved that workload
   ([benchmarks](../benchmarks.md)), but a right-sized batch is still
   cheaper.
4. **`buffer.memory`** (default 32 MiB): the total unsent-record budget. Size
   it ≈ `target throughput × worst-case broker stall you want to absorb`.
   When it fills, `send()` blocks up to `max.block.ms` — that back-pressure
   is a feature ([failure modes](../failure-modes.md)); raising the buffer
   only *delays* it.

Two field lessons that outrank any key:

- **Never `flush()` per record.** `flush` waits for everything in flight —
  per-record it serializes the pipeline down to one round-trip at a time.
  Flush at commit points and shutdown (`close` flushes for you).
- **Keep `send()` cheap.** The send path is an in-memory append; anything
  slow *around* it (per-record `env::var` on macOS took a global libc lock
  and cost 28% of throughput in our own harness) dominates long before the
  client does.

## Chasing latency

- **`linger.ms=0`** (default) dispatches as soon as the pipeline has a slot.
- **`max.in.flight.requests.per.connection`**: depth 5 (default) maximizes
  pipeline utilization; on a single low-RTT broker it adds queue latency
  (p99 ~13 ms in our bench vs ~2 ms at depth 1 — same throughput). But depth
  also *absorbs broker pauses*: at depth 5 a GC/fsync stall on one request
  lets four others drain; at depth 1 everything waits (p99.9 ~10 ms vs
  ~100 ms under an injected pause). Rule of thumb: keep 5 unless you've
  measured your own tail on your own RTT.
- `acks=1` buys a little latency at a real durability cost (leader-only
  confirmation). Prefer keeping `acks=all` and spending the latency budget
  on `linger.ms=0` + right-sized batches.

## Large records

`max.request.size` (1 MiB default) caps a single request; the broker's
`message.max.bytes` caps what it accepts — raise both together or the
producer's larger request just gets rejected. Oversized *batches* are split
and requeued automatically on `MESSAGE_TOO_LARGE`; a single unsplittable
record fails its delivery. If a payload doesn't have to be a Kafka record
(files, images), a blob store + reference usually beats raising the caps.

## Keys and partitioning

- **A record key is an ordering contract**: same key → same partition → same
  order, byte-identically between kacrab and Java
  ([partitioning](../producer/partitioning.md)). Choose keys for the
  *ordering you need*, and check cardinality — a hot key is a hot partition
  no client setting can fix.
- **Null keys are fine** — the sticky/adaptive partitioner batches them
  efficiently and steers around slow leaders
  (`partitioner.adaptive.partitioning.enable`, on by default). Don't force
  `RoundRobinPartitioner` for "fairness"; per-record scatter defeats
  batching.
- `partitioner.ignore.keys=true` gives keyed records sticky treatment too —
  only when you *don't* rely on key ordering.

## Transactions

For atomic multi-partition writes (or consume-transform-produce):

- Set **`transactional.id`** — stable per logical producer instance, unique
  per instance. Two producers sharing an id fence each other by design
  (that's the failover mechanism, not a bug).
- Keep transactions **short**; every open transaction holds coordinator
  state and delays `read_committed` consumers.
- The consumer side needs `isolation.level=read_committed`
  ([consumer tuning](./consumer.md)).

## Field notes

| Goal | Change | Leave alone |
|---|---|---|
| Maximum safety | `min.insync.replicas=2` (broker) | `acks`, idempotence, `retries` — already right |
| Throughput | `linger.ms=5–20`, `compression.type=zstd`, `batch.size=64Ki+` | `max.in.flight` |
| Low latency | `linger.ms=0`; measure before touching `max.in.flight` | `acks` |
| Bounded staleness | `delivery.timeout.ms` down to your real budget | `retries` |
| Large records | `max.request.size` **and** broker `message.max.bytes` | — |

And the two habits: flush at boundaries, not per record; state a reason for
every non-default line in the config.
