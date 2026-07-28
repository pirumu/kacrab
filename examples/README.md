# kacrab-examples

Runnable examples for the public `kacrab` API. **Not published.**

## Scope

| Example | Feature | What it shows |
| --- | --- | --- |
| `producer` | `producer` | public `Producer` usage: send, send-with-callback, batching, idempotence, and an optional transactional path |
| `consumer` | `consumer` | classic consumer group: subscribe, poll loop, manual synchronous commit (with the auto-commit alternative noted), graceful close |
| `share_consumer` | `share-consumer` | KIP-932 share group: join, poll acquired records, explicit per-record acknowledgement, delivery counts, close |
| `transactions` | `producer` + `consumer` | exactly-once consume-transform-produce: `init_transactions`, `begin`, send, `send_offsets_to_transaction`, `commit`, and the abort path, verified with a `read_committed` reader |
| `admin` | `admin` | public `AdminClient` usage: describe cluster, create/list/describe topics, describe/incremental-alter configs, add partitions, list offsets, list consumer groups, delete topics |
| `config` | — | Kafka config facade and typed config conversion |
| `typed_serializer` | `producer` | a custom `ProducerSerializer<T>` wired through `build_with_serializers` to send a strongly-typed value |

Every example is gated on the feature it demonstrates, and `default` turns them
all on — so the commands below work as written, and a single surface can also be
built alone:

```bash
cargo build -p kacrab-examples --no-default-features --features consumer --example consumer
```

## Running

All examples except `config` need a local broker, and broker-side topic
auto-creation is off in the fixture, so create the topics first:

```bash
docker compose -f docker-compose.kafka.yml up -d
for topic in kacrab-example kacrab-example-out kacrab-share-example kacrab-orders; do
  docker exec kacrab-kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --create --if-not-exists \
    --topic "$topic" --partitions 1 --replication-factor 1
done
```

Then, from the workspace root:

```bash
cargo run -p kacrab-examples --example config
cargo run -p kacrab-examples --example admin
cargo run -p kacrab-examples --example typed_serializer

# produce, then read the same records back
cargo run -p kacrab-examples --example producer
cargo run -p kacrab-examples --example consumer

# exactly-once consume-transform-produce (needs unread input, so produce first)
cargo run -p kacrab-examples --example producer
cargo run -p kacrab-examples --example transactions
```

Positional arguments (all optional, in order):

| Example | Arguments | Defaults |
| --- | --- | --- |
| `producer` | `bootstrap topic partition messages` | `127.0.0.1:9092 kacrab-example 0 10` |
| `consumer` | `bootstrap topic group messages` | `127.0.0.1:9092 kacrab-example kacrab-example-group 12` |
| `share_consumer` | `bootstrap topic group messages` | `127.0.0.1:9092 kacrab-share-example kacrab-share-example-group 12` |
| `transactions` | `bootstrap input-topic output-topic group messages` | `127.0.0.1:9092 kacrab-example kacrab-example-out kacrab-eos-group 12` |
| `admin` | `bootstrap topic partitions` | `127.0.0.1:9092 kacrab-admin-example 3` |
| `config` | `producer\|consumer\|admin\|all` | `all` |
| `typed_serializer` | `bootstrap topic partition` | `127.0.0.1:9092 kacrab-orders 0` |

```bash
cargo run -p kacrab-examples --example admin -- 127.0.0.1:9092 my-topic 3
```

The `producer` example also reads `KACRAB_TRANSACTIONAL_ID`; setting it runs the
same writes inside a transaction instead of on the plain idempotent path.

### Share consumer

`share_consumer` needs Apache Kafka **4.3+** (the fixture pins 4.3.0). A share
group starts at the *log end*, so set the group's start offset before producing,
or produce while the example is already polling:

```bash
docker exec kacrab-kafka /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server localhost:9092 --alter \
  --entity-type groups --entity-name kacrab-share-example-group \
  --add-config share.auto.offset.reset=earliest
cargo run -p kacrab-examples --example producer -- 127.0.0.1:9092 kacrab-share-example
cargo run -p kacrab-examples --example share_consumer
```

## Author

`kacrab-examples` is authored and maintained by `pirumu`.

## License

This crate is licensed under either MIT or Apache-2.0, matching the workspace.
