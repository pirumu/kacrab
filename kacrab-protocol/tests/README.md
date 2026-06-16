# kacrab-protocol test strategy

This directory tests Kafka wire compatibility at two levels:

1. Fast Rust-only tests that run in normal workspace test loops.
2. Ignored Java oracle tests that compare kacrab bytes against Apache Kafka's
   Java client message implementation.

Kafka protocol code is generated from upstream schemas and supports many API
versions per message. A few hand-written fixtures are useful, but they are not
enough to prove the generator is still producing compatible wire code. The Java
oracle matrix exists to make that proof cheap and repeatable.

## Commands

From the workspace root:

```sh
make test
```

Runs the normal workspace test suite with all Rust features enabled. This
compiles `java_interop.rs`, but the Java oracle tests are marked `#[ignore]`,
so Cargo lists them as ignored instead of running them.

```sh
make test-protocol
```

Runs the Rust protocol crate tests only. This includes primitive/protocol unit
tests and local generated round-trip fixtures. Java oracle tests are still
compiled but ignored.

```sh
make test-protocol-java
```

Compiles the Rust `java_interop` test binary without running ignored tests. Use
this when changing the Rust harness types or generated support modules and you
only need a quick compile check. It does not compile the Java helper because
that happens when the ignored tests execute.

```sh
make test-protocol-java-matrix
```

Runs the ignored Java oracle tests. This requires:

- Java 17+
- Maven
- access to `org.apache.kafka:kafka-clients:4.3.0`, either already in the local
  Maven cache or downloadable by Maven

```sh
make test-protocol-full
```

Runs `make test-protocol` and then `make test-protocol-java-matrix`.

## Why the Java tests are ignored by default

The Java oracle is intentionally not part of `make test` or `make
test-protocol`. It shells out to Maven/Javac, compiles a Java helper, loads the
Kafka Java client jar, and executes hundreds of cross-language cases. That is
the right level of evidence before changing protocol generation, but it is too
slow and environment-dependent for every local edit.

Cargo still compiles `java_interop.rs` during normal test runs. Seeing output
like this is expected:

```text
test rust_preserves_all_java_default_protocol_messages ... ignored, requires Java 17+, Maven, and org.apache.kafka:kafka-clients:4.3.0
```

It means the test binary compiled and Cargo skipped execution because the test
has `#[ignore]`. Run `make test-protocol-java-matrix` to execute those cases.

## Files

- `generated_roundtrip.rs`
  - Fast Rust-only smoke tests for selected generated protocol types.
  - Verifies local encode/decode paths and unknown tagged fields.

- `java_interop.rs`
  - Rust test harness for Java oracle execution.
  - Compiles `tests/java/KafkaProtocolInterop.java` when ignored tests run.
  - Resolves `kafka-clients-4.3.0.jar` through Maven when needed.
  - Defines `MatrixCase` and `TestInstance`, then includes generated test
    support from `support/generated_test_utils.rs`.

- `java/KafkaProtocolInterop.java`
  - Small command-line adapter around Apache Kafka's Java message classes.
  - Supports hand-written smoke fixture commands.
  - Supports reflection-based generic commands:
    - `roundtrip-hex <className> <version> <hex>`
    - `encode-default <className> <version>`

- `support/generated_test_utils.rs`
  - Generated module registry for every Kafka schema.
  - Exposes `protocol_cases() -> Vec<MatrixCase>`.

- `support/generated_test_utils/*.rs`
  - Generated fixture and matrix code per Kafka schema.
  - Implements `TestInstance` for generated Rust message types.
  - Emits Rust encode/re-encode helpers and matrix case entries.

## How the oracle matrix works

The matrix checks both directions.

### Rust encode -> Java decode -> Java encode

For each generated `MatrixCase`:

1. Rust builds a generated message fixture.
2. Rust encodes it with `message.write(&mut out, version)`.
3. Rust sends the hex bytes to the Java helper.
4. Java reflectively constructs the matching Kafka Java message with
   `(Readable, short)` and decodes the bytes.
5. Java asserts the input buffer was fully consumed.
6. Java re-encodes the message with Kafka's own `Message.write(...)`.
7. Rust compares the original hex with Java's output hex byte-for-byte.

This catches Rust encoding bugs and schema-shape mismatches for fields,
nullable values, compact encodings, tagged fields, arrays, nested structs, and
version guards.

### Java encode -> Rust decode -> Rust encode

For each unique `(schema_name, java_class, version)`:

1. Java creates a default instance of the Kafka message class.
2. Java encodes it for the target version.
3. Rust decodes the Java hex with the generated `read(...)` implementation.
4. Rust asserts the input buffer was fully consumed.
5. Rust re-encodes the decoded message.
6. Rust compares Java's original hex with Rust's output hex byte-for-byte.

This catches Rust decoding bugs and default/null handling mismatches.

## Fixture coverage

Generated support currently emits two fixture families:

- `null_optionals`
  - One case for every valid version of every schema.
  - Uses null values where the field is nullable in the schema version being
    tested.
  - Uses non-null defaults where a Rust type is optional only because a
    different historical version allowed null.

- `populated`
  - One case for the latest valid version of each schema.
  - Uses deterministic non-default values and unknown tagged fields where the
    schema supports flexible versions.

At the time of writing this produces:

- 625 `null_optionals` cases
- 190 `populated` cases

Re-count after regenerating with:

```sh
rg -n 'fixture: "null_optionals"' kacrab-protocol/tests/support/generated_test_utils | wc -l
rg -n 'fixture: "populated"' kacrab-protocol/tests/support/generated_test_utils | wc -l
```

## What this guarantees

Passing the Java matrix means:

- Generated Rust encoders produce bytes that Kafka Java can decode for every
  schema version represented in the matrix.
- Generated Rust decoders can consume bytes produced by Kafka Java default
  messages for every schema version represented in the matrix.
- Rust and Java preserve the exact byte sequence after decode/re-encode in the
  tested paths.
- The generator's version handling, nullable handling, compact/flexible version
  handling, tagged field handling, and nested schema traversal agree with
  Kafka Java for the tested fixtures.

This is stronger than testing only Rust round trips. A Rust-only round trip can
pass even if Rust consistently writes the wrong wire shape and then reads its
own wrong shape back. The Java client is treated as the external source of
truth for Kafka's wire contract.

## What this does not guarantee

The matrix is not exhaustive over every possible value. It does not prove:

- every numeric boundary is correct;
- every invalid input is rejected with the ideal error;
- every semantically meaningful Kafka value combination is valid;
- record batch compression behavior is covered by Java message tests;
- broker/client behavior outside message serialization is correct.

Those cases still need focused unit tests, fuzz/property tests, and later
integration tests against real Kafka brokers. The Java oracle matrix proves
cross-language message serialization compatibility for the generated schema
surface covered by its fixtures.

## Updating the generated test support

When changing schema parsing, protocol generation, or fixture generation,
regenerate the test support alongside the runtime generated protocol snapshot.
For test support only:

```sh
tmp=$(mktemp -d)
cargo run -p kacrab-codegen -- protocol \
  --schemas-dir kacrab-codegen/schemas \
  --source-ref apache/kafka@4.3.0 \
  --output-dir "$tmp/generated" \
  --test-utils-dir kacrab-protocol/tests/support/generated_test_utils
```

Then run:

```sh
make fmt
make test-protocol-java
make test-protocol-java-matrix
make test-protocol
make clippy
```

Use `make test-protocol-full` when you want the protocol-local Rust tests and
the Java matrix in one command.
