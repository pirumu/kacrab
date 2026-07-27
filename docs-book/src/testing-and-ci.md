# Testing, coverage & CI

An expedition's claims are worth what its instruments can prove. Everything
asserted in Parts I–V rests on three layers of evidence, each catching what
the layer below cannot:

| Layer | What it proves | Where it runs |
|---|---|---|
| In-process tests | kacrab is internally consistent: state machines, framing, routing, backoff | every push / PR (uninstrumented) |
| Java oracle fixtures | kacrab is *byte-for-byte* compatible with Apache Kafka's own algorithms | same suite (committed fixtures) |
| Real-broker verification | kacrab is *Kafka*-consistent: it talks to real brokers, not just to itself | on demand (`#[ignore]` + docker) |

The first two are the bulk of the suite and gate every change. The third —
SASL/TLS, compression on disk, multi-broker failover — is covered in
[Verification against real brokers](./verification.md).

## The Java oracle

A Rust-only round trip is a closed loop: kacrab encodes and kacrab decodes, so a
subtly wrong CRC, varint, or murmur2 seed passes anyway. The oracle breaks the
loop by pinning kacrab's output to values produced by Kafka's *own* Java code:
murmur2 hashes for every key length, CRC32C frames, zig-zag varints, the sticky
partitioner's distribution. These are committed fixtures, so the parity check
runs with no Java toolchain in CI. See
[Design decisions & Java parity](./design-decisions.md).

## Fuzzing

The Java oracle proves the decoders are correct on *well-formed* input. The
decoders that parse untrusted broker bytes are separately fuzzed for the other
half — garbage, truncation, hostile length prefixes — because
`forbid(unsafe_code)` rules out memory corruption but not a panic, an unbounded
allocation, or a non-terminating loop, and a panic on a client's decode path is
a denial of service. Eleven [`cargo-fuzz`][cargo-fuzz] targets cover every
parser that reads untrusted bytes, from the socket inward: the length-prefixed
frame, record-batch decoding, the generated response structs, consumer-group
member metadata, every compression codec, the SASL handshake, and the
OAUTHBEARER token endpoint:

```bash
cargo +nightly fuzz run record_batch_framed \
  fuzz/corpus/record_batch_framed fuzz/seeds/record_batch_framed -- \
  -dict=fuzz/kafka.dict -max_total_time=60
```

**The seeds and the dictionary do more work than the runtime does.** The Kafka
wire format is length-prefixed and version-gated, so an unseeded run spends its
budget rediscovering framing instead of exercising decoders. The committed seed
corpus in `fuzz/seeds/` is generated from the *same fixtures the Java oracle
uses* — every generated message, at every schema version, across six fixture
shapes — then minimised with `cargo fuzz cmin` to the subset that carries the
coverage. Regenerate it after adding a schema version:

```bash
cargo test -p kacrab-protocol --test java_interop -- \
  --ignored --nocapture generate_fuzz_corpus
```

`fuzz/kafka.dict` supplies the tokens random mutation will never find, above
all Kafka's `-1` length prefix: null is encoded as a negative length, so
without that token the fuzzer only ever explores the non-null side of every
nullable field. Measured effect, edges covered:

| target | unseeded | seeded + dictionary |
| --- | ---: | ---: |
| `record_batch_decode` | 150 | **984** |
| `record_batch_framed` | 774 | **1591** |
| `response_decode` | — | **14740** |
| `decompress` | — | **1230** |
| `frame_decode` | — | **79** |
| `consumer_protocol_metadata` | — | **386** |
| `oauth_http_response` | — | **1013** |
| `scram_server_first` | — | **245** |
| `scram_server_first_nonced` | 199 | **743** |
| `scram_server_final` | — | **322** |
| `jaas_option` | — | **137** |

The `—` cells are not omitted results: an unseeded baseline is only meaningful
for a target whose framing a fuzzer can reach unaided. The other eight sit
behind a length prefix, an API-version gate, or a text grammar, so an unseeded
run stalls in front of the decoder and its edge count measures the gate rather
than the parser. The three targets carrying both columns are the ones where the
comparison says something — and they are why the seed corpus exists.

`consumer_protocol_metadata` sits at a trust boundary none of the others do.
Every other parser here reads bytes from the broker or the operator; this one
reads bytes *another consumer wrote*. `ConsumerProtocolSubscription` is decoded
by the group leader for every member, and `ConsumerProtocolAssignment` by every
follower and by any admin client describing the group — so anyone authorised to
join a group can feed the decoder of every other member. `response_decode` does
not reach it: these travel as opaque `Bytes` inside `JoinGroupResponse` and
`SyncGroupResponse`, with their own version prefix.

Three libFuzzer flags are set deliberately rather than left to default, and the
reasons are worth repeating because two of them bit this suite:

- **`-max_len`** does *not* default to 4096 when a corpus is supplied — it
  becomes the size of the largest file in it. The committed seeds are small, so
  leaving it unset had silently capped `frame_decode` at **12 bytes**, making
  the seeded target worse than an unseeded one.
- **`-malloc_limit_mb`** bounds a single allocation and defaults to the rss
  limit. At 4096 it could not see the ~120 MB preallocation a 49-byte record
  batch can request. It is 64 MiB for every target that does not decompress,
  and above `MAX_DECOMPRESSED_LEN` for those that do, since a 1 GiB expansion
  there is by design.
- **`-timeout`** catches CPU amplification, which surfaces as a hang, not a
  crash. Tuned per target: the SCRAM targets legitimately run PBKDF2 up to
  `MAX_SCRAM_ITERATIONS`.

**The SASL targets are the ones that matter most.** SCRAM is mutual
authentication, but the client only proves the *server* at server-final — so
everything the server-first parser touches is reachable by anyone who can
answer on the broker's address. Those parsers are `pub(crate)`, so `kacrab`
exposes them to `fuzz/` as `fn(&[u8])` shims behind an internal `__fuzzing`
feature that is `#[doc(hidden)]`, off by default, and exempt from semver.

`scram_server_first` gets the same two-target treatment as record batches, for
the same reason. `client_final` rejects any server-first whose nonce does not
extend the client's own randomly generated nonce, which a fuzzer cannot guess,
so raw bytes stall at 199 edges and never reach the salt decode or the PBKDF2
derivation. `scram_server_first_nonced` satisfies that gate in the harness so
mutations land on the fields behind it — which is how the unbounded iteration
count became reproducible.

Record batches get two targets, and the reason is worth stating because it is
the difference between fuzzing and the appearance of it. `decode_next_batch`
validates CRC32C *before* it reads the magic byte, the record count, the varint
record headers, or the compressed blob. Random bytes clear a CRC32C check with
probability 2^-32, so raw bytes alone only ever exercise framing and rejection.
`record_batch_framed` hands the fuzzer the CRC-covered region and builds
correct framing around it, so every mutation lands inside the decoder — that is
what found the header-count OOM fixed in `Record::decode`. Both targets are
kept: the framed one constructs CRC and length prefixes correctly by
definition, so it can never find a bug in them.

None of this makes the decoders *proven* safe. It makes them survivors of
~20M structured inputs per campaign, which is a different and weaker claim.

They run [nightly in CI][fuzz-workflow] at 15 minutes per target, and as a
60-second smoke on any PR touching `kacrab-protocol/`. The fuzz crate lives
outside the workspace (`fuzz/`) because cargo-fuzz needs nightly and a
sanitizer, which the pinned stable toolchain cannot provide. Decompression is
bounded by `MAX_DECOMPRESSED_LEN` in every codec, so a declared-size zip bomb
is rejected rather than allocated; the `decompress` target asserts that bound
holds.

[cargo-fuzz]: https://github.com/rust-fuzz/cargo-fuzz
[fuzz-workflow]: https://github.com/pirumu/kacrab/actions/workflows/fuzz.yml

## Coverage — measured with `cargo-llvm-cov`

CI gates line coverage with [`cargo-llvm-cov`][llvm-cov]. Maintained-source
coverage is **~87.5%** (generated protocol/config artifacts excluded via
`--ignore-filename-regex`), with the producer module around **92%**. Generated
code is held to a different standard — it is validated by the generator's own
tests and the Java oracle, not by line coverage — so counting it would only
dilute the signal (the raw all-files figure is ~63%, dominated by message
structs for APIs not yet wired).

```bash
cargo llvm-cov --workspace --all-features \
  --ignore-filename-regex '(benches/|kacrab-codegen/src/main\.rs|kacrab-macros/src/lib\.rs|kacrab/src/config/catalog\.rs|kacrab-protocol/src/generated)'
```

> **Why not tarpaulin**
>
> The coverage tool *itself* turned out to matter. kacrab's test suite leans
> heavily on real timeouts and blocking windows — `max.block.ms`,
> `delivery.timeout.ms`, leadership-retry deadlines — often set to a few
> milliseconds so a test can assert the timeout *fires*. `cargo-tarpaulin`'s
> instrumentation slows execution by ~10–50×, enough to blow through those
> windows: a `FindCoordinator` + `InitProducerId` round trip that takes
> microseconds bare would exceed a 30 ms budget under instrumentation, so the
> producer correctly — but spuriously — timed out. The result was a *shifting*
> set of flaky failures, different tests from one run to the next.
>
> `cargo-llvm-cov` uses LLVM source-based coverage and runs the tests at
> near-native speed. The same timeout tests that flaked under tarpaulin finish
> in 0.07 s instrumented versus 0.06 s bare — the timing windows hold, so
> coverage is both **reliable** and a **real gate**, not a best-effort report.

The lesson generalises: for an async, timeout-driven codebase, prefer a coverage
tool that doesn't perturb timing. A coverage job that flakes is worse than none
— it trains everyone to ignore red.

## The CI pipeline

Three jobs run on every push to `master` and every pull request:

| Job | Enforces |
|---|---|
| `fmt · clippy · test` | nightly `rustfmt`, the strict clippy lint set, and the **full** suite — uninstrumented, so it is the authoritative correctness gate |
| `coverage (llvm-cov)` | the `--fail-under-lines` floor described above, and publishes a Cobertura report |
| `cargo-deny` | license, advisory (RUSTSEC), and dependency-ban policy |

Two deliberate refinements keep the pipeline honest and cheap:

- **The test job is the source of truth, not coverage.** It runs the whole suite
  at native speed; the coverage job measures the same suite but is judged only on
  the coverage floor. Correctness and measurement are separated on purpose.
- **Docs-only changes skip the code CI.** A `paths-ignore` filter means editing
  this book, a README, or a license file does not trigger the ~20-minute
  fmt/clippy/test/coverage run. The book has its own deploy
  (`docs.yml` → GitHub Pages); a change that touches both code and docs still
  runs the full pipeline.

[llvm-cov]: https://github.com/taiki-e/cargo-llvm-cov
