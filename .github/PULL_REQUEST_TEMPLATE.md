# Summary

Describe the change and why it is needed.

## Scope

- [ ] Wire layer
- [ ] Producer
- [ ] Consumer / share consumer
- [ ] Admin
- [ ] Auth (TLS/SASL)
- [ ] Protocol/codegen
- [ ] Config
- [ ] Benchmarks
- [ ] Documentation
- [ ] Other

## Verification

Paste the commands you ran and the relevant result.

- [ ] `make fmt-check`
- [ ] `make clippy`
- [ ] `make test`
- [ ] `make check-features` — required if anything is feature-gated; the main
      suite runs `--all-features`, which is the one configuration nobody ships
- [ ] `make deny` — required if dependencies changed
- [ ] Real-broker suite(s), if the change affects broker-facing behavior
      (compose file + the matching `--ignored` test)
- [ ] `CHANGELOG.md` entry, if the change is user-facing
- [ ] Other:

## Pure Rust / Unsafe Check

- [ ] This does not add native Kafka client bindings or C wrappers.
- [ ] This does not add unsafe code.
- [ ] If dependencies changed: `make check-pure-rust-tls` still passes
      (`aws-lc-sys` and `rsa` stay out of `pure-rust-tls` builds).

## Untrusted Input

If this touches a parser that reads untrusted bytes (framing, record batches,
response decoding, SASL, the OAUTHBEARER endpoint), update the matching fuzz
target and seeds in `fuzz/` — regenerate the corpus via `generate_fuzz_corpus`
after schema changes.

## Performance Impact

If this affects hot paths, batching, routing, backpressure, allocations, or
multi-broker dispatch, describe the expected impact and include benchmark data.

## Notes for Reviewers

Call out risky areas, known gaps, or follow-up work.
