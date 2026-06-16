# Summary

Describe the change and why it is needed.

## Scope

- [ ] Wire layer
- [ ] Producer
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
- [ ] Other:

## Pure Rust / Unsafe Check

- [ ] This does not add native Kafka client bindings or C wrappers.
- [ ] This does not add unsafe code.

## Performance Impact

If this affects hot paths, batching, routing, backpressure, allocations, or
multi-broker dispatch, describe the expected impact and include benchmark data.

## Notes for Reviewers

Call out risky areas, known gaps, or follow-up work.
