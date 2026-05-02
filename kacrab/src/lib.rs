//! # kacrab
//!
//! A production-oriented Kafka client for Rust, built from the protocol up.
//!
//! This is the public entry point users depend on. Submodules land
//! phase-by-phase per the workspace [`ROADMAP.md`]:
//!
//! - **Phase 0** — `protocol`: primitive types, varints, tagged fields.
//! - **Phase 1** — `wire`: TCP framing, request/response headers, correlation manager.
//! - **Phase 2** — `metadata`: cluster state, record batch v2 codec.
//! - **Phase 3+** — `producer`, `consumer`, transactions, group coordination.
//!
//! The companion crates [`kacrab-macros`], [`kacrab-util`], and [`kacrab-test`]
//! provide procedural macros, adapters, and test fixtures respectively.
//!
//! [`ROADMAP.md`]: https://github.com/pirumu/kacrab/blob/main/ROADMAP.md
//! [`kacrab-macros`]: https://docs.rs/kacrab-macros
//! [`kacrab-util`]: https://docs.rs/kacrab-util
//! [`kacrab-test`]: https://docs.rs/kacrab-test

#![no_std]
