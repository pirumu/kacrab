//! The JAAS config string parser.
//!
//! Lower stakes than the two SCRAM targets: `sasl.jaas.config` is operator
//! input, not network input, so a hostile value implies the operator's own
//! config is hostile. It is fuzzed anyway because it is cheap, because the
//! parser is hand-written index arithmetic over quotes and delimiters, and
//! because it is the code that extracts the SASL *password* — a panic there is
//! a startup failure, and a mis-parse hands the wrong bytes to authentication.

#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    kacrab::__fuzz::jaas_option(data);
});
