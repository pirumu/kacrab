//! The SCRAM server-first message, *past* the client-nonce check.
//!
//! `client_final` rejects any server-first whose `r=` does not extend the
//! client's own nonce, and that nonce is fresh random per exchange — a fuzzer
//! cannot guess it. So `scram_server_first`, handed raw bytes, dies at the nonce
//! comparison every time and never reaches the base64 salt decode, the iteration
//! count, or the PBKDF2 derivation. It plateaued at 264 edges.
//!
//! This is the same trap CRC32C sets in front of the record-batch decoder, and
//! it needs the same answer: satisfy the gate in the harness so mutations land
//! on the fields behind it. Here the harness writes `r=<client_nonce>` and the
//! fuzzer supplies the rest, reaching `s=` and `i=`.
//!
//! `i=` is the one that matters. It is not data the client stores, it is work
//! the client performs — so an unbounded value is CPU amplification, and it
//! surfaces as a libFuzzer timeout rather than a crash. Run this target with
//! `-timeout` set.
//!
//! Keep both targets: this one satisfies the nonce check by construction, so it
//! can never find a bug in the nonce check.

#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    kacrab::__fuzz::scram_server_first_nonced(data);
});
