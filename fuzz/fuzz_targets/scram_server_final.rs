//! The SCRAM server-final message, the step that authenticates the server.
//!
//! Reached with the peer still unauthenticated. The message either carries an
//! error (`e=`) or the server verifier (`v=`), and the verifier is compared
//! against a signature the client derived itself. Both the message and the
//! expected signature are fuzzer-controlled here, so mismatched lengths and
//! empty inputs are covered rather than only the well-formed comparison.
//!
//! What this is looking for is a panic or an index out of range in the
//! comparison path — a decode of attacker base64 into a fixed buffer, or a
//! length assumption that holds for real brokers and not for hostile ones.

#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    kacrab::__fuzz::scram_server_final(data);
});
