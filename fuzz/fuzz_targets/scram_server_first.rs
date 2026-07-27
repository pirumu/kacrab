//! The SCRAM server-first message, parsed before the peer is authenticated.
//!
//! This is the most security-relevant untrusted input the client handles. SCRAM
//! is mutual authentication, but the client only proves the *server* at
//! server-final — so everything reached here is reachable by anyone who can
//! answer on the broker's address: a MITM, a DNS or BGP hijack, a compromised
//! broker, or a misconfigured bootstrap pointing somewhere hostile.
//!
//! The message carries three attacker-chosen fields, and each is a different
//! class of hazard:
//!
//! - `r=` the server nonce, which must extend the client nonce;
//! - `s=` the salt, base64 that is decoded before use;
//! - `i=` the PBKDF2 iteration count, which the client then *executes*. A count
//!   is not data the client stores, it is work the client performs, so it is a
//!   CPU amplification primitive unless it is bounded on both sides.
//!
//! `-timeout` matters more than usual for this target: an unbounded iteration
//! count shows up as a libFuzzer timeout, not a crash.

#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    kacrab::__fuzz::scram_server_first(data);
});
