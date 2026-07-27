//! The OAUTHBEARER token-endpoint HTTP response parser.
//!
//! Reached whenever `sasl.oauthbearer.token.endpoint.url` is configured, and it
//! is a hand-written HTTP parser over raw socket bytes rather than a library
//! one: it splits headers from body on `\r\n\r\n`, takes the status code by
//! whitespace position in the first line, then parses the body as JSON and pulls
//! `access_token` and `expires_in` out of it.
//!
//! Lower exposure than the SCRAM targets — the peer is a URL the operator chose,
//! normally over TLS — but not zero. It is reachable by whatever answers at that
//! endpoint, which covers a compromised or misconfigured identity provider, and
//! a plaintext or unverified endpoint puts it in reach of the network.
//!
//! `serde_json` is heavily fuzzed upstream; what is not is the framing around it
//! and the field extraction after it, which is what this target covers.

#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    kacrab::__fuzz::oauthbearer_http_response(data);
});
