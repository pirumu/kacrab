//! Consumer group member metadata — the only decoder fed by a *peer client*.
//!
//! Every other parser in this crate reads bytes from the broker or from the
//! operator. These two read bytes another consumer wrote:
//!
//! - `ConsumerProtocolSubscription` is the `metadata` field of each entry in
//!   `JoinGroupResponse.members[]`. The group **leader** decodes every other
//!   member's blob to compute the assignment
//!   (`kacrab/src/consumer/coordinator.rs`, via `assignor::decode_subscription`
//!   and `assignor::decode_owned`).
//! - `ConsumerProtocolAssignment` is `SyncGroupResponse.assignment`, which every
//!   **follower** decodes, and `DescribeGroupsResponse.member_assignment`, which
//!   every **admin client** decodes (`kacrab/src/admin/client.rs`, via
//!   `decode_member_assignment`).
//!
//! So the trust boundary is: anyone authorised to join a consumer group can feed
//! bytes to the decoder of every other member of that group, and to any admin
//! client that describes it. The broker only relays them.
//!
//! `response_decode` does not reach this. It decodes `JoinGroupResponse` and
//! `SyncGroupResponse` fine, but stops at their opaque `Bytes` fields — the
//! payload inside is a separate decode with its own version dispatch.
//!
//! That version dispatch is the point of interest. Both call sites read an `i16`
//! straight off the front of the blob and pass it to `read` unvalidated, so the
//! fuzzer chooses which field set the decoder expects, including negative and
//! out-of-range versions the encoder can never produce.

#![no_main]

use bytes::{Buf, Bytes};
use kacrab_protocol::generated::{ConsumerProtocolAssignmentData, ConsumerProtocolSubscriptionData};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // Mirrors the call sites: a blob shorter than the version prefix is
    // rejected before `get_i16`, so the fuzzer must supply at least that.
    let Some((selector, blob)) = data.split_first() else {
        return;
    };
    if blob.len() < 2 {
        return;
    }

    let mut buf = Bytes::copy_from_slice(blob);
    let version = buf.get_i16();

    // The selector picks which of the two decoders sees the blob. Both are
    // driven from the same corpus on purpose: the two schemas share a shape, so
    // an input that reaches deep into one is a useful starting point for the
    // other.
    if selector % 2 == 0 {
        drop(ConsumerProtocolSubscriptionData::read(&mut buf, version));
    } else {
        drop(ConsumerProtocolAssignmentData::read(&mut buf, version));
    }
});
