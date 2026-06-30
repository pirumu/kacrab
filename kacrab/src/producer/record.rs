//! Public producer record, delivery, and receipt types.

use std::{
    cell::Cell,
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU8, Ordering},
    },
    task::{Context, Poll, Waker},
};

use bytes::Bytes;
pub use kacrab_protocol::record::RecordHeader;

use super::{ProducerError, Result};
use crate::wire::WireError;

/// `Header` alias for a Kafka record header.
pub type Header = RecordHeader;

/// `Headers` alias for an ordered mutable record header collection.
pub type Headers = RecordHeaders;

/// Ordered mutable collection of Kafka record headers.
///
/// This mirrors Kafka's `RecordHeaders` shape while keeping mutation failures
/// explicit through [`Result`] instead of throwing `IllegalStateException`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RecordHeaders {
    entries: Vec<RecordHeader>,
    read_only: bool,
}

impl RecordHeaders {
    /// Create an empty header collection.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            entries: Vec::new(),
            read_only: false,
        }
    }

    /// Create a header collection from headers in insertion order.
    #[must_use]
    pub fn from_headers(headers: impl IntoIterator<Item = RecordHeader>) -> Self {
        Self {
            entries: headers.into_iter().collect(),
            read_only: false,
        }
    }

    /// Append a non-null header value.
    ///
    /// # Errors
    ///
    /// Returns [`ProducerError::InvalidRecord`] when the collection is read-only.
    pub fn add(&mut self, key: impl Into<Bytes>, value: impl Into<Bytes>) -> Result<&mut Self> {
        self.add_header(RecordHeader {
            key: key.into(),
            value: Some(value.into()),
        })
    }

    /// Append a null-valued header.
    ///
    /// # Errors
    ///
    /// Returns [`ProducerError::InvalidRecord`] when the collection is read-only.
    pub fn add_null(&mut self, key: impl Into<Bytes>) -> Result<&mut Self> {
        self.add_header(RecordHeader {
            key: key.into(),
            value: None,
        })
    }

    /// Append an existing header.
    ///
    /// # Errors
    ///
    /// Returns [`ProducerError::InvalidRecord`] when the collection is read-only.
    pub fn add_header(&mut self, header: RecordHeader) -> Result<&mut Self> {
        self.ensure_writable()?;
        self.entries.push(header);
        Ok(self)
    }

    /// Remove all headers for `key` while preserving remaining insertion order.
    ///
    /// # Errors
    ///
    /// Returns [`ProducerError::InvalidRecord`] when the collection is read-only.
    pub fn remove(&mut self, key: &[u8]) -> Result<&mut Self> {
        self.ensure_writable()?;
        self.entries.retain(|header| header.key.as_ref() != key);
        Ok(self)
    }

    /// Return the last header for `key`, if present.
    #[must_use]
    pub fn last_header(&self, key: &[u8]) -> Option<&RecordHeader> {
        self.entries
            .iter()
            .rev()
            .find(|header| header.key.as_ref() == key)
    }

    /// Return all headers for `key` in insertion order.
    pub fn headers<'a>(&'a self, key: &'a [u8]) -> impl Iterator<Item = &'a RecordHeader> + 'a {
        self.entries
            .iter()
            .filter(move |header| header.key.as_ref() == key)
    }

    /// Return all headers as a cloned vector in insertion order.
    #[must_use]
    pub fn to_array(&self) -> Vec<RecordHeader> {
        self.entries.clone()
    }

    /// Borrow all headers in insertion order.
    #[must_use]
    pub fn as_slice(&self) -> &[RecordHeader] {
        &self.entries
    }

    /// Number of headers in the collection.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the collection is empty.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Mark this collection read-only, matching Kafka `RecordHeaders#setReadOnly`.
    pub const fn set_read_only(&mut self) {
        self.read_only = true;
    }

    /// Whether this collection is read-only.
    #[must_use]
    pub const fn is_read_only(&self) -> bool {
        self.read_only
    }

    const fn ensure_writable(&self) -> Result<()> {
        if self.read_only {
            return Err(ProducerError::InvalidRecord {
                field: "headers",
                message: "headers are read-only",
            });
        }
        Ok(())
    }

    /// Iterate over all headers in insertion order.
    pub fn iter(&self) -> std::slice::Iter<'_, RecordHeader> {
        self.entries.iter()
    }
}

impl From<Vec<RecordHeader>> for RecordHeaders {
    fn from(headers: Vec<RecordHeader>) -> Self {
        Self::from_headers(headers)
    }
}

impl From<RecordHeaders> for Vec<RecordHeader> {
    fn from(headers: RecordHeaders) -> Self {
        headers.entries
    }
}

impl FromIterator<RecordHeader> for RecordHeaders {
    fn from_iter<T: IntoIterator<Item = RecordHeader>>(iter: T) -> Self {
        Self::from_headers(iter)
    }
}

impl IntoIterator for RecordHeaders {
    type IntoIter = std::vec::IntoIter<RecordHeader>;
    type Item = RecordHeader;

    fn into_iter(self) -> Self::IntoIter {
        self.entries.into_iter()
    }
}

impl<'a> IntoIterator for &'a RecordHeaders {
    type IntoIter = std::slice::Iter<'a, RecordHeader>;
    type Item = &'a RecordHeader;

    fn into_iter(self) -> Self::IntoIter {
        self.entries.iter()
    }
}

/// Callback invoked when a produced record is acknowledged or dropped.
pub type DeliveryCallback = Box<dyn FnOnce(Result<RecordMetadata>) + Send + 'static>;

/// Sentinel used by [`ProducerRecord::unassigned`] before metadata-based
/// partitioning selects a concrete topic partition.
pub(crate) const UNASSIGNED_PARTITION: i32 = -1;
const DELIVERY_PENDING: u8 = 0;
const DELIVERY_COMPLETED: u8 = 1;
const DELIVERY_CLOSED: u8 = 2;

std::thread_local! {
    static DELIVERY_CALLBACK_DEPTH: Cell<u32> = const { Cell::new(0) };
}

pub(crate) struct DeliveryCallbackScopeGuard {
    _private: (),
}

impl Drop for DeliveryCallbackScopeGuard {
    fn drop(&mut self) {
        DELIVERY_CALLBACK_DEPTH.with(|depth| {
            depth.set(depth.get().saturating_sub(1));
        });
    }
}

pub(crate) fn in_delivery_callback() -> bool {
    DELIVERY_CALLBACK_DEPTH.with(|depth| depth.get() > 0)
}

fn enter_delivery_callback_scope() -> DeliveryCallbackScopeGuard {
    DELIVERY_CALLBACK_DEPTH.with(|depth| {
        depth.set(depth.get().saturating_add(1));
    });
    DeliveryCallbackScopeGuard { _private: () }
}

#[cfg(test)]
std::thread_local! {
    static PRODUCER_RECORD_CLONES: Cell<usize> = const { Cell::new(0) };
}

/// One record targeted at an explicit topic partition.
#[derive(Debug, PartialEq, Eq)]
pub struct ProducerRecord {
    /// Topic name.
    pub topic: Arc<str>,
    /// Partition index.
    pub partition: i32,
    /// Optional create-time timestamp in milliseconds since Unix epoch.
    pub timestamp_ms: Option<i64>,
    /// Optional record key.
    pub key: Option<Bytes>,
    /// Optional record value.
    pub value: Option<Bytes>,
    /// Kafka record headers.
    pub headers: Vec<RecordHeader>,
}

impl Clone for ProducerRecord {
    fn clone(&self) -> Self {
        #[cfg(test)]
        PRODUCER_RECORD_CLONES.with(|clones| {
            clones.set(clones.get().saturating_add(1));
        });
        Self {
            topic: Arc::clone(&self.topic),
            partition: self.partition,
            timestamp_ms: self.timestamp_ms,
            key: self.key.clone(),
            value: self.value.clone(),
            headers: self.headers.clone(),
        }
    }
}

impl ProducerRecord {
    #[cfg(test)]
    pub(crate) fn reset_clone_count_for_test() {
        PRODUCER_RECORD_CLONES.with(|clones| clones.set(0));
    }

    #[cfg(test)]
    pub(crate) fn clone_count_for_test() -> usize {
        PRODUCER_RECORD_CLONES.with(Cell::get)
    }

    /// Create a producer record for an explicit topic partition.
    pub fn new(topic: impl Into<Arc<str>>, partition: i32) -> Self {
        assert!(partition >= 0, "partition must be non-negative");
        Self {
            topic: topic.into(),
            partition,
            timestamp_ms: None,
            key: None,
            value: None,
            headers: Vec::new(),
        }
    }

    /// Try to create a producer record for an explicit topic partition.
    ///
    /// # Errors
    ///
    /// Returns [`ProducerError::InvalidRecord`] when `partition` is negative.
    pub fn try_new(topic: impl Into<Arc<str>>, partition: i32) -> Result<Self> {
        if partition < 0 {
            return Err(ProducerError::InvalidRecord {
                field: "partition",
                message: "partition must be non-negative",
            });
        }
        Ok(Self::new(topic, partition))
    }

    /// Create a producer record whose partition will be selected from metadata.
    pub fn unassigned(topic: impl Into<Arc<str>>) -> Self {
        Self {
            topic: topic.into(),
            partition: UNASSIGNED_PARTITION,
            timestamp_ms: None,
            key: None,
            value: None,
            headers: Vec::new(),
        }
    }

    /// Create an unassigned record with a non-null value, matching Kafka's
    /// `ProducerRecord(topic, value)` constructor shape.
    #[must_use]
    pub fn topic_value(topic: impl Into<Arc<str>>, value: impl Into<Bytes>) -> Self {
        Self::unassigned(topic).value(value)
    }

    /// Create an unassigned record with a non-null key and value, matching
    /// Kafka's `ProducerRecord(topic, key, value)` constructor shape.
    #[must_use]
    pub fn topic_key_value(
        topic: impl Into<Arc<str>>,
        key: impl Into<Bytes>,
        value: impl Into<Bytes>,
    ) -> Self {
        Self::unassigned(topic).key(key).value(value)
    }

    /// Create an explicit-partition record with a non-null value.
    #[must_use]
    pub fn partition_value(
        topic: impl Into<Arc<str>>,
        partition: i32,
        value: impl Into<Bytes>,
    ) -> Self {
        Self::new(topic, partition).value(value)
    }

    /// Create an explicit-partition record with a non-null key and value,
    /// matching Kafka's `ProducerRecord(topic, partition, key, value)`.
    #[must_use]
    pub fn partition_key_value(
        topic: impl Into<Arc<str>>,
        partition: i32,
        key: impl Into<Bytes>,
        value: impl Into<Bytes>,
    ) -> Self {
        Self::new(topic, partition).key(key).value(value)
    }

    /// Try to create an explicit-partition timestamped record with a non-null
    /// key and value, matching Kafka's timestamped constructor validation.
    ///
    /// # Errors
    ///
    /// Returns [`ProducerError::InvalidRecord`] when `partition` or
    /// `timestamp_ms` is negative.
    pub fn try_partition_timestamp_key_value(
        topic: impl Into<Arc<str>>,
        partition: i32,
        timestamp_ms: i64,
        key: impl Into<Bytes>,
        value: impl Into<Bytes>,
    ) -> Result<Self> {
        Ok(Self::try_new(topic, partition)?
            .try_timestamp_ms(timestamp_ms)?
            .key(key)
            .value(value))
    }

    /// Return whether this record already has a concrete partition.
    #[must_use]
    pub const fn has_assigned_partition(&self) -> bool {
        self.partition >= 0
    }

    /// Set the record key.
    #[must_use]
    pub fn key(mut self, key: impl Into<Bytes>) -> Self {
        self.key = Some(key.into());
        self
    }

    /// Set the record value.
    #[must_use]
    pub fn value(mut self, value: impl Into<Bytes>) -> Self {
        self.value = Some(value.into());
        self
    }

    /// Set the create-time timestamp in milliseconds since Unix epoch.
    ///
    /// # Errors
    ///
    /// Returns [`ProducerError::InvalidRecord`] when `timestamp_ms` is negative.
    pub fn try_timestamp_ms(mut self, timestamp_ms: i64) -> Result<Self> {
        if timestamp_ms < 0 {
            return Err(ProducerError::InvalidRecord {
                field: "timestamp_ms",
                message: "timestamp must be non-negative",
            });
        }
        self.timestamp_ms = Some(timestamp_ms);
        Ok(self)
    }

    /// Set the create-time timestamp in milliseconds since Unix epoch.
    ///
    /// This is an alias for [`Self::try_timestamp_ms`] that reads naturally in
    /// Constructor tests.
    ///
    /// # Errors
    ///
    /// Returns [`ProducerError::InvalidRecord`] when `timestamp_ms` is negative.
    pub fn timestamp_ms(self, timestamp_ms: i64) -> Result<Self> {
        self.try_timestamp_ms(timestamp_ms)
    }

    /// Append one non-null Kafka record header.
    #[must_use]
    pub fn header(mut self, key: impl Into<Bytes>, value: impl Into<Bytes>) -> Self {
        self.headers.push(RecordHeader {
            key: key.into(),
            value: Some(value.into()),
        });
        self
    }

    /// Append one Kafka record header with a null value.
    #[must_use]
    pub fn header_null(mut self, key: impl Into<Bytes>) -> Self {
        self.headers.push(RecordHeader {
            key: key.into(),
            value: None,
        });
        self
    }

    /// Replace this record's headers with an iterable of record headers.
    #[must_use]
    pub fn with_headers(mut self, headers: impl IntoIterator<Item = RecordHeader>) -> Self {
        self.headers = headers.into_iter().collect();
        self
    }

    /// Return all headers in insertion order.
    #[must_use]
    pub fn headers(&self) -> &[RecordHeader] {
        &self.headers
    }

    /// Return all headers for `key` in insertion order, matching Kafka
    /// `Headers.headers(key)`.
    pub fn headers_for_key<'a>(
        &'a self,
        key: &'a [u8],
    ) -> impl Iterator<Item = &'a RecordHeader> + 'a {
        self.headers
            .iter()
            .filter(move |header| header.key.as_ref() == key)
    }

    /// Return the last header for `key`, matching Kafka `Headers.lastHeader`.
    #[must_use]
    pub fn last_header(&self, key: &[u8]) -> Option<&RecordHeader> {
        self.headers
            .iter()
            .rev()
            .find(|header| header.key.as_ref() == key)
    }

    /// Remove all headers for `key` while preserving remaining insertion order.
    #[must_use]
    pub fn remove_headers(mut self, key: &[u8]) -> Self {
        self.headers.retain(|header| header.key.as_ref() != key);
        self
    }

    /// Remove all headers for `key` in place while preserving remaining order.
    pub fn remove_headers_mut(&mut self, key: &[u8]) -> &mut Self {
        self.headers.retain(|header| header.key.as_ref() != key);
        self
    }
}

/// Produce acknowledgement metadata for one appended record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordMetadata {
    /// Topic name.
    pub topic: Arc<str>,
    /// Partition index.
    pub partition: i32,
    /// Leader broker id used for routing.
    pub leader_id: i32,
    /// Absolute broker offset for this record, or `-1` when `acks=0`.
    pub offset: i64,
    /// Broker append timestamp, or `-1` when unavailable/create-time is used.
    pub timestamp_ms: i64,
    /// Serialized key size in bytes, or `-1` when the key is null/unknown.
    pub serialized_key_size: i32,
    /// Serialized value size in bytes, or `-1` when the value is null/unknown.
    pub serialized_value_size: i32,
}

impl RecordMetadata {
    /// Sentinel metadata used for callbacks when delivery fails.
    #[must_use]
    pub fn failed() -> Self {
        Self {
            topic: Arc::from(""),
            partition: -1,
            leader_id: -1,
            offset: -1,
            timestamp_ms: -1,
            serialized_key_size: -1,
            serialized_value_size: -1,
        }
    }
}

/// Future-like delivery handle returned by [`crate::producer::Producer::send`].
#[derive(Debug)]
pub struct SendFuture {
    state: Arc<DeliveryState>,
    record_index: usize,
}

#[derive(Debug)]
pub(crate) struct DeliverySender {
    state: Arc<DeliveryState>,
    completed: bool,
    next_record_index: usize,
}

struct DeliveryState {
    status: AtomicU8,
    receipts: Mutex<Vec<Option<RecordMetadata>>>,
    error: Mutex<Option<ProducerError>>,
    record_metadata: Mutex<Vec<RecordDeliveryMetadata>>,
    wakers: Mutex<Vec<Waker>>,
    callbacks: Mutex<Vec<(usize, DeliveryCallback)>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RecordDeliveryMetadata {
    serialized_key_size: i32,
    serialized_value_size: i32,
}

impl Default for DeliveryState {
    fn default() -> Self {
        Self {
            status: AtomicU8::new(DELIVERY_PENDING),
            receipts: Mutex::new(Vec::new()),
            error: Mutex::new(None),
            record_metadata: Mutex::new(Vec::new()),
            wakers: Mutex::new(Vec::new()),
            callbacks: Mutex::new(Vec::new()),
        }
    }
}

impl std::fmt::Debug for DeliveryState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let error = self
            .error
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        f.debug_struct("DeliveryState")
            .field("status", &self.status.load(Ordering::Relaxed))
            .field("receipt_count", &lock_len(&self.receipts))
            .field("error", &*error)
            .field("record_metadata_count", &lock_len(&self.record_metadata))
            .field("waker_count", &lock_len(&self.wakers))
            .field("callback_count", &lock_len(&self.callbacks))
            .finish()
    }
}

impl SendFuture {
    #[cfg(test)]
    pub(crate) fn channel() -> (DeliverySender, Self) {
        Self::channel_with_record_metadata(RecordDeliveryMetadata::unknown(), 1)
    }

    #[cfg(test)]
    pub(crate) fn channel_for_record(record: &ProducerRecord) -> (DeliverySender, Self) {
        Self::channel_for_record_with_metadata_capacity(record, 1)
    }

    pub(crate) fn channel_for_record_with_metadata_capacity(
        record: &ProducerRecord,
        metadata_capacity: usize,
    ) -> (DeliverySender, Self) {
        Self::channel_with_record_metadata(RecordDeliveryMetadata::from(record), metadata_capacity)
    }

    fn channel_with_record_metadata(
        record_metadata: RecordDeliveryMetadata,
        metadata_capacity: usize,
    ) -> (DeliverySender, Self) {
        let state = Arc::new(DeliveryState {
            record_metadata: Mutex::new(Vec::with_capacity(metadata_capacity.max(1))),
            ..DeliveryState::default()
        });
        {
            let mut metadata = state
                .record_metadata
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            metadata.push(record_metadata);
        }
        {
            let mut receipts = state
                .receipts
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            receipts.push(None);
        }
        (
            DeliverySender {
                state: Arc::clone(&state),
                completed: false,
                next_record_index: 1,
            },
            Self {
                state,
                record_index: 0,
            },
        )
    }

    fn completed_receipt(&self) -> Poll<Result<RecordMetadata>> {
        if let Some(error) = delivery_error(&self.state) {
            return Poll::Ready(Err(error));
        }
        record_delivery_receipt(&self.state, self.record_index).map_or(
            Poll::Ready(Err(ProducerError::DeliveryDropped)),
            Poll::Ready,
        )
    }

    fn register_pending_waker(&self, cx: &Context<'_>) -> Poll<Result<RecordMetadata>> {
        let mut wakers = self
            .state
            .wakers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match self.state.status.load(Ordering::Acquire) {
            DELIVERY_COMPLETED => self.completed_receipt(),
            DELIVERY_CLOSED => Poll::Ready(Err(ProducerError::DeliveryDropped)),
            _ => {
                if !wakers.iter().any(|waker| waker.will_wake(cx.waker())) {
                    wakers.push(cx.waker().clone());
                }
                Poll::Pending
            },
        }
    }

    pub(crate) fn register_callback(&self, callback: DeliveryCallback) {
        let mut callbacks = self
            .state
            .callbacks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        callbacks.push((self.record_index, callback));
    }
}

impl DeliverySender {
    #[cfg(test)]
    pub(crate) fn delivery(&mut self) -> SendFuture {
        self.delivery_with_record_metadata(RecordDeliveryMetadata::unknown())
    }

    pub(crate) fn delivery_for_record(&mut self, record: &ProducerRecord) -> SendFuture {
        self.delivery_with_record_metadata(RecordDeliveryMetadata::from(record))
    }

    fn delivery_with_record_metadata(
        &mut self,
        record_metadata: RecordDeliveryMetadata,
    ) -> SendFuture {
        let record_index = self.next_record_index;
        self.next_record_index = self.next_record_index.saturating_add(1);
        {
            let mut metadata = self
                .state
                .record_metadata
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            metadata.push(record_metadata);
        }
        {
            let mut receipts = self
                .state
                .receipts
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            receipts.push(None);
        }
        SendFuture {
            state: Arc::clone(&self.state),
            record_index,
        }
    }

    pub(crate) fn has_receivers(&self) -> bool {
        Arc::strong_count(&self.state) > 1 || !lock_is_empty(&self.state.callbacks)
    }

    pub(crate) fn record_count(&self) -> usize {
        lock_len(&self.state.record_metadata)
    }

    pub(crate) fn send(self, receipt: &RecordMetadata) {
        let metadata = record_delivery_metadata_snapshot(&self.state);
        let receipts = metadata
            .iter()
            .enumerate()
            .map(|(record_index, metadata)| receipt_for_record(receipt, record_index, *metadata))
            .collect();
        self.send_records(receipts);
    }

    pub(crate) fn send_records(mut self, receipts: Vec<RecordMetadata>) {
        let record_metadata = record_delivery_metadata_snapshot(&self.state);
        let receipts = receipts
            .into_iter()
            .enumerate()
            .map(|(record_index, mut receipt)| {
                let metadata = record_metadata
                    .get(record_index)
                    .copied()
                    .unwrap_or_else(RecordDeliveryMetadata::unknown);
                receipt.serialized_key_size = metadata.serialized_key_size;
                receipt.serialized_value_size = metadata.serialized_value_size;
                receipt
            })
            .collect();
        store_record_receipts(&self.state, receipts);
        if !all_record_receipts_ready(&self.state) {
            return;
        }
        let callbacks = take_callbacks(&self.state);
        self.state
            .status
            .store(DELIVERY_COMPLETED, Ordering::Release);
        let wakers = take_wakers(&self.state);
        self.completed = true;
        for waker in wakers {
            waker.wake();
        }
        for (record_index, callback) in callbacks {
            let result = record_delivery_receipt(&self.state, record_index)
                .unwrap_or(Err(ProducerError::DeliveryDropped));
            invoke_delivery_callback(callback, result);
        }
    }

    pub(crate) fn send_error(mut self, error: &ProducerError) {
        store_delivery_error(&self.state, clone_producer_error_for_delivery(error));
        let callbacks = take_callbacks(&self.state);
        self.state
            .status
            .store(DELIVERY_COMPLETED, Ordering::Release);
        let wakers = take_wakers(&self.state);
        self.completed = true;
        for waker in wakers {
            waker.wake();
        }
        for (_record_index, callback) in callbacks {
            invoke_delivery_callback(callback, Err(clone_producer_error_for_delivery(error)));
        }
    }
}

impl Drop for DeliverySender {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        if self
            .state
            .status
            .compare_exchange(
                DELIVERY_PENDING,
                DELIVERY_CLOSED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            return;
        }
        let wakers = take_wakers(&self.state);
        let callbacks = take_callbacks(&self.state);
        for waker in wakers {
            waker.wake();
        }
        for (_record_index, callback) in callbacks {
            invoke_delivery_callback(callback, Err(ProducerError::DeliveryDropped));
        }
    }
}

impl Future for SendFuture {
    type Output = Result<RecordMetadata>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.state.status.load(Ordering::Acquire) {
            DELIVERY_COMPLETED => self.completed_receipt(),
            DELIVERY_CLOSED => Poll::Ready(Err(ProducerError::DeliveryDropped)),
            _ => self.register_pending_waker(cx),
        }
    }
}

fn take_wakers(state: &DeliveryState) -> Vec<Waker> {
    let mut wakers = state
        .wakers
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    std::mem::take(&mut *wakers)
}

fn take_callbacks(state: &DeliveryState) -> Vec<(usize, DeliveryCallback)> {
    let mut callbacks = state
        .callbacks
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    std::mem::take(&mut *callbacks)
}

fn invoke_delivery_callback(callback: DeliveryCallback, result: Result<RecordMetadata>) {
    let _ignored = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _scope = enter_delivery_callback_scope();
        callback(result);
    }));
}

fn lock_is_empty<T>(mutex: &Mutex<Vec<T>>) -> bool {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .is_empty()
}

fn lock_len<T>(mutex: &Mutex<Vec<T>>) -> usize {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .len()
}

fn delivery_error(state: &DeliveryState) -> Option<ProducerError> {
    state
        .error
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .as_ref()
        .map(clone_producer_error_for_delivery)
}

fn store_delivery_error(state: &DeliveryState, error: ProducerError) {
    let mut stored = state
        .error
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    *stored = Some(error);
}

#[expect(
    clippy::too_many_lines,
    reason = "Delivery errors need value semantics but several public error variants are not \
              Clone."
)]
pub(crate) fn clone_producer_error_for_delivery(error: &ProducerError) -> ProducerError {
    match error {
        ProducerError::Backpressure => ProducerError::Backpressure,
        ProducerError::ProducerClosed => ProducerError::ProducerClosed,
        ProducerError::InvalidRecord { field, message } => {
            ProducerError::InvalidRecord { field, message }
        },
        ProducerError::RecordTooLarge {
            size,
            max_request_size,
        } => ProducerError::RecordTooLarge {
            size: *size,
            max_request_size: *max_request_size,
        },
        ProducerError::RecordExceedsBufferMemory {
            size,
            buffer_memory,
        } => ProducerError::RecordExceedsBufferMemory {
            size: *size,
            buffer_memory: *buffer_memory,
        },
        ProducerError::FlushIncomplete => ProducerError::FlushIncomplete,
        ProducerError::BatchLifecycle(message) => ProducerError::BatchLifecycle(message),
        ProducerError::CallbackOperation { operation } => {
            ProducerError::CallbackOperation { operation }
        },
        ProducerError::DeliveryTimeout { topic, partition } => ProducerError::DeliveryTimeout {
            topic: topic.clone(),
            partition: *partition,
        },
        ProducerError::UnknownTopic(topic) => ProducerError::UnknownTopic(topic.clone()),
        ProducerError::UnknownPartition { topic, partition } => ProducerError::UnknownPartition {
            topic: topic.clone(),
            partition: *partition,
        },
        ProducerError::LeaderNotFound {
            topic,
            partition,
            leader_id,
        } => ProducerError::LeaderNotFound {
            topic: topic.clone(),
            partition: *partition,
            leader_id: *leader_id,
        },
        ProducerError::MissingProduceResponse { topic, partition } => {
            ProducerError::MissingProduceResponse {
                topic: topic.clone(),
                partition: *partition,
            }
        },
        ProducerError::Broker {
            topic,
            partition,
            error,
        } => ProducerError::Broker {
            topic: topic.clone(),
            partition: *partition,
            error: *error,
        },
        ProducerError::Transaction { operation, error } => ProducerError::Transaction {
            operation,
            error: *error,
        },
        ProducerError::TransactionalIdRequired => ProducerError::TransactionalIdRequired,
        ProducerError::InvalidTransactionState(message) => {
            ProducerError::InvalidTransactionState(message)
        },
        ProducerError::TransactionStateBusy => ProducerError::TransactionStateBusy,
        ProducerError::InvalidConsumerGroupMetadata(message) => {
            ProducerError::InvalidConsumerGroupMetadata(message)
        },
        ProducerError::SequenceOverflow { topic, partition } => ProducerError::SequenceOverflow {
            topic: topic.clone(),
            partition: *partition,
        },
        ProducerError::UnresolvedSequence { topic, partition } => {
            ProducerError::UnresolvedSequence {
                topic: topic.clone(),
                partition: *partition,
            }
        },
        ProducerError::DispatchTask(message) => ProducerError::DispatchTask(message.clone()),
        ProducerError::DeliveryDropped => ProducerError::DeliveryDropped,
        ProducerError::UnsupportedOperation(operation) => {
            ProducerError::UnsupportedOperation(operation)
        },
        ProducerError::TelemetryDisabled => ProducerError::TelemetryDisabled,
        ProducerError::Telemetry { operation, error } => ProducerError::Telemetry {
            operation,
            error: *error,
        },
        ProducerError::InvalidTelemetrySubscription(message) => {
            ProducerError::InvalidTelemetrySubscription(message)
        },
        ProducerError::InvalidTelemetryTimeout { timeout_ms } => {
            ProducerError::InvalidTelemetryTimeout {
                timeout_ms: *timeout_ms,
            }
        },
        ProducerError::InvalidCloseTimeout { timeout_ms } => ProducerError::InvalidCloseTimeout {
            timeout_ms: *timeout_ms,
        },
        ProducerError::InvalidConfig { key, value } => ProducerError::InvalidConfig {
            key,
            value: value.clone(),
        },
        ProducerError::Config { error } => ProducerError::Config {
            error: error.clone(),
        },
        ProducerError::Wire(error) => clone_wire_error_for_delivery(error).map_or_else(
            || ProducerError::DispatchTask(error.to_string()),
            ProducerError::Wire,
        ),
        ProducerError::Record(error) => ProducerError::DispatchTask(error.to_string()),
    }
}

fn clone_wire_error_for_delivery(error: &WireError) -> Option<WireError> {
    match error {
        WireError::Timeout => Some(WireError::Timeout),
        WireError::ConnectionClosed => Some(WireError::ConnectionClosed),
        WireError::Backpressure => Some(WireError::Backpressure),
        WireError::InvalidSecurityProtocol(protocol) => {
            Some(WireError::InvalidSecurityProtocol(protocol.clone()))
        },
        WireError::InvalidTlsConfig(message) => Some(WireError::InvalidTlsConfig(message.clone())),
        WireError::TlsHandshake(message) => Some(WireError::TlsHandshake(message.clone())),
        WireError::UnsupportedTlsOption(option) => {
            Some(WireError::UnsupportedTlsOption(option.clone()))
        },
        WireError::InvalidSaslConfig(message) => {
            Some(WireError::InvalidSaslConfig(message.clone()))
        },
        WireError::UnsupportedSaslMechanism(mechanism) => {
            Some(WireError::UnsupportedSaslMechanism(mechanism.clone()))
        },
        WireError::SaslHandshake(message) => Some(WireError::SaslHandshake(message.clone())),
        WireError::SaslAuthentication(message) => {
            Some(WireError::SaslAuthentication(message.clone()))
        },
        WireError::SaslServerSignatureMismatch => Some(WireError::SaslServerSignatureMismatch),
        WireError::TokenRefresh(message) => Some(WireError::TokenRefresh(message.clone())),
        WireError::GssapiBackendUnavailable => Some(WireError::GssapiBackendUnavailable),
        WireError::UnknownBroker(broker_id) => Some(WireError::UnknownBroker(*broker_id)),
        WireError::NoBrokerAvailable => Some(WireError::NoBrokerAvailable),
        WireError::InvalidBrokerEndpoint {
            node_id,
            host,
            port,
        } => Some(WireError::InvalidBrokerEndpoint {
            node_id: *node_id,
            host: host.clone(),
            port: *port,
        }),
        WireError::Kafka(error) => Some(WireError::Kafka(*error)),
        WireError::MetadataTopic { topic, error } => Some(WireError::MetadataTopic {
            topic: topic.clone(),
            error: *error,
        }),
        WireError::MetadataPartition {
            topic,
            partition,
            error,
        } => Some(WireError::MetadataPartition {
            topic: topic.clone(),
            partition: *partition,
            error: *error,
        }),
        WireError::RandomBytes(message) => Some(WireError::RandomBytes(message.clone())),
        WireError::UnsupportedApiVersion(api_key) => {
            Some(WireError::UnsupportedApiVersion(*api_key))
        },
        WireError::CorrelationIdMismatch { expected, actual } => {
            Some(WireError::CorrelationIdMismatch {
                expected: *expected,
                actual: *actual,
            })
        },
        WireError::Io(_) | WireError::Protocol(_) | WireError::Frame(_) => None,
    }
}

fn receipt_for_record(
    receipt: &RecordMetadata,
    record_index: usize,
    record_metadata: RecordDeliveryMetadata,
) -> RecordMetadata {
    let mut receipt = receipt.clone();
    if receipt.offset >= 0 {
        let offset_delta = i64::try_from(record_index).unwrap_or(i64::MAX);
        receipt.offset = receipt.offset.checked_add(offset_delta).unwrap_or(i64::MAX);
    }
    receipt.serialized_key_size = record_metadata.serialized_key_size;
    receipt.serialized_value_size = record_metadata.serialized_value_size;
    receipt
}

fn record_delivery_metadata_snapshot(state: &DeliveryState) -> Vec<RecordDeliveryMetadata> {
    state
        .record_metadata
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone()
}

fn record_delivery_receipt(
    state: &DeliveryState,
    record_index: usize,
) -> Option<Result<RecordMetadata>> {
    state
        .receipts
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(record_index)
        .cloned()
        .map(|receipt| receipt.ok_or(ProducerError::DeliveryDropped))
}

fn store_record_receipts(state: &DeliveryState, receipts: Vec<RecordMetadata>) {
    let mut stored = state
        .receipts
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    for (index, receipt) in receipts.into_iter().enumerate() {
        if let Some(slot) = stored.get_mut(index) {
            *slot = Some(receipt);
        }
    }
}

fn all_record_receipts_ready(state: &DeliveryState) -> bool {
    state
        .receipts
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .iter()
        .all(Option::is_some)
}

impl RecordDeliveryMetadata {
    const fn unknown() -> Self {
        Self {
            serialized_key_size: -1,
            serialized_value_size: -1,
        }
    }
}

impl From<&ProducerRecord> for RecordDeliveryMetadata {
    fn from(record: &ProducerRecord) -> Self {
        Self {
            serialized_key_size: serialized_size(record.key.as_ref()),
            serialized_value_size: serialized_size(record.value.as_ref()),
        }
    }
}

fn serialized_size(bytes: Option<&Bytes>) -> i32 {
    bytes
        .map(Bytes::len)
        .and_then(|len| i32::try_from(len).ok())
        .unwrap_or(-1)
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::expect_used,
        clippy::missing_assert_message,
        clippy::unwrap_used,
        reason = "Unit test fixtures fail fastest with contextual unwrap/expect calls."
    )]

    use std::{
        future::Future,
        pin::Pin,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        task::{Context, Poll, Wake, Waker},
    };

    use bytes::Bytes;

    use super::{
        DELIVERY_CLOSED, ProducerError, ProducerRecord, RecordHeader, RecordMetadata, SendFuture,
    };

    #[test]
    fn producer_record_rejects_invalid_java_fields() {
        assert!(ProducerRecord::try_new("orders", -1).is_err());
        assert!(
            ProducerRecord::unassigned("orders")
                .timestamp_ms(-1)
                .is_err()
        );
    }

    #[test]
    #[should_panic(expected = "partition must be non-negative")]
    fn producer_record_new_rejects_negative_partition_like_java() {
        let _record = ProducerRecord::new("orders", -1);
    }

    #[test]
    fn producer_record_retains_timestamp_and_headers() {
        let record = ProducerRecord::unassigned("orders")
            .try_timestamp_ms(1_700_000_000_000)
            .expect("timestamp")
            .header("trace-id", Bytes::from_static(b"abc"))
            .header_null("delete-me");

        assert_eq!(record.timestamp_ms, Some(1_700_000_000_000));
        assert_eq!(record.headers.len(), 2);
        assert_eq!(record.headers[0].key, Bytes::from_static(b"trace-id"));
        assert_eq!(record.headers[0].value, Some(Bytes::from_static(b"abc")));
        assert_eq!(record.headers[1].key, Bytes::from_static(b"delete-me"));
        assert_eq!(record.headers[1].value, None);
    }

    #[test]
    fn producer_record_convenience_constructors_match_java_overloads() {
        let value_only = ProducerRecord::topic_value("orders", Bytes::from_static(b"created"));
        assert_eq!(value_only.topic.as_ref(), "orders");
        assert_eq!(value_only.partition, super::UNASSIGNED_PARTITION);
        assert_eq!(value_only.key, None);
        assert_eq!(value_only.value, Some(Bytes::from_static(b"created")));

        let keyed = ProducerRecord::topic_key_value(
            "orders",
            Bytes::from_static(b"customer-42"),
            Bytes::from_static(b"created"),
        );
        assert_eq!(keyed.partition, super::UNASSIGNED_PARTITION);
        assert_eq!(keyed.key, Some(Bytes::from_static(b"customer-42")));
        assert_eq!(keyed.value, Some(Bytes::from_static(b"created")));

        let partitioned = ProducerRecord::partition_key_value(
            "orders",
            2,
            Bytes::from_static(b"customer-42"),
            Bytes::from_static(b"created"),
        );
        assert_eq!(partitioned.partition, 2);
        assert_eq!(partitioned.key, Some(Bytes::from_static(b"customer-42")));
        assert_eq!(partitioned.value, Some(Bytes::from_static(b"created")));

        let timestamped = ProducerRecord::try_partition_timestamp_key_value(
            "orders",
            2,
            1_700_000_000_000,
            Bytes::from_static(b"customer-42"),
            Bytes::from_static(b"created"),
        )
        .expect("timestamped record");
        assert_eq!(timestamped.partition, 2);
        assert_eq!(timestamped.timestamp_ms, Some(1_700_000_000_000));
        assert_eq!(timestamped.key, Some(Bytes::from_static(b"customer-42")));
        assert_eq!(timestamped.value, Some(Bytes::from_static(b"created")));

        let headers = [
            RecordHeader {
                key: Bytes::from_static(b"trace-id"),
                value: Some(Bytes::from_static(b"abc")),
            },
            RecordHeader {
                key: Bytes::from_static(b"delete-me"),
                value: None,
            },
        ];
        let with_headers = ProducerRecord::topic_value("orders", Bytes::from_static(b"created"))
            .with_headers(headers);
        assert_eq!(with_headers.headers().len(), 2);
        assert_eq!(
            with_headers.headers()[0].key,
            Bytes::from_static(b"trace-id")
        );
        assert_eq!(with_headers.headers()[1].value, None);
    }

    #[test]
    fn producer_record_headers_match_java_ordered_lookup_and_remove() {
        let record = ProducerRecord::unassigned("orders")
            .header("trace-id", Bytes::from_static(b"first"))
            .header("user", Bytes::from_static(b"42"))
            .header("trace-id", Bytes::from_static(b"last"))
            .header_null("trace-id");

        assert_eq!(
            record
                .last_header(b"trace-id")
                .expect("last trace header")
                .value,
            None
        );
        let trace_values: Vec<_> = record
            .headers_for_key(b"trace-id")
            .map(|header| header.value.clone())
            .collect();
        assert_eq!(
            trace_values,
            vec![
                Some(Bytes::from_static(b"first")),
                Some(Bytes::from_static(b"last")),
                None
            ]
        );

        let record = record.remove_headers(b"trace-id");

        assert!(record.last_header(b"trace-id").is_none());
        assert_eq!(record.headers().len(), 1);
        assert_eq!(record.headers()[0].key, Bytes::from_static(b"user"));
    }

    #[test]
    fn public_header_aliases_match_java_header_shape() {
        use crate::producer::{Header as PublicHeader, RecordHeader as PublicRecordHeader};

        let header: PublicHeader = PublicRecordHeader {
            key: Bytes::from_static(b"trace-id"),
            value: None,
        };
        let record = ProducerRecord::unassigned("orders").with_headers([header]);

        assert_eq!(
            record
                .last_header(b"trace-id")
                .expect("public header alias")
                .value,
            None
        );
    }

    #[test]
    fn public_record_headers_match_java_headers_collection_shape() {
        use crate::producer::{Headers as PublicHeaders, RecordHeaders as PublicRecordHeaders};

        let mut headers: PublicHeaders = PublicRecordHeaders::new();
        assert!(
            headers
                .add("trace-id", Bytes::from_static(b"first"))
                .is_ok()
        );
        assert!(headers.add("user", Bytes::from_static(b"42")).is_ok());
        assert!(headers.add("trace-id", Bytes::from_static(b"last")).is_ok());
        assert!(headers.add_null("trace-id").is_ok());

        assert_eq!(
            headers.last_header(b"trace-id").expect("last trace").value,
            None
        );
        let trace_values: Vec<_> = headers
            .headers(b"trace-id")
            .map(|header| header.value.clone())
            .collect();
        assert_eq!(
            trace_values,
            vec![
                Some(Bytes::from_static(b"first")),
                Some(Bytes::from_static(b"last")),
                None
            ]
        );

        let mut copied = headers.to_array();
        assert_eq!(copied.len(), 4);
        copied.clear();
        assert_eq!(headers.as_slice().len(), 4);

        assert!(headers.remove(b"trace-id").is_ok());

        assert!(headers.last_header(b"trace-id").is_none());
        assert_eq!(headers.as_slice().len(), 1);
        assert_eq!(headers.as_slice()[0].key, Bytes::from_static(b"user"));
    }

    #[test]
    fn public_record_headers_reject_mutation_when_read_only_like_java() {
        let mut headers = super::RecordHeaders::new();
        assert!(
            headers
                .add("trace-id", Bytes::from_static(b"first"))
                .is_ok()
        );
        headers.set_read_only();

        assert!(headers.is_read_only());
        assert!(matches!(
            headers.add("trace-id", Bytes::from_static(b"second")),
            Err(ProducerError::InvalidRecord {
                field: "headers",
                message: "headers are read-only"
            })
        ));
        assert!(matches!(
            headers.remove(b"trace-id"),
            Err(ProducerError::InvalidRecord {
                field: "headers",
                message: "headers are read-only"
            })
        ));
    }

    #[derive(Debug, Default)]
    struct WakeCounter {
        count: AtomicUsize,
    }

    impl Wake for WakeCounter {
        fn wake(self: Arc<Self>) {
            let _previous = self.count.fetch_add(1, Ordering::Relaxed);
        }

        fn wake_by_ref(self: &Arc<Self>) {
            let _previous = self.count.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[tokio::test]
    async fn delivery_sender_completes_multiple_batch_delivery_handles() {
        let (mut sender, first) = SendFuture::channel();
        let second = sender.delivery();
        let receipt = metadata(40);

        sender.send(&receipt);

        assert_eq!(first.await.unwrap().topic.as_ref(), "orders");
        assert_eq!(second.await.unwrap().offset, 41);
    }

    #[tokio::test]
    async fn delivery_sender_offsets_batch_handles_by_record_index() {
        let (mut sender, first) = SendFuture::channel();
        let second = sender.delivery();
        let third = sender.delivery();
        let receipt = metadata(40);

        sender.send(&receipt);

        assert_eq!(first.await.unwrap().offset, 40);
        assert_eq!(second.await.unwrap().offset, 41);
        assert_eq!(third.await.unwrap().offset, 42);
    }

    #[test]
    fn callback_counts_as_delivery_receiver_after_handle_is_dropped() {
        let (sender, delivery) = SendFuture::channel();
        let callback_base_offset = Arc::new(AtomicUsize::new(0));
        let callback_sink = Arc::clone(&callback_base_offset);
        delivery.register_callback(Box::new(move |result| {
            let receipt = result.expect("callback receipt");
            callback_sink.store(
                usize::try_from(receipt.offset).expect("non-negative test offset"),
                Ordering::Relaxed,
            );
        }));

        drop(delivery);

        assert!(sender.has_receivers());
        sender.send(&metadata(40));
        assert_eq!(callback_base_offset.load(Ordering::Relaxed), 40);
    }

    #[tokio::test]
    async fn delivery_callback_panic_is_ignored_like_java() {
        let (sender, delivery) = SendFuture::channel();
        delivery.register_callback(Box::new(|_result| {
            panic!("callback panic should not escape delivery completion");
        }));

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            sender.send(&metadata(41));
        }));

        assert!(result.is_ok());
        assert_eq!(
            delivery.await.expect("delivery remains completed").offset,
            41
        );
    }

    #[tokio::test]
    async fn dropped_delivery_sender_wakes_handles_with_error() {
        let (sender, delivery) = SendFuture::channel();

        drop(sender);

        assert!(matches!(
            delivery.await.unwrap_err(),
            ProducerError::DeliveryDropped
        ));
    }

    #[test]
    #[cfg_attr(tarpaulin, ignore = "timing-sensitive under coverage instrumentation")]
    fn pending_delivery_registers_one_waker_and_sender_wakes_it() {
        let (sender, mut delivery) = SendFuture::channel();
        let counter = Arc::new(WakeCounter::default());
        let waker = Waker::from(Arc::clone(&counter));
        let mut context = Context::from_waker(&waker);

        assert!(matches!(
            Pin::new(&mut delivery).poll(&mut context),
            Poll::Pending
        ));
        assert!(matches!(
            Pin::new(&mut delivery).poll(&mut context),
            Poll::Pending
        ));

        sender.send(&metadata(40));

        assert_eq!(counter.count.load(Ordering::Relaxed), 1);
        assert!(matches!(
            Pin::new(&mut delivery).poll(&mut context),
            Poll::Ready(Ok(RecordMetadata { offset: 40, .. }))
        ));
    }

    #[tokio::test]
    async fn delivery_sender_preserves_per_record_serialized_sizes() {
        let first_record = ProducerRecord::new("orders", 0)
            .key(Bytes::from_static(b"k1"))
            .value(Bytes::from_static(b"value-1"));
        let second_record = ProducerRecord::new("orders", 0).value(Bytes::from_static(b"v2"));
        let (mut sender, first) = SendFuture::channel_for_record(&first_record);
        let second = sender.delivery_for_record(&second_record);

        sender.send(&metadata(40));

        let first = first.await.expect("first metadata");
        let second = second.await.expect("second metadata");
        assert_eq!(first.offset, 40);
        assert_eq!(first.serialized_key_size, 2);
        assert_eq!(first.serialized_value_size, 7);
        assert_eq!(second.offset, 41);
        assert_eq!(second.serialized_key_size, -1);
        assert_eq!(second.serialized_value_size, 2);
    }

    #[test]
    fn pending_delivery_is_woken_when_sender_is_dropped() {
        let (sender, mut delivery) = SendFuture::channel();
        let counter = Arc::new(WakeCounter::default());
        let waker = Waker::from(Arc::clone(&counter));
        let mut context = Context::from_waker(&waker);

        assert!(matches!(
            Pin::new(&mut delivery).poll(&mut context),
            Poll::Pending
        ));

        drop(sender);

        assert_eq!(counter.count.load(Ordering::Relaxed), 1);
        assert!(matches!(
            Pin::new(&mut delivery).poll(&mut context),
            Poll::Ready(Err(ProducerError::DeliveryDropped))
        ));
    }

    #[test]
    fn dropping_sender_is_noop_when_delivery_is_already_closed() {
        let (sender, _delivery) = SendFuture::channel();
        sender
            .state
            .status
            .store(DELIVERY_CLOSED, Ordering::Release);

        drop(sender);
    }

    fn metadata(offset: i64) -> RecordMetadata {
        RecordMetadata {
            topic: Arc::from("orders"),
            partition: 0,
            leader_id: 7,
            offset,
            timestamp_ms: -1,
            serialized_key_size: -1,
            serialized_value_size: -1,
        }
    }
}
