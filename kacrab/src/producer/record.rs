//! Public producer record, delivery, and receipt types.

use core::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use std::{
    sync::{Arc, Mutex},
    task::Waker,
};

use bytes::Bytes;

use super::{ProducerError, Result};

/// Sentinel used by [`ProducerRecord::unassigned`] before metadata-based
/// partitioning selects a concrete topic partition.
pub(crate) const UNASSIGNED_PARTITION: i32 = -1;

/// One record targeted at an explicit topic partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProducerRecord {
    /// Topic name.
    pub topic: Arc<str>,
    /// Partition index.
    pub partition: i32,
    /// Optional record key.
    pub key: Option<Bytes>,
    /// Optional record value.
    pub value: Option<Bytes>,
}

impl ProducerRecord {
    /// Create a producer record for an explicit topic partition.
    pub fn new(topic: impl Into<Arc<str>>, partition: i32) -> Self {
        Self {
            topic: topic.into(),
            partition,
            key: None,
            value: None,
        }
    }

    /// Create a producer record whose partition will be selected from metadata.
    pub fn unassigned(topic: impl Into<Arc<str>>) -> Self {
        Self::new(topic, UNASSIGNED_PARTITION)
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
}

/// Produce acknowledgement for one partition append.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProduceReceipt {
    /// Topic name.
    pub topic: String,
    /// Partition index.
    pub partition: i32,
    /// Leader broker id used for routing.
    pub leader_id: i32,
    /// Base offset returned by the broker.
    pub base_offset: i64,
    /// Broker append timestamp, or `-1` for create-time topics.
    pub log_append_time_ms: i64,
}

/// Future-like delivery handle returned by [`crate::producer::KafkaProducer::send`].
#[derive(Debug)]
pub struct Delivery {
    state: Arc<DeliveryState>,
}

#[derive(Debug)]
pub(crate) struct DeliverySender {
    state: Arc<DeliveryState>,
    completed: bool,
}

#[derive(Debug, Default)]
struct DeliveryState {
    inner: Mutex<DeliveryStateInner>,
}

#[derive(Debug, Default)]
struct DeliveryStateInner {
    receipt: Option<ProduceReceipt>,
    closed: bool,
    wakers: Vec<Waker>,
}

impl Delivery {
    pub(crate) fn channel() -> (DeliverySender, Self) {
        let state = Arc::new(DeliveryState::default());
        (
            DeliverySender {
                state: Arc::clone(&state),
                completed: false,
            },
            Self { state },
        )
    }
}

impl DeliverySender {
    pub(crate) fn delivery(&self) -> Delivery {
        Delivery {
            state: Arc::clone(&self.state),
        }
    }

    pub(crate) fn has_receivers(&self) -> bool {
        Arc::strong_count(&self.state) > 1
    }

    pub(crate) fn send(mut self, receipt: ProduceReceipt) {
        let wakers = {
            let mut inner = self
                .state
                .inner
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            inner.receipt = Some(receipt);
            core::mem::take(&mut inner.wakers)
        };
        self.completed = true;
        for waker in wakers {
            waker.wake();
        }
    }
}

impl Drop for DeliverySender {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        let wakers = {
            let mut inner = self
                .state
                .inner
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if inner.receipt.is_some() || inner.closed {
                return;
            }
            inner.closed = true;
            core::mem::take(&mut inner.wakers)
        };
        for waker in wakers {
            waker.wake();
        }
    }
}

impl Future for Delivery {
    type Output = Result<ProduceReceipt>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut inner = self
            .state
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(receipt) = &inner.receipt {
            return Poll::Ready(Ok(receipt.clone()));
        }
        if inner.closed {
            return Poll::Ready(Err(ProducerError::DeliveryDropped));
        }
        if !inner.wakers.iter().any(|waker| waker.will_wake(cx.waker())) {
            inner.wakers.push(cx.waker().clone());
        }
        Poll::Pending
    }
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

    use super::{Delivery, ProduceReceipt, ProducerError};

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
        let (sender, first) = Delivery::channel();
        let second = sender.delivery();
        let receipt = ProduceReceipt {
            topic: "orders".to_owned(),
            partition: 0,
            leader_id: 7,
            base_offset: 40,
            log_append_time_ms: -1,
        };

        sender.send(receipt);

        assert_eq!(first.await.unwrap().topic, "orders");
        assert_eq!(second.await.unwrap().base_offset, 40);
    }

    #[tokio::test]
    async fn dropped_delivery_sender_wakes_handles_with_error() {
        let (sender, delivery) = Delivery::channel();

        drop(sender);

        assert!(matches!(
            delivery.await.unwrap_err(),
            ProducerError::DeliveryDropped
        ));
    }

    #[test]
    fn pending_delivery_registers_one_waker_and_sender_wakes_it() {
        let (sender, mut delivery) = Delivery::channel();
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

        sender.send(ProduceReceipt {
            topic: "orders".to_owned(),
            partition: 0,
            leader_id: 7,
            base_offset: 40,
            log_append_time_ms: -1,
        });

        assert_eq!(counter.count.load(Ordering::Relaxed), 1);
        assert!(matches!(
            Pin::new(&mut delivery).poll(&mut context),
            Poll::Ready(Ok(ProduceReceipt {
                base_offset: 40,
                ..
            }))
        ));
    }

    #[test]
    fn pending_delivery_is_woken_when_sender_is_dropped() {
        let (sender, mut delivery) = Delivery::channel();
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
        let (sender, _delivery) = Delivery::channel();
        {
            let mut inner = sender
                .state
                .inner
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            inner.closed = true;
        }

        drop(sender);
    }
}
