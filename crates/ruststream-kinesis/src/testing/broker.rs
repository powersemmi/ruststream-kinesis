//! [`KinesisTestBroker`]: the in-process transport and its connected form.

use std::future::{Future, ready};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use ruststream::testing::{Coordinator, TestableBroker};
use ruststream::{
    Broker, ConnectedBroker, DefaultPublish, HeaderMap, OutgoingMessage, Publisher, RawMessage,
    Subscribe,
};

use crate::error::KinesisError;
use crate::message::PARTITION_KEY_HEADER;
use crate::publisher::KinesisPublish;
use crate::testing::router::AddressRouter;
use crate::testing::subscriber::KinesisTestSubscriber;

/// Shared state of one in-process broker: the retained log and its subscriptions, plus the
/// harness coordinator.
#[derive(Debug, Default)]
pub(crate) struct TestState {
    pub(crate) router: AddressRouter,
    coordinator: OnceLock<Coordinator>,
    /// Set by `shutdown`. A seek handle a handler holds is an aliasing handle, and the real
    /// broker's aliasing handles report `NotConnected` after shutdown rather than succeeding
    /// against a dead connection; the stand-in reports the same.
    closed: AtomicBool,
}

impl TestState {
    pub(crate) fn coordinator(&self) -> Option<&Coordinator> {
        self.coordinator.get()
    }

    pub(crate) fn publish(&self, name: &str, payload: Bytes, headers: HeaderMap) {
        self.router
            .publish(name, payload, headers, self.coordinator());
    }

    pub(crate) fn ensure_open(&self) -> Result<(), KinesisError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(KinesisError::NotConnected);
        }
        Ok(())
    }
}

/// An in-process stand-in for [`KinesisBroker`](crate::KinesisBroker): same core routing, no server.
///
/// # Examples
///
/// ```
/// use ruststream_kinesis::testing::KinesisTestBroker;
///
/// let broker = KinesisTestBroker::new();
/// # let _ = broker;
/// ```
#[derive(Debug, Clone, Default)]
#[must_use]
pub struct KinesisTestBroker {
    state: Arc<TestState>,
}

impl KinesisTestBroker {
    /// Creates an empty in-process broker. Synchronous and I/O-free, like the real `new`.
    pub fn new() -> Self {
        Self::default()
    }

    /// A publisher usable before `connect`, mirroring the real broker's early-publisher path.
    #[must_use]
    pub fn publisher(&self) -> KinesisTestPublisher {
        KinesisTestPublisher {
            state: Arc::clone(&self.state),
            partition_key: None,
        }
    }
}

impl Broker for KinesisTestBroker {
    type Error = KinesisError;
    type Connected = ConnectedKinesisTestBroker;

    fn connect(self) -> impl Future<Output = Result<Self::Connected, Self::Error>> {
        ready(Ok(ConnectedKinesisTestBroker { state: self.state }))
    }
}

/// The connected form of [`KinesisTestBroker`]; implements
/// [`TestableBroker`](ruststream::testing::TestableBroker) for the harness and the conformance
/// suite.
#[derive(Debug, Clone)]
pub struct ConnectedKinesisTestBroker {
    state: Arc<TestState>,
}

impl ConnectedKinesisTestBroker {
    /// A publisher from the connected form.
    #[must_use]
    pub fn publisher(&self) -> KinesisTestPublisher {
        KinesisTestPublisher {
            state: Arc::clone(&self.state),
            partition_key: None,
        }
    }

    /// The same publisher carrying the partition key a mount site named on its policy. This is
    /// what [`KinesisPublish`](crate::KinesisPublish) pairs into here, so the policy a service
    /// ships is the policy its unit tests mount.
    pub(crate) fn publisher_keyed(&self, partition_key: Option<Arc<str>>) -> KinesisTestPublisher {
        KinesisTestPublisher {
            state: Arc::clone(&self.state),
            partition_key,
        }
    }

    /// Opens a subscription on `name`. The subscription starts at the tip of the retained log,
    /// which is where a Kinesis shard without a checkpoint starts; `start_at(..)` and the seek
    /// handle move it from there.
    pub(crate) fn open(&self, name: &str) -> KinesisTestSubscriber {
        KinesisTestSubscriber::new(Arc::clone(&self.state), name)
    }
}

impl ConnectedBroker for ConnectedKinesisTestBroker {
    type Error = KinesisError;
    type Closed = ();

    fn shutdown(self) -> impl Future<Output = Result<(), Self::Error>> {
        self.state.closed.store(true, Ordering::Release);
        self.state.router.clear();
        ready(Ok(()))
    }
}

impl Subscribe for ConnectedKinesisTestBroker {
    type Subscriber = KinesisTestSubscriber;

    fn subscribe(&self, name: &str) -> impl Future<Output = Result<Self::Subscriber, Self::Error>> {
        ready(Ok(self.open(name)))
    }
}

impl TestableBroker for ConnectedKinesisTestBroker {
    fn install_coordinator(&self, coordinator: Coordinator) {
        let _ = self.state.coordinator.set(coordinator);
    }

    fn inject(&self, message: OutgoingMessage<'_>) {
        self.state.publish(
            message.name(),
            Bytes::copy_from_slice(message.payload()),
            message.headers().clone(),
        );
    }

    fn published(&self, name: &str) -> Vec<RawMessage> {
        self.state.router.published(name)
    }
}

ruststream::register_testable_broker!(ConnectedKinesisTestBroker);

/// Publisher for the in-process broker.
#[derive(Debug, Clone)]
pub struct KinesisTestPublisher {
    state: Arc<TestState>,
    /// The partition key the mount site named on the policy, exactly as on the real publisher.
    partition_key: Option<Arc<str>>,
}

impl Publisher for KinesisTestPublisher {
    type Error = KinesisError;

    fn publish(&self, msg: OutgoingMessage<'_>) -> impl Future<Output = Result<(), Self::Error>> {
        let mut headers = msg.headers().clone();
        // The service carries the key in the record's own field and this transport carries it in
        // the header its deliveries read, so a record that names none takes the publisher's key
        // here - the same ladder, at the same point of the publish.
        if let Some(key) = &self.partition_key
            && !headers.contains(PARTITION_KEY_HEADER)
        {
            headers.insert(PARTITION_KEY_HEADER, key.to_string());
        }
        self.state
            .publish(msg.name(), Bytes::copy_from_slice(msg.payload()), headers);
        ready(Ok(()))
    }
}

impl crate::sealed::Sealed for KinesisTestPublisher {}
impl crate::KinesisPublishExt for KinesisTestPublisher {}

/// The stand-in replies through the crate's own policy, so a mount that names no publisher gets
/// [`KinesisPublish`] here exactly as it does against the service.
impl DefaultPublish for ConnectedKinesisTestBroker {
    type Policy = KinesisPublish;
}
