//! [`KinesisTestBroker`]: the in-process transport and its connected form.

use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use ruststream::testing::{Coordinator, TestableBroker};
use ruststream::{
    Broker, ConnectedBroker, DefaultPublish, OutgoingMessage, PairError, PublishPolicy, Publisher,
    RawMessage, Subscribe,
};

use crate::error::KinesisError;
use crate::testing::router::AddressRouter;
use crate::testing::subscriber::KinesisTestSubscriber;

/// Shared state of one in-process broker: the router plus the harness coordinator.
#[derive(Debug, Default)]
pub(crate) struct TestState {
    pub(crate) router: AddressRouter,
    coordinator: OnceLock<Coordinator>,
}

impl TestState {
    fn coordinator(&self) -> Option<&Coordinator> {
        self.coordinator.get()
    }

    pub(crate) fn publish(&self, name: &str, payload: Bytes, headers: ruststream::Headers) {
        self.router
            .publish(name, payload, headers, self.coordinator());
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
        }
    }
}

impl Broker for KinesisTestBroker {
    type Error = KinesisError;
    type Connected = ConnectedKinesisTestBroker;

    async fn connect(self) -> Result<Self::Connected, Self::Error> {
        Ok(ConnectedKinesisTestBroker { state: self.state })
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
        }
    }
}

impl ConnectedBroker for ConnectedKinesisTestBroker {
    type Error = KinesisError;
    type Closed = ();

    async fn shutdown(self) -> Result<(), Self::Error> {
        self.state.router.clear();
        Ok(())
    }
}

impl Subscribe for ConnectedKinesisTestBroker {
    type Subscriber = KinesisTestSubscriber;

    async fn subscribe(&self, name: &str) -> Result<Self::Subscriber, Self::Error> {
        let (id, requeue, rx) = self.state.router.subscribe(name.to_owned());
        Ok(KinesisTestSubscriber::new(
            Arc::clone(&self.state),
            id,
            rx,
            requeue,
            self.state.coordinator().cloned(),
        ))
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
}

impl Publisher for KinesisTestPublisher {
    type Error = KinesisError;

    async fn publish(&self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error> {
        self.state.publish(
            msg.name(),
            Bytes::copy_from_slice(msg.payload()),
            msg.headers().clone(),
        );
        Ok(())
    }
}

/// The publish policy for [`KinesisTestPublisher`], mirroring
/// [`KinesisPublish`](crate::KinesisPublish) on the real broker.
///
/// # Examples
///
/// ```
/// use ruststream_kinesis::testing::KinesisTestPublish;
///
/// let policy = KinesisTestPublish::default();
/// # let _ = policy;
/// ```
#[derive(Debug, Clone, Copy, Default)]
#[must_use]
pub struct KinesisTestPublish;

impl PublishPolicy<ConnectedKinesisTestBroker> for KinesisTestPublish {
    type Live = KinesisTestPublisher;

    async fn pair(self, connected: &ConnectedKinesisTestBroker) -> Result<Self::Live, PairError> {
        Ok(connected.publisher())
    }
}

impl DefaultPublish for ConnectedKinesisTestBroker {
    type Policy = KinesisTestPublish;
}
