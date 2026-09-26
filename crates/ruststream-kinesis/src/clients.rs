//! One SDK client per runtime: [`RuntimeClients`].
//!
//! The SDK's HTTP client opens a connection on the runtime of the request that needs it (the
//! connection's driver task is spawned there and its socket registers with that runtime's
//! reactor), and every clone of a client shares one pool. A connection opened from a handler's
//! dedicated thread would therefore sit in the pool the broker's runtime draws from, and a request
//! from the broker's runtime that picked it up would wait until the dedicated thread yields. The
//! SDK keeps its executor private, so the pool cannot be told where to spawn; instead each runtime
//! gets a client of its own, and a connection only ever serves the runtime that opened it.
//!
//! The home runtime keeps the client built at connect, so a multi-thread runtime shares one pool
//! across its workers. Any other runtime gets a client built for its thread, once, from the same
//! [`SdkConfig`]: the credentials provider and its cache are shared, only the HTTP client and its
//! pool differ.

use std::cell::RefCell;
use std::fmt;
use std::rc::Rc;
use std::sync::{Arc, OnceLock, Weak};
use std::thread::LocalKey;

use aws_config::SdkConfig;
use tokio::runtime::{Handle, Id};

/// What this thread uses for one [`RuntimeClients`]: `None` is the home client, `Some` the
/// thread's own.
pub(crate) struct Slot<Client> {
    /// Names the [`RuntimeClients`] this slot belongs to. A weak reference keeps the allocation's
    /// address from being reused while the slot exists, so two brokers never share a slot, and it
    /// tells a slot whose broker is gone.
    owner: Weak<()>,
    /// Counted without atomics, so a lookup hands the client out of the thread's slots and builds
    /// the request outside their borrow.
    own: Option<Rc<Client>>,
}

/// An SDK client a thread can keep a copy of.
pub(crate) trait SdkClient: Sized + 'static {
    /// Builds a client with a pool of its own from the shared configuration.
    fn build(config: &SdkConfig) -> Self;

    /// This client type's per-thread slots.
    fn slots() -> &'static LocalKey<RefCell<Vec<Slot<Self>>>>;
}

macro_rules! sdk_client {
    ($client:ty) => {
        impl SdkClient for $client {
            fn build(config: &SdkConfig) -> Self {
                Self::new(config)
            }

            fn slots() -> &'static LocalKey<RefCell<Vec<Slot<Self>>>> {
                thread_local! {
                    static SLOTS: RefCell<Vec<Slot<$client>>> = const { RefCell::new(Vec::new()) };
                }
                &SLOTS
            }
        }
    };
}

sdk_client!(aws_sdk_kinesis::Client);
#[cfg(feature = "dynamodb-lease")]
sdk_client!(aws_sdk_dynamodb::Client);

/// The SDK client a request uses, chosen by the runtime the request runs on.
///
/// Per request this costs one thread-local lookup, plus a non-atomic count on a thread's own
/// client: the first request of a thread asks tokio for the current runtime (a handle clone, one
/// atomic pair, compared by [`Handle::id`]) and the answer stays in the thread's slot, so later
/// requests touch no atomic and allocate nothing. A client is built once per thread that is not on
/// the home runtime.
///
/// A thread is taken to stay on the runtime it first made a request from, which holds for a
/// runtime's worker threads and for a dedicated handler thread alike.
pub(crate) struct RuntimeClients<Client> {
    home: OnceLock<Id>,
    client: Client,
    config: SdkConfig,
    token: Arc<()>,
}

impl<Client: SdkClient> RuntimeClients<Client> {
    /// A selection whose home is the runtime `runtime` names.
    pub(crate) fn homed(config: SdkConfig, runtime: &Handle) -> Self {
        let clients = Self::new(config);
        clients.home.get_or_init(|| runtime.id());
        clients
    }

    /// A selection whose home is the runtime of its first request.
    ///
    /// Which runtime is home decides only which one shares the client built here; correctness does
    /// not depend on it, since every other runtime gets a pool of its own either way.
    pub(crate) fn new(config: SdkConfig) -> Self {
        Self {
            home: OnceLock::new(),
            client: Client::build(&config),
            config,
            token: Arc::new(()),
        }
    }

    /// The client built with this selection, for tasks that run on the home runtime by
    /// construction.
    pub(crate) const fn home(&self) -> &Client {
        &self.client
    }

    /// Runs `request` with the client of the runtime this call runs on.
    ///
    /// `request` builds the operation (the SDK's fluent builder takes its own reference to the
    /// client), so no client is cloned here.
    pub(crate) fn with<Output>(&self, request: impl FnOnce(&Client) -> Output) -> Output {
        let owner = Arc::as_ptr(&self.token);
        let found = Client::slots().try_with(|slots| {
            let mut slots = slots.borrow_mut();
            if let Some(slot) = slots.iter().find(|slot| slot.owner.as_ptr() == owner) {
                return Some(slot.own.clone());
            }
            // Outside any runtime there is nothing to bind a connection to, and nothing to
            // remember: the home client serves, and the next call inside a runtime decides.
            let current = Handle::try_current().ok()?.id();
            let own = (*self.home.get_or_init(|| current) != current)
                .then(|| Rc::new(Client::build(&self.config)));
            // Slots of brokers that are gone would keep their clients alive for the thread's life.
            slots.retain(|slot| slot.owner.strong_count() > 0);
            slots.push(Slot {
                owner: Arc::downgrade(&self.token),
                own: own.clone(),
            });
            Some(own)
        });
        match found {
            Ok(Some(Some(own))) => request(&own),
            // The home runtime, no runtime, or a thread tearing down its locals.
            _ => request(&self.client),
        }
    }
}

impl<Client> fmt::Debug for RuntimeClients<Client> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RuntimeClients")
            .field("home", &self.home.get())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::ptr;
    use std::thread;

    use aws_config::BehaviorVersion;
    use tokio::runtime::{Builder, Runtime};

    use super::*;

    type Kinesis = aws_sdk_kinesis::Client;

    fn config() -> SdkConfig {
        SdkConfig::builder()
            .behavior_version(BehaviorVersion::latest())
            .build()
    }

    fn current_thread() -> Runtime {
        Builder::new_current_thread()
            .build()
            .expect("a current-thread runtime builds")
    }

    /// Which client a request made here would use.
    fn chosen(clients: &RuntimeClients<Kinesis>) -> *const Kinesis {
        clients.with(ptr::from_ref)
    }

    /// Every worker of the runtime `connect` ran on uses the client built at connect: one pool for
    /// the whole runtime.
    #[test]
    fn the_home_runtime_uses_the_connect_client_on_every_worker() {
        let runtime = Builder::new_multi_thread()
            .worker_threads(2)
            .build()
            .expect("a multi-thread runtime builds");
        let clients = Arc::new(RuntimeClients::<Kinesis>::homed(config(), runtime.handle()));
        let home = ptr::from_ref(clients.home()) as usize;
        let workers = runtime.block_on(async {
            let tasks: Vec<_> = (0..8)
                .map(|_| {
                    let clients = Arc::clone(&clients);
                    tokio::spawn(async move { chosen(&clients) as usize })
                })
                .collect();
            let mut seen = Vec::new();
            for task in tasks {
                seen.push(task.await.expect("the task runs"));
            }
            seen
        });
        assert!(workers.iter().all(|&client| client == home));
    }

    /// A thread on another runtime builds a client of its own, once, and keeps it.
    #[test]
    fn another_runtime_gets_its_own_client_once() {
        let home_runtime = current_thread();
        let clients = RuntimeClients::<Kinesis>::homed(config(), home_runtime.handle());
        let home = ptr::from_ref(clients.home()) as usize;
        assert_eq!(
            home_runtime.block_on(async { chosen(&clients) as usize }),
            home
        );

        let (first, second) = thread::scope(|scope| {
            scope
                .spawn(|| {
                    let runtime = current_thread();
                    let first = runtime.block_on(async { chosen(&clients) as usize });
                    let second = runtime.block_on(async { chosen(&clients) as usize });
                    (first, second)
                })
                .join()
                .expect("the other thread finishes")
        });
        assert_ne!(first, home);
        assert_eq!(first, second);
    }

    /// Two brokers seen from one thread keep separate slots: neither uses the other's client.
    #[test]
    fn two_selections_on_one_thread_do_not_share_a_slot() {
        let home_runtime = current_thread();
        let one = RuntimeClients::<Kinesis>::homed(config(), home_runtime.handle());
        let two = RuntimeClients::<Kinesis>::homed(config(), home_runtime.handle());
        let (from_one, from_two) = thread::scope(|scope| {
            scope
                .spawn(|| {
                    let runtime = current_thread();
                    runtime.block_on(async { (chosen(&one) as usize, chosen(&two) as usize) })
                })
                .join()
                .expect("the other thread finishes")
        });
        assert_ne!(from_one, from_two);
        assert_ne!(from_one, ptr::from_ref(one.home()) as usize);
        assert_ne!(from_two, ptr::from_ref(two.home()) as usize);
    }

    /// Without a fixed home, the runtime of the first request shares the client built up front.
    #[test]
    fn an_unhomed_selection_is_homed_by_its_first_request() {
        let clients = RuntimeClients::<Kinesis>::new(config());
        let home = ptr::from_ref(clients.home()) as usize;
        let runtime = current_thread();
        assert_eq!(runtime.block_on(async { chosen(&clients) as usize }), home);
        let other = thread::scope(|scope| {
            scope
                .spawn(|| current_thread().block_on(async { chosen(&clients) as usize }))
                .join()
                .expect("the other thread finishes")
        });
        assert_ne!(other, home);
    }
}
