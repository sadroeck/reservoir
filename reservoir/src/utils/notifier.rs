use std::future::{ready, Future};
use std::pin::Pin;
use std::sync::Arc;

pub use event_listener::{Event, EventListener, IntoNotification};

pub trait FlushNotifier: Clone + Send + Sync + 'static {
    type Waiter: Future<Output = ()> + Send;

    fn new() -> Self;

    /// Notify [`count`] waiters that a storage flush has occurred.
    fn notify_transactions(&self, count: usize);

    /// Retrieves a handle to wait for the sync event.
    fn get_waiter(&self) -> Self::Waiter;
}

#[derive(Clone, Default)]
pub struct NopNotifier;

impl FlushNotifier for NopNotifier {
    type Waiter = std::future::Ready<()>;

    fn new() -> Self {
        Self
    }

    fn notify_transactions(&self, _count: usize) {
        // Do nothing
    }

    fn get_waiter(&self) -> Self::Waiter {
        ready(())
    }
}

// TODO: Replace with simpler signaling mechanism just using the offset of the fsync
impl FlushNotifier for Arc<Event> {
    type Waiter = Pin<Box<EventListener<()>>>;

    fn new() -> Self {
        Arc::new(Event::new())
    }

    fn notify_transactions(&self, count: usize) {
        self.notify(count.relaxed());
    }

    fn get_waiter(&self) -> Self::Waiter {
        self.as_ref().listen()
    }
}
