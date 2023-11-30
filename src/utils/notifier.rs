use event_listener::{Event, EventListener, IntoNotification};
use std::future::{ready, Future};
use std::pin::Pin;
use std::sync::Arc;

pub trait SyncNotifier: Clone + Send + 'static {
    type NotificationWaiter: Future<Output = ()>;

    fn new() -> Self;

    /// Notify [`count`] waiters that a sync has occurred.
    fn notify_transactions(&self, count: usize);

    /// Retrieves a handle to wait for the sync event.
    fn get_waiter(&self) -> Self::NotificationWaiter;
}

#[derive(Clone, Default)]
pub struct NopNotifier;

impl SyncNotifier for NopNotifier {
    type NotificationWaiter = std::future::Ready<()>;

    fn new() -> Self {
        Self
    }

    fn notify_transactions(&self, _count: usize) {
        // Do nothing
    }

    fn get_waiter(&self) -> Self::NotificationWaiter {
        ready(())
    }
}

impl SyncNotifier for Arc<Event> {
    type NotificationWaiter = Pin<Box<EventListener<()>>>;

    fn new() -> Self {
        Arc::new(Event::new())
    }

    fn notify_transactions(&self, count: usize) {
        self.notify(count.relaxed());
    }

    fn get_waiter(&self) -> Self::NotificationWaiter {
        self.as_ref().listen()
    }
}
