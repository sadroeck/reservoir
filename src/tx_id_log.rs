use crate::tx_id::TransactionId;
use event_listener::Event;
use flume::Sender;
use std::sync::Arc;
use std::time::Duration;
use tracing::info;

const MAX_ID_LOG_BUFFER_SIZE: usize = 4096;

/// A cheap to clone handle to commit serialized transaction IDs to the secondary log.
/// A handle can submit a single TX ID to the log. If `SYNC` is set to `true`, the handle will
/// wait for durable commit to storage of the TX ID before returning. If `SYNC` is set to `false`,
/// the ID will be durably committed in the background.
#[derive(Clone)]
pub struct DamControl<const SYNC: bool> {
    tx: Sender<TransactionId>,
    event: Arc<Event>,
}

impl<const SYNC: bool> DamControl<SYNC> {
    pub async fn commit(&self, id: TransactionId) {
        // Ignore any send error, as this can only happen when we've closed the channel.
        let _ = self.tx.send_async(id).await;

        if SYNC {
            // Wait for the next sync point
            // TODO: prevent an allocation here
            self.event.listen().await;
        }
    }
}

/// Task to write the committed transaction IDs to the secondary log,
/// on a dedicated interval in the background.
pub struct DamFlusher<const SYNC: bool>;

impl<const SYNC: bool> DamFlusher<SYNC> {
    pub fn run(flush_interval: Duration) -> DamControl<SYNC> {
        let (tx, rx) = flume::bounded(MAX_ID_LOG_BUFFER_SIZE);
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async move {
            let mut interval = tokio::time::interval(flush_interval);
            loop {
                interval.tick().await;
                'ids: loop {
                    match rx.try_recv() {
                        Ok(id) => info!("Flushing transaction ID: {id}"),
                        Err(flume::TryRecvError::Empty) => continue 'ids,
                        Err(flume::TryRecvError::Disconnected) => return,
                    }
                }
            }
        });
        DamControl {
            tx,
            event: Arc::new(Default::default()),
        }
    }
}
