use crate::tx_id::TransactionId;
use crate::utils::{FixedBuffer, FixedBufferError};
use event_listener::Event;
use flume::{Receiver, Sender, TryRecvError};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use tracing::trace;

const MAX_PENDING_TRANSACTIONS: usize = 4096;

#[derive(Debug, Copy, Clone)]
pub struct SerializedTransaction {
    pub id: TransactionId,
    pub size: usize,
    pub crc: u32,
}

impl Default for SerializedTransaction {
    fn default() -> Self {
        Self {
            id: TransactionId(u64::MAX),
            size: 0,
            crc: 0,
        }
    }
}

/// A cheap to clone handle to commit serialized transaction IDs to the secondary log.
/// A handle can submit a single transaction ID to the log. If [`SYNC`] is set to `true`, the handle will
/// wait for durable commit to storage of the transaction ID before returning.
/// If [`SYNC`] is set to `false`, the ID will be durably committed in the background.
#[derive(Clone)]
pub struct DamControl<const SYNC: bool> {
    tx: Sender<SerializedTransaction>,
    event: Arc<Event>,
}

impl<const SYNC: bool> DamControl<SYNC> {
    pub async fn commit(&self, id: TransactionId, crc: u32, size: usize) {
        // Ignore any send error, as this can only happen when we've closed the channel.
        let _ = self
            .tx
            .send_async(SerializedTransaction { id, crc, size })
            .await;

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

pub type SyncDamFlusher = DamFlusher<true>;
pub type AsyncDamFlusher = DamFlusher<false>;

impl<const SYNC: bool> DamFlusher<SYNC> {
    /// Spawns a background [`tokio::task`] to flush committed transaction IDs to the secondary log.
    /// Note: this requires a running [`tokio`] runtime.
    pub fn spawn_task(flush_interval: Duration) -> (JoinHandle<()>, DamControl<SYNC>) {
        let (tx, rx) = flume::bounded(MAX_PENDING_TRANSACTIONS);
        let task = tokio::spawn(Self::run(flush_interval, rx));
        (
            task,
            DamControl {
                tx,
                event: Arc::new(Default::default()),
            },
        )
    }

    pub fn spawn_thread(
        flush_interval: Duration,
    ) -> (std::thread::JoinHandle<()>, DamControl<SYNC>) {
        let (tx, rx) = flume::bounded(MAX_PENDING_TRANSACTIONS);
        let thread = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(Self::run(flush_interval, rx));
        });
        (
            thread,
            DamControl {
                tx,
                event: Arc::new(Default::default()),
            },
        )
    }

    fn write_current_txs(
        txs: &mut FixedBuffer<SerializedTransaction, MAX_PENDING_TRANSACTIONS>,
        rx: &Receiver<SerializedTransaction>,
    ) -> Result<(), TryRecvError> {
        // Drain the channel
        loop {
            match rx.try_recv() {
                Ok(txn) => match txs.push(txn) {
                    Ok(()) => {}
                    Err(FixedBufferError::BufferOverflow) => {
                        // We've filled up the buffer, so we need to flush it
                        Self::flush_transactions(txs);
                    }
                },
                Err(TryRecvError::Empty) => {
                    // There are no more transactions to drain, so flush & return.
                    Self::flush_transactions(txs);
                    return Ok(());
                }
                Err(TryRecvError::Disconnected) => return Err(TryRecvError::Disconnected),
            }
        }
    }

    fn flush_transactions(txs: &mut FixedBuffer<SerializedTransaction, MAX_PENDING_TRANSACTIONS>) {
        trace!(count = %txs.len(), "Flushing txs");
        txs.clear();
    }

    /// Flushes the committed transaction IDs to the secondary log.
    /// Note: in [`SYNC`] mode, this will flush all pending transaction IDs immediately.
    /// In non-[`SYNC`] mode, this will batch up all pending transaction IDs and flush them on the next
    /// interval.
    pub async fn run(flush_interval: Duration, rx: Receiver<SerializedTransaction>) {
        // TODO: Implement me
        let mut buffered_txs =
            FixedBuffer::<SerializedTransaction, MAX_PENDING_TRANSACTIONS>::new();

        if SYNC {
            loop {
                match rx.recv_async().await {
                    Ok(txn) => {
                        if let Err(FixedBufferError::BufferOverflow) = buffered_txs.push(txn) {
                            Self::flush_transactions(&mut buffered_txs);
                        }

                        if let Err(TryRecvError::Disconnected) =
                            Self::write_current_txs(&mut buffered_txs, &rx)
                        {
                            return;
                        }
                    }
                    Err(_) => return,
                }
            }
        } else {
            let mut interval = tokio::time::interval(flush_interval);
            loop {
                interval.tick().await;

                if let Err(TryRecvError::Disconnected) =
                    Self::write_current_txs(&mut buffered_txs, &rx)
                {
                    return;
                }
            }
        }
    }
}
