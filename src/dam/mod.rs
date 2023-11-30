use crate::tx_id::TransactionId;
use crate::utils::{FlushNotifier, NopNotifier};
use event_listener::Event;
use flume::Sender;
use std::sync::Arc;

mod flush_strategy;

const MAX_PENDING_TRANSACTIONS: usize = 4096;

#[derive(Debug, Copy, Clone)]
pub struct SerializedTransaction {
    pub id: TransactionId,
    pub size: u32,
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

impl SerializedTransaction {
    #[inline]
    pub fn into_bytes(self) -> [u8; 16] {
        let mut buf = [0u8; 16];
        buf[0..8].copy_from_slice(&self.id.0.to_le_bytes());
        buf[8..12].copy_from_slice(&self.size.to_le_bytes());
        buf[12..16].copy_from_slice(&self.crc.to_le_bytes());
        buf
    }

    #[inline]
    pub fn from_bytes(buf: &[u8]) -> Self {
        let mut id_buf = [0u8; 8];
        id_buf.copy_from_slice(&buf[0..8]);
        let mut size_buf = [0u8; 4];
        size_buf.copy_from_slice(&buf[8..12]);
        let mut crc_buf = [0u8; 4];
        crc_buf.copy_from_slice(&buf[12..16]);
        Self {
            id: TransactionId(u64::from_le_bytes(id_buf)),
            size: u32::from_le_bytes(size_buf),
            crc: u32::from_le_bytes(crc_buf),
        }
    }

    pub fn is_invalid(&self) -> bool {
        self.id.0 == u64::MAX || self.id.0 == 0
    }
}

/// A cheap to clone handle to commit serialized transaction IDs to the secondary log.
/// A handle can submit a single transaction ID to the log. If [`SYNC`] is set to `true`, the handle will
/// wait for durable commit to storage of the transaction ID before returning.
/// If [`SYNC`] is set to `false`, the ID will be durably committed in the background.
#[derive(Clone)]
pub struct DamControl<F: FlushNotifier> {
    tx: Sender<SerializedTransaction>,
    notifier: F,
}

impl<F: FlushNotifier> DamControl<F> {
    pub async fn commit(&self, id: TransactionId, crc: u32, size: u32) {
        // Ignore any send error, as this can only happen when we've closed the channel.
        let _ = self
            .tx
            .send_async(SerializedTransaction { id, crc, size })
            .await;

        // Wait for the next sync
        self.notifier.get_waiter().await;
    }
}

pub type SyncDamFlusher = DamFlusher<Arc<Event>>;
pub type AsyncDamFlusher = DamFlusher<NopNotifier>;

#[cfg(feature = "io_uring")]
mod io_uring;
#[cfg(feature = "io_uring")]
pub use io_uring::DamFlusher;

#[cfg(not(feature = "io_uring"))]
mod fs;

#[cfg(not(feature = "io_uring"))]
pub use fs::DamFlusher;

pub use flush_strategy::FlushStrategy;
