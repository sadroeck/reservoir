use crate::error::StoreResult;
use crate::write_handle::WriteHandle;
use crate::TxIdLogHandle;
use std::fmt;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use tokio::io::AsyncWrite;
use tracing::debug;

/// Unique transaction identifier.
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct TransactionId(u64);

impl fmt::Display for TransactionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<TransactionId> for u64 {
    #[inline]
    fn from(value: TransactionId) -> Self {
        value.0
    }
}

pub struct WriteAheadLog<S> {
    store: S,
    next_tx_id: AtomicU64,
    log_id_writer: TxIdLogHandle,
}

#[async_trait::async_trait]
pub trait StorageLayer: Send + Sync + 'static {
    type Writer: AsyncWrite + Unpin;

    /// Get the highest committed transaction ID.
    async fn get_highest_committed_tx_id(&self) -> StoreResult<u64>;

    /// Retrieves a write buffer of the specified size.
    async fn get_write_buffer(&self, size: usize) -> StoreResult<Self::Writer>;
}

impl<S: StorageLayer> WriteAheadLog<S> {
    pub async fn new(store: S, log_id_writer: TxIdLogHandle) -> StoreResult<Self> {
        let highest_committed_tx_id = store.get_highest_committed_tx_id().await?;
        Ok(Self {
            store,
            next_tx_id: AtomicU64::new(highest_committed_tx_id),
            log_id_writer,
        })
    }
    /// Create a new write transaction with a fixed size.
    pub async fn new_transaction_fixed(&self, size: usize) -> StoreResult<WriteHandle<S::Writer>> {
        let data_writer = self.store.get_write_buffer(size).await?;
        let tx_id = TransactionId(self.next_tx_id.fetch_add(1, Ordering::Relaxed));
        debug!(%tx_id, %size, "Created transaction");
        Ok(WriteHandle::new(
            tx_id,
            data_writer,
            self.log_id_writer.clone(),
        ))
    }

    /// Creates a new write transactions with a variable size.
    /// Note: If your transaction has a predetermined size, prefer to use [`new_transaction_fixed`].
    pub async fn new_transaction(&self) -> StoreResult<WriteHandle<S::Writer>> {
        todo!("new_transaction")
    }
}
