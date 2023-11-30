use crate::error::ReservoirResult;
use crate::tx_id::TransactionId;
use crate::utils::FlushNotifier;
use crate::write_handle::WriteHandle;
use crate::{DamControl, StorageLayer};
use std::mem::size_of;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

/// A reservoir is structure that allows for the creation of uniquely identified write
/// transactions into dedicated buffers. These unordered transactions can then be committed to the
/// underlying append-only transaction ID log.
pub struct Reservoir<S, N: FlushNotifier> {
    storage: S,
    next_tx_id: AtomicU64,
    log_id_writer: DamControl<N>,
}

impl<S, N> Reservoir<S, N>
where
    S: StorageLayer,
    N: FlushNotifier,
{
    pub async fn new(
        storage: S,
        starting_tx_id: TransactionId,
        log_id_writer: DamControl<N>,
    ) -> ReservoirResult<Self> {
        Ok(Self {
            storage,
            next_tx_id: AtomicU64::new(starting_tx_id.0),
            log_id_writer,
        })
    }
    /// Create a new write transaction with a fixed size.
    pub async fn new_transaction_fixed(
        &self,
        size: usize,
    ) -> ReservoirResult<WriteHandle<S::Writer, N>> {
        // We need to store the payload + the transaction ID + the CRC checksum
        let buffer_size = size + size_of::<TransactionId>() + size_of::<u32>();
        let data_writer = self.storage.get_write_buffer(buffer_size).await?;
        let tx_id = TransactionId(self.next_tx_id.fetch_add(1, Ordering::AcqRel));
        Ok(WriteHandle::new(
            tx_id,
            data_writer,
            self.log_id_writer.clone(),
        ))
    }

    /// Creates a new write transactions with a variable size.
    /// Note: If your transaction has a predetermined size, prefer to use [`new_transaction_fixed`].
    pub async fn new_transaction(&self) -> ReservoirResult<WriteHandle<S::Writer, N>> {
        todo!("new_transaction")
    }
}
