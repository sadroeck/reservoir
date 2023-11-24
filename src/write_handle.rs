use crate::error::StoreResult;
use crate::tx_id_log::TxIdLogHandle;
use crate::write_ahead_log::TransactionId;
use tokio::io::{AsyncWrite, AsyncWriteExt};

pub struct WriteHandle<W>
where
    W: AsyncWrite + Unpin,
{
    /// Unique identifier of the transaction.
    id: TransactionId,
    /// Flag to indicate if we've written the tx ID to the storage layer.
    have_written_tx_id: bool,
    /// Async write abstraction for the underlying storage layer
    data_writer: W,
    /// Access to the secondary log for transaction IDs.
    txn_id_log: TxIdLogHandle,
    /// Running CRC checksum of the data written to the storage layer.
    crc: crc32fast::Hasher,
}

impl<W> WriteHandle<W>
where
    W: AsyncWrite + Unpin,
{
    pub fn new(id: TransactionId, data_writer: W, txn_id_log: TxIdLogHandle) -> WriteHandle<W> {
        WriteHandle {
            id,
            have_written_tx_id: false,
            data_writer,
            txn_id_log,
            crc: crc32fast::Hasher::new(),
        }
    }

    pub async fn write_bytes(&mut self, buf: &[u8]) -> StoreResult<()> {
        if !self.have_written_tx_id {
            self.data_writer.write_u64(self.id.into()).await?;
            self.have_written_tx_id = true;
        }
        self.data_writer.write_all(buf).await?;
        self.crc.update(buf);
        Ok(())
    }

    /// Commits the transaction by computing the final CRC value, writing+flushing it to the storage layer
    /// & committing the transaction ID to the secondary log.
    pub async fn commit(mut self) -> StoreResult<()> {
        let checksum = self.crc.finalize();
        self.data_writer.write_u32(checksum).await?;
        self.txn_id_log.commit(self.id).await;
        Ok(())
    }
}
