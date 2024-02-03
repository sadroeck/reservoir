use crate::dam::DamControl;
use crate::error::ReservoirResult;
use crate::tx_id::TransactionId;
use crate::utils::FlushNotifier;
use crate::StorageWriter;
use tokio::io::AsyncWriteExt;

pub struct WriteHandle<W, F>
where
    W: StorageWriter,
    F: FlushNotifier,
{
    /// Unique identifier of the transaction.
    id: TransactionId,
    /// Async write abstraction for the underlying storage layer
    data_writer: W,
    /// Access to the secondary log for transaction IDs.
    txn_id_log: DamControl<F>,
    /// Running CRC checksum of the data written to the storage layer.
    crc: crc32fast::Hasher,
    /// Number of bytes written to the storage layer.
    payload_bytes: u32,
}

impl<W, F> WriteHandle<W, F>
where
    W: StorageWriter,
    F: FlushNotifier,
{
    pub fn new(id: TransactionId, data_writer: W, txn_id_log: DamControl<F>) -> WriteHandle<W, F> {
        WriteHandle {
            id,
            data_writer,
            txn_id_log,
            crc: crc32fast::Hasher::new(),
            payload_bytes: 0,
        }
    }

    pub fn transaction_id(&self) -> TransactionId {
        self.id
    }

    async fn write_preamble(&mut self, size: u32) -> ReservoirResult<()> {
        self.data_writer.write_u64(self.id.into()).await?;
        self.data_writer.write_u32(size).await?;
        Ok(())
    }

    pub async fn write_all_bytes(&mut self, buf: &[u8]) -> ReservoirResult<()> {
        // TODO: Check max transaction size
        self.write_preamble(buf.len() as u32).await?;
        self.data_writer.write_all(buf).await?;
        self.crc.update(buf);
        self.payload_bytes += buf.len() as u32;
        Ok(())
    }

    pub async fn add_bytes(&mut self, buf: &[u8]) -> ReservoirResult<()> {
        // TODO: Check max transaction size
        if self.payload_bytes == 0 {
            self.write_preamble(self.data_writer.payload_size()).await?;
        }
        self.data_writer.write_all(buf).await?;
        self.crc.update(buf);
        self.payload_bytes += buf.len() as u32;
        Ok(())
    }

    /// Commits the transaction by computing the final CRC value, writing+flushing it to the storage layer
    /// & committing the transaction ID to the secondary log.
    pub async fn commit(mut self) -> ReservoirResult<()> {
        let checksum = self.crc.finalize();
        self.data_writer.write_u32(checksum).await?;
        self.data_writer.flush().await?;
        self.txn_id_log
            .commit(self.id, checksum, self.payload_bytes)
            .await;
        Ok(())
    }
}
