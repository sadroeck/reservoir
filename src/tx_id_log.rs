use crate::write_ahead_log::TransactionId;
use tracing::info;

/// A cheap to clone handle to commit serialized transaction IDs to the secondary log.
#[derive(Clone)]
pub struct TxIdLogHandle {
    tx: flume::Sender<TransactionId>,
}

impl TxIdLogHandle {
    pub async fn commit(&self, id: TransactionId) {
        // Ignore any send error, as this can only happen when we've closed the channel.
        let _ = self.tx.send_async(id).await;
    }
}

/// Task to write the committed transaction IDs to the secondary log,
/// on a dedicated interval in the background.
pub struct AsyncTxIdTask {
    /// Interval at which to flush the transaction IDs to the secondary log.
    flush_interval: std::time::Duration,
}

impl AsyncTxIdTask {
    pub fn run(self) -> TxIdLogHandle {
        let (tx, rx) = flume::bounded(4096);
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async move {
            let mut interval = tokio::time::interval(self.flush_interval);
            loop {
                interval.tick().await;
                while let Ok(id) = rx.try_recv() {
                    info!("Flushing transaction ID: {id}");
                }
            }
        });
        TxIdLogHandle { tx }
    }
}
