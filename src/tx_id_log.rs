use crate::write_ahead_log::TransactionId;
use flume::Sender;
use std::time::Duration;
use tracing::info;

const MAX_ID_LOG_BUFFER_SIZE: usize = 4096;

/// A cheap to clone handle to commit serialized transaction IDs to the secondary log.
#[derive(Clone)]
pub struct TxIdLogHandle {
    tx: Sender<TransactionId>,
}

impl TxIdLogHandle {
    pub async fn commit(&self, id: TransactionId) {
        // Ignore any send error, as this can only happen when we've closed the channel.
        let _ = self.tx.send_async(id).await;
    }
}

/// Task to write the committed transaction IDs to the secondary log,
/// on a dedicated interval in the background.
pub struct AsyncTxIdTask;

impl AsyncTxIdTask {
    pub fn run(flush_interval: Duration) -> TxIdLogHandle {
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
        TxIdLogHandle { tx }
    }
}
