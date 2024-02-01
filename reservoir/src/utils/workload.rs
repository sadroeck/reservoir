use crate::{FlushNotifier, Reservoir, ReservoirResult, StorageLayer};
use std::sync::Arc;

pub struct Workload {
    pub tasks: usize,
    pub iterations: usize,
}

impl Workload {
    pub async fn run(
        &self,
        payload: &'static [u8],
        reservoir: Arc<Reservoir<impl StorageLayer, impl FlushNotifier>>,
    ) {
        let iterations = self.iterations;
        for _ in 0..self.tasks {
            let reservoir_clone = reservoir.clone();
            tokio::spawn(async move {
                for _ in 0..iterations {
                    let _ = write_payload(&reservoir_clone, payload).await;
                }
            });
        }
    }
}

async fn write_payload(
    reservoir: &Reservoir<impl StorageLayer, impl FlushNotifier>,
    payload: &[u8],
) -> ReservoirResult<()> {
    let mut tx = reservoir.new_transaction_fixed(payload.len()).await?;
    tx.write_bytes(payload).await?;
    tx.commit().await?;
    Ok(())
}
