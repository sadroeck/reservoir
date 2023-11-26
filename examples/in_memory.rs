use reservoir::{AsyncDamFlusher, BufferPool, Reservoir, ReservoirResult};
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info};

const ONE_MEBIBYTE: usize = 1024 * 1024;
const SYNC_INTERVAL: Duration = Duration::from_millis(5);
const PAYLOAD: [u8; 512 - 12] = [42u8; 512 - 12];

async fn write_payload(reservoir: Arc<Reservoir<BufferPool, false>>) -> ReservoirResult<()> {
    let mut tx = reservoir.new_transaction_fixed(PAYLOAD.len()).await?;
    tx.write_bytes(&PAYLOAD).await?;
    tx.commit().await?;
    Ok(())
}

#[tokio::main]
async fn main() -> ReservoirResult<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    // Create a reservoir with a 1MiB buffer pool and a 5ms sync interval
    let storage = BufferPool::new(ONE_MEBIBYTE);
    let (dam_task, dam_handle) = AsyncDamFlusher::spawn_task(SYNC_INTERVAL);
    let reservoir = Arc::new(Reservoir::new(storage, dam_handle).await?);

    let start = std::time::Instant::now();

    // Spawn 100k tasks to write "Hello world" to the reservoir
    for _ in 0..100 {
        let reservoir_clone = reservoir.clone();
        tokio::spawn(async move {
            for _ in 0..1000 {
                let _ = write_payload(reservoir_clone.clone()).await;
            }
        });
    }

    let write_end = std::time::Instant::now();

    std::mem::drop(reservoir);

    if let Err(err) = dam_task.await {
        error!(%err, "Error in dam task");
    }

    let sync_end = std::time::Instant::now();

    info!("Write time: {:?}", write_end - start);
    info!("Sync time: {:?}", sync_end - start);
    info!(
        "Write throughput: {} MiB/s",
        (100_000.0 * PAYLOAD.len() as f64 / (write_end - start).as_secs_f64())
            / ONE_MEBIBYTE as f64
    );
    info!(
        "TPS (write) : {}",
        100_000.0 / (write_end - start).as_secs_f64()
    );
    info!(
        "TPS (sync)  : {}",
        100_000.0 / (sync_end - start).as_secs_f64()
    );

    Ok(())
}
