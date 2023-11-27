use reservoir::{AsyncDamFlusher, BufferPool, Reservoir, ReservoirResult};
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info};

const ONE_MEBIBYTE: usize = 1024 * 1024;
const SYNC_INTERVAL: Duration = Duration::from_millis(5);
const PAYLOAD: [u8; 512 - 12] = [42u8; 512 - 12];
const TASK_COUNT: usize = 100;
const TASK_ITERATIONS: usize = 1000;
const NUM_WRITES: usize = TASK_COUNT * TASK_ITERATIONS;

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

    // Spawn 100 tasks to write 1000 transactions each to a reservoir
    for _ in 0..TASK_COUNT {
        let reservoir_clone = reservoir.clone();
        tokio::spawn(async move {
            for _ in 0..TASK_ITERATIONS {
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
        (NUM_WRITES as f64 * PAYLOAD.len() as f64 / (write_end - start).as_secs_f64())
            / ONE_MEBIBYTE as f64
    );
    info!(
        "TPS (write) : {}",
        NUM_WRITES as f64 / (write_end - start).as_secs_f64()
    );
    info!(
        "TPS (sync)  : {}",
        NUM_WRITES as f64 / (sync_end - start).as_secs_f64()
    );

    Ok(())
}
