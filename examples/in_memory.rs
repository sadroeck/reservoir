use reservoir::*;
use std::mem::size_of;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tracing::info;

const ONE_MEBIBYTE: usize = 1024 * 1024;
const SYNC_INTERVAL: Duration = Duration::from_millis(5);
const PAYLOAD: [u8; 512 - 12] = [42u8; 512 - 12];
const TASK_COUNT: usize = 100;
const TASK_ITERATIONS: usize = 1000;
const NUM_WRITES: usize = TASK_COUNT * TASK_ITERATIONS;

async fn write_payload(
    reservoir: &Reservoir<MemBufferPool, impl FlushNotifier>,
) -> ReservoirResult<()> {
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
    let storage = MemBufferPool::new(ONE_MEBIBYTE);
    let dam_log = DamLog::new(Path::new("./dam.log"))?;
    let dam = AsyncDamFlusher::new(dam_log, FlushStrategy::Interval(SYNC_INTERVAL))?;
    let start_tx_id = dam.highest_committed_transaction_id();
    let (dam_task, dam_handle) = dam.spawn_thread();
    let reservoir = Arc::new(Reservoir::new(storage, start_tx_id, dam_handle).await?);

    let start = std::time::Instant::now();

    // Spawn 100 tasks to write 1000 transactions each to a reservoir
    for _ in 0..TASK_COUNT {
        let reservoir_clone = reservoir.clone();
        tokio::spawn(async move {
            for _ in 0..TASK_ITERATIONS {
                let _ = write_payload(&reservoir_clone).await;
            }
        });
    }

    let write_end = std::time::Instant::now();

    drop(reservoir);

    dam_task.join().expect("Could not join dam task");

    let sync_end = std::time::Instant::now();

    const WRITE_SIZE: usize = PAYLOAD.len() + size_of::<SerializedTransaction>();

    info!("Write time: {:?}", write_end - start);
    info!("Sync time: {:?}", sync_end - start);
    info!(
        "Wrote {}MiB",
        (NUM_WRITES as f64 * WRITE_SIZE as f64) / ONE_MEBIBYTE as f64
    );
    info!(
        "Write throughput: {} MiB/s",
        (NUM_WRITES as f64 * WRITE_SIZE as f64 / (write_end - start).as_secs_f64())
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
