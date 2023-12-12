use reservoir::*;
use std::mem::size_of;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tracing::info;

const ONE_MEBIBYTE: u64 = 1024 * 1024;
const SYNC_INTERVAL: Duration = Duration::from_millis(5);
const PAYLOAD: [u8; 512 - 12] = [42u8; 512 - 12];
const TASK_COUNT: usize = 100;
const TASK_ITERATIONS: usize = 1000;
const NUM_WRITES: usize = TASK_COUNT * TASK_ITERATIONS;

const FILE_POOL_PATH: &str = "./pool";

#[tokio::main]
async fn main() -> ReservoirResult<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    // Create a reservoir with a 8*32 MiB file pool and a 5ms sync interval
    let storage = FilePool::open(Path::new(FILE_POOL_PATH), 8, 32 * ONE_MEBIBYTE)?;
    let dam_log = DamLog::new(Path::new("./dam.log"))?;
    let dam = AsyncDamFlusher::new(dam_log, FlushStrategy::Interval(SYNC_INTERVAL))?;
    let start_tx_id = dam.highest_committed_transaction_id();
    let (dam_task, dam_handle) = dam.spawn_thread();
    let reservoir = Arc::new(Reservoir::new(storage, start_tx_id, dam_handle).await?);

    let workload = Workload {
        tasks: TASK_COUNT,
        iterations: TASK_ITERATIONS,
    };

    let start = std::time::Instant::now();
    workload.run(&PAYLOAD, reservoir.clone()).await;
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
