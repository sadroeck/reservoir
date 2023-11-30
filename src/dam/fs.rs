use crate::dam::{FlushStrategy, SerializedTransaction, MAX_PENDING_TRANSACTIONS};
use crate::utils::{FixedBufferWriter, SyncNotifier};
use crate::{DamControl, ReservoirResult, TransactionId};
use flume::{Receiver, TryRecvError};
use std::fs::File;
use std::mem::size_of;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::time::Duration;
use tracing::{error, info};

// Each log file stores 1 million transaction IDs of 16B each, for a total of ~15MiB.
// const LOG_FILE_SIZE: usize = 1_000_000 * size_of::<SerializedTransaction>();
const LOG_FILE_SIZE: usize = 1_000_000 * size_of::<SerializedTransaction>();
const LOG_BUFFER_SIZE: usize = MAX_PENDING_TRANSACTIONS * size_of::<SerializedTransaction>();

/// Utility to scan the contents of the Dam log
pub struct DamIterator<'a, N: SyncNotifier> {
    buf: [u8; 128 * size_of::<SerializedTransaction>()],
    dam: &'a DamFlusher<N>,
    buf_idx: usize,
    file_offset: u64,
}

impl<'a, N: SyncNotifier> Iterator for DamIterator<'a, N> {
    /// Pair of [`SerializedTransaction`] and the offset in the file where it was found.
    type Item = (SerializedTransaction, u64);

    fn next(&mut self) -> Option<Self::Item> {
        if self.buf_idx == 128 {
            // We've reached the end of the file
            if self.file_offset == LOG_FILE_SIZE as u64 {
                return None;
            }
            // Read the next buffer's worth of IDs
            self.dam
                .log_file
                .inner()
                .read_exact_at(&mut self.buf, self.file_offset)
                .ok()?;
            self.buf_idx = 0;
        }

        let txn = SerializedTransaction::from_bytes(
            &self.buf[self.buf_idx * size_of::<SerializedTransaction>()
                ..(self.buf_idx + 1) * size_of::<SerializedTransaction>()],
        );
        if txn.is_invalid() {
            return None;
        }

        let res = Some((txn, self.file_offset));
        self.file_offset += size_of::<SerializedTransaction>() as u64;
        res
    }
}

/// Task to write the committed transaction IDs to the secondary log,
/// on a dedicated interval in the background.
pub struct DamFlusher<N: SyncNotifier> {
    /// The flush strategy to use.
    flush_strategy: FlushStrategy,
    /// The file handle to the secondary log.
    log_file: FixedBufferWriter<LOG_BUFFER_SIZE>,
    /// A mechanism to signal sync events to transactions submitting IDs.
    notifier: N,
}

impl<N: SyncNotifier> DamFlusher<N> {
    pub fn new(log_file: &Path, flush_strategy: FlushStrategy) -> ReservoirResult<Self> {
        let (log_file, current_offset) = if log_file.exists() {
            // If the file already exists, just verify the size and open it
            let file = File::options().read(true).write(true).open(log_file)?;
            let current_file_size = file.metadata()?.len();
            if current_file_size != LOG_FILE_SIZE as u64 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    format!("Dam file {log_file:?} is not the correct size: current={current_file_size} vs expected={LOG_FILE_SIZE}"),
                )
                .into());
            }
            let mut current_offset = 0;
            // Find the offset in the file where we should start writing
            'find_offset: loop {
                let mut buf = [0u8; 128 * size_of::<SerializedTransaction>()];
                file.read_exact_at(&mut buf, current_offset)?;
                for i in 0..128 {
                    let txn = SerializedTransaction::from_bytes(
                        &buf[i * size_of::<SerializedTransaction>()
                            ..(i + 1) * size_of::<SerializedTransaction>()],
                    );
                    if txn.is_invalid() {
                        break 'find_offset;
                    }
                    current_offset += size_of::<SerializedTransaction>() as u64;
                }
            }

            info!("Opened dam file at {log_file:?}:{current_offset}");
            (file, current_offset)
        } else {
            let file = File::create(log_file)?;
            file.set_len(LOG_FILE_SIZE as u64)?;
            file.sync_all()?;
            info!("Created dam file at {log_file:?}:0");
            (file, 0)
        };
        let log_file = FixedBufferWriter::new(log_file, current_offset)?;

        Ok(Self {
            flush_strategy,
            log_file,
            notifier: N::new(),
        })
    }

    pub fn with_strategy(&mut self, flush_strategy: FlushStrategy) {
        self.flush_strategy = flush_strategy;
    }

    pub fn iter(&self) -> DamIterator<'_, N> {
        DamIterator {
            buf: [0u8; 128 * size_of::<SerializedTransaction>()],
            dam: self,
            buf_idx: 128,
            file_offset: 0,
        }
    }

    /// Scans the Dam log & finds the highest committed [`TransactionId`]. This is used to inform
    /// the storage layer on which transaction buffers are safe to redistribute.
    pub fn highest_committed_transaction_id(&self) -> TransactionId {
        self.iter()
            .map(|(txn, _)| txn.id)
            .max()
            .unwrap_or(TransactionId(1))
    }

    pub fn spawn_thread(self) -> (std::thread::JoinHandle<()>, DamControl<N>) {
        let (tx, rx) = flume::bounded(MAX_PENDING_TRANSACTIONS);
        let notifier = self.notifier.clone();
        let thread = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(self.run(rx));
        });
        (thread, DamControl { tx, notifier })
    }

    fn write_txn_buffered(&mut self, txn: SerializedTransaction) -> std::io::Result<()> {
        // TODO: add IO retries & proper error handling
        // TODO: Open new file if current one fills up
        self.log_file.write_transaction_id_bytes(&txn.into_bytes())
    }

    fn write_current_txs(
        &mut self,
        rx: &Receiver<SerializedTransaction>,
    ) -> Result<(), TryRecvError> {
        // Drain the channel
        loop {
            match rx.try_recv() {
                Ok(txn) => {
                    self.write_txn_buffered(txn).expect("Could not write txn");
                }
                Err(TryRecvError::Empty) => {
                    // TODO: Handle IO errors
                    // There are no more transactions to drain, so flush & return.
                    if let Err(err) = self.log_file.flush_all(&self.notifier) {
                        error!(%err, "Could not flush transactions");
                    }
                    return Ok(());
                }
                Err(TryRecvError::Disconnected) => return Err(TryRecvError::Disconnected),
            }
        }
    }

    /// In this mode, this will batch up all pending transaction IDs and flush them on the next
    /// interval.
    async fn run_interval(mut self, rx: Receiver<SerializedTransaction>, sync_interval: Duration) {
        let mut interval = tokio::time::interval(sync_interval);
        loop {
            interval.tick().await;
            if self.write_current_txs(&rx).is_err() {
                return;
            }
        }
    }

    /// In this mode, upon receiving a new txn ID, flush it & all pending transaction IDs
    /// immediately.
    async fn run_eager(mut self, rx: Receiver<SerializedTransaction>) {
        loop {
            match rx.recv_async().await {
                Ok(txn) => {
                    self.write_txn_buffered(txn).expect("Could not write txn");
                }
                Err(_) => return,
            }
            if self.write_current_txs(&rx).is_err() {
                return;
            }
        }
    }

    /// Flushes the committed transaction IDs to the secondary log.
    /// Dispatches to the relevant strategy [`run`] functions, i.e.
    /// * [`run_interval`]
    /// * [`run_eager`]
    pub async fn run(self, rx: Receiver<SerializedTransaction>) {
        match self.flush_strategy {
            FlushStrategy::Interval(sync_interval) => {
                self.run_interval(rx, sync_interval).await;
            }
            FlushStrategy::Eager => {
                self.run_eager(rx).await;
            }
        }
    }
}
