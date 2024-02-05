use crate::dam::{FlushStrategy, SerializedTransaction, MAX_PENDING_TRANSACTIONS};
use crate::utils::{FixedBufferWriter, FlushNotifier};
use crate::{DamControl, ReservoirResult, TransactionId};
use flume::{Receiver, TryRecvError};
use futures_util::Stream;
use std::fs::File;
use std::future::Future;
use std::io;
use std::io::Write;
use std::mem::size_of;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;
use tracing::{error, info};

// Each log file stores 1 million transaction IDs of 16B each, for a total of ~15MiB.
// const LOG_FILE_SIZE: usize = 1_000_000 * size_of::<SerializedTransaction>();
const LOG_FILE_SIZE: usize = 1_000_000 * size_of::<SerializedTransaction>();
const LOG_BUFFER_SIZE: usize = MAX_PENDING_TRANSACTIONS * size_of::<SerializedTransaction>();

/// Utility to scan the contents of the Dam log
pub struct DamIterator {
    buf: Vec<u8>,
    dam_file: File,
    buf_idx: usize,
    file_offset: u64,
}

impl DamIterator {
    pub fn new(dam_file: &Path) -> io::Result<Self> {
        // Open dam file read-only.
        assert!(dam_file.exists(), "Dam log does not exist");
        let dam_file = File::options()
            .read(true)
            .open(dam_file)
            .expect("Could not open dam file for reading");
        let mut s = Self {
            buf: vec![0u8; 128 * size_of::<SerializedTransaction>()],
            dam_file,
            buf_idx: 128,
            file_offset: 0,
        };
        s.fill_id_buffer();
        Ok(s)
    }

    pub fn reset(&mut self, offset: u64) {
        self.file_offset = offset;
        self.buf_idx = 128;
    }

    /// Read the next buffer's worth of IDs
    fn fill_id_buffer(&mut self) {
        // let read_bytes = self.dam_file.read(&mut self.buf).unwrap_or_else(|e| {
        //     if e.kind() == io::ErrorKind::WouldBlock {
        //         0
        //     } else {
        //         panic!("Could not read from dam {e}");
        //     }
        // });
        self.dam_file
            .read_exact_at(&mut self.buf, self.file_offset)
            .expect("Could not read from dam file");
        self.buf_idx = 0;
    }

    #[inline]
    fn get_current_txn(&self) -> SerializedTransaction {
        SerializedTransaction::from_bytes(
            &self.buf[self.buf_idx * size_of::<SerializedTransaction>()
                ..(self.buf_idx + 1) * size_of::<SerializedTransaction>()],
        )
    }
}

impl Iterator for DamIterator {
    /// Pair of [`SerializedTransaction`] and the offset in the file where it was found.
    type Item = (SerializedTransaction, u64);

    fn next(&mut self) -> Option<Self::Item> {
        if self.buf_idx == 128 {
            // We've reached the end of the buffer
            if self.file_offset == LOG_FILE_SIZE as u64 {
                // We've reached the end of the file
                return None;
            }
            self.fill_id_buffer();
        }

        let mut txn = self.get_current_txn();
        if txn.is_invalid() {
            // Re-full the buffer, to see if there's any new content
            self.fill_id_buffer();

            // Try again with the latest buffer
            txn = self.get_current_txn();
            if txn.is_invalid() {
                return None;
            }
        }

        self.file_offset += size_of::<SerializedTransaction>() as u64;
        self.buf_idx += 1;
        Some((txn, self.file_offset))
    }
}

pub struct DamStream {
    iter: DamIterator,
    dam_offset: Arc<AtomicU64>,
    stream_offset: u64,
    sleep: Option<Pin<Box<tokio::time::Sleep>>>,
}

impl DamStream {
    fn needs_sleep(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> bool {
        // Check if we're still waiting for new dam content to arrive
        if let Some(ref mut sleep) = self.sleep.as_mut() {
            let sleep = Pin::new(sleep);
            match sleep.poll(cx) {
                Poll::Ready(_) => {
                    self.sleep = None;
                    false
                }
                Poll::Pending => true,
            }
        } else {
            false
        }
    }
}

impl Unpin for DamStream {}

impl Stream for DamStream {
    type Item = (TransactionId, u64);

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        {
            // Check if we're still waiting for new dam content to arrive
            if self.as_mut().needs_sleep(cx) {
                return Poll::Pending;
            }
        }

        let mut max_offset = self.dam_offset.load(Ordering::Relaxed);
        loop {
            match self.iter.next() {
                Some((segment, offset)) => {
                    if segment.id.0 > self.stream_offset {
                        self.stream_offset = segment.id.0;
                        return Poll::Ready(Some((segment.id, offset)));
                    }
                }
                None => {
                    let new_max_offset = self.dam_offset.load(Ordering::Relaxed);
                    if new_max_offset == max_offset {
                        // There's no more data currently available, so wait.
                        self.sleep = Some(Box::pin(tokio::time::sleep(Duration::from_millis(1))));
                        if self.as_mut().needs_sleep(cx) {
                            return Poll::Pending;
                        }
                    }

                    // Read the next buffer's worth of IDs
                    max_offset = new_max_offset;
                    self.as_mut().iter.reset(max_offset);
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct DamLog {
    /// The path to the log file.
    pub path: PathBuf,
    /// The underlying file handle.
    pub file: File,
    /// The latest offset that's been f-synced.
    pub current_offset: Arc<AtomicU64>,
}

impl DamLog {
    pub fn new(log_file: &Path) -> ReservoirResult<Self> {
        let (file, current_offset) = if log_file.exists() {
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

            // Find the offset in the file where we should start writing
            let mut current_offset = 0;
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
            let mut file = File::options().write(true).create(true).open(log_file)?;
            // file.set_len(LOG_FILE_SIZE as u64)?;
            for _ in 0..LOG_FILE_SIZE / 4096 {
                file.write_all(&[255u8; 4096])?;
            }
            file.flush()?;
            file.sync_all()?;
            info!("Created dam file at {log_file:?}:0");
            (file, 0)
        };

        Ok(Self {
            path: log_file.to_path_buf(),
            file,
            current_offset: Arc::new(AtomicU64::new(current_offset)),
        })
    }

    pub fn iter(&self) -> DamIterator {
        let dam_file = File::open(&self.path).expect("Could not open dam file");
        DamIterator {
            buf: vec![0u8; 128 * size_of::<SerializedTransaction>()],
            dam_file,
            buf_idx: 128,
            file_offset: 0,
        }
    }

    pub fn highest_committed_transaction_id(&self) -> TransactionId {
        self.iter()
            .map(|(txn, _)| txn.id)
            .max()
            .unwrap_or(TransactionId(1))
    }
}

/// Task to write the committed transaction IDs to the secondary log,
/// on a dedicated interval in the background.
pub struct DamFlusher<N: FlushNotifier> {
    /// The flush strategy to use.
    flush_strategy: FlushStrategy,
    /// The file handle to the secondary log.
    log_file: FixedBufferWriter<LOG_BUFFER_SIZE>,
    /// A mechanism to signal sync events to transactions submitting IDs.
    notifier: N,
}

impl<N: FlushNotifier> DamFlusher<N> {
    pub fn new(log: DamLog, flush_strategy: FlushStrategy) -> ReservoirResult<Self> {
        let log_file = FixedBufferWriter::new(log)?;
        Ok(Self {
            flush_strategy,
            log_file,
            notifier: N::new(),
        })
    }

    pub fn with_strategy(&mut self, flush_strategy: FlushStrategy) {
        self.flush_strategy = flush_strategy;
    }

    /// Scans the Dam log & finds the highest committed [`TransactionId`]. This is used to inform
    /// the storage layer on which transaction buffers are safe to redistribute.
    pub fn highest_committed_transaction_id(&self) -> TransactionId {
        self.log_file
            .inner()
            .iter()
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
