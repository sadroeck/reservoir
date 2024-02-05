use crate::{ReservoirError, ReservoirResult, StorageLayer, StorageWriter, TransactionId};
use range_alloc::RangeAllocator;
use std::cmp::min;
use std::fs::File;
use std::io::ErrorKind;
use std::mem::size_of;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::task::{Context, Poll};
use std::thread;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::time::sleep;
use tracing::info;

// const BUFFER_ACQ_RETRY_DELAY: Duration = Duration::from_millis(1);
const BUFFER_ACQ_RETRY_DELAY: Duration = Duration::from_secs(1);

/// A file pool implementation providing segments of arbitrary sizes,
/// The buffers are shared between a set of files, with each file tracking multiple buffers.
pub struct FilePool {
    files: Vec<FileBufferAlloc>,
}

impl Drop for FilePool {
    fn drop(&mut self) {
        for FileBufferAlloc {
            file,
            alloc,
            active_readers,
        } in &self.files
        {
            'drain_file: loop {
                if let Ok(alloc_access) = alloc.try_lock() {
                    if alloc_access.alloc.total_available()
                        == file.metadata().unwrap().len() as usize
                    {
                        break 'drain_file;
                    }
                }
                thread::yield_now();
            }

            while active_readers.load(Ordering::SeqCst) > 0 {
                thread::yield_now();
            }
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct FileBufferSegment {
    /// The segment ID of the serialized segment
    txn_id: TransactionId,
    /// The binary offset within the file where the segment is stored
    offset: u64,
}

pub struct FileAlloc {
    pub alloc: RangeAllocator<usize>,
    pub segments: Vec<FileBufferSegment>,
}

impl FileAlloc {
    pub fn new(size: usize) -> Self {
        Self {
            alloc: RangeAllocator::new(0..size),
            segments: Vec::with_capacity(size / 512),
        }
    }
}

/// A set of buffers backed by a single [`File`].
pub struct FileBufferAlloc {
    file: File,
    alloc: Mutex<FileAlloc>,
    active_readers: AtomicUsize,
}

impl FilePool {
    #[inline]
    pub fn file_name(file_id: usize) -> String {
        format!("buffers_{file_id}")
    }

    pub fn open(path: &Path, file_count: usize, file_size: u64) -> std::io::Result<Self> {
        if !path.exists() {
            Self::create(path, file_count, file_size)?;
        }

        // Check if the directory is empty
        if std::fs::read_dir(path)?.count() > file_count {
            return Err(std::io::Error::new(
                ErrorKind::AlreadyExists,
                format!("Cannot create file pool at {path:?}: too many existing files"),
            ));
        }

        let mut dir_entry = std::fs::read_dir(path)?;
        let mut files = Vec::with_capacity(file_count);
        for entry in &mut dir_entry {
            let entry = entry?;
            if !entry.file_name().to_string_lossy().contains("buffers_") {
                continue;
            }

            if entry.metadata()?.len() != file_size {
                return Err(std::io::Error::new(
                    ErrorKind::AlreadyExists,
                    format!(
                        "Cannot create file pool at {path:?}: file {name:?} is not the correct size",
                        name = entry.file_name(),
                    ),
                ));
            }

            let file = File::options()
                .create(true)
                .write(true)
                .read(true)
                .open(entry.path())?;
            files.push(FileBufferAlloc {
                file,
                alloc: Mutex::new(FileAlloc::new(file_size as usize)),
                active_readers: AtomicUsize::new(0),
            });
        }

        if files.len() != file_count {
            return Err(std::io::Error::new(
                ErrorKind::NotFound,
                format!("Cannot create file pool at {path:?}: not enough files",),
            ));
        }

        Ok(Self { files })
    }

    /// Creates the dir & files for the [`FilePool`] with the specified number of files
    /// and buffer size in the provided [`Path`].
    fn create(path: &Path, file_count: usize, file_size: u64) -> std::io::Result<()> {
        // Create the dir & files
        info!("Creating pool dir {path:?}");
        std::fs::create_dir_all(path)?;

        // Optimistically, check the amount of available space.
        // This isn't perfect, but better than failing halfway through.
        if fs2::available_space(path)? < file_size * file_count as u64 {
            return Err(std::io::Error::new(
                ErrorKind::Other,
                format!("Cannot create file pool at {path:?}: not enough space"),
            ));
        }

        for i in 0..file_count {
            let file = File::create(path.join(Self::file_name(i)))?;
            file.set_len(file_size)?;
            file.sync_all()?;
        }

        Ok(())
    }

    pub fn try_alloc_file_slice(&self, txn_id: TransactionId, size: u32) -> Option<FileSlice> {
        for i in 0..self.files.len() {
            // Start from the most likely file (based on the txn id).
            let file_idx = txn_id.0 as usize % self.files.len() + i;
            let file_buffer = &self.files[file_idx];
            if let Ok(mut alloc_access) = file_buffer.alloc.try_lock() {
                if let Ok(range) = alloc_access.alloc.allocate_range(size as usize) {
                    alloc_access.segments.push(FileBufferSegment {
                        txn_id,
                        offset: range.start as u64,
                    });
                    file_buffer.active_readers.fetch_add(1, Ordering::SeqCst);
                    return Some(FileSlice {
                        file_buffer: unsafe { &*(file_buffer as *const FileBufferAlloc) },
                        offset: range.start as u64,
                        size,
                        written_bytes: 0,
                    });
                }
            }
        }
        None
    }
}

pub struct FileSlice {
    /// Reference to underlying [`FileBufferAlloc`].
    /// Note: The file is guaranteed not to be dropped before the [`FilePool`] is dropped.
    file_buffer: &'static FileBufferAlloc,
    /// Offset within the file where the slice starts.
    offset: u64,
    /// Size of the slice.
    size: u32,
    /// Number of bytes written to the slice.
    written_bytes: usize,
}

impl StorageWriter for FileSlice {
    fn payload_size(&self) -> u32 {
        self.size
    }
}

impl Drop for FileSlice {
    fn drop(&mut self) {
        self.file_buffer
            .alloc
            .lock()
            .unwrap()
            .alloc
            .free_range(self.offset as usize..self.offset as usize + self.size as usize);
    }
}

pub struct FileSliceReader {
    /// Reference to underlying [`FileBufferAlloc`].
    file_buffer: &'static FileBufferAlloc,
    /// Offset within the file where the slice starts.
    offset: u64,
    /// Size of the slice.
    pub size: usize,
    /// Number of bytes read from the slice.
    read_bytes: usize,
}

impl AsyncRead for FileSliceReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let remaining_bytes = self.size - self.read_bytes;
        if remaining_bytes == 0 {
            return Poll::Ready(Ok(()));
        }
        let available_buffer = min(buf.remaining(), remaining_bytes);
        let read_bytes = self.file_buffer.file.read_at(
            &mut buf.initialize_unfilled()[..available_buffer],
            self.offset + self.read_bytes as u64,
        )?;
        self.read_bytes += read_bytes;
        buf.advance(read_bytes);
        Poll::Ready(Ok(()))
    }
}

impl Drop for FileSliceReader {
    fn drop(&mut self) {
        self.file_buffer
            .active_readers
            .fetch_sub(1, Ordering::SeqCst);
    }
}

#[async_trait::async_trait]
impl StorageLayer for FilePool {
    type Writer = FileSlice;
    type Reader = FileSliceReader;

    /// Retrieves a write buffer of the specified size.
    /// Note: This will retry until a buffer becomes available, with a 1ms delay between attempts.
    async fn write_transaction(
        &self,
        txn_id: TransactionId,
        size: u32,
    ) -> ReservoirResult<Self::Writer> {
        loop {
            if let Some(slice) = self.try_alloc_file_slice(txn_id, size) {
                return Ok(slice);
            }
            sleep(BUFFER_ACQ_RETRY_DELAY).await;
        }
    }

    async fn read_transaction(
        &self,
        transaction_id: TransactionId,
    ) -> ReservoirResult<Self::Reader> {
        for file in &self.files {
            let alloc_access = file.alloc.lock().unwrap();
            if let Some(segment) = alloc_access
                .segments
                .iter()
                .find(|s| s.txn_id == transaction_id)
                .copied()
            {
                drop(alloc_access);

                // Deserialize the size of the transaction
                let tx_size_offset = segment.offset as usize + size_of::<TransactionId>();
                let mut txn_size_buf = [0u8; size_of::<u32>()];
                file.file
                    .read_exact_at(&mut txn_size_buf, tx_size_offset as u64)?;
                let txn_size = u32::from_be_bytes(txn_size_buf) as usize;

                return Ok(FileSliceReader {
                    file_buffer: unsafe { &*(file as *const FileBufferAlloc) },
                    offset: segment.offset
                        + size_of::<TransactionId>() as u64
                        + size_of::<u32>() as u64,
                    size: txn_size,
                    read_bytes: 0,
                });
            }
        }
        Err(ReservoirError::NoSuchTransaction(transaction_id))
    }
}

impl AsyncWrite for FileSlice {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let buf_size = buf.len();
        if (self.written_bytes + buf_size) > self.size as usize {
            return Poll::Ready(Err(std::io::Error::new(
                ErrorKind::Other,
                "Buffer overflow",
            )));
        }

        self.file_buffer
            .file
            .write_at(buf, self.offset + self.written_bytes as u64)?;

        self.written_bytes += buf_size;
        Poll::Ready(Ok(buf_size))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        // TODO: offload this more efficiently
        self.file_buffer.file.sync_data()?;
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{AsyncDamFlusher, DamLog, FlushStrategy, Reservoir};
    use temp_testdir::TempDir;
    use tokio::io::AsyncReadExt;

    const SYNC_INTERVAL: Duration = Duration::from_millis(5);

    #[tokio::test]
    async fn test_single_file_buffer() {
        let temp_dir = TempDir::new("single_file_buffer", true);
        let pool =
            FilePool::open(&temp_dir.join("pool"), 2, 1024).expect("Could not create file pool");
        let dam_log = DamLog::new(&temp_dir.join("dam.log")).expect("Cold not create dam log");
        let dam = AsyncDamFlusher::new(dam_log, FlushStrategy::Interval(SYNC_INTERVAL))
            .expect("Could not create dam flusher");
        let start_tx_id = dam.highest_committed_transaction_id();
        let (dam_task, dam_handle) = dam.spawn_thread();
        let reservoir = Reservoir::new(pool, start_tx_id, dam_handle)
            .await
            .expect("Could not create reservoir");

        let transaction_id;
        let payload = b"Hello world";
        {
            let mut write_handle = reservoir
                .new_transaction_fixed(payload.len() as u32)
                .await
                .expect("Could not create transaction");
            transaction_id = write_handle.transaction_id();
            write_handle
                .write_all_bytes(payload)
                .await
                .expect("Could not write transaction");
            write_handle
                .commit()
                .await
                .expect("Could not commit transaction");
        }
        let mut buffer = Vec::new();
        {
            let mut read_handle = reservoir
                .get_transaction(transaction_id)
                .await
                .expect("Could not get transaction reader");

            read_handle
                .read_to_end(&mut buffer)
                .await
                .expect("Could not read transaction");
        }

        assert_eq!(buffer, payload);
        drop(reservoir);
        dam_task.join().unwrap();
    }
}
