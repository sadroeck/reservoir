use crate::{ReservoirResult, StorageLayer};
use range_alloc::RangeAllocator;
use std::fs::File;
use std::io::ErrorKind;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::pin::Pin;
use std::sync::Mutex;
use std::task::{Context, Poll};
use std::thread;
use std::time::Duration;
use tokio::io::AsyncWrite;
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
        for FileBufferAlloc { file, alloc } in &self.files {
            'drain_file: loop {
                if let Ok(alloc_access) = alloc.try_lock() {
                    if alloc_access.total_available() == file.metadata().unwrap().len() as usize {
                        break 'drain_file;
                    }
                }
                thread::yield_now();
            }
        }
    }
}

/// A set of buffers backed by a single [`File`].
pub struct FileBufferAlloc {
    file: File,
    alloc: Mutex<RangeAllocator<usize>>,
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

            let file = File::open(entry.path())?;
            files.push(FileBufferAlloc {
                file,
                alloc: Mutex::new(RangeAllocator::new(0..file_size as usize)),
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

        // Optimistically check the amount of available space. This isn't perfect, but better
        // than failing halfway through.
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

    pub fn try_alloc_segment(&self, size: usize) -> Option<FileSlice> {
        for filer_buffer in &self.files {
            if let Ok(mut alloc_access) = filer_buffer.alloc.try_lock() {
                if let Ok(range) = alloc_access.allocate_range(size) {
                    return Some(FileSlice {
                        file_buffer: unsafe { &*(filer_buffer as *const FileBufferAlloc) },
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
    size: usize,
    /// Number of bytes written to the slice.
    written_bytes: usize,
}

#[async_trait::async_trait]
impl StorageLayer for FilePool {
    type Writer = FileSlice;

    /// Retrieves a write buffer of the specified size.
    /// Note: This will retry until a buffer becomes available, with a 1ms delay between attempts.
    async fn get_write_buffer(&self, size: usize) -> ReservoirResult<Self::Writer> {
        loop {
            if let Some(slice) = self.try_alloc_segment(size) {
                return Ok(slice);
            }
            sleep(BUFFER_ACQ_RETRY_DELAY).await;
        }
    }
}

impl AsyncWrite for FileSlice {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let buf_size = buf.len();
        if (self.written_bytes + buf_size) > self.size {
            return Poll::Ready(Err(std::io::Error::new(
                ErrorKind::Other,
                "Buffer overflow",
            )));
        }
        self.file_buffer.file.write_at(buf, self.offset)?;
        self.written_bytes += buf_size;
        Poll::Ready(Ok(buf_size))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl Drop for FileSlice {
    fn drop(&mut self) {
        self.file_buffer
            .alloc
            .lock()
            .unwrap()
            .free_range(self.offset as usize..self.offset as usize + self.size);
    }
}
