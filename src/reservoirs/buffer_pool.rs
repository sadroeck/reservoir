use crate::{StorageLayer, StoreResult};
use range_alloc::RangeAllocator;
use std::io::Error;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::sync::Mutex;
use std::task::{Context, Poll};
use tokio::io::AsyncWrite;

/// A in-memory buffer pool implementation providing segments of arbitrary sizes,
/// tracked by a list of offset+size tuples.
pub struct BufferPool {
    buffer: Vec<u8>,
    alloc: Mutex<RangeAllocator<usize>>,
}

impl Drop for BufferPool {
    fn drop(&mut self) {
        let full_size = self.buffer.len();
        loop {
            let alloc = self.alloc.lock().unwrap();
            if alloc.total_available() == full_size {
                return;
            }
        }
    }
}

impl BufferPool {
    /// Creates a new [`BufferPool`] with the specified size.
    pub fn new(size: usize) -> Self {
        Self {
            buffer: vec![0; size],
            alloc: Mutex::new(RangeAllocator::new(0..size)),
        }
    }

    /// Attempts to allocate a segment of the specified size.
    /// Returns `None` if the pool is exhausted.
    /// The returned segment is valid until the pool is dropped.
    pub fn try_alloc_segment(&self, size: usize) -> Option<PoolSegment> {
        let mut alloc = self.alloc.lock().unwrap();
        let range = alloc.allocate_range(size).ok()?;
        Some(PoolSegment {
            /// Safety: The [`Drop`] implementation of [`PoolSegment`] ensures that the pool is not
            /// dropped before the segment.
            pool: unsafe { &*(self as *const BufferPool) },
            buffer_start: unsafe { self.buffer.as_ptr().add(range.start) as *mut u8 },
            size,
            bytes_written: 0,
        })
    }
}

pub struct PoolSegment {
    /// Reference to the parent pool.
    /// Note: The BufferPool is guaranteed not to be dropped before the entire pool has been
    /// reclaimed.
    pool: &'static BufferPool,
    /// Pointer to the start of the segment.
    buffer_start: *mut u8,
    /// Size of the segment.
    size: usize,
    /// Number of bytes written to the segment.
    bytes_written: usize,
}

impl PoolSegment {
    /// Indicates the number of bytes written to the segment.
    #[inline]
    pub fn bytes_written(&self) -> usize {
        self.bytes_written
    }

    /// Indicates if the segment has not been written to.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.bytes_written == 0
    }

    /// Returns the size of the segment.
    #[inline]
    pub fn len(&self) -> usize {
        self.size
    }

    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        self
    }

    #[inline]
    pub fn as_bytes_mut(&mut self) -> &mut [u8] {
        self.deref_mut()
    }
}

unsafe impl Send for PoolSegment {}

impl Drop for PoolSegment {
    fn drop(&mut self) {
        let mut alloc = self.pool.alloc.lock().unwrap();
        alloc.free_range(self.buffer_start as usize..self.buffer_start as usize + self.size);
    }
}

impl Deref for PoolSegment {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        unsafe { std::slice::from_raw_parts(self.buffer_start, self.size) }
    }
}

impl DerefMut for PoolSegment {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { std::slice::from_raw_parts_mut(self.buffer_start, self.size) }
    }
}

impl AsyncWrite for PoolSegment {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, Error>> {
        let buf_size = buf.len();
        let bytes_written = self.bytes_written;
        if (bytes_written + buf_size) > self.size {
            return Poll::Ready(Err(Error::new(
                std::io::ErrorKind::Other,
                "Buffer overflow",
            )));
        }
        self.as_bytes_mut()[bytes_written..bytes_written + buf_size].copy_from_slice(buf);
        self.bytes_written += buf_size;
        Poll::Ready(Ok(buf_size))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        Poll::Ready(Ok(()))
    }
}

#[async_trait::async_trait]
impl StorageLayer for BufferPool {
    type Writer = PoolSegment;

    /// This is a non-persistent solution, so we always return 0.
    async fn get_highest_committed_tx_id(&self) -> StoreResult<u64> {
        Ok(0)
    }

    /// Retrieves a write buffer of the specified size.
    /// Note: This will retry until a buffer becomes available, with a 1ms delay between attempts.
    async fn get_write_buffer(&self, size: usize) -> StoreResult<Self::Writer> {
        loop {
            match self.try_alloc_segment(size) {
                Some(segment) => return Ok(segment),
                None => {
                    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                }
            }
        }
    }
}
