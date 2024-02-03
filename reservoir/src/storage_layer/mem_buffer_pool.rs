use crate::{ReservoirError, ReservoirResult, StorageLayer, StorageWriter, TransactionId};
use range_alloc::RangeAllocator;
use std::io::Error;
use std::mem::size_of;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::Mutex;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// A in-memory buffer pool implementation providing segments of arbitrary sizes,
/// tracked by a list of offset+size tuples.
pub struct MemBufferPool {
    buffer: Vec<u8>,
    active_readers: AtomicUsize,
    alloc: Mutex<PoolAlloc>,
}

impl Drop for MemBufferPool {
    fn drop(&mut self) {
        let full_size = self.buffer.len();
        loop {
            let alloc = self.alloc.lock().unwrap();
            if alloc.alloc.total_available() == full_size {
                return;
            }
        }
    }
}

impl MemBufferPool {
    /// Creates a new [`MemBufferPool`] with the specified size.
    pub fn new(size: usize) -> Self {
        Self {
            buffer: vec![0; size],
            active_readers: AtomicUsize::new(0),
            alloc: Mutex::new(PoolAlloc::new(size)),
        }
    }

    /// Attempts to allocate a segment of the specified size.
    /// Returns `None` if the pool is exhausted.
    /// The returned segment is valid until the pool is dropped.
    pub fn try_alloc_segment(&self, size: u32) -> Option<PoolSegment> {
        let mut alloc = self.alloc.lock().unwrap();
        let range = alloc.alloc.allocate_range(size as usize).ok()?;
        Some(PoolSegment {
            // Safety: The [`Drop`] implementation of [`PoolSegment`] ensures that the pool is not
            // dropped before the segment.
            pool: unsafe { &*(self as *const MemBufferPool) },
            buffer_start: unsafe { self.buffer.as_ptr().add(range.start) as *mut u8 },
            size,
            bytes_written: 0,
        })
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct PoolSegmentLoc {
    /// The segment ID of the serialized segment
    segment_id: TransactionId,
    /// The binary offset within the buffer
    offset: u64,
}

struct PoolAlloc {
    alloc: RangeAllocator<usize>,
    segments: Vec<PoolSegmentLoc>,
}

impl PoolAlloc {
    pub fn new(size: usize) -> Self {
        Self {
            alloc: RangeAllocator::new(0..size),
            segments: Vec::with_capacity(size / 512),
        }
    }
}

pub struct PoolSegment {
    /// Reference to the parent pool.
    /// Note: The BufferPool is guaranteed not to be dropped before the entire pool has been
    /// reclaimed.
    pool: &'static MemBufferPool,
    /// Pointer to the start of the segment.
    buffer_start: *mut u8,
    /// Size of the segment.
    size: u32,
    /// Number of bytes written to the segment.
    bytes_written: usize,
}

impl StorageWriter for PoolSegment {
    fn payload_size(&self) -> u32 {
        self.size
    }
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
    pub fn len(&self) -> u32 {
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
        let start_offset = self.buffer_start as usize - self.pool.buffer.as_ptr() as usize;
        let mut alloc = self.pool.alloc.lock().unwrap();
        alloc
            .alloc
            .free_range(start_offset..start_offset + self.size as usize);
    }
}

impl Deref for PoolSegment {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        unsafe { std::slice::from_raw_parts(self.buffer_start, self.size as usize) }
    }
}

impl DerefMut for PoolSegment {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { std::slice::from_raw_parts_mut(self.buffer_start, self.size as usize) }
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
        if (bytes_written + buf_size) > self.size as usize {
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
        // TODO: fsync the individual files efficiently
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        Poll::Ready(Ok(()))
    }
}

pub struct PoolSegmentReader {
    pool: &'static MemBufferPool,
    buffer: &'static [u8],
    bytes_read: usize,
}

impl Drop for PoolSegmentReader {
    fn drop(&mut self) {
        self.pool.active_readers.fetch_sub(1, SeqCst);
    }
}

impl AsyncRead for PoolSegmentReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let buf_size = buf.remaining();
        if (self.bytes_read + buf_size) > self.buffer.len() {
            return Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Buffer overflow",
            )));
        }
        buf.put_slice(&self.buffer[self.bytes_read..self.bytes_read + buf_size]);
        self.bytes_read += buf_size;
        Poll::Ready(Ok(()))
    }
}

#[async_trait::async_trait]
impl StorageLayer for MemBufferPool {
    type Writer = PoolSegment;
    type Reader = PoolSegmentReader;

    /// Retrieves a write buffer of the specified size.
    /// Note: This will retry until a buffer becomes available, with a 1ms delay between attempts.
    async fn write_transaction(&self, size: u32) -> ReservoirResult<Self::Writer> {
        loop {
            match self.try_alloc_segment(size) {
                Some(segment) => return Ok(segment),
                None => {
                    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                }
            }
        }
    }

    async fn read_transaction(&self, segment_id: TransactionId) -> ReservoirResult<Self::Reader> {
        let alloc_access = self.alloc.lock().unwrap();
        if let Some(segment) = alloc_access
            .segments
            .iter()
            .find(|loc| loc.segment_id == segment_id)
            .copied()
        {
            drop(alloc_access);

            let size_offset = segment.offset as usize + size_of::<TransactionId>();
            let size = u32::from_be_bytes(
                self.buffer[size_offset..size_offset + size_of::<u32>()]
                    .try_into()
                    .unwrap(),
            );
            let data_offset = size_offset + size_of::<u32>();

            return Ok(PoolSegmentReader {
                pool: unsafe { &*(self as *const MemBufferPool) },
                buffer: unsafe {
                    std::slice::from_raw_parts(self.buffer.as_ptr().add(data_offset), size as usize)
                },
                bytes_read: 0,
            });
        }
        Err(ReservoirError::NoSuchSegment(segment_id))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::StorageLayer;
    use futures_util::FutureExt;
    use tokio::io::AsyncWriteExt;

    #[tokio::test]
    async fn test_single_buffer() {
        let pool = MemBufferPool::new(11);
        let payload = b"Hello world";
        {
            let mut writer = pool
                .write_transaction(payload.len() as u32)
                .await
                .expect("Could not allocate buffer");
            writer
                .write_all(payload)
                .await
                .expect("Could not write payload");
        }
        assert_eq!(&pool.buffer[0..payload.len()], payload);
    }

    #[tokio::test]
    async fn test_multiple_buffers_unsaturated() {
        let payload = [42u8, 42u8, 42u8, 42u8];
        let pool = MemBufferPool::new(10 * payload.len());
        {
            let mut segments = vec![];
            for _ in 0..10 {
                let mut writer = pool
                    .write_transaction(payload.len() as u32)
                    .await
                    .expect("Could not allocate buffer");
                writer
                    .write_all(&payload)
                    .await
                    .expect("Could not write payload");
                segments.push(writer);
            }
        }
        assert_eq!(pool.buffer, vec![42u8; 10 * payload.len()]);
    }

    #[tokio::test]
    async fn test_multiple_buffers_saturated() {
        let pool = MemBufferPool::new(50);
        let mut segments = vec![];
        for _ in 0..5 {
            let writer = pool
                .write_transaction(10)
                .await
                .expect("Could not allocate buffer");
            segments.push(writer);
        }

        // We should not be able to allocate another segment
        assert!(pool.write_transaction(10).now_or_never().is_none())
    }
}
