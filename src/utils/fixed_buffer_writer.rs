use crate::utils::{FixedBuffer, FixedBufferError, FlushNotifier};
use std::fs::File;
use std::io::{ErrorKind, Write};
use std::os::unix::fs::FileExt;

/// An equivalent to [`BufWriter`] but overwriting an existing file at a predefined offset
/// instead of appending to a file.
/// [`BufWriter`]: std::io:BufWriter
pub struct FixedBufferWriter<const N: usize> {
    /// The underlying file being written to.
    /// Note: This should be pre-allocated on the underlying filesystem.
    file: File,
    /// The maximum number of bytes the file is supposed to contain.
    file_size: u64,
    /// The offset of the next write within the file
    write_offset: u64,
    /// The number of unflushed writes
    buf_count_to_flush: u64,
    /// Enables the use [`sync_file_range`] or equivalent. If disabled a regular [`fdatasync`] is
    /// used to flush changes durably to the underlying storage device.
    use_range_sync: bool,
    /// A fixed-size buffer for pending writes.
    buffer: FixedBuffer<u8, N>,
}

#[allow(dead_code)]
impl<const N: usize> FixedBufferWriter<N> {
    pub fn new(file: File, offset: u64) -> std::io::Result<Self> {
        let file_size = file.metadata()?.len();
        let use_range_sync = Self::determine_range_sync_capability(&file)?;
        Ok(Self {
            file,
            file_size,
            write_offset: offset,
            buf_count_to_flush: 0,
            use_range_sync,
            buffer: Default::default(),
        })
    }

    /// Determine if it's safe to use [`libc::file_sync_range`] for syncing durably to the storage
    /// device. This depends on the underlying filesystem & the write caching strategy of the raw
    /// storage device.
    fn determine_range_sync_capability(_file: &File) -> std::io::Result<bool> {
        // TODO: Determine filesystem: ext4 & xfs should be ok, rest are not.
        // TODO: Query underlying block dev to verify if it has a Volatile Write Cache.
        // For now, let's assume it's not.
        Ok(false)
    }

    pub fn write_transaction_id_bytes(&mut self, buf: &[u8]) -> std::io::Result<()> {
        if self.write_offset + buf.len() as u64 > self.file_size {
            return Err(std::io::Error::from(ErrorKind::UnexpectedEof));
        }
        match self.buffer.write(buf) {
            Ok(()) => {
                // TODO: Check if full/nearly-full & flush?
                self.buf_count_to_flush += 1;
                Ok(())
            }
            Err(FixedBufferError::BufferOverflow) => {
                self.write_buffer_to_file()?;
                self.buffer.write(buf)?;
                self.buf_count_to_flush += 1;
                Ok(())
            }
        }
    }

    /// Write the contents of the current buffer to file & fsync the contents.
    pub fn flush_all(&mut self, sync_event: &impl FlushNotifier) -> std::io::Result<()> {
        self.write_buffer_to_file()?;
        self.flush_file(sync_event)?;
        Ok(())
    }

    /// Durably flush the cached bytes to the storage device.
    fn flush_file(&mut self, sync_event: &impl FlushNotifier) -> std::io::Result<()> {
        if self.use_range_sync {
            todo!("file_sync_range")
        } else {
            self.file.flush()?;
            self.file.sync_data()?;
        }

        // Inform any transactions waiting for flush confirmation
        sync_event.notify_transactions(self.buf_count_to_flush as usize);
        self.buf_count_to_flush = 0;
        Ok(())
    }

    /// Writes the contents of the local buffer to the current file.
    pub fn write_buffer_to_file(&mut self) -> std::io::Result<()> {
        let buffer_size = self.buffer.len() as u64;
        // Write to the underlying file
        self.file
            .write_all_at(self.buffer.as_slice(), self.write_offset)?;
        self.write_offset += buffer_size;

        // The current buffer has been written to the filesystem cache
        self.buffer.clear();
        Ok(())
    }

    pub fn inner(&self) -> &File {
        &self.file
    }
}
