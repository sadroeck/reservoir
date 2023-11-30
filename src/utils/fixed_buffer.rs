use std::io::{ErrorKind, Write};

#[derive(Debug, thiserror::Error)]
pub enum FixedBufferError {
    #[error("buffer overflow")]
    BufferOverflow,
}

impl From<FixedBufferError> for std::io::Error {
    fn from(value: FixedBufferError) -> Self {
        match value {
            FixedBufferError::BufferOverflow => {
                std::io::Error::new(ErrorKind::UnexpectedEof, value)
            }
        }
    }
}

pub struct FixedBuffer<T: Default, const N: usize> {
    buffer: Box<[T; N]>,
    size: usize,
}

impl<T: Default + Copy, const N: usize> Default for FixedBuffer<T, N> {
    fn default() -> Self {
        Self::new()
    }
}

impl<const N: usize> Write for FixedBuffer<u8, N> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.write(buf).map(|()| buf.len()).map_err(Into::into)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<const N: usize> FixedBuffer<u8, N> {
    pub fn write(&mut self, buf: &[u8]) -> Result<(), FixedBufferError> {
        if self.size > (N - buf.len()) {
            Err(FixedBufferError::BufferOverflow)
        } else {
            self.buffer[self.size..self.size + buf.len()].copy_from_slice(buf);
            self.size += buf.len();
            Ok(())
        }
    }
}

#[allow(dead_code)]
impl<T: Default + Copy, const N: usize> FixedBuffer<T, N> {
    #[inline]
    pub fn new() -> Self {
        Self {
            buffer: Box::new([T::default(); N]),
            size: 0,
        }
    }

    #[inline]
    pub fn push(&mut self, item: T) -> Result<(), FixedBufferError> {
        if self.size == N {
            return Err(FixedBufferError::BufferOverflow);
        }
        self.buffer[self.size] = item;
        self.size += 1;
        Ok(())
    }

    #[inline]
    pub fn clear(&mut self) {
        self.size = 0;
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.size
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.size == 0
    }

    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.buffer[0..self.size].iter()
    }

    #[inline]
    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut T> {
        self.buffer[0..self.size].iter_mut()
    }

    #[inline]
    pub fn drain(&mut self) -> impl Iterator<Item = T> + '_ {
        self.buffer[0..self.size]
            .iter_mut()
            .map(|item| std::mem::take(item))
    }

    #[inline]
    pub fn as_slice(&self) -> &[T] {
        &self.buffer[0..self.size]
    }

    #[inline]
    pub fn as_mut_slice(&mut self) -> &mut [T] {
        &mut self.buffer[0..self.size]
    }

    #[inline]
    pub fn as_ptr(&self) -> *const T {
        self.buffer.as_ptr()
    }

    #[inline]
    pub fn as_mut_ptr(&mut self) -> *mut T {
        self.buffer.as_mut_ptr()
    }
}
