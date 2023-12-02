mod file_pool;
mod mem_buffer_pool;

use crate::ReservoirResult;
use tokio::io::AsyncWrite;

pub use file_pool::*;
pub use mem_buffer_pool::*;

#[async_trait::async_trait]
pub trait StorageLayer: Send + Sync + 'static {
    type Writer: AsyncWrite + Unpin + Send;

    /// Retrieves a write buffer of the specified size.
    async fn get_write_buffer(&self, size: usize) -> ReservoirResult<Self::Writer>;
}
