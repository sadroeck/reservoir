mod mem_buffer_pool;

use crate::ReservoirResult;
pub use mem_buffer_pool::*;
use tokio::io::AsyncWrite;

#[async_trait::async_trait]
pub trait StorageLayer: Send + Sync + 'static {
    type Writer: AsyncWrite + Unpin;

    /// Retrieves a write buffer of the specified size.
    async fn get_write_buffer(&self, size: usize) -> ReservoirResult<Self::Writer>;
}
