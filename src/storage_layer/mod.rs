mod buffer_pool;

use crate::ReservoirResult;
pub use buffer_pool::*;
use tokio::io::AsyncWrite;

#[async_trait::async_trait]
pub trait StorageLayer: Send + Sync + 'static {
    type Writer: AsyncWrite + Unpin;

    /// Get the highest committed transaction ID.
    async fn get_highest_committed_tx_id(&self) -> ReservoirResult<u64>;

    /// Retrieves a write buffer of the specified size.
    async fn get_write_buffer(&self, size: usize) -> ReservoirResult<Self::Writer>;
}
