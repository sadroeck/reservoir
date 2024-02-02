mod file_pool;
mod mem_buffer_pool;

use crate::{ReservoirResult, TransactionId};
use tokio::io::{AsyncRead, AsyncWrite};

pub use file_pool::*;
pub use mem_buffer_pool::*;

#[async_trait::async_trait]
pub trait StorageLayer: Send + Sync + 'static {
    type Writer: AsyncWrite + Unpin + Send;
    type Reader: AsyncRead + Unpin + Send;

    /// Retrieves a write buffer of the specified size.
    /// TODO: Does this need to be async??
    async fn write_transaction(&self, size: usize) -> ReservoirResult<Self::Writer>;

    async fn read_transaction(&self, segment_id: TransactionId) -> ReservoirResult<Self::Reader>;
}
