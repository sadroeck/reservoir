/// Enumeration of the possible errors that can occur in the storage layer.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// Generic IO error
    #[error(transparent)]
    IoError(#[from] std::io::Error),
}

/// A specialized `Result` type for this crate's storage operations.
pub type StoreResult<T> = std::result::Result<T, StoreError>;