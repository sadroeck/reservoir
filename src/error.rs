/// Enumeration of the possible errors that can occur in the storage layer.
#[derive(Debug, thiserror::Error)]
pub enum ReservoirError {
    /// Generic IO error
    #[error(transparent)]
    IoError(#[from] std::io::Error),
}

/// A specialized `Result` type for this crate's storage operations.
pub type ReservoirResult<T> = std::result::Result<T, ReservoirError>;
