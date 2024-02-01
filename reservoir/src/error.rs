use crate::TransactionId;

/// Enumeration of the possible errors that can occur in the storage layer.
#[derive(Debug, thiserror::Error)]
pub enum ReservoirError {
    /// Generic IO error
    #[error(transparent)]
    IoError(#[from] std::io::Error),

    #[error("Unknown segment {0}")]
    NoSuchSegment(TransactionId),
}

/// A specialized `Result` type for this crate's storage operations.
pub type ReservoirResult<T> = Result<T, ReservoirError>;
