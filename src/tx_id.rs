use std::fmt;

/// Unique transaction identifier.
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct TransactionId(pub(crate) u64);

impl fmt::Display for TransactionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<TransactionId> for u64 {
    #[inline]
    fn from(value: TransactionId) -> Self {
        value.0
    }
}
