use std::time::Duration;

#[derive(Debug, Copy, Clone)]
pub enum FlushStrategy {
    /// Flushes the buffered txn IDs to storage on the specified interval.
    Interval(Duration),
    /// Flushes the current & all queued transactions upon submission.
    Eager,
}
