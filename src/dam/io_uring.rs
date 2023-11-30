use crate::dam::MAX_PENDING_TRANSACTIONS;
use crate::FlushNotifier;
use std::marker::PhantomData;

pub struct DamFlusher<N: FlushNotifier> {
    _phantom: PhantomData<N>,
}

impl<N: FlushNotifier> DamFlusher<N> {
    pub const fn get_max_pending_transactions() -> usize {
        MAX_PENDING_TRANSACTIONS
    }
}
