#![allow(clippy::missing_safety_doc)]

use reservoir::{
    DamLog, Event, FilePool, FileSlice, FlushStrategy, Reservoir, ReservoirError, ReservoirResult,
    SyncDamFlusher, TransactionId, WriteHandle,
};
use std::ffi::{c_char, c_void};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::{Handle, Runtime};

const ONE_MEBIBYTE: u64 = 1024 * 1024;
const SYNC_INTERVAL: Duration = Duration::from_millis(5);

pub struct ReservoirImpl {
    /// The reservoir instance.
    reservoir: Reservoir<FilePool, Arc<Event>>,
    /// A handle to the dam thread.
    /// This is used to ensure that the dam thread is joined when the ReservoirImpl is dropped.
    dam_thread: Option<std::thread::JoinHandle<()>>,
}

impl ReservoirImpl {
    thread_local! {
        pub static RUNTIME: Runtime = tokio::runtime::Builder::new_current_thread().build().unwrap();
    }
    fn new(path: &str) -> Self {
        let storage = FilePool::open(Path::new(path), 8, 32 * ONE_MEBIBYTE)
            .expect("Could not open file pool");
        let dam_log = DamLog::new(Path::new("./dam.log")).expect("Could not open/create Dam log");
        let dam = SyncDamFlusher::new(dam_log, FlushStrategy::Interval(SYNC_INTERVAL))
            .expect("Could not create dam flusher");
        let start_tx_id = dam.highest_committed_transaction_id();
        let (dam_thread, dam_handle) = dam.spawn_thread();

        let reservoir = Self::RUNTIME.with(|rt| {
            rt.block_on(Reservoir::new(storage, start_tx_id, dam_handle))
                .expect("Could not create reservoir")
        });
        Self {
            reservoir,
            dam_thread: Some(dam_thread),
        }
    }

    #[inline]
    fn runtime_handle(&self) -> Handle {
        Self::RUNTIME.with(|rt| rt.handle().clone())
    }

    fn reserve(&self, size: usize) -> ReservoirResult<WriteHandle<FileSlice, Arc<Event>>> {
        Self::RUNTIME.with(|rt| rt.block_on(self.reservoir.new_transaction_fixed(size)))
    }
}

pub struct WriteHandleImpl {
    reservoir_handle: WriteHandle<FileSlice, Arc<Event>>,
    runtime_handle: Handle,
}

#[no_mangle]
pub unsafe fn reservoir_new(path: *const c_char, reservoir_out: *mut *mut ReservoirImpl) -> i32 {
    match unsafe { std::ffi::CStr::from_ptr(path) }.to_str() {
        Ok(path) => {
            let boxed = Box::new(ReservoirImpl::new(path));
            *reservoir_out = Box::leak(boxed) as *mut ReservoirImpl;
            0
        }
        Err(_) => 30_000,
    }
}

#[no_mangle]
pub unsafe fn reservoir_reserve(
    reservoir: *mut ReservoirImpl,
    size: usize,
    handle_out: *mut *mut c_void,
) -> i32 {
    let reservoir = unsafe { &*(reservoir) };
    match reservoir.reserve(size) {
        Ok(handle) => {
            let handle = WriteHandleImpl {
                reservoir_handle: handle,
                runtime_handle: reservoir.runtime_handle(),
            };
            let handle_ptr = Box::leak(Box::new(handle));
            unsafe { *handle_out = handle_ptr as *mut _ as *mut c_void };
            0
        }
        Err(err) => result_into_error_no(err),
    }
}

#[no_mangle]
pub unsafe fn reservoir_free(reservoir: *mut ReservoirImpl) {
    if !reservoir.is_null() {
        let mut boxed = Box::from_raw(reservoir);
        let dam_thread = boxed.dam_thread.take();
        drop(boxed);
        if let Some(handle) = dam_thread {
            handle.join().unwrap();
        }
    }
}

#[no_mangle]
pub unsafe fn reservoir_handle_write_bytes(
    handle: *mut WriteHandleImpl,
    bytes: *const u8,
    size: usize,
) -> i32 {
    let handle = &mut *handle;
    let bytes = unsafe { std::slice::from_raw_parts(bytes, size) };
    handle
        .runtime_handle
        .block_on(handle.reservoir_handle.write_bytes(bytes))
        .map(|_| 0)
        .unwrap_or_else(result_into_error_no)
}

#[no_mangle]
pub unsafe fn reservoir_get_transaction(reservoir: *mut ReservoirImpl, transaction_id: u64) -> i32 {
    let transaction_id = TransactionId::from(transaction_id);
    let reservoir = unsafe { &*(reservoir) };
    match reservoir
        .runtime_handle()
        .block_on(reservoir.reservoir.get_transaction(transaction_id))
    {
        Ok(reader) => 0,
        Err(err) => result_into_error_no(err),
    }
}

#[no_mangle]
pub unsafe fn reservoir_handle_transaction_id(handle: *mut WriteHandleImpl) -> u64 {
    let handle = &*handle;
    handle.reservoir_handle.transaction_id().into()
}

#[no_mangle]
pub unsafe fn reservoir_handle_commit(handle: *mut WriteHandleImpl) -> i32 {
    let handle = Box::from_raw(handle);
    handle
        .runtime_handle
        .block_on(handle.reservoir_handle.commit())
        .map(|_| 0)
        .unwrap_or_else(result_into_error_no)
}

#[no_mangle]
pub unsafe fn reservoir_handle_free(handle: *mut c_void) {
    if !handle.is_null() {
        let _ = Box::from_raw(handle as *mut WriteHandleImpl);
    }
}

#[inline]
fn result_into_error_no(err: ReservoirError) -> i32 {
    match err {
        ReservoirError::IoError(err) => err.raw_os_error().map(|e| e + 10_000).unwrap_or(-1),
        ReservoirError::NoSuchSegment(_) => 20_000,
    }
}
