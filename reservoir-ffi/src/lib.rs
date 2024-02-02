#![allow(clippy::missing_safety_doc)]

use reservoir::{
    DamIterator, DamLog, Event, FilePool, FileSlice, FlushStrategy, Reservoir, ReservoirError,
    ReservoirResult, SyncDamFlusher, TransactionId, WriteHandle,
};
use std::ffi::{c_char, c_void};
use std::path::Path;
use std::ptr::null;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::runtime::{Handle, Runtime};

const ONE_MEBIBYTE: u64 = 1024 * 1024;
const SYNC_INTERVAL: Duration = Duration::from_millis(5);

pub struct ReservoirImpl {
    /// The reservoir instance.
    reservoir: Reservoir<FilePool, Arc<Event>>,
    /// A handle to the dam thread.
    /// This is used to ensure that the dam thread is joined when the ReservoirImpl is dropped.
    dam_thread: Option<std::thread::JoinHandle<()>>,
    /// The dam log.
    /// The dam log is used to store transaction IDs and their corresponding checksums in order of commit.
    dam_log: DamLog,
}

impl ReservoirImpl {
    thread_local! {
        pub static RUNTIME: Runtime = tokio::runtime::Builder::new_current_thread().build().unwrap();
    }
    fn new(path: &str) -> Self {
        let storage = FilePool::open(Path::new(path), 8, 32 * ONE_MEBIBYTE)
            .expect("Could not open file pool");
        let dam_log = DamLog::new(Path::new("./dam.log")).expect("Could not open/create Dam log");
        let dam_for_thread = dam_log.clone();
        let dam = SyncDamFlusher::new(dam_for_thread, FlushStrategy::Interval(SYNC_INTERVAL))
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
            dam_log,
        }
    }

    #[inline]
    fn runtime_handle(&self) -> Handle {
        Self::RUNTIME.with(|rt| rt.handle().clone())
    }

    fn reserve(&self, size: usize) -> ReservoirResult<WriteHandle<FileSlice, Arc<Event>>> {
        Self::RUNTIME.with(|rt| rt.block_on(self.reservoir.new_transaction_fixed(size)))
    }

    fn read_into_buffer_blocking(
        &mut self,
        transaction_id: TransactionId,
        mem_buffer: &mut Vec<u8>,
    ) -> ReservoirResult<usize> {
        mem_buffer.clear();
        Self::RUNTIME.with(|rt| {
            rt.block_on(async {
                let mut reader = self.reservoir.get_transaction(transaction_id).await?;
                reader.read_to_end(mem_buffer).await?;
                Ok(reader.size)
            })
        })
    }
}

pub struct WriteHandleImpl {
    reservoir_handle: WriteHandle<FileSlice, Arc<Event>>,
    runtime_handle: Handle,
}

pub struct ReadBuffer {
    /// Buffer to deserialize any reads from the reservoir
    /// The maximum size of a transaction is 1 MiB.
    /// Note: This is to offer mmap type behavior to any readers.
    pub mem_buffer: Vec<u8>,
}

pub struct IterHandle {
    pub iter: DamIterator<'static>,
    /// Buffer to deserialize any reads from the reservoir
    /// The maximum size of a transaction is 1 MiB.
    /// Note: This is to offer mmap type behavior to any readers.
    pub mem_buffer: Vec<u8>,
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
pub unsafe fn reservoir_get_transaction(
    reservoir: *mut ReservoirImpl,
    read_buffer: *mut ReadBuffer,
    transaction_id: u64,
    data_out: *mut *const u8,
    size_out: *mut u64,
) -> i32 {
    let transaction_id = TransactionId::from(transaction_id);
    let reservoir = unsafe { &mut *(reservoir) };
    let read_buffer = unsafe { &mut *(read_buffer) };
    reservoir
        .read_into_buffer_blocking(transaction_id, &mut read_buffer.mem_buffer)
        .map(|size| {
            *size_out = size as u64;
            *data_out = read_buffer.mem_buffer.as_mut_ptr();
            0
        })
        .unwrap_or_else(result_into_error_no)
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

#[no_mangle]
pub unsafe fn reservoir_get_iter(reservoir: *mut ReservoirImpl, iter_out: *mut *const c_void) {
    let reservoir = &mut *(reservoir);
    let iter = reservoir.dam_log.clone().owned_iter();
    *iter_out = Box::into_raw(Box::new(iter)) as *mut c_void
}

#[no_mangle]
pub unsafe fn reservoir_make_buffer(buffer_out: *mut *const ReadBuffer) {
    let boxed = Box::new(ReadBuffer {
        mem_buffer: Vec::with_capacity(ONE_MEBIBYTE as usize),
    });
    *buffer_out = Box::leak(boxed) as *mut ReadBuffer;
}

#[no_mangle]
pub unsafe fn reservoir_buffer_free(buffer: *mut ReadBuffer) {
    if !buffer.is_null() {
        let _ = Box::from_raw(buffer);
    }
}

#[no_mangle]
pub unsafe fn reservoir_iter_next(
    reservoir_impl: *mut ReservoirImpl,
    iter: *mut IterHandle,
    tx_id: *mut u64,
    size: &mut usize,
    lsn: *mut u64,
    data_out: *mut *const u8,
    is_done: *mut bool,
) -> i32 {
    let iter_handle = &mut *(iter);
    let reservoir = &mut *(reservoir_impl);

    *data_out = null();
    match iter_handle.iter.next() {
        Some((serialized_txn, file_offset)) => {
            *tx_id = serialized_txn.id.into();
            *size = serialized_txn.size as usize;
            *lsn = file_offset.into();
            *is_done = false;
            reservoir
                .read_into_buffer_blocking(serialized_txn.id, &mut iter_handle.mem_buffer)
                .map(|_| {
                    // Set the output pointer to the start of the buffer
                    *data_out = iter_handle.mem_buffer.as_mut_ptr();
                    0
                })
                .unwrap_or_else(result_into_error_no)
        }
        None => {
            *is_done = true;
            0
        }
    }
}

#[no_mangle]
pub unsafe fn reservoir_iter_free(iter: *mut DamIterator) {
    if !iter.is_null() {
        let _ = Box::from_raw(iter);
    }
}

#[inline]
fn result_into_error_no(err: ReservoirError) -> i32 {
    match err {
        ReservoirError::IoError(err) => err.raw_os_error().map(|e| e + 10_000).unwrap_or(-1),
        ReservoirError::NoSuchSegment(_) => 20_000,
    }
}
