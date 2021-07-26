// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::ResourceMeteringTag;

use std::sync::atomic::Ordering::SeqCst;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use collections::HashMap;

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "linux")]
pub use linux::{init_recorder, Guard};

#[cfg(not(target_os = "linux"))]
mod dummy;
#[cfg(not(target_os = "linux"))]
pub use dummy::{init_recorder, Guard};

#[derive(Clone, Default)]
pub struct RecorderHandle {
    inner: Option<Arc<RecorderHandleInner>>,
}

pub struct RecorderHandleInner {
    join_handle: JoinHandle<()>,
    pause: Arc<AtomicBool>,
    precision_ms: Arc<AtomicU64>,
}

impl RecorderHandle {
    pub fn new(
        join_handle: JoinHandle<()>,
        pause: Arc<AtomicBool>,
        precision_ms: Arc<AtomicU64>,
    ) -> Self {
        Self {
            inner: Some(Arc::new(RecorderHandleInner {
                join_handle,
                pause,
                precision_ms,
            })),
        }
    }

    pub fn pause(&self) {
        if let Some(inner) = self.inner.as_ref() {
            inner.pause.store(true, SeqCst);
        }
    }

    pub fn resume(&self) {
        if let Some(inner) = self.inner.as_ref() {
            inner.pause.store(false, SeqCst);
            inner.join_handle.thread().unpark();
        }
    }

    pub fn set_precision(&self, value: Duration) {
        if let Some(inner) = self.inner.as_ref() {
            inner.precision_ms.store(value.as_millis() as _, SeqCst);
        }
    }
}

#[derive(Debug)]
pub struct CpuRecords {
    pub begin_unix_time_secs: u64,
    pub duration: Duration,

    // tag -> ms
    pub records: HashMap<ResourceMeteringTag, Record>,
}

impl Default for CpuRecords {
    fn default() -> Self {
        let now_unix_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Clock may have gone backwards");
        Self {
            begin_unix_time_secs: now_unix_time.as_secs(),
            duration: Duration::default(),
            records: HashMap::default(),
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct Record {
    pub cpu_time_ms: u32,
    pub scan_rows: u64,
}

impl Record {
    pub fn new(cpu_time_ms: u32, scan_rows: u64) -> Self {
        Self {
            cpu_time_ms,
            scan_rows,
        }
    }
    pub fn merge(&mut self, cpu_time_ms: u32, scan_rows: u64) {
        self.cpu_time_ms += cpu_time_ms;
        self.scan_rows += scan_rows;
    }
}

pub struct LocalReqRowStatistics {
    scan_row_count: AtomicU64,
}

impl LocalReqRowStatistics {
    fn new() -> Self {
        Self {
            scan_row_count: AtomicU64::new(0),
        }
    }

    pub fn add_scan_row_count(&self, count: u64) {
        self.scan_row_count.fetch_add(count, Ordering::Relaxed);
    }

    pub fn get_scan_row_count(&self) -> u64 {
        self.scan_row_count.fetch_or(0, Ordering::Relaxed)
    }
}

pub struct ThreadLocalReq;

impl ThreadLocalReq {
    thread_local! {
        pub static LOCAL_REQ_SCAN_ROW_STATISTICS: Arc<LocalReqRowStatistics> = Arc::new(LocalReqRowStatistics::new());
    }
}
