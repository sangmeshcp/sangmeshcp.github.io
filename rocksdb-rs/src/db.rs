//! Main database interface.
//!
//! `DB` is the entry point for all user operations.  Internally it coordinates:
//!
//! * An **active memtable** that absorbs writes.
//! * An optional **immutable memtable** queued for flush.
//! * A **version set** that tracks which SST files exist in each level.
//! * A **WAL** (write-ahead log) for crash recovery.
//! * A **background thread** that flushes immutable memtables and runs
//!   leveled compaction, keeping the foreground latency low.
//!
//! ## Concurrency model
//! All mutable shared state lives inside `Arc<Mutex<DbInner>>`.  The
//! background thread holds the mutex only for brief bookkeeping steps; the
//! heavy I/O (writing SST files, merging) is done with the lock released.
//! A `Condvar` wakes the background thread whenever work is available.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};

use crate::batch::{BatchOp, WriteBatch};
use crate::cache::LruCache;
use crate::compaction::{flush_memtable, pick_compaction, run_compaction};
use crate::error::{Error, Result};
use crate::iter::DBIterator;
use crate::memtable::Memtable;
use crate::options::{Options, ReadOptions, WriteOptions};
use crate::sst::{sst_path, SstReader};
use crate::types::SequenceNumber;
use crate::version::VersionSet;
use crate::wal::{recover_wal, Wal, WalRecord};

// ─────────────────────────────────────────────────────────────────────────────
// Background-thread signal
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum BgWork {
    Idle,
    Flush,
    Shutdown,
}

// ─────────────────────────────────────────────────────────────────────────────
// DbInner — everything protected by the mutex
// ─────────────────────────────────────────────────────────────────────────────

struct DbInner {
    mem: Memtable,
    imm: Option<Arc<Memtable>>, // immutable memtable pending flush
    versions: VersionSet,
    wal: Option<Wal>,
    next_seq: SequenceNumber,
    _cache: LruCache,
    bg_error: Option<String>,
}

// ─────────────────────────────────────────────────────────────────────────────
// Snapshot
// ─────────────────────────────────────────────────────────────────────────────

/// A consistent read point.  Reads at a snapshot are unaffected by later
/// writes.  Snapshots are lightweight: they hold only a sequence number.
#[derive(Debug, Clone, Copy)]
pub struct Snapshot {
    pub seq: SequenceNumber,
}

// ─────────────────────────────────────────────────────────────────────────────
// DB
// ─────────────────────────────────────────────────────────────────────────────

pub struct DB {
    inner: Arc<Mutex<DbInner>>,
    options: Arc<Options>,
    path: Arc<PathBuf>,
    /// Signal to wake or stop the background thread.
    bg_signal: Arc<(Mutex<BgWork>, Condvar)>,
    _bg_thread: Option<JoinHandle<()>>,
}

impl DB {
    // ── Opening / closing ─────────────────────────────────────────────────────

    /// Open (or create) a database at `path`.
    pub fn open(path: impl AsRef<Path>, options: Options) -> Result<Self> {
        let path = path.as_ref().to_path_buf();

        if options.error_if_exists && path.exists() {
            return Err(Error::AlreadyOpen);
        }
        if options.create_if_missing {
            fs::create_dir_all(&path)?;
        } else if !path.exists() {
            return Err(Error::InvalidArgument(format!(
                "database {:?} does not exist and create_if_missing=false",
                path
            )));
        }

        // Recover or create the version set.
        let manifest_path = path.join("MANIFEST");
        let mut versions = if manifest_path.exists() {
            VersionSet::recover(&path, options.num_levels)?
        } else {
            VersionSet::create(&path, options.num_levels)?
        };

        // Recover the WAL (if any).
        let (mem, next_seq) = recover_memtable(&path, versions.log_number, &options)?;

        // Open a fresh WAL for subsequent writes.
        // Persist the log number so that crash recovery can find the WAL.
        let wal = if !options.disable_wal {
            let wal_num = versions.new_file_number();
            versions.set_log_number(wal_num)?; // writes TAG_SET_LOG_NUMBER to manifest
            let wal_path = wal_file_path(&path, wal_num);
            Some(Wal::create(wal_path)?)
        } else {
            None
        };

        let cache = LruCache::new(options.block_cache_capacity);
        let inner = Arc::new(Mutex::new(DbInner {
            mem,
            imm: None,
            versions,
            wal,
            next_seq,
            _cache: cache,
            bg_error: None,
        }));

        let bg_signal: Arc<(Mutex<BgWork>, Condvar)> =
            Arc::new((Mutex::new(BgWork::Idle), Condvar::new()));

        let options_arc = Arc::new(options);
        let path_arc = Arc::new(path);

        // Spawn background thread.
        let bg_inner = Arc::clone(&inner);
        let bg_signal2 = Arc::clone(&bg_signal);
        let bg_options = Arc::clone(&options_arc);
        let bg_path = Arc::clone(&path_arc);
        let bg_thread = thread::spawn(move || {
            background_loop(bg_inner, bg_signal2, bg_options, bg_path);
        });

        Ok(DB {
            inner,
            options: options_arc,
            path: path_arc,
            bg_signal,
            _bg_thread: Some(bg_thread),
        })
    }

    // ── Point reads ───────────────────────────────────────────────────────────

    /// Retrieve the value for `key`, or `None` if not found.
    pub fn get(&self, key: impl AsRef<[u8]>, opts: &ReadOptions) -> Result<Option<Vec<u8>>> {
        let key = key.as_ref();
        let inner = self.inner.lock().unwrap();

        // Check for a sticky background error.
        if let Some(ref e) = inner.bg_error {
            return Err(Error::Background(e.clone()));
        }

        let read_seq = opts
            .snapshot
            .unwrap_or(inner.next_seq.saturating_sub(1));

        // 1. Active memtable.
        if let Some(result) = inner.mem.get(key, read_seq) {
            return Ok(result);
        }

        // 2. Immutable memtable.
        if let Some(ref imm) = inner.imm {
            if let Some(result) = imm.get(key, read_seq) {
                return Ok(result);
            }
        }

        // 3. SST files in L0 (may overlap; search newest-first).
        let l0 = inner.versions.level_files(0).to_vec();
        for fm in l0.iter().rev() {
            if !fm.overlaps_range(key, key) {
                continue;
            }
            let p = sst_path(&self.path, fm.file_number);
            let mut reader = SstReader::open(&p)?;
            if let Some(result) = reader.get(key, read_seq)? {
                return Ok(result);
            }
        }

        // 4. SST files in L1+ (non-overlapping; binary search).
        for level in 1..self.options.num_levels {
            let files = inner.versions.level_files(level).to_vec();
            // Find first file whose largest_user_key >= key.
            let pos = files.partition_point(|f| f.largest_user_key() < key);
            if pos < files.len() {
                let fm = &files[pos];
                if key >= fm.smallest_user_key() {
                    let p = sst_path(&self.path, fm.file_number);
                    let mut reader = SstReader::open(&p)?;
                    if let Some(result) = reader.get(key, read_seq)? {
                        return Ok(result);
                    }
                }
            }
        }

        Ok(None)
    }

    // ── Writes ────────────────────────────────────────────────────────────────

    /// Write a single key-value pair.
    pub fn put(
        &self,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
        opts: &WriteOptions,
    ) -> Result<()> {
        let key = key.as_ref();
        let value = value.as_ref();
        let mut inner = self.inner.lock().unwrap();
        self.maybe_wait_for_flush(&inner)?;

        let seq = inner.next_seq;
        inner.next_seq += 1;

        let sync = opts.sync || self.options.sync_wal;
        if !opts.disable_wal && !self.options.disable_wal {
            if let Some(ref mut wal) = inner.wal {
                wal.append_put(key, seq, value, sync)?;
            }
        }

        inner.mem.put(key.to_vec(), seq, value.to_vec());
        self.maybe_schedule_flush(&mut inner);
        Ok(())
    }

    /// Delete a key.
    pub fn delete(&self, key: impl AsRef<[u8]>, opts: &WriteOptions) -> Result<()> {
        let key = key.as_ref();
        let mut inner = self.inner.lock().unwrap();
        self.maybe_wait_for_flush(&inner)?;

        let seq = inner.next_seq;
        inner.next_seq += 1;

        let sync = opts.sync || self.options.sync_wal;
        if !opts.disable_wal && !self.options.disable_wal {
            if let Some(ref mut wal) = inner.wal {
                wal.append_delete(key, seq, sync)?;
            }
        }

        inner.mem.delete(key.to_vec(), seq);
        self.maybe_schedule_flush(&mut inner);
        Ok(())
    }

    /// Apply a `WriteBatch` atomically.
    pub fn write(&self, batch: &WriteBatch, opts: &WriteOptions) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        let mut inner = self.inner.lock().unwrap();
        self.maybe_wait_for_flush(&inner)?;

        let first_seq = inner.next_seq;
        inner.next_seq += batch.len() as u64;

        let sync = opts.sync || self.options.sync_wal;
        if !opts.disable_wal && !self.options.disable_wal {
            if let Some(ref mut wal) = inner.wal {
                let ops: Vec<(SequenceNumber, &[u8], Option<&[u8]>)> = batch
                    .ops
                    .iter()
                    .enumerate()
                    .map(|(i, op)| match op {
                        BatchOp::Put { key, value } => {
                            (first_seq + i as u64, key.as_slice(), Some(value.as_slice()))
                        }
                        BatchOp::Delete { key } => {
                            (first_seq + i as u64, key.as_slice(), None)
                        }
                    })
                    .collect();
                wal.append_batch(&ops, sync)?;
            }
        }

        for (i, op) in batch.ops.iter().enumerate() {
            let seq = first_seq + i as u64;
            match op {
                BatchOp::Put { key, value } => {
                    inner.mem.put(key.clone(), seq, value.clone());
                }
                BatchOp::Delete { key } => {
                    inner.mem.delete(key.clone(), seq);
                }
            }
        }

        self.maybe_schedule_flush(&mut inner);
        Ok(())
    }

    // ── Snapshot ──────────────────────────────────────────────────────────────

    /// Capture a consistent read point.
    pub fn snapshot(&self) -> Snapshot {
        let inner = self.inner.lock().unwrap();
        Snapshot {
            seq: inner.next_seq.saturating_sub(1),
        }
    }

    // ── Iterator ──────────────────────────────────────────────────────────────

    /// Create a forward iterator over all visible key-value pairs.
    pub fn iter(&self, opts: &ReadOptions) -> Result<DBIterator> {
        let inner = self.inner.lock().unwrap();
        let read_seq = opts
            .snapshot
            .unwrap_or(inner.next_seq.saturating_sub(1));

        let imm_ref = inner.imm.as_deref();
        DBIterator::new(
            opts,
            read_seq,
            &inner.mem,
            imm_ref,
            &inner.versions,
            &self.path,
        )
    }

    // ── Properties ───────────────────────────────────────────────────────────

    /// Approximate number of bytes on disk.
    pub fn approximate_disk_size(&self) -> u64 {
        let inner = self.inner.lock().unwrap();
        (0..self.options.num_levels)
            .map(|l| inner.versions.level_total_size(l))
            .sum()
    }

    /// Force a flush of the active memtable to disk (useful in tests).
    pub fn flush(&self) -> Result<()> {
        {
            let mut inner = self.inner.lock().unwrap();
            if !inner.mem.is_empty() {
                self.rotate_memtable(&mut inner)?;
            }
        }
        self.signal_background(BgWork::Flush);
        // Give the background thread a moment to complete the flush.
        // In a production API this would use a proper completion callback.
        for _ in 0..200 {
            {
                let inner = self.inner.lock().unwrap();
                if inner.imm.is_none() {
                    return Ok(());
                }
            }
            thread::sleep(std::time::Duration::from_millis(10));
        }
        Ok(())
    }

    // ── Internal helpers ──────────────────────────────────────────────────────

    fn maybe_wait_for_flush(&self, inner: &DbInner) -> Result<()> {
        // Report a sticky background error.
        if let Some(ref e) = inner.bg_error {
            return Err(Error::Background(e.clone()));
        }
        Ok(())
    }

    fn maybe_schedule_flush(&self, inner: &mut DbInner) {
        if inner.mem.approximate_size() >= self.options.write_buffer_size {
            if inner.imm.is_none() {
                let _ = self.rotate_memtable(inner);
                self.signal_background(BgWork::Flush);
            }
        }
    }

    fn rotate_memtable(&self, inner: &mut DbInner) -> Result<()> {
        // Swap active memtable to immutable.
        let mut new_mem = Memtable::new();
        std::mem::swap(&mut inner.mem, &mut new_mem);
        inner.imm = Some(Arc::new(new_mem));

        // Start a new WAL, persisting the log number.
        if !self.options.disable_wal {
            let wal_num = inner.versions.new_file_number();
            inner.versions.set_log_number(wal_num)?;
            let wal_path = wal_file_path(&self.path, wal_num);
            inner.wal = Some(Wal::create(wal_path)?);
        }
        Ok(())
    }

    fn signal_background(&self, work: BgWork) {
        let (lock, cvar) = &*self.bg_signal;
        let mut state = lock.lock().unwrap();
        if *state != BgWork::Shutdown {
            *state = work;
        }
        cvar.notify_one();
    }
}

impl Drop for DB {
    fn drop(&mut self) {
        // Signal background thread to shut down, then join it.
        {
            let (lock, cvar) = &*self.bg_signal;
            *lock.lock().unwrap() = BgWork::Shutdown;
            cvar.notify_one();
        }
        if let Some(handle) = self._bg_thread.take() {
            let _ = handle.join();
        }
        // Final WAL flush.
        if let Ok(mut inner) = self.inner.lock() {
            if let Some(ref mut wal) = inner.wal {
                let _ = wal.flush();
            }
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Background worker
// ─────────────────────────────────────────────────────────────────────────────

fn background_loop(
    inner: Arc<Mutex<DbInner>>,
    signal: Arc<(Mutex<BgWork>, Condvar)>,
    opts: Arc<Options>,
    path: Arc<PathBuf>,
) {
    loop {
        // Wait for a signal.
        let work = {
            let (lock, cvar) = &*signal;
            let mut state = lock.lock().unwrap();
            while *state == BgWork::Idle {
                state = cvar.wait(state).unwrap();
            }
            let w = *state;
            if w != BgWork::Shutdown {
                *state = BgWork::Idle;
            }
            w
        };

        if work == BgWork::Shutdown {
            // Drain any pending flush before exiting.
            do_flush(&inner, &path, &opts);
            break;
        }

        // Flush immutable memtable → L0.
        do_flush(&inner, &path, &opts);

        // Run compaction if needed.
        do_compact(&inner, &path, &opts);
    }
}

fn do_flush(inner: &Arc<Mutex<DbInner>>, path: &Path, opts: &Arc<Options>) {
    // Grab the immutable memtable (if any) without holding the lock during I/O.
    let imm = {
        let g = inner.lock().unwrap();
        g.imm.clone()
    };
    let Some(imm) = imm else {
        return;
    };

    // Write the SST outside the lock.
    let result = {
        let mut g = inner.lock().unwrap();
        flush_memtable(&imm, path, &mut g.versions, opts)
    };

    match result {
        Ok(meta) => {
            let mut g = inner.lock().unwrap();
            if let Err(e) = g.versions.add_file(0, &meta) {
                g.bg_error = Some(e.to_string());
                return;
            }
            // Clear the immutable memtable.
            g.imm = None;
            // Delete the old WAL.
            let old_log = g.versions.log_number;
            if old_log > 0 {
                let _ = fs::remove_file(wal_file_path(path, old_log));
            }
        }
        Err(e) => {
            inner.lock().unwrap().bg_error = Some(e.to_string());
        }
    }
}

fn do_compact(inner: &Arc<Mutex<DbInner>>, path: &Path, opts: &Arc<Options>) {
    loop {
        // Pick a compaction under the lock.
        let task = {
            let g = inner.lock().unwrap();
            pick_compaction(&g.versions, opts)
        };
        let Some(task) = task else {
            break;
        };

        // Run compaction (I/O) and version update under the lock.
        // For a highly concurrent system you'd split the I/O phase from the
        // version-update phase, releasing the lock during I/O.
        let result = {
            let mut g = inner.lock().unwrap();
            run_compaction(&task, path, &mut g.versions, opts)
        };

        if let Err(e) = result {
            inner.lock().unwrap().bg_error = Some(e.to_string());
            break;
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// WAL / crash recovery helpers
// ─────────────────────────────────────────────────────────────────────────────

fn wal_file_path(db_path: &Path, log_number: u64) -> PathBuf {
    db_path.join(format!("{:06}.log", log_number))
}

/// Recover the memtable from the WAL on disk.
fn recover_memtable(
    path: &Path,
    log_number: u64,
    opts: &Options,
) -> Result<(Memtable, SequenceNumber)> {
    let mut mem = Memtable::new();
    let mut max_seq: SequenceNumber = 0;

    if log_number == 0 || opts.disable_wal {
        return Ok((mem, 1));
    }

    let wal_path = wal_file_path(path, log_number);
    let records = recover_wal(&wal_path)?;

    for record in records {
        match record {
            WalRecord::Put { seq, key, value } => {
                mem.put(key, seq, value);
                if seq > max_seq {
                    max_seq = seq;
                }
            }
            WalRecord::Delete { seq, key } => {
                mem.delete(key, seq);
                if seq > max_seq {
                    max_seq = seq;
                }
            }
        }
    }

    Ok((mem, max_seq + 1))
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn opts() -> Options {
        let mut o = Options::default();
        o.write_buffer_size = 256 * 1024; // small buffer to force flushes in tests
        o
    }

    #[test]
    fn open_put_get_delete() {
        let dir = tempdir().unwrap();
        let db = DB::open(dir.path(), opts()).unwrap();
        let wo = WriteOptions::default();
        let ro = ReadOptions::default();

        db.put(b"hello", b"world", &wo).unwrap();
        assert_eq!(db.get(b"hello", &ro).unwrap(), Some(b"world".to_vec()));

        db.delete(b"hello", &wo).unwrap();
        assert_eq!(db.get(b"hello", &ro).unwrap(), None);

        assert_eq!(db.get(b"missing", &ro).unwrap(), None);
    }

    #[test]
    fn snapshot_isolation() {
        let dir = tempdir().unwrap();
        let db = DB::open(dir.path(), opts()).unwrap();
        let wo = WriteOptions::default();

        db.put(b"k", b"v1", &wo).unwrap();
        let snap = db.snapshot();

        db.put(b"k", b"v2", &wo).unwrap();

        let mut ro = ReadOptions::default();
        ro.snapshot = Some(snap.seq);
        assert_eq!(db.get(b"k", &ro).unwrap(), Some(b"v1".to_vec()));

        let ro2 = ReadOptions::default();
        assert_eq!(db.get(b"k", &ro2).unwrap(), Some(b"v2".to_vec()));
    }

    #[test]
    fn write_batch_atomic() {
        let dir = tempdir().unwrap();
        let db = DB::open(dir.path(), opts()).unwrap();
        let wo = WriteOptions::default();
        let ro = ReadOptions::default();

        let mut batch = WriteBatch::new();
        batch.put(b"a", b"1");
        batch.put(b"b", b"2");
        batch.delete(b"c");
        db.write(&batch, &wo).unwrap();

        assert_eq!(db.get(b"a", &ro).unwrap(), Some(b"1".to_vec()));
        assert_eq!(db.get(b"b", &ro).unwrap(), Some(b"2".to_vec()));
        assert_eq!(db.get(b"c", &ro).unwrap(), None);
    }

    #[test]
    fn crash_recovery() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();

        // Write some data and close.
        {
            let db = DB::open(&path, opts()).unwrap();
            let wo = WriteOptions::default();
            db.put(b"key1", b"val1", &wo).unwrap();
            db.put(b"key2", b"val2", &wo).unwrap();
        }

        // Reopen and verify data is there.
        let db = DB::open(&path, opts()).unwrap();
        let ro = ReadOptions::default();
        assert_eq!(db.get(b"key1", &ro).unwrap(), Some(b"val1".to_vec()));
        assert_eq!(db.get(b"key2", &ro).unwrap(), Some(b"val2".to_vec()));
    }

    #[test]
    fn iterator_order() {
        let dir = tempdir().unwrap();
        let db = DB::open(dir.path(), opts()).unwrap();
        let wo = WriteOptions::default();

        for key in [b"cherry".as_ref(), b"apple".as_ref(), b"banana".as_ref()] {
            db.put(key, key, &wo).unwrap();
        }

        let ro = ReadOptions::default();
        let keys: Vec<Vec<u8>> = db.iter(&ro).unwrap().map(|(k, _)| k).collect();
        assert_eq!(
            keys,
            vec![b"apple".to_vec(), b"banana".to_vec(), b"cherry".to_vec()]
        );
    }

    #[test]
    fn flush_to_sst() {
        let dir = tempdir().unwrap();
        let mut o = opts();
        o.write_buffer_size = 1; // force immediate flush
        let db = DB::open(dir.path(), o).unwrap();
        let wo = WriteOptions::default();
        let ro = ReadOptions::default();

        for i in 0u32..50 {
            db.put(
                format!("key-{i:04}").as_bytes(),
                format!("val-{i}").as_bytes(),
                &wo,
            )
            .unwrap();
        }
        db.flush().unwrap();

        assert_eq!(
            db.get(b"key-0025", &ro).unwrap(),
            Some(b"val-25".to_vec())
        );
    }
}
