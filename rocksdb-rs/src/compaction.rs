//! Leveled Compaction
//!
//! Compaction merges overlapping / stale SST files to reclaim space, remove
//! tombstones and keep read amplification low.
//!
//! ## Strategy: Leveled Compaction
//! * **L0 → L1**: when the number of L0 files exceeds
//!   `level0_file_num_compaction_trigger`, *all* L0 files plus every L1 file
//!   whose key range overlaps with any L0 file are merged into new L1 files.
//! * **Ln → L(n+1)** (n ≥ 1): when the total size of Ln exceeds its budget, a
//!   single "compaction file" is picked (round-robin) and merged with all
//!   overlapping files in L(n+1).
//!
//! After merging, tombstones are dropped if the compaction covers the bottom-
//! most level that contains data (there can be no older version below).
//!
//! ## Output file size
//! Merged entries are split into new SST files of at most
//! `options.target_file_size_base` bytes.  Each output file is assigned a new
//! file number from the version set.

use std::fs;
use std::path::Path;
use std::sync::Arc;

use crate::error::{Error, Result};
use crate::options::Options;
use crate::sst::{sst_path, SstBuilder, SstMeta, SstReader};
use crate::types::{InternalKey, KeyKind};
use crate::version::{FileMetadata, VersionSet};

// ─────────────────────────────────────────────────────────────────────────────
// CompactionTask — what to merge
// ─────────────────────────────────────────────────────────────────────────────

pub struct CompactionTask {
    /// Source level (files being merged *from*).
    pub src_level: usize,
    /// Files from `src_level` included in the compaction.
    pub inputs_src: Vec<Arc<FileMetadata>>,
    /// Files from `src_level + 1` whose ranges overlap.
    pub inputs_dst: Vec<Arc<FileMetadata>>,
    /// Whether tombstones can be dropped (we're at the bottom-most level with data).
    pub is_bottommost: bool,
}

// ─────────────────────────────────────────────────────────────────────────────
// pick_compaction — decide what (if anything) to compact
// ─────────────────────────────────────────────────────────────────────────────

/// Return a compaction task if any level needs compaction, `None` otherwise.
pub fn pick_compaction(versions: &VersionSet, opts: &Options) -> Option<CompactionTask> {
    // 1. L0 compaction trigger.
    if versions.num_level_files(0) >= opts.level0_file_num_compaction_trigger {
        let inputs_src: Vec<Arc<FileMetadata>> =
            versions.level_files(0).to_vec();

        // Key range covered by all L0 files.
        let (lo, hi) = key_range_of(&inputs_src);
        let inputs_dst: Vec<Arc<FileMetadata>> = versions
            .level_files(1)
            .iter()
            .filter(|f| f.overlaps_range(&lo, &hi))
            .cloned()
            .collect();

        let is_bottommost =
            is_bottommost_with_files(versions, 1, opts.num_levels);

        return Some(CompactionTask {
            src_level: 0,
            inputs_src,
            inputs_dst,
            is_bottommost,
        });
    }

    // 2. Ln size trigger (L1 and above).
    for level in 1..opts.num_levels - 1 {
        let total = versions.level_total_size(level);
        let budget = opts.max_bytes_for_level(level);
        if total > budget {
            let files = versions.level_files(level);
            if files.is_empty() {
                continue;
            }
            // Pick the file with the largest number of entries to maximise
            // bang-per-compaction.  A production implementation uses a
            // round-robin pointer to spread I/O evenly across the level.
            let candidate = files
                .iter()
                .max_by_key(|f| f.num_entries)
                .cloned()
                .unwrap();

            let (lo, hi) = (
                candidate.smallest_user_key().to_vec(),
                candidate.largest_user_key().to_vec(),
            );
            let inputs_dst: Vec<Arc<FileMetadata>> = versions
                .level_files(level + 1)
                .iter()
                .filter(|f| f.overlaps_range(&lo, &hi))
                .cloned()
                .collect();

            let is_bottommost =
                is_bottommost_with_files(versions, level + 1, opts.num_levels);

            return Some(CompactionTask {
                src_level: level,
                inputs_src: vec![candidate],
                inputs_dst,
                is_bottommost,
            });
        }
    }
    None
}

// ─────────────────────────────────────────────────────────────────────────────
// run_compaction
// ─────────────────────────────────────────────────────────────────────────────

/// Execute a compaction: read input files, merge-sort, write output files,
/// then update the version set and delete obsolete files.
///
/// This function is called **without** the DB mutex held so that foreground
/// reads and writes can continue concurrently.  The version-set update at the
/// end acquires the mutex briefly.
pub fn run_compaction(
    task: &CompactionTask,
    db_path: &Path,
    versions: &mut VersionSet,
    opts: &Options,
) -> Result<()> {
    let dst_level = task.src_level + 1;

    // ── 1. Collect all input entries ──────────────────────────────────────────
    let mut all_entries: Vec<(InternalKey, Vec<u8>)> = Vec::new();

    for fm in task.inputs_src.iter().chain(task.inputs_dst.iter()) {
        let path = sst_path(db_path, fm.file_number);
        let mut reader = SstReader::open(&path).map_err(|e| {
            Error::Background(format!(
                "compaction: cannot open {:?}: {e}",
                path
            ))
        })?;
        let entries = reader.iter_all().map_err(|e| {
            Error::Background(format!(
                "compaction: cannot read {:?}: {e}",
                path
            ))
        })?;
        all_entries.extend(entries);
    }

    // ── 2. Sort + deduplicate ─────────────────────────────────────────────────
    // Stable sort so that for identical InternalKeys the first occurrence
    // (which was read from the newer file) wins after dedup.
    all_entries.sort_by(|(a, _), (b, _)| a.cmp(b));

    let merged = dedup_and_filter(all_entries, task.is_bottommost);

    if merged.is_empty() {
        // All input files turned out to be empty after filtering.  Just remove
        // them from the version set.
        let removes: Vec<(usize, u64)> = task
            .inputs_src
            .iter()
            .map(|f| (task.src_level, f.file_number))
            .chain(task.inputs_dst.iter().map(|f| (dst_level, f.file_number)))
            .collect();
        versions.apply_compaction(&[], &removes)?;
        delete_files(db_path, &task.inputs_src);
        delete_files(db_path, &task.inputs_dst);
        return Ok(());
    }

    // ── 3. Write output SST files ─────────────────────────────────────────────
    let mut output_metas: Vec<SstMeta> = Vec::new();
    let mut batch: Vec<(InternalKey, Vec<u8>)> = Vec::new();
    let mut batch_size: usize = 0;

    for (ikey, value) in merged {
        batch_size += ikey.user_key.len() + 8 + value.len();
        batch.push((ikey, value));

        if batch_size >= opts.target_file_size_base as usize {
            let meta = flush_batch(&mut batch, db_path, versions, opts)?;
            output_metas.push(meta);
            batch_size = 0;
        }
    }
    if !batch.is_empty() {
        let meta = flush_batch(&mut batch, db_path, versions, opts)?;
        output_metas.push(meta);
    }

    // ── 4. Update version set ─────────────────────────────────────────────────
    let adds: Vec<(usize, &SstMeta)> =
        output_metas.iter().map(|m| (dst_level, m)).collect();
    let removes: Vec<(usize, u64)> = task
        .inputs_src
        .iter()
        .map(|f| (task.src_level, f.file_number))
        .chain(task.inputs_dst.iter().map(|f| (dst_level, f.file_number)))
        .collect();
    versions.apply_compaction(&adds, &removes)?;

    // ── 5. Delete obsolete files ──────────────────────────────────────────────
    delete_files(db_path, &task.inputs_src);
    delete_files(db_path, &task.inputs_dst);

    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// flush_memtable
// ─────────────────────────────────────────────────────────────────────────────

/// Write an immutable memtable to a new L0 SST file, return its metadata.
pub fn flush_memtable(
    mem: &crate::memtable::Memtable,
    db_path: &Path,
    versions: &mut VersionSet,
    opts: &Options,
) -> Result<SstMeta> {
    let file_number = versions.new_file_number();
    let path = sst_path(db_path, file_number);

    let mut builder = SstBuilder::new(
        &path,
        opts.block_size,
        opts.use_bloom_filter,
        opts.bloom_filter_bits_per_key,
    )?;

    for (ikey, value) in mem.iter() {
        builder.add(ikey.clone(), value.clone())?;
    }

    let meta = builder.finish(file_number)?;
    Ok(meta)
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Compute the union key range [smallest_user_key, largest_user_key] over a
/// set of files.
fn key_range_of(files: &[Arc<FileMetadata>]) -> (Vec<u8>, Vec<u8>) {
    let lo = files
        .iter()
        .map(|f| f.smallest_user_key())
        .min()
        .unwrap_or(&[])
        .to_vec();
    let hi = files
        .iter()
        .map(|f| f.largest_user_key())
        .max()
        .unwrap_or(&[])
        .to_vec();
    (lo, hi)
}

/// True if there are no SST files in levels > `level`.
fn is_bottommost_with_files(
    versions: &VersionSet,
    level: usize,
    num_levels: usize,
) -> bool {
    for l in (level + 1)..num_levels {
        if versions.num_level_files(l) > 0 {
            return false;
        }
    }
    true
}

/// Merge-sort result: for each user key keep only the most-recent version
/// visible across all input files.  Optionally drop tombstones at the
/// bottommost level.
fn dedup_and_filter(
    sorted: Vec<(InternalKey, Vec<u8>)>,
    drop_tombstones: bool,
) -> Vec<(InternalKey, Vec<u8>)> {
    let mut out: Vec<(InternalKey, Vec<u8>)> = Vec::with_capacity(sorted.len());
    let mut prev_user_key: Option<Vec<u8>> = None;

    for (ikey, value) in sorted {
        // Skip older versions of the same user key (already kept the newest).
        if let Some(ref prev) = prev_user_key {
            if &ikey.user_key == prev {
                continue;
            }
        }
        // Drop tombstones at the bottommost level: there is no older data
        // below that could re-surface the key.
        if drop_tombstones && ikey.kind == KeyKind::Deletion {
            prev_user_key = Some(ikey.user_key.clone());
            continue;
        }
        prev_user_key = Some(ikey.user_key.clone());
        out.push((ikey, value));
    }
    out
}

/// Write `batch` to a new SST file and return its metadata.
/// Clears `batch` after writing.
fn flush_batch(
    batch: &mut Vec<(InternalKey, Vec<u8>)>,
    db_path: &Path,
    versions: &mut VersionSet,
    opts: &Options,
) -> Result<SstMeta> {
    let file_number = versions.new_file_number();
    let path = sst_path(db_path, file_number);
    let mut builder = SstBuilder::new(
        &path,
        opts.block_size,
        opts.use_bloom_filter,
        opts.bloom_filter_bits_per_key,
    )?;
    for (ikey, value) in batch.drain(..) {
        builder.add(ikey, value)?;
    }
    builder.finish(file_number)
}

fn delete_files(db_path: &Path, files: &[Arc<FileMetadata>]) {
    for fm in files {
        let p = sst_path(db_path, fm.file_number);
        let _ = fs::remove_file(&p); // best-effort
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memtable::Memtable;
    use crate::types::KeyKind;
    use tempfile::tempdir;

    fn make_opts() -> Options {
        let mut o = Options::default();
        o.target_file_size_base = 1024 * 1024;
        o.level0_file_num_compaction_trigger = 4;
        o
    }

    #[test]
    fn flush_and_compact_basic() {
        let dir = tempdir().unwrap();
        let path = dir.path();
        let opts = make_opts();

        let mut vs = VersionSet::create(path, opts.num_levels).unwrap();

        // Flush three small memtables → three L0 files.
        for i in 0u64..3 {
            let mut mem = Memtable::new();
            mem.put(format!("key-{i:03}").into_bytes(), i + 1, format!("val-{i}").into_bytes());
            let meta = flush_memtable(&mem, path, &mut vs, &opts).unwrap();
            vs.add_file(0, &meta).unwrap();
        }
        assert_eq!(vs.num_level_files(0), 3);

        // Not enough L0 files to trigger compaction yet (trigger = 4).
        assert!(pick_compaction(&vs, &opts).is_none());

        // Add a fourth.
        let mut mem = Memtable::new();
        mem.put(b"key-003".to_vec(), 4, b"v4".to_vec());
        let meta = flush_memtable(&mem, path, &mut vs, &opts).unwrap();
        vs.add_file(0, &meta).unwrap();

        // Now compaction should be triggered.
        let task = pick_compaction(&vs, &opts).unwrap();
        assert_eq!(task.src_level, 0);
        assert_eq!(task.inputs_src.len(), 4);

        run_compaction(&task, path, &mut vs, &opts).unwrap();

        // After compaction: L0 is empty, L1 has files.
        assert_eq!(vs.num_level_files(0), 0);
        assert!(vs.num_level_files(1) > 0);
    }

    #[test]
    fn dedup_keeps_newest_version() {
        let entries: Vec<(InternalKey, Vec<u8>)> = vec![
            (InternalKey::new(b"k".to_vec(), 5, KeyKind::Value), b"new".to_vec()),
            (InternalKey::new(b"k".to_vec(), 2, KeyKind::Value), b"old".to_vec()),
        ];
        let merged = dedup_and_filter(entries, false);
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].1, b"new");
    }

    #[test]
    fn tombstone_dropped_at_bottommost() {
        let entries: Vec<(InternalKey, Vec<u8>)> = vec![
            (InternalKey::new(b"k".to_vec(), 3, KeyKind::Deletion), vec![]),
        ];
        let merged = dedup_and_filter(entries, true);
        assert!(merged.is_empty(), "tombstone should be dropped at bottommost level");
    }
}
