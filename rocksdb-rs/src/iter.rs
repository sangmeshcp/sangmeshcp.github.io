//! Database Iterator
//!
//! A forward-only iterator that presents a merged, deduplicated, snapshot-
//! consistent view across the memtable(s) and every SST file in all levels.
//!
//! ## Implementation strategy
//! At creation time the iterator collects *all* visible entries from every
//! source into a `Vec<(user_key, value)>` sorted by user key.  This "eager
//! materialisation" approach is simple, correct, and avoids complex lifetime
//! management.
//!
//! A production implementation would instead use a lazy k-way merge heap to
//! avoid loading every key into memory at once; the interface here is a clean
//! drop-in for such an upgrade.

use std::path::Path;
use crate::error::Result;
use crate::memtable::Memtable;
use crate::options::ReadOptions;
use crate::sst::{sst_path, SstReader};
use crate::types::{InternalKey, KeyKind, SequenceNumber};
use crate::version::VersionSet;

// ─────────────────────────────────────────────────────────────────────────────
// DBIterator
// ─────────────────────────────────────────────────────────────────────────────

/// Forward-only iterator over the database.
///
/// Yields `(user_key, value)` pairs in ascending user-key order, bounded by
/// the `read_seq` supplied at construction.  Tombstones and superseded
/// versions are not exposed.
pub struct DBIterator {
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    pos: usize,
}

impl DBIterator {
    /// Build the iterator by collecting all visible entries from every source.
    pub fn new(
        _read_opts: &ReadOptions,
        read_seq: SequenceNumber,
        mem: &Memtable,
        imm: Option<&Memtable>,
        versions: &VersionSet,
        db_path: &Path,
    ) -> Result<Self> {
        // Gather raw (InternalKey, value) from all sources.
        let mut raw: Vec<(InternalKey, Vec<u8>)> = Vec::new();

        // Active memtable.
        for (ikey, val) in mem.iter() {
            raw.push((ikey.clone(), val.clone()));
        }
        // Immutable memtable.
        if let Some(imm) = imm {
            for (ikey, val) in imm.iter() {
                raw.push((ikey.clone(), val.clone()));
            }
        }
        // SST files — all levels.
        for level in 0..versions.level_files(0).len().max(7) {
            for fm in versions.level_files(level).iter() {
                let path = sst_path(db_path, fm.file_number);
                match SstReader::open(&path) {
                    Ok(mut reader) => {
                        if let Ok(entries) = reader.iter_all() {
                            raw.extend(entries);
                        }
                    }
                    Err(_) => {} // skip unreadable files
                }
            }
        }

        // Sort by InternalKey (user_key asc, seq desc).
        raw.sort_by(|(a, _), (b, _)| a.cmp(b));

        // Deduplicate: for each user_key, keep the most-recent visible version.
        let mut entries: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut prev_key: Option<Vec<u8>> = None;

        for (ikey, value) in raw {
            if ikey.seq > read_seq {
                continue; // not visible at this snapshot
            }
            if let Some(ref prev) = prev_key {
                if &ikey.user_key == prev {
                    continue; // already emitted newer version
                }
            }
            prev_key = Some(ikey.user_key.clone());
            if ikey.kind == KeyKind::Deletion {
                continue; // tombstone — key is absent
            }
            entries.push((ikey.user_key, value));
        }

        Ok(DBIterator { entries, pos: 0 })
    }

    /// True if there are more entries to visit.
    pub fn valid(&self) -> bool {
        self.pos < self.entries.len()
    }

    /// Return the current key.  Panics if `!valid()`.
    pub fn key(&self) -> &[u8] {
        &self.entries[self.pos].0
    }

    /// Return the current value.  Panics if `!valid()`.
    pub fn value(&self) -> &[u8] {
        &self.entries[self.pos].1
    }

    /// Advance to the next entry (cursor-style; use `Iterator::next` for the
    /// standard Rust iterator interface).
    pub fn advance(&mut self) {
        if self.valid() {
            self.pos += 1;
        }
    }

    /// Seek to the first entry with key >= `target`.
    pub fn seek(&mut self, target: &[u8]) {
        self.pos = self
            .entries
            .partition_point(|(k, _)| k.as_slice() < target);
    }

    /// Reset to the beginning.
    pub fn seek_to_first(&mut self) {
        self.pos = 0;
    }

    /// Collect remaining entries into a `Vec`.  Consumes the iterator.
    pub fn collect_remaining(mut self) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.entries.split_off(self.pos)
    }

    pub fn total_entries(&self) -> usize {
        self.entries.len()
    }
}

impl Iterator for DBIterator {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos < self.entries.len() {
            let entry = self.entries[self.pos].clone();
            self.pos += 1;
            Some(entry)
        } else {
            None
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// InternalIterator — used during compaction
// ─────────────────────────────────────────────────────────────────────────────

/// Sorted iterator over raw `(InternalKey, value)` pairs from a single source.
/// Used internally by the compaction engine.
pub struct InternalIterator {
    entries: Vec<(InternalKey, Vec<u8>)>,
    pos: usize,
}

impl InternalIterator {
    pub fn from_vec(mut entries: Vec<(InternalKey, Vec<u8>)>) -> Self {
        entries.sort_by(|(a, _), (b, _)| a.cmp(b));
        InternalIterator { entries, pos: 0 }
    }

    pub fn valid(&self) -> bool {
        self.pos < self.entries.len()
    }

    pub fn key(&self) -> &InternalKey {
        &self.entries[self.pos].0
    }

    pub fn value(&self) -> &[u8] {
        &self.entries[self.pos].1
    }

    pub fn next(&mut self) {
        if self.valid() {
            self.pos += 1;
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memtable::Memtable;
    use crate::options::ReadOptions;
    use crate::version::VersionSet;
    use tempfile::tempdir;

    #[test]
    fn iterate_memtable_only() {
        let dir = tempdir().unwrap();
        let path = dir.path();
        let vs = VersionSet::create(path, 7).unwrap();

        let mut mem = Memtable::new();
        mem.put(b"aaa".to_vec(), 1, b"A".to_vec());
        mem.put(b"bbb".to_vec(), 2, b"B".to_vec());
        mem.put(b"ccc".to_vec(), 3, b"C".to_vec());

        let opts = ReadOptions::default();
        let it = DBIterator::new(&opts, 10, &mem, None, &vs, path).unwrap();
        let got: Vec<_> = it.collect();

        assert_eq!(got[0], (b"aaa".to_vec(), b"A".to_vec()));
        assert_eq!(got[1], (b"bbb".to_vec(), b"B".to_vec()));
        assert_eq!(got[2], (b"ccc".to_vec(), b"C".to_vec()));
        assert_eq!(got.len(), 3);
    }

    #[test]
    fn tombstones_hidden() {
        let dir = tempdir().unwrap();
        let path = dir.path();
        let vs = VersionSet::create(path, 7).unwrap();

        let mut mem = Memtable::new();
        mem.put(b"k".to_vec(), 1, b"v".to_vec());
        mem.delete(b"k".to_vec(), 2);

        let opts = ReadOptions::default();
        let it = DBIterator::new(&opts, 10, &mem, None, &vs, path).unwrap();
        let entries: Vec<_> = it.collect();
        assert!(entries.is_empty(), "deleted key should not appear in iteration");
    }

    #[test]
    fn snapshot_visibility() {
        let dir = tempdir().unwrap();
        let path = dir.path();
        let vs = VersionSet::create(path, 7).unwrap();

        let mut mem = Memtable::new();
        mem.put(b"k".to_vec(), 1, b"v1".to_vec());
        mem.put(b"k".to_vec(), 5, b"v5".to_vec());

        let opts = ReadOptions::default();
        // At seq=3, only the write at seq=1 is visible.
        let it = DBIterator::new(&opts, 3, &mem, None, &vs, path).unwrap();
        let entries: Vec<_> = it.collect();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].1, b"v1");
    }

    #[test]
    fn seek_works() {
        let dir = tempdir().unwrap();
        let path = dir.path();
        let vs = VersionSet::create(path, 7).unwrap();

        let mut mem = Memtable::new();
        for (i, c) in [b"a", b"b", b"c", b"d"].iter().enumerate() {
            mem.put(c.to_vec(), i as u64 + 1, c.to_vec());
        }

        let opts = ReadOptions::default();
        let mut it = DBIterator::new(&opts, 10, &mem, None, &vs, path).unwrap();
        it.seek(b"c");
        assert_eq!(it.key(), b"c");
    }
}
