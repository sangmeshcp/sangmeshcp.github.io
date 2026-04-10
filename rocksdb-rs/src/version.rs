//! Version Set and Manifest
//!
//! The `VersionSet` is the single source of truth about which SST files exist
//! in each level of the LSM tree.  Every change (file added, file removed) is
//! atomically appended to the **MANIFEST** before being applied in memory,
//! ensuring the database can reconstruct its state after a crash.
//!
//! ## Manifest format
//! A sequential log of `VersionEdit` records using the same length-prefixed,
//! CRC32-checked framing as the WAL:
//!
//! ```text
//! [payload_len : u32 LE][crc32 : u32 LE][payload : ...]
//! ```
//!
//! Payload starts with a 1-byte tag:
//! * `0x01` AddFile   — level (u8) + file_number (u64) + file_size (u64) +
//!                      smallest_len (u32) + smallest + largest_len (u32) +
//!                      largest + num_entries (u64) + num_deletions (u64)
//! * `0x02` RemoveFile — level (u8) + file_number (u64)
//! * `0x03` SetSequence — seq (u64)
//! * `0x04` SetNextFile — next_file_number (u64)
//! * `0x05` SetLogNumber — log_number (u64)

use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::error::{Error, Result};
use crate::sst::SstMeta;
use crate::types::SequenceNumber;

const TAG_ADD_FILE: u8 = 0x01;
const TAG_REMOVE_FILE: u8 = 0x02;
const TAG_SET_SEQUENCE: u8 = 0x03;
const TAG_SET_NEXT_FILE: u8 = 0x04;
const TAG_SET_LOG_NUMBER: u8 = 0x05;

const MANIFEST_NAME: &str = "MANIFEST";
const CURRENT_NAME: &str = "CURRENT";

// ─────────────────────────────────────────────────────────────────────────────
// FileMetadata — one SST file tracked by the version set
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct FileMetadata {
    pub file_number: u64,
    pub file_size: u64,
    /// Encoded InternalKey of the smallest key in the file.
    pub smallest: Vec<u8>,
    /// Encoded InternalKey of the largest key in the file.
    pub largest: Vec<u8>,
    pub num_entries: u64,
    pub num_deletions: u64,
}

impl FileMetadata {
    pub fn from_meta(meta: &SstMeta) -> Self {
        FileMetadata {
            file_number: meta.file_number,
            file_size: meta.file_size,
            smallest: meta.smallest.clone(),
            largest: meta.largest.clone(),
            num_entries: meta.num_entries,
            num_deletions: meta.num_deletions,
        }
    }

    pub fn smallest_user_key(&self) -> &[u8] {
        if self.smallest.len() >= 8 {
            &self.smallest[..self.smallest.len() - 8]
        } else {
            &self.smallest
        }
    }

    pub fn largest_user_key(&self) -> &[u8] {
        if self.largest.len() >= 8 {
            &self.largest[..self.largest.len() - 8]
        } else {
            &self.largest
        }
    }

    pub fn overlaps_range(&self, lo: &[u8], hi: &[u8]) -> bool {
        lo <= self.largest_user_key() && hi >= self.smallest_user_key()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// VersionSet
// ─────────────────────────────────────────────────────────────────────────────

pub struct VersionSet {
    db_path: PathBuf,
    pub last_sequence: SequenceNumber,
    pub next_file_number: u64,
    pub log_number: u64, // current WAL file number
    levels: Vec<Vec<Arc<FileMetadata>>>,
    manifest: Option<BufWriter<File>>,
    num_levels: usize,
}

impl VersionSet {
    // ── Construction / recovery ───────────────────────────────────────────────

    /// Create a brand-new `VersionSet` for a freshly opened database.
    pub fn create(db_path: &Path, num_levels: usize) -> Result<Self> {
        let mut vs = VersionSet {
            db_path: db_path.to_path_buf(),
            last_sequence: 0,
            next_file_number: 1,
            log_number: 0,
            levels: vec![Vec::new(); num_levels],
            manifest: None,
            num_levels,
        };
        vs.open_manifest()?;
        Ok(vs)
    }

    /// Recover a `VersionSet` by replaying the manifest.
    pub fn recover(db_path: &Path, num_levels: usize) -> Result<Self> {
        let manifest_path = db_path.join(MANIFEST_NAME);

        let mut last_sequence = 0u64;
        let mut next_file_number = 2u64;
        let mut log_number = 0u64;
        let mut levels: Vec<Vec<Arc<FileMetadata>>> = vec![Vec::new(); num_levels];

        // Replay the manifest.
        if manifest_path.exists() {
            let file = File::open(&manifest_path)?;
            let mut reader = BufReader::new(file);
            loop {
                let mut hdr = [0u8; 8];
                match reader.read_exact(&mut hdr) {
                    Ok(()) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                    Err(e) => return Err(Error::Io(e)),
                }
                let payload_len = u32::from_le_bytes(hdr[0..4].try_into().unwrap()) as usize;
                let expected_crc = u32::from_le_bytes(hdr[4..8].try_into().unwrap());

                let mut payload = vec![0u8; payload_len];
                match reader.read_exact(&mut payload) {
                    Ok(()) => {}
                    Err(_) => break, // torn record
                }
                if crc32fast::hash(&payload) != expected_crc {
                    break; // torn record
                }

                apply_edit(
                    &payload,
                    num_levels,
                    &mut levels,
                    &mut last_sequence,
                    &mut next_file_number,
                    &mut log_number,
                )?;
            }
        }

        let mut vs = VersionSet {
            db_path: db_path.to_path_buf(),
            last_sequence,
            next_file_number,
            log_number,
            levels,
            manifest: None,
            num_levels,
        };
        vs.open_manifest()?;
        Ok(vs)
    }

    // ── File-number allocation ────────────────────────────────────────────────

    pub fn new_file_number(&mut self) -> u64 {
        let n = self.next_file_number;
        self.next_file_number += 1;
        // Persist the updated counter so that a crash doesn't reuse file numbers.
        if self.manifest.is_some() {
            let _ = self.log_set_next_file(self.next_file_number);
        }
        n
    }

    // ── Level queries ─────────────────────────────────────────────────────────

    pub fn level_files(&self, level: usize) -> &[Arc<FileMetadata>] {
        if level < self.num_levels {
            &self.levels[level]
        } else {
            &[]
        }
    }

    pub fn num_level_files(&self, level: usize) -> usize {
        if level < self.num_levels {
            self.levels[level].len()
        } else {
            0
        }
    }

    pub fn level_total_size(&self, level: usize) -> u64 {
        if level < self.num_levels {
            self.levels[level].iter().map(|f| f.file_size).sum()
        } else {
            0
        }
    }

    // ── Mutations ─────────────────────────────────────────────────────────────

    /// Record adding a new SST file to `level` and persist to manifest.
    pub fn add_file(&mut self, level: usize, meta: &SstMeta) -> Result<()> {
        let fm = Arc::new(FileMetadata::from_meta(meta));
        self.log_add_file(level, &fm)?;
        self.levels[level].push(fm);
        // Keep L1+ files sorted by smallest key for binary-search lookups.
        if level > 0 {
            self.levels[level]
                .sort_by(|a, b| a.smallest_user_key().cmp(b.smallest_user_key()));
        }
        Ok(())
    }

    /// Record removing an SST file from `level` and persist to manifest.
    pub fn remove_file(&mut self, level: usize, file_number: u64) -> Result<()> {
        self.log_remove_file(level, file_number)?;
        self.levels[level].retain(|f| f.file_number != file_number);
        Ok(())
    }

    /// Atomically add new files and remove old files (used after compaction).
    pub fn apply_compaction(
        &mut self,
        add: &[(usize, &SstMeta)],
        remove: &[(usize, u64)],
    ) -> Result<()> {
        for (level, meta) in add {
            let fm = Arc::new(FileMetadata::from_meta(meta));
            self.log_add_file(*level, &fm)?;
            self.levels[*level].push(fm);
        }
        for (level, file_number) in remove {
            self.log_remove_file(*level, *file_number)?;
            self.levels[*level].retain(|f| f.file_number != *file_number);
        }
        // Re-sort L1+ levels.
        for level in 1..self.num_levels {
            self.levels[level]
                .sort_by(|a, b| a.smallest_user_key().cmp(b.smallest_user_key()));
        }
        self.flush_manifest()?;
        Ok(())
    }

    pub fn set_log_number(&mut self, log_number: u64) -> Result<()> {
        self.log_number = log_number;
        self.log_set_log_number(log_number)?;
        self.flush_manifest()
    }

    pub fn set_last_sequence(&mut self, seq: SequenceNumber) -> Result<()> {
        self.last_sequence = seq;
        self.log_set_sequence(seq)?;
        self.flush_manifest()
    }

    // ── All live file numbers (for garbage collection) ────────────────────────

    pub fn live_file_numbers(&self) -> Vec<u64> {
        self.levels
            .iter()
            .flat_map(|l| l.iter().map(|f| f.file_number))
            .collect()
    }

    // ── Manifest I/O ──────────────────────────────────────────────────────────

    fn open_manifest(&mut self) -> Result<()> {
        let path = self.db_path.join(MANIFEST_NAME);
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;
        self.manifest = Some(BufWriter::new(file));

        // Write a CURRENT file pointing to the manifest.
        let current_path = self.db_path.join(CURRENT_NAME);
        fs::write(&current_path, MANIFEST_NAME)?;

        // Persist current in-memory state as a baseline edit.
        self.log_set_sequence(self.last_sequence)?;
        self.log_set_next_file(self.next_file_number)?;
        self.log_set_log_number(self.log_number)?;
        for level in 0..self.num_levels {
            let files: Vec<Arc<FileMetadata>> = self.levels[level].clone();
            for fm in files {
                self.log_add_file(level, &fm)?;
            }
        }
        self.flush_manifest()
    }

    fn write_edit(&mut self, payload: &[u8]) -> Result<()> {
        let crc = crc32fast::hash(payload);
        let m = self.manifest.as_mut().unwrap();
        m.write_all(&(payload.len() as u32).to_le_bytes())?;
        m.write_all(&crc.to_le_bytes())?;
        m.write_all(payload)?;
        Ok(())
    }

    fn flush_manifest(&mut self) -> Result<()> {
        if let Some(ref mut m) = self.manifest {
            m.flush()?;
        }
        Ok(())
    }

    fn log_add_file(&mut self, level: usize, fm: &FileMetadata) -> Result<()> {
        let mut p = Vec::new();
        p.push(TAG_ADD_FILE);
        p.push(level as u8);
        p.extend_from_slice(&fm.file_number.to_le_bytes());
        p.extend_from_slice(&fm.file_size.to_le_bytes());
        p.extend_from_slice(&(fm.smallest.len() as u32).to_le_bytes());
        p.extend_from_slice(&fm.smallest);
        p.extend_from_slice(&(fm.largest.len() as u32).to_le_bytes());
        p.extend_from_slice(&fm.largest);
        p.extend_from_slice(&fm.num_entries.to_le_bytes());
        p.extend_from_slice(&fm.num_deletions.to_le_bytes());
        self.write_edit(&p)
    }

    fn log_remove_file(&mut self, level: usize, file_number: u64) -> Result<()> {
        let mut p = Vec::new();
        p.push(TAG_REMOVE_FILE);
        p.push(level as u8);
        p.extend_from_slice(&file_number.to_le_bytes());
        self.write_edit(&p)
    }

    fn log_set_sequence(&mut self, seq: u64) -> Result<()> {
        let mut p = vec![TAG_SET_SEQUENCE];
        p.extend_from_slice(&seq.to_le_bytes());
        self.write_edit(&p)
    }

    fn log_set_next_file(&mut self, n: u64) -> Result<()> {
        let mut p = vec![TAG_SET_NEXT_FILE];
        p.extend_from_slice(&n.to_le_bytes());
        self.write_edit(&p)
    }

    fn log_set_log_number(&mut self, n: u64) -> Result<()> {
        let mut p = vec![TAG_SET_LOG_NUMBER];
        p.extend_from_slice(&n.to_le_bytes());
        self.write_edit(&p)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Manifest replay helper
// ─────────────────────────────────────────────────────────────────────────────

fn apply_edit(
    payload: &[u8],
    num_levels: usize,
    levels: &mut Vec<Vec<Arc<FileMetadata>>>,
    last_sequence: &mut u64,
    next_file_number: &mut u64,
    log_number: &mut u64,
) -> Result<()> {
    if payload.is_empty() {
        return Ok(());
    }
    match payload[0] {
        TAG_ADD_FILE => {
            if payload.len() < 2 + 8 + 8 + 4 {
                return Err(Error::Corruption("AddFile record too short".into()));
            }
            let level = payload[1] as usize;
            let file_number = u64::from_le_bytes(payload[2..10].try_into().unwrap());
            let file_size = u64::from_le_bytes(payload[10..18].try_into().unwrap());
            let sml_len = u32::from_le_bytes(payload[18..22].try_into().unwrap()) as usize;
            let sml = payload[22..22 + sml_len].to_vec();
            let lrg_off = 22 + sml_len;
            let lrg_len =
                u32::from_le_bytes(payload[lrg_off..lrg_off + 4].try_into().unwrap())
                    as usize;
            let lrg = payload[lrg_off + 4..lrg_off + 4 + lrg_len].to_vec();
            let ne_off = lrg_off + 4 + lrg_len;
            let num_entries = u64::from_le_bytes(payload[ne_off..ne_off + 8].try_into().unwrap());
            let num_deletions =
                u64::from_le_bytes(payload[ne_off + 8..ne_off + 16].try_into().unwrap());

            if level < num_levels {
                levels[level].push(Arc::new(FileMetadata {
                    file_number,
                    file_size,
                    smallest: sml,
                    largest: lrg,
                    num_entries,
                    num_deletions,
                }));
            }
        }
        TAG_REMOVE_FILE => {
            let level = payload[1] as usize;
            let file_number = u64::from_le_bytes(payload[2..10].try_into().unwrap());
            if level < num_levels {
                levels[level].retain(|f| f.file_number != file_number);
            }
        }
        TAG_SET_SEQUENCE => {
            *last_sequence = u64::from_le_bytes(payload[1..9].try_into().unwrap());
        }
        TAG_SET_NEXT_FILE => {
            *next_file_number = u64::from_le_bytes(payload[1..9].try_into().unwrap());
        }
        TAG_SET_LOG_NUMBER => {
            *log_number = u64::from_le_bytes(payload[1..9].try_into().unwrap());
        }
        t => {
            return Err(Error::Corruption(format!(
                "unknown manifest record type: {t:#x}"
            )));
        }
    }
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sst::SstMeta;
    use tempfile::tempdir;

    fn dummy_meta(file_number: u64) -> SstMeta {
        SstMeta {
            file_number,
            file_size: 1024,
            smallest: b"aaa\x01\x00\x00\x00\x00\x00\x00\x00\x00".to_vec(),
            largest: b"zzz\x01\x00\x00\x00\x00\x00\x00\x00\x00".to_vec(),
            num_entries: 10,
            num_deletions: 0,
        }
    }

    #[test]
    fn create_and_recover() {
        let dir = tempdir().unwrap();
        let path = dir.path();

        // Create and add files.
        {
            let mut vs = VersionSet::create(path, 7).unwrap();
            vs.add_file(0, &dummy_meta(1)).unwrap();
            vs.add_file(0, &dummy_meta(2)).unwrap();
            vs.add_file(1, &dummy_meta(3)).unwrap();
            vs.set_last_sequence(99).unwrap();
        }

        // Recover.
        let vs = VersionSet::recover(path, 7).unwrap();
        assert_eq!(vs.num_level_files(0), 2);
        assert_eq!(vs.num_level_files(1), 1);
        assert_eq!(vs.last_sequence, 99);
    }

    #[test]
    fn remove_file_persists() {
        let dir = tempdir().unwrap();
        let path = dir.path();

        {
            let mut vs = VersionSet::create(path, 7).unwrap();
            vs.add_file(0, &dummy_meta(10)).unwrap();
            vs.add_file(0, &dummy_meta(11)).unwrap();
            vs.remove_file(0, 10).unwrap();
        }

        let vs = VersionSet::recover(path, 7).unwrap();
        assert_eq!(vs.num_level_files(0), 1);
        assert_eq!(vs.level_files(0)[0].file_number, 11);
    }
}
