//! Write-Ahead Log (WAL)
//!
//! Every mutation is appended here *before* being applied to the in-memory
//! memtable, guaranteeing that no acknowledged write is ever lost even if the
//! process crashes mid-operation.
//!
//! ## Record format
//! Each record is a framed payload:
//! ```text
//! ┌─────────────────────────────────────────────┐
//! │ payload_len : u32 LE  (4 bytes)             │
//! │ crc32       : u32 LE  (4 bytes, of payload) │
//! │ payload     : [u8; payload_len]             │
//! └─────────────────────────────────────────────┘
//! ```
//!
//! The payload itself begins with a 1-byte record type:
//! * `0x01` — `Put` : `[seq: u64 LE][key_len: u32 LE][key][val_len: u32 LE][value]`
//! * `0x02` — `Delete` : `[seq: u64 LE][key_len: u32 LE][key]`
//! * `0x03` — `Batch` : `[count: u32 LE]` followed by *count* Put/Delete payloads
//!
//! A truncated or checksum-failing record terminates recovery gracefully
//! (last record may be torn during a crash).

use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use crate::error::{Error, Result};
use crate::types::SequenceNumber;

const RECORD_PUT: u8 = 0x01;
const RECORD_DELETE: u8 = 0x02;
const RECORD_BATCH: u8 = 0x03;

// ─────────────────────────────────────────────────────────────────────────────
// WalRecord — recovered entries
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum WalRecord {
    Put {
        seq: SequenceNumber,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Delete {
        seq: SequenceNumber,
        key: Vec<u8>,
    },
}

// ─────────────────────────────────────────────────────────────────────────────
// Wal — writer
// ─────────────────────────────────────────────────────────────────────────────

/// Append-only write-ahead log.
pub struct Wal {
    writer: BufWriter<File>,
    path: PathBuf,
}

impl Wal {
    /// Create a new WAL file, truncating any existing file at `path`.
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path.as_ref())?;
        Ok(Wal {
            writer: BufWriter::new(file),
            path: path.as_ref().to_path_buf(),
        })
    }

    /// Open an existing WAL for appending (used after crash recovery).
    pub fn open_append(path: impl AsRef<Path>) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path.as_ref())?;
        Ok(Wal {
            writer: BufWriter::new(file),
            path: path.as_ref().to_path_buf(),
        })
    }

    // ── Append operations ─────────────────────────────────────────────────────

    pub fn append_put(
        &mut self,
        key: &[u8],
        seq: SequenceNumber,
        value: &[u8],
        sync: bool,
    ) -> Result<()> {
        let mut payload = Vec::with_capacity(1 + 8 + 4 + key.len() + 4 + value.len());
        payload.push(RECORD_PUT);
        payload.extend_from_slice(&seq.to_le_bytes());
        payload.extend_from_slice(&(key.len() as u32).to_le_bytes());
        payload.extend_from_slice(key);
        payload.extend_from_slice(&(value.len() as u32).to_le_bytes());
        payload.extend_from_slice(value);
        self.write_framed(&payload, sync)
    }

    pub fn append_delete(
        &mut self,
        key: &[u8],
        seq: SequenceNumber,
        sync: bool,
    ) -> Result<()> {
        let mut payload = Vec::with_capacity(1 + 8 + 4 + key.len());
        payload.push(RECORD_DELETE);
        payload.extend_from_slice(&seq.to_le_bytes());
        payload.extend_from_slice(&(key.len() as u32).to_le_bytes());
        payload.extend_from_slice(key);
        self.write_framed(&payload, sync)
    }

    /// Append a batch of operations.  Each `ops` element is either a Put or
    /// Delete encoded the same way as the single-operation variants above
    /// (minus the outer framing — the batch provides one outer frame).
    pub fn append_batch(
        &mut self,
        ops: &[(SequenceNumber, &[u8], Option<&[u8]>)],
        sync: bool,
    ) -> Result<()> {
        let mut payload = Vec::new();
        payload.push(RECORD_BATCH);
        payload.extend_from_slice(&(ops.len() as u32).to_le_bytes());
        for (seq, key, val_opt) in ops {
            match val_opt {
                Some(value) => {
                    payload.push(RECORD_PUT);
                    payload.extend_from_slice(&seq.to_le_bytes());
                    payload.extend_from_slice(&(key.len() as u32).to_le_bytes());
                    payload.extend_from_slice(key);
                    payload.extend_from_slice(&(value.len() as u32).to_le_bytes());
                    payload.extend_from_slice(value);
                }
                None => {
                    payload.push(RECORD_DELETE);
                    payload.extend_from_slice(&seq.to_le_bytes());
                    payload.extend_from_slice(&(key.len() as u32).to_le_bytes());
                    payload.extend_from_slice(key);
                }
            }
        }
        self.write_framed(&payload, sync)
    }

    pub fn flush(&mut self) -> Result<()> {
        self.writer.flush().map_err(Error::Io)
    }

    pub fn sync(&mut self) -> Result<()> {
        self.writer.flush()?;
        self.writer.get_ref().sync_data().map_err(Error::Io)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    // ── Internal helpers ──────────────────────────────────────────────────────

    fn write_framed(&mut self, payload: &[u8], sync: bool) -> Result<()> {
        let crc = crc32fast::hash(payload);
        let len = payload.len() as u32;

        self.writer.write_all(&len.to_le_bytes())?;
        self.writer.write_all(&crc.to_le_bytes())?;
        self.writer.write_all(payload)?;

        if sync {
            self.writer.flush()?;
            self.writer.get_ref().sync_data()?;
        }
        Ok(())
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Recovery
// ─────────────────────────────────────────────────────────────────────────────

/// Read back all valid records from a WAL file.
///
/// A missing file is treated as an empty log (returns `Ok(vec![])`).
/// A checksum mismatch or truncated record terminates iteration cleanly — the
/// last record in a WAL may be partial if the process was killed mid-write.
pub fn recover_wal(path: impl AsRef<Path>) -> Result<Vec<WalRecord>> {
    let file = match File::open(path.as_ref()) {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(Error::Io(e)),
    };

    let mut reader = BufReader::new(file);
    let mut records = Vec::new();

    loop {
        // Read 8-byte frame header: [payload_len: u32][crc32: u32]
        let mut header = [0u8; 8];
        match reader.read_exact(&mut header) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(Error::Io(e)),
        }

        let payload_len = u32::from_le_bytes(header[0..4].try_into().unwrap()) as usize;
        let expected_crc = u32::from_le_bytes(header[4..8].try_into().unwrap());

        // Guard against corrupt length fields claiming huge reads.
        if payload_len > 256 * 1024 * 1024 {
            break; // treat as truncation
        }

        let mut payload = vec![0u8; payload_len];
        match reader.read_exact(&mut payload) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(Error::Io(e)),
        }

        let actual_crc = crc32fast::hash(&payload);
        if actual_crc != expected_crc {
            // Torn record at the tail — stop recovery here.
            break;
        }

        parse_payload(&payload, &mut records)?;
    }

    Ok(records)
}

fn parse_payload(payload: &[u8], out: &mut Vec<WalRecord>) -> Result<()> {
    if payload.is_empty() {
        return Err(Error::Corruption("empty WAL payload".into()));
    }

    match payload[0] {
        RECORD_PUT => {
            let rec = parse_put(&payload[1..])?;
            out.push(rec);
        }
        RECORD_DELETE => {
            let rec = parse_delete(&payload[1..])?;
            out.push(rec);
        }
        RECORD_BATCH => {
            if payload.len() < 5 {
                return Err(Error::Corruption("batch header too short".into()));
            }
            let count = u32::from_le_bytes(payload[1..5].try_into().unwrap()) as usize;
            let mut cursor = 5usize;
            for _ in 0..count {
                if cursor >= payload.len() {
                    return Err(Error::Corruption("batch record truncated".into()));
                }
                match payload[cursor] {
                    RECORD_PUT => {
                        let rec = parse_put(&payload[cursor + 1..])?;
                        // advance cursor by the bytes consumed
                        cursor += 1 + 8 + 4
                            + if let WalRecord::Put { ref key, ref value, .. } = rec {
                                key.len() + 4 + value.len()
                            } else {
                                unreachable!()
                            };
                        out.push(rec);
                    }
                    RECORD_DELETE => {
                        let rec = parse_delete(&payload[cursor + 1..])?;
                        cursor += 1 + 8 + 4
                            + if let WalRecord::Delete { ref key, .. } = rec {
                                key.len()
                            } else {
                                unreachable!()
                            };
                        out.push(rec);
                    }
                    t => {
                        return Err(Error::Corruption(format!(
                            "unknown batch op type: {t:#x}"
                        )));
                    }
                }
            }
        }
        t => {
            return Err(Error::Corruption(format!(
                "unknown WAL record type: {t:#x}"
            )));
        }
    }
    Ok(())
}

fn parse_put(data: &[u8]) -> Result<WalRecord> {
    if data.len() < 8 + 4 {
        return Err(Error::Corruption("Put record too short".into()));
    }
    let seq = u64::from_le_bytes(data[0..8].try_into().unwrap());
    let key_len = u32::from_le_bytes(data[8..12].try_into().unwrap()) as usize;
    let end_key = 12 + key_len;
    if data.len() < end_key + 4 {
        return Err(Error::Corruption("Put key truncated".into()));
    }
    let key = data[12..end_key].to_vec();
    let val_len = u32::from_le_bytes(data[end_key..end_key + 4].try_into().unwrap()) as usize;
    let end_val = end_key + 4 + val_len;
    if data.len() < end_val {
        return Err(Error::Corruption("Put value truncated".into()));
    }
    let value = data[end_key + 4..end_val].to_vec();
    Ok(WalRecord::Put { seq, key, value })
}

fn parse_delete(data: &[u8]) -> Result<WalRecord> {
    if data.len() < 8 + 4 {
        return Err(Error::Corruption("Delete record too short".into()));
    }
    let seq = u64::from_le_bytes(data[0..8].try_into().unwrap());
    let key_len = u32::from_le_bytes(data[8..12].try_into().unwrap()) as usize;
    if data.len() < 12 + key_len {
        return Err(Error::Corruption("Delete key truncated".into()));
    }
    let key = data[12..12 + key_len].to_vec();
    Ok(WalRecord::Delete { seq, key })
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn roundtrip_put_delete() {
        let dir = tempdir().unwrap();
        let p = dir.path().join("test.wal");

        {
            let mut wal = Wal::create(&p).unwrap();
            wal.append_put(b"hello", 1, b"world", false).unwrap();
            wal.append_delete(b"bye", 2, false).unwrap();
            wal.flush().unwrap();
        }

        let records = recover_wal(&p).unwrap();
        assert_eq!(records.len(), 2);

        match &records[0] {
            WalRecord::Put { seq, key, value } => {
                assert_eq!(*seq, 1);
                assert_eq!(key.as_slice(), b"hello");
                assert_eq!(value.as_slice(), b"world");
            }
            _ => panic!("expected Put"),
        }
        match &records[1] {
            WalRecord::Delete { seq, key } => {
                assert_eq!(*seq, 2);
                assert_eq!(key.as_slice(), b"bye");
            }
            _ => panic!("expected Delete"),
        }
    }

    #[test]
    fn missing_wal_returns_empty() {
        let records = recover_wal("/tmp/definitely_does_not_exist_rocksdb_rs.wal").unwrap();
        assert!(records.is_empty());
    }

    #[test]
    fn batch_roundtrip() {
        let dir = tempdir().unwrap();
        let p = dir.path().join("batch.wal");
        {
            let mut wal = Wal::create(&p).unwrap();
            let ops: Vec<(SequenceNumber, &[u8], Option<&[u8]>)> = vec![
                (10, b"k1", Some(b"v1")),
                (11, b"k2", None),
                (12, b"k3", Some(b"v3")),
            ];
            wal.append_batch(&ops, false).unwrap();
            wal.flush().unwrap();
        }
        let records = recover_wal(&p).unwrap();
        assert_eq!(records.len(), 3);
    }
}
