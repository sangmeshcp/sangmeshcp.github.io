//! Sorted String Table (SST) — on-disk immutable sorted key-value store.
//!
//! ## File layout
//! ```text
//! ┌──────────────────────────────────────────────────────────┐
//! │  Data blocks  (one or more 4 KiB blocks of KV entries)   │
//! ├──────────────────────────────────────────────────────────┤
//! │  Bloom-filter block                                       │
//! ├──────────────────────────────────────────────────────────┤
//! │  Index block  (one entry per data block)                  │
//! ├──────────────────────────────────────────────────────────┤
//! │  Footer  (40 bytes, always at end of file)                │
//! └──────────────────────────────────────────────────────────┘
//! ```
//!
//! ### Data block format
//! ```text
//! [num_entries : u32 LE]
//! for each entry:
//!   [ikey_len : u32 LE][encoded InternalKey : ikey_len bytes]
//!   [val_len  : u32 LE][value              : val_len  bytes]
//! [crc32 : u32 LE]   ← over all bytes above
//! ```
//!
//! ### Index block format
//! ```text
//! [num_entries : u32 LE]
//! for each block i:
//!   [last_key_len : u32 LE][last_user_key : last_key_len bytes]
//!   [block_offset : u64 LE]
//!   [block_size   : u32 LE]
//! ```
//!
//! ### Footer layout (40 bytes)
//! ```text
//! [filter_offset : u64 LE]
//! [filter_size   : u64 LE]
//! [index_offset  : u64 LE]
//! [index_size    : u64 LE]
//! [magic         : u64 LE]  = 0x88e241b785f4cff7
//! ```

use std::fs::File;
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::bloom::BloomFilter;
use crate::error::{Error, Result};
use crate::types::{InternalKey, KeyKind, SequenceNumber};

const FOOTER_SIZE: u64 = 40;
const SST_MAGIC: u64 = 0x88e2_41b7_85f4_cff7;
// ─────────────────────────────────────────────────────────────────────────────
// Public helper — file path convention
// ─────────────────────────────────────────────────────────────────────────────

pub fn sst_path(db_path: &Path, file_number: u64) -> std::path::PathBuf {
    db_path.join(format!("{:06}.sst", file_number))
}

// ─────────────────────────────────────────────────────────────────────────────
// Metadata returned after building
// ─────────────────────────────────────────────────────────────────────────────

/// Summary of an SST file, persisted in the manifest.
#[derive(Debug, Clone)]
pub struct SstMeta {
    pub file_number: u64,
    pub file_size: u64,
    /// Smallest InternalKey (encoded) in the file.
    pub smallest: Vec<u8>,
    /// Largest InternalKey (encoded) in the file.
    pub largest: Vec<u8>,
    pub num_entries: u64,
    pub num_deletions: u64,
}

impl SstMeta {
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

    /// Returns true if `user_key` might be covered by this file's key range.
    pub fn overlaps_user_key(&self, user_key: &[u8]) -> bool {
        user_key >= self.smallest_user_key() && user_key <= self.largest_user_key()
    }

    /// Returns true if the user-key range [lo, hi] overlaps with this file.
    pub fn overlaps_range(&self, lo: &[u8], hi: &[u8]) -> bool {
        lo <= self.largest_user_key() && hi >= self.smallest_user_key()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// SstBuilder
// ─────────────────────────────────────────────────────────────────────────────

struct BlockEntry {
    ikey: InternalKey,
    value: Vec<u8>,
}

/// Writes a sorted sequence of `(InternalKey, value)` pairs to a new SST file.
///
/// Keys **must** be added in `InternalKey` sorted order (user-key ascending,
/// seq descending).  The builder splits data into fixed-size blocks, builds a
/// bloom filter over all user keys, and writes an index block before sealing
/// the file footer.
pub struct SstBuilder {
    writer: BufWriter<File>,
    block_size: usize,
    use_bloom: bool,
    bloom_bits_per_key: usize,

    // current data block being accumulated
    current: Vec<BlockEntry>,
    current_raw_size: usize,

    // completed block metadata
    block_index: Vec<IndexEntry>,
    data_written: u64, // bytes written so far (data blocks only)

    // stats
    num_entries: u64,
    num_deletions: u64,
    smallest: Option<Vec<u8>>, // encoded InternalKey
    largest: Option<Vec<u8>>,  // encoded InternalKey

    // user keys for bloom filter
    user_keys: Vec<Vec<u8>>,
}

struct IndexEntry {
    last_user_key: Vec<u8>,
    offset: u64,
    size: u32,
}

impl SstBuilder {
    pub fn new(
        path: &Path,
        block_size: usize,
        use_bloom: bool,
        bloom_bits_per_key: usize,
    ) -> Result<Self> {
        let file = File::create(path)?;
        Ok(SstBuilder {
            writer: BufWriter::new(file),
            block_size: block_size.max(512),
            use_bloom,
            bloom_bits_per_key,
            current: Vec::new(),
            current_raw_size: 0,
            block_index: Vec::new(),
            data_written: 0,
            num_entries: 0,
            num_deletions: 0,
            smallest: None,
            largest: None,
            user_keys: Vec::new(),
        })
    }

    /// Add one entry.  Must be called in ascending `InternalKey` order.
    pub fn add(&mut self, ikey: InternalKey, value: Vec<u8>) -> Result<()> {
        let encoded = ikey.encode();

        if self.smallest.is_none() {
            self.smallest = Some(encoded.clone());
        }
        self.largest = Some(encoded);

        if ikey.kind == KeyKind::Deletion {
            self.num_deletions += 1;
        }
        self.num_entries += 1;
        self.current_raw_size += 4 + ikey.encode().len() + 4 + value.len();
        self.user_keys.push(ikey.user_key.clone());
        self.current.push(BlockEntry { ikey, value });

        if self.current_raw_size >= self.block_size {
            self.flush_block()?;
        }
        Ok(())
    }

    /// Finalise the file and return its metadata.
    pub fn finish(mut self, file_number: u64) -> Result<SstMeta> {
        // Flush any remaining entries.
        self.flush_block()?;

        let filter_offset = self.data_written;

        // ── Bloom filter block ────────────────────────────────────────────────
        let filter_bytes = if self.use_bloom && !self.user_keys.is_empty() {
            let mut bf = BloomFilter::new(self.user_keys.len(), self.bloom_bits_per_key);
            for k in &self.user_keys {
                bf.insert(k);
            }
            bf.encode()
        } else {
            vec![0u8] // num_probes = 0 → "no filter" sentinel
        };
        self.writer.write_all(&filter_bytes)?;
        let filter_size = filter_bytes.len() as u64;

        // ── Index block ───────────────────────────────────────────────────────
        let index_offset = filter_offset + filter_size;
        let mut index_data = Vec::new();
        index_data.extend_from_slice(&(self.block_index.len() as u32).to_le_bytes());
        for entry in &self.block_index {
            index_data
                .extend_from_slice(&(entry.last_user_key.len() as u32).to_le_bytes());
            index_data.extend_from_slice(&entry.last_user_key);
            index_data.extend_from_slice(&entry.offset.to_le_bytes());
            index_data.extend_from_slice(&entry.size.to_le_bytes());
        }
        self.writer.write_all(&index_data)?;
        let index_size = index_data.len() as u64;

        // ── Footer ────────────────────────────────────────────────────────────
        let mut footer = [0u8; FOOTER_SIZE as usize];
        footer[0..8].copy_from_slice(&filter_offset.to_le_bytes());
        footer[8..16].copy_from_slice(&filter_size.to_le_bytes());
        footer[16..24].copy_from_slice(&index_offset.to_le_bytes());
        footer[24..32].copy_from_slice(&index_size.to_le_bytes());
        footer[32..40].copy_from_slice(&SST_MAGIC.to_le_bytes());
        self.writer.write_all(&footer)?;
        self.writer.flush()?;

        let file_size = index_offset + index_size + FOOTER_SIZE;

        Ok(SstMeta {
            file_number,
            file_size,
            smallest: self.smallest.unwrap_or_default(),
            largest: self.largest.unwrap_or_default(),
            num_entries: self.num_entries,
            num_deletions: self.num_deletions,
        })
    }

    // ── Private helpers ───────────────────────────────────────────────────────

    fn flush_block(&mut self) -> Result<()> {
        if self.current.is_empty() {
            return Ok(());
        }

        let last_user_key = self.current.last().unwrap().ikey.user_key.clone();
        let block_offset = self.data_written;

        // Serialise block body.
        let mut body: Vec<u8> = Vec::new();
        body.extend_from_slice(&(self.current.len() as u32).to_le_bytes());
        for entry in &self.current {
            let enc = entry.ikey.encode();
            body.extend_from_slice(&(enc.len() as u32).to_le_bytes());
            body.extend_from_slice(&enc);
            body.extend_from_slice(&(entry.value.len() as u32).to_le_bytes());
            body.extend_from_slice(&entry.value);
        }

        let checksum = crc32fast::hash(&body);
        body.extend_from_slice(&checksum.to_le_bytes());

        let block_size = body.len() as u32;
        self.writer.write_all(&body)?;
        self.data_written += block_size as u64;

        self.block_index.push(IndexEntry {
            last_user_key,
            offset: block_offset,
            size: block_size,
        });

        self.current.clear();
        self.current_raw_size = 0;
        Ok(())
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// SstReader
// ─────────────────────────────────────────────────────────────────────────────

/// Reads data from a sealed SST file.
///
/// The reader eagerly loads the index and optional bloom filter on open; data
/// blocks are read on demand and can be cached externally via [`BlockHandle`].
pub struct SstReader {
    file: File,
    index: Vec<ReaderIndexEntry>,
    filter: Option<BloomFilter>,
}

struct ReaderIndexEntry {
    last_user_key: Vec<u8>,
    offset: u64,
    size: u32,
}

/// Raw block contents, suitable for caching.
#[derive(Debug, Clone)]
pub struct Block {
    pub data: Vec<u8>,
}

impl Block {
    /// Read the block at `(offset, size)` from the file, verify checksum.
    fn read_from(file: &mut File, offset: u64, size: u32) -> Result<Self> {
        file.seek(SeekFrom::Start(offset))?;
        let mut data = vec![0u8; size as usize];
        file.read_exact(&mut data)?;

        if data.len() < 4 {
            return Err(Error::Corruption("block too small".into()));
        }
        let stored_crc = u32::from_le_bytes(
            data[data.len() - 4..].try_into().unwrap(),
        );
        let actual_crc = crc32fast::hash(&data[..data.len() - 4]);
        if stored_crc != actual_crc {
            return Err(Error::Corruption(format!(
                "block checksum mismatch: stored={stored_crc:#010x} actual={actual_crc:#010x}"
            )));
        }
        Ok(Block { data })
    }

    /// Find the most-recent value for `user_key` at `read_seq` in this block.
    ///
    /// * `Ok(None)` → not found in block
    /// * `Ok(Some(None))` → found as deletion tombstone
    /// * `Ok(Some(Some(v)))` → found with value `v`
    fn get(
        &self,
        user_key: &[u8],
        read_seq: SequenceNumber,
    ) -> Result<Option<Option<Vec<u8>>>> {
        let body = &self.data[..self.data.len() - 4]; // strip CRC

        let mut pos = 0usize;
        if body.len() < 4 {
            return Err(Error::Corruption("block body too short".into()));
        }
        let num_entries = u32::from_le_bytes(body[0..4].try_into().unwrap()) as usize;
        pos += 4;

        // Entries are in InternalKey order (user_key asc, seq desc).
        // We scan linearly; for large blocks a binary search would be faster.
        let mut best: Option<Option<Vec<u8>>> = None;

        for _ in 0..num_entries {
            // ikey
            if pos + 4 > body.len() {
                return Err(Error::Corruption("block entry ikey_len truncated".into()));
            }
            let ikey_len =
                u32::from_le_bytes(body[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;
            if pos + ikey_len > body.len() {
                return Err(Error::Corruption("block entry ikey truncated".into()));
            }
            let ikey = InternalKey::decode(&body[pos..pos + ikey_len])
                .ok_or_else(|| Error::Corruption("invalid InternalKey encoding".into()))?;
            pos += ikey_len;

            // value
            if pos + 4 > body.len() {
                return Err(Error::Corruption("block entry val_len truncated".into()));
            }
            let val_len =
                u32::from_le_bytes(body[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;
            if pos + val_len > body.len() {
                return Err(Error::Corruption("block entry value truncated".into()));
            }
            let value = body[pos..pos + val_len].to_vec();
            pos += val_len;

            if ikey.user_key.as_slice() == user_key && ikey.seq <= read_seq {
                // First match is the most recent version (entries sorted seq desc).
                best = Some(match ikey.kind {
                    KeyKind::Value => Some(value),
                    KeyKind::Deletion => None,
                });
                break;
            }
        }
        Ok(best)
    }

    /// Iterate over all entries in the block.
    pub fn iter_entries(&self) -> Result<Vec<(InternalKey, Vec<u8>)>> {
        let body = &self.data[..self.data.len() - 4];
        if body.len() < 4 {
            return Err(Error::Corruption("block body too short".into()));
        }
        let num_entries = u32::from_le_bytes(body[0..4].try_into().unwrap()) as usize;
        let mut pos = 4usize;
        let mut out = Vec::with_capacity(num_entries);

        for _ in 0..num_entries {
            let ikey_len =
                u32::from_le_bytes(body[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;
            let ikey = InternalKey::decode(&body[pos..pos + ikey_len])
                .ok_or_else(|| Error::Corruption("invalid InternalKey".into()))?;
            pos += ikey_len;
            let val_len =
                u32::from_le_bytes(body[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;
            let value = body[pos..pos + val_len].to_vec();
            pos += val_len;
            out.push((ikey, value));
        }
        Ok(out)
    }
}

impl SstReader {
    /// Open and read the index + bloom filter from `path`.
    pub fn open(path: &Path) -> Result<Self> {
        let mut file = File::open(path)?;

        // Read footer.
        let file_size = file.metadata()?.len();
        if file_size < FOOTER_SIZE {
            return Err(Error::Corruption(format!(
                "{path:?}: file too small to contain footer ({file_size} bytes)"
            )));
        }
        file.seek(SeekFrom::End(-(FOOTER_SIZE as i64)))?;
        let mut footer_buf = [0u8; FOOTER_SIZE as usize];
        file.read_exact(&mut footer_buf)?;

        let filter_offset = u64::from_le_bytes(footer_buf[0..8].try_into().unwrap());
        let filter_size = u64::from_le_bytes(footer_buf[8..16].try_into().unwrap());
        let index_offset = u64::from_le_bytes(footer_buf[16..24].try_into().unwrap());
        let index_size = u64::from_le_bytes(footer_buf[24..32].try_into().unwrap());
        let magic = u64::from_le_bytes(footer_buf[32..40].try_into().unwrap());

        if magic != SST_MAGIC {
            return Err(Error::Corruption(format!(
                "{path:?}: bad SST magic {magic:#018x}"
            )));
        }

        // Read bloom filter.
        let filter = if filter_size > 0 {
            file.seek(SeekFrom::Start(filter_offset))?;
            let mut fbuf = vec![0u8; filter_size as usize];
            file.read_exact(&mut fbuf)?;
            BloomFilter::decode(&fbuf)
        } else {
            None
        };

        // Read index.
        file.seek(SeekFrom::Start(index_offset))?;
        let mut ibuf = vec![0u8; index_size as usize];
        file.read_exact(&mut ibuf)?;

        let num_idx = u32::from_le_bytes(ibuf[0..4].try_into().unwrap()) as usize;
        let mut ipos = 4usize;
        let mut index = Vec::with_capacity(num_idx);
        for _ in 0..num_idx {
            let key_len =
                u32::from_le_bytes(ibuf[ipos..ipos + 4].try_into().unwrap()) as usize;
            ipos += 4;
            let last_user_key = ibuf[ipos..ipos + key_len].to_vec();
            ipos += key_len;
            let offset = u64::from_le_bytes(ibuf[ipos..ipos + 8].try_into().unwrap());
            ipos += 8;
            let size = u32::from_le_bytes(ibuf[ipos..ipos + 4].try_into().unwrap());
            ipos += 4;
            index.push(ReaderIndexEntry {
                last_user_key,
                offset,
                size,
            });
        }

        Ok(SstReader { file, index, filter })
    }

    /// Look up `user_key` at `read_seq`.
    ///
    /// * `Ok(None)` → key is absent from this file
    /// * `Ok(Some(None))` → key was deleted
    /// * `Ok(Some(Some(v)))` → key exists with value `v`
    pub fn get(
        &mut self,
        user_key: &[u8],
        read_seq: SequenceNumber,
    ) -> Result<Option<Option<Vec<u8>>>> {
        // Bloom-filter short-circuit.
        if let Some(ref bf) = self.filter {
            if !bf.may_contain(user_key) {
                return Ok(None);
            }
        }

        // Use the index to find candidate block(s).
        // A block at index[i] covers keys from (exclusive) index[i-1].last_user_key
        // to (inclusive) index[i].last_user_key.
        // We need the first block whose `last_user_key >= user_key`.
        let block_idx = self
            .index
            .partition_point(|e| e.last_user_key.as_slice() < user_key);

        if block_idx >= self.index.len() {
            return Ok(None);
        }

        let entry = &self.index[block_idx];
        let block = Block::read_from(&mut self.file, entry.offset, entry.size)?;
        block.get(user_key, read_seq)
    }

    /// Iterate over every entry in the file in sorted order.
    /// Used for compaction merges.
    pub fn iter_all(&mut self) -> Result<Vec<(InternalKey, Vec<u8>)>> {
        let mut out = Vec::new();
        for i in 0..self.index.len() {
            let (offset, size) = (self.index[i].offset, self.index[i].size);
            let block = Block::read_from(&mut self.file, offset, size)?;
            out.extend(block.iter_entries()?);
        }
        Ok(out)
    }

    pub fn num_blocks(&self) -> usize {
        self.index.len()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::KeyKind;
    use tempfile::tempdir;

    fn make_ikey(key: &[u8], seq: u64) -> InternalKey {
        InternalKey::new(key.to_vec(), seq, KeyKind::Value)
    }
    fn make_del(key: &[u8], seq: u64) -> InternalKey {
        InternalKey::new(key.to_vec(), seq, KeyKind::Deletion)
    }

    #[test]
    fn build_and_read_basic() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.sst");

        // Build
        let mut b = SstBuilder::new(&path, 4096, true, 10).unwrap();
        b.add(make_ikey(b"aaa", 1), b"valA".to_vec()).unwrap();
        b.add(make_ikey(b"bbb", 2), b"valB".to_vec()).unwrap();
        b.add(make_ikey(b"ccc", 3), b"valC".to_vec()).unwrap();
        let meta = b.finish(42).unwrap();

        assert_eq!(meta.file_number, 42);
        assert_eq!(meta.num_entries, 3);
        assert_eq!(meta.num_deletions, 0);

        // Read
        let mut r = SstReader::open(&path).unwrap();
        assert_eq!(r.get(b"bbb", 10).unwrap(), Some(Some(b"valB".to_vec())));
        assert_eq!(r.get(b"zzz", 10).unwrap(), None);
    }

    #[test]
    fn deletion_is_reported() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("del.sst");

        // Entries must be in InternalKey order: same user-key, higher seq first.
        let mut b = SstBuilder::new(&path, 4096, true, 10).unwrap();
        b.add(make_del(b"k", 2), vec![]).unwrap();  // seq=2 deletion comes first
        b.add(make_ikey(b"k", 1), b"v".to_vec()).unwrap(); // seq=1 value comes second
        b.finish(1).unwrap();

        let mut r = SstReader::open(&path).unwrap();
        // seq=2: deletion is most-recent visible entry
        assert_eq!(r.get(b"k", 2).unwrap(), Some(None));
        // seq=1: only the value at seq=1 is visible (deletion at seq=2 is hidden)
        assert_eq!(r.get(b"k", 1).unwrap(), Some(Some(b"v".to_vec())));
    }

    #[test]
    fn multi_block_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("multi.sst");

        // Use tiny block size to force multiple blocks.
        let mut b = SstBuilder::new(&path, 64, true, 10).unwrap();
        for i in 0u64..100 {
            let key = format!("key-{i:04}");
            let val = format!("val-{i:04}");
            b.add(InternalKey::new(key.into_bytes(), i + 1, KeyKind::Value), val.into_bytes())
                .unwrap();
        }
        let meta = b.finish(99).unwrap();
        assert_eq!(meta.num_entries, 100);

        let mut r = SstReader::open(&path).unwrap();
        assert!(r.num_blocks() > 1);
        let all = r.iter_all().unwrap();
        assert_eq!(all.len(), 100);

        // Check a few entries
        assert_eq!(r.get(b"key-0050", 51).unwrap(), Some(Some(b"val-0050".to_vec())));
        assert_eq!(r.get(b"key-0099", 100).unwrap(), Some(Some(b"val-0099".to_vec())));
    }

    #[test]
    fn bloom_prevents_false_negatives() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("bloom.sst");

        let mut b = SstBuilder::new(&path, 4096, true, 10).unwrap();
        b.add(make_ikey(b"present", 1), b"yes".to_vec()).unwrap();
        b.finish(5).unwrap();

        let mut r = SstReader::open(&path).unwrap();
        // The bloom filter must never produce false negatives.
        assert!(r.get(b"present", 1).unwrap().is_some());
    }
}
