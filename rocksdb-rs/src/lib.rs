//! # rocksdb-rs
//!
//! A RocksDB-inspired LSM-tree storage engine written in **pure, safe Rust**.
//!
//! ## Architecture overview
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                        DB (public API)                       │
//! ├───────────────────────────┬─────────────────────────────────┤
//! │   Active Memtable         │  Immutable Memtable (flushing)  │
//! │   BTreeMap<InternalKey>   │  BTreeMap<InternalKey>          │
//! ├───────────────────────────┴─────────────────────────────────┤
//! │              Write-Ahead Log  (crash recovery)              │
//! ├──────┬──────┬────────────────────────────────────────────── ┤
//! │  L0  │  L1  │  L2  …  L6       (SST files, leveled)        │
//! │ (4)  │(256M)│(2.5G) …                                       │
//! └──────┴──────┴────────────────────────────────────────────── ┘
//! ```
//!
//! ## Key properties
//! * **No unsafe code** — all concurrency via `Arc<Mutex<…>>` + background thread.
//! * **Deterministic memory** — no GC, no reference cycles.
//! * **MVCC snapshots** — consistent point-in-time reads via sequence numbers.
//! * **CRC32 checksums** — every WAL record and SST block is verified on read.
//! * **Bloom filters** — per-file filters cut unnecessary I/O for missing keys.
//! * **Leveled compaction** — bounded space amplification.
//!
//! ## Quick start
//! ```no_run
//! use rocksdb_rs::{DB, Options, ReadOptions, WriteOptions};
//!
//! let opts = Options::default();
//! let db = DB::open("/tmp/mydb", opts).unwrap();
//!
//! let wo = WriteOptions::default();
//! db.put(b"hello", b"world", &wo).unwrap();
//!
//! let ro = ReadOptions::default();
//! let val = db.get(b"hello", &ro).unwrap();
//! assert_eq!(val, Some(b"world".to_vec()));
//!
//! // Iterate in sorted order
//! for (key, value) in db.iter(&ro).unwrap() {
//!     println!("{} = {}", String::from_utf8_lossy(&key), String::from_utf8_lossy(&value));
//! }
//! ```

// ─────────────────────────────────────────────────────────────────────────────
// Internal modules
// ─────────────────────────────────────────────────────────────────────────────

pub mod batch;
pub mod bloom;
pub mod cache;
pub mod compaction;
pub mod db;
pub mod error;
pub mod iter;
pub mod memtable;
pub mod options;
pub mod sst;
pub mod types;
pub mod version;
pub mod wal;

// ─────────────────────────────────────────────────────────────────────────────
// Public re-exports
// ─────────────────────────────────────────────────────────────────────────────

pub use batch::WriteBatch;
pub use db::{Snapshot, DB};
pub use error::{Error, Result};
pub use iter::DBIterator;
pub use options::{Options, ReadOptions, WriteOptions};
pub use types::{KeyKind, SequenceNumber};
