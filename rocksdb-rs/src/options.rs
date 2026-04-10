/// Options governing how a database is opened and maintained.
#[derive(Debug, Clone)]
pub struct Options {
    /// Create the database directory if it doesn't exist (default: true).
    pub create_if_missing: bool,
    /// Return an error if the database already exists (default: false).
    pub error_if_exists: bool,

    // ── Memtable ──────────────────────────────────────────────────────────────
    /// Byte threshold at which the active memtable is converted to immutable
    /// and a flush to L0 is scheduled (default: 64 MiB).
    pub write_buffer_size: usize,
    /// Maximum number of simultaneous write buffers (active + immutable).
    /// Writes stall when this limit is hit (default: 2).
    pub max_write_buffer_number: usize,

    // ── Compaction ────────────────────────────────────────────────────────────
    /// Number of L0 files that triggers a compaction (default: 4).
    pub level0_file_num_compaction_trigger: usize,
    /// Number of L0 files at which writes slow down (default: 8).
    pub level0_slowdown_writes_trigger: usize,
    /// Number of L0 files at which writes stop (default: 12).
    pub level0_stop_writes_trigger: usize,
    /// Total byte budget for L1 (default: 256 MiB).
    pub max_bytes_for_level_base: u64,
    /// Each level's budget is `max_bytes_for_level_base * multiplier^(level-1)`
    /// (default: 10).
    pub max_bytes_for_level_multiplier: u64,
    /// Number of levels in the LSM tree (default: 7).
    pub num_levels: usize,
    /// Target size for individual SST files (default: 64 MiB).
    pub target_file_size_base: u64,

    // ── SST / Block format ────────────────────────────────────────────────────
    /// Block size inside SST files in bytes (default: 4 KiB).
    pub block_size: usize,
    /// Enable per-file Bloom filters (default: true).
    pub use_bloom_filter: bool,
    /// Bits per key for Bloom filters — higher = fewer false positives but
    /// more memory.  10 bits ≈ 1 % FP rate (default: 10).
    pub bloom_filter_bits_per_key: usize,

    // ── Block cache ───────────────────────────────────────────────────────────
    /// Maximum number of uncompressed blocks held in the LRU cache
    /// (default: 1 000).
    pub block_cache_capacity: usize,

    // ── WAL ───────────────────────────────────────────────────────────────────
    /// Call `fsync` after every WAL write (default: false).
    pub sync_wal: bool,
    /// Skip WAL entirely — data loss on crash (default: false).
    pub disable_wal: bool,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            create_if_missing: true,
            error_if_exists: false,

            write_buffer_size: 64 * 1024 * 1024,
            max_write_buffer_number: 2,

            level0_file_num_compaction_trigger: 4,
            level0_slowdown_writes_trigger: 8,
            level0_stop_writes_trigger: 12,
            max_bytes_for_level_base: 256 * 1024 * 1024,
            max_bytes_for_level_multiplier: 10,
            num_levels: 7,
            target_file_size_base: 64 * 1024 * 1024,

            block_size: 4 * 1024,
            use_bloom_filter: true,
            bloom_filter_bits_per_key: 10,

            block_cache_capacity: 1_000,

            sync_wal: false,
            disable_wal: false,
        }
    }
}

impl Options {
    pub fn new() -> Self {
        Self::default()
    }

    /// Maximum byte budget for `level` (1-based index matches RocksDB convention).
    pub fn max_bytes_for_level(&self, level: usize) -> u64 {
        assert!(level >= 1);
        self.max_bytes_for_level_base
            * self
                .max_bytes_for_level_multiplier
                .pow((level - 1) as u32)
    }
}

/// Per-read options.
#[derive(Debug, Clone)]
pub struct ReadOptions {
    /// If set, reads are bounded to entries visible at this sequence number.
    /// Set by `DB::snapshot()`.
    pub snapshot: Option<u64>,
    /// Verify block checksums when reading (default: true).
    pub verify_checksums: bool,
    /// Populate the block cache with data read (default: true).
    pub fill_cache: bool,
}

impl Default for ReadOptions {
    fn default() -> Self {
        ReadOptions {
            snapshot: None,
            verify_checksums: true,
            fill_cache: true,
        }
    }
}

impl ReadOptions {
    pub fn new() -> Self {
        Self::default()
    }
}

/// Per-write options.
#[derive(Debug, Clone, Default)]
pub struct WriteOptions {
    /// `fsync` before acknowledging the write (default: false).
    pub sync: bool,
    /// Skip the WAL for this write — data loss on crash (default: false).
    pub disable_wal: bool,
}

impl WriteOptions {
    pub fn new() -> Self {
        Self::default()
    }
}
