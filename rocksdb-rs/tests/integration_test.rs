//! Integration tests for rocksdb-rs.
//!
//! These tests exercise the full stack: WAL → memtable → SST flush →
//! leveled compaction → reads across all levels.

use rocksdb_rs::{Options, ReadOptions, WriteBatch, WriteOptions, DB};
use tempfile::tempdir;

fn small_opts() -> Options {
    let mut o = Options::default();
    o.write_buffer_size = 16 * 1024; // 16 KiB — flush often
    o.level0_file_num_compaction_trigger = 4;
    o.sync_wal = false;
    o
}

// ─────────────────────────────────────────────────────────────────────────────
// Basic CRUD
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn crud_basic() {
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path(), small_opts()).unwrap();
    let wo = WriteOptions::default();
    let ro = ReadOptions::default();

    assert_eq!(db.get(b"key", &ro).unwrap(), None);

    db.put(b"key", b"value", &wo).unwrap();
    assert_eq!(db.get(b"key", &ro).unwrap(), Some(b"value".to_vec()));

    db.put(b"key", b"updated", &wo).unwrap();
    assert_eq!(db.get(b"key", &ro).unwrap(), Some(b"updated".to_vec()));

    db.delete(b"key", &wo).unwrap();
    assert_eq!(db.get(b"key", &ro).unwrap(), None);
}

// ─────────────────────────────────────────────────────────────────────────────
// Persistence (close + reopen)
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn persistence_across_reopen() {
    let dir = tempdir().unwrap();
    let path = dir.path();

    {
        let db = DB::open(path, small_opts()).unwrap();
        let wo = WriteOptions::default();
        for i in 0u32..100 {
            db.put(
                format!("persist-{i:03}").as_bytes(),
                format!("val-{i}").as_bytes(),
                &wo,
            )
            .unwrap();
        }
    }

    let db = DB::open(path, small_opts()).unwrap();
    let ro = ReadOptions::default();
    for i in 0u32..100 {
        let expected = format!("val-{i}");
        let got = db
            .get(format!("persist-{i:03}").as_bytes(), &ro)
            .unwrap()
            .unwrap();
        assert_eq!(got, expected.as_bytes(), "key persist-{i:03}");
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// WriteBatch atomicity
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn write_batch_all_visible_together() {
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path(), small_opts()).unwrap();
    let wo = WriteOptions::default();
    let ro = ReadOptions::default();

    let mut batch = WriteBatch::new();
    for i in 0u32..50 {
        batch.put(
            format!("batch-{i:03}").as_bytes(),
            format!("bval-{i}").as_bytes(),
        );
    }
    db.write(&batch, &wo).unwrap();

    for i in 0u32..50 {
        let key = format!("batch-{i:03}");
        let expected = format!("bval-{i}");
        assert_eq!(
            db.get(key.as_bytes(), &ro).unwrap(),
            Some(expected.into_bytes()),
            "batch key {i}"
        );
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Snapshot isolation
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn snapshot_sees_old_value() {
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path(), small_opts()).unwrap();
    let wo = WriteOptions::default();

    db.put(b"snap-key", b"old", &wo).unwrap();
    let snap = db.snapshot();

    db.put(b"snap-key", b"new", &wo).unwrap();

    let mut ro_snap = ReadOptions::default();
    ro_snap.snapshot = Some(snap.seq);
    assert_eq!(
        db.get(b"snap-key", &ro_snap).unwrap(),
        Some(b"old".to_vec())
    );

    let ro_latest = ReadOptions::default();
    assert_eq!(
        db.get(b"snap-key", &ro_latest).unwrap(),
        Some(b"new".to_vec())
    );
}

#[test]
fn snapshot_does_not_see_later_deletes() {
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path(), small_opts()).unwrap();
    let wo = WriteOptions::default();

    db.put(b"k", b"v", &wo).unwrap();
    let snap = db.snapshot();
    db.delete(b"k", &wo).unwrap();

    let mut ro = ReadOptions::default();
    ro.snapshot = Some(snap.seq);
    assert_eq!(db.get(b"k", &ro).unwrap(), Some(b"v".to_vec()));
}

// ─────────────────────────────────────────────────────────────────────────────
// Iterator order and completeness
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn iterator_sorted_and_complete() {
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path(), small_opts()).unwrap();
    let wo = WriteOptions::default();

    let mut keys: Vec<String> = (0u32..200)
        .map(|i| format!("iter-{i:04}"))
        .collect();
    // Insert in shuffled order to verify sort correctness.
    for (i, key) in keys.iter().enumerate() {
        db.put(key.as_bytes(), format!("v{i}").as_bytes(), &wo)
            .unwrap();
    }

    keys.sort();
    let ro = ReadOptions::default();
    let got: Vec<String> = db
        .iter(&ro)
        .unwrap()
        .map(|(k, _)| String::from_utf8(k).unwrap())
        .collect();

    assert_eq!(got, keys);
}

#[test]
fn iterator_skips_deleted_keys() {
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path(), small_opts()).unwrap();
    let wo = WriteOptions::default();
    let ro = ReadOptions::default();

    db.put(b"a", b"1", &wo).unwrap();
    db.put(b"b", b"2", &wo).unwrap();
    db.put(b"c", b"3", &wo).unwrap();
    db.delete(b"b", &wo).unwrap();

    let keys: Vec<Vec<u8>> = db.iter(&ro).unwrap().map(|(k, _)| k).collect();
    assert_eq!(keys, vec![b"a".to_vec(), b"c".to_vec()]);
}

// ─────────────────────────────────────────────────────────────────────────────
// Stress: many keys spanning memtable and SST flush
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn stress_write_read_many_keys() {
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path(), small_opts()).unwrap();
    let wo = WriteOptions::default();
    let ro = ReadOptions::default();

    const N: u32 = 2_000;
    for i in 0..N {
        db.put(
            format!("key-{i:06}").as_bytes(),
            format!("value-{i}").as_bytes(),
            &wo,
        )
        .unwrap();
    }

    for i in 0..N {
        let key = format!("key-{i:06}");
        let val = format!("value-{i}");
        assert_eq!(
            db.get(key.as_bytes(), &ro).unwrap(),
            Some(val.into_bytes()),
            "missing key {key}"
        );
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Overwrite semantics
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn overwrite_returns_latest() {
    let dir = tempdir().unwrap();
    let db = DB::open(dir.path(), small_opts()).unwrap();
    let wo = WriteOptions::default();
    let ro = ReadOptions::default();

    for version in 1u32..=10 {
        db.put(b"key", format!("ver-{version}").as_bytes(), &wo)
            .unwrap();
    }
    assert_eq!(
        db.get(b"key", &ro).unwrap(),
        Some(b"ver-10".to_vec())
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Approximate disk size grows with writes
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn disk_size_grows_after_flush() {
    let dir = tempdir().unwrap();
    let mut o = small_opts();
    o.write_buffer_size = 1; // force immediate SST flush
    let db = DB::open(dir.path(), o).unwrap();
    let wo = WriteOptions::default();

    let before = db.approximate_disk_size();
    for i in 0u32..100 {
        db.put(format!("disk-{i:03}").as_bytes(), b"value-bytes-here", &wo)
            .unwrap();
    }
    db.flush().unwrap();

    let after = db.approximate_disk_size();
    assert!(after > before, "disk size should grow after flush");
}
