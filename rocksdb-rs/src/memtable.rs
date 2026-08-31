use std::collections::BTreeMap;
use std::sync::Arc;

use crate::types::{InternalKey, KeyKind, SequenceNumber};

// ─────────────────────────────────────────────────────────────────────────────
// Memtable
// ─────────────────────────────────────────────────────────────────────────────

/// In-memory write buffer that accumulates mutations before they are flushed
/// to an immutable L0 SST file.
///
/// The backing store is a `BTreeMap<InternalKey, Vec<u8>>`.  Because
/// `InternalKey` orders equal user keys by *descending* sequence number, a
/// point-lookup with `BTreeMap::range(lookup_key..)` finds the most-recent
/// version of a key as its very first element — no heap allocation needed.
///
/// Thread safety: `Memtable` itself is not `Sync`.  The `DB` wraps the active
/// memtable in a `Mutex`; once a memtable becomes immutable it is wrapped in
/// an `Arc` and read-only for the rest of its lifetime.
pub struct Memtable {
    data: BTreeMap<InternalKey, Vec<u8>>,
    /// Approximate byte usage (keys + values + fixed overhead per entry).
    approximate_size: usize,
}

impl Memtable {
    pub fn new() -> Self {
        Memtable {
            data: BTreeMap::new(),
            approximate_size: 0,
        }
    }

    // ── Write operations ──────────────────────────────────────────────────────

    /// Record a value write.
    pub fn put(&mut self, user_key: Vec<u8>, seq: SequenceNumber, value: Vec<u8>) {
        // size delta: key + 8-byte tag + value + rough overhead
        self.approximate_size += user_key.len() + 8 + value.len() + 32;
        let ikey = InternalKey::new(user_key, seq, KeyKind::Value);
        self.data.insert(ikey, value);
    }

    /// Record a deletion tombstone.  The value stored for tombstones is empty.
    pub fn delete(&mut self, user_key: Vec<u8>, seq: SequenceNumber) {
        self.approximate_size += user_key.len() + 8 + 32;
        let ikey = InternalKey::new(user_key, seq, KeyKind::Deletion);
        self.data.insert(ikey, Vec::new());
    }

    // ── Read operations ───────────────────────────────────────────────────────

    /// Look up `user_key` with visibility bounded to `read_seq`.
    ///
    /// Returns:
    /// * `None` — no entry for this key exists at or before `read_seq`
    /// * `Some(Some(v))` — the key has value `v`
    /// * `Some(None)` — the key was deleted
    pub fn get(&self, user_key: &[u8], read_seq: SequenceNumber) -> Option<Option<Vec<u8>>> {
        // Construct a lookup key: (user_key, read_seq, Value).
        // Due to InternalKey ordering — same user_key → descending seq —
        // this key sits *before* any real entry for that user key whose
        // seq > read_seq, and *at or after* the most-recent entry with seq
        // <= read_seq.  The first element yielded by `range(lookup..)` is
        // therefore exactly the answer we want.
        let lookup = InternalKey::lookup_key(user_key, read_seq);

        for (ikey, value) in self.data.range(lookup..) {
            if ikey.user_key.as_slice() != user_key {
                // Moved past the target user key — not found.
                break;
            }
            // ikey.seq <= read_seq guaranteed by the ordering.
            return Some(match ikey.kind {
                KeyKind::Value => Some(value.clone()),
                KeyKind::Deletion => None,
            });
        }
        None
    }

    // ── Introspection ─────────────────────────────────────────────────────────

    /// Approximate memory usage in bytes.
    pub fn approximate_size(&self) -> usize {
        self.approximate_size
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Iterate over all entries in sorted `InternalKey` order.
    /// Used when flushing to an SST file.
    pub fn iter(&self) -> impl Iterator<Item = (&InternalKey, &Vec<u8>)> {
        self.data.iter()
    }
}

impl Default for Memtable {
    fn default() -> Self {
        Self::new()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// ImmutableMemtable
// ─────────────────────────────────────────────────────────────────────────────

/// A frozen `Memtable` that is being flushed to disk.
/// Holds an `Arc` so the background thread can own it independently.
pub type ImmutableMemtable = Arc<Memtable>;

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_and_get_basic() {
        let mut mt = Memtable::new();
        mt.put(b"key".to_vec(), 1, b"v1".to_vec());
        mt.put(b"key".to_vec(), 2, b"v2".to_vec());

        assert_eq!(mt.get(b"key", 1), Some(Some(b"v1".to_vec())));
        assert_eq!(mt.get(b"key", 2), Some(Some(b"v2".to_vec())));
        // read_seq=5 > any written seq → returns most-recent visible value
        assert_eq!(mt.get(b"key", 5), Some(Some(b"v2".to_vec())));
    }

    #[test]
    fn get_at_seq_zero_returns_nothing() {
        let mut mt = Memtable::new();
        mt.put(b"k".to_vec(), 1, b"v".to_vec());
        // seq=0 is before any write
        assert_eq!(mt.get(b"k", 0), None);
    }

    #[test]
    fn delete_is_visible() {
        let mut mt = Memtable::new();
        mt.put(b"k".to_vec(), 1, b"v".to_vec());
        mt.delete(b"k".to_vec(), 2);

        assert_eq!(mt.get(b"k", 1), Some(Some(b"v".to_vec())));
        assert_eq!(mt.get(b"k", 2), Some(None)); // tombstone
        assert_eq!(mt.get(b"k", 9), Some(None)); // still deleted
    }

    #[test]
    fn missing_key_returns_none() {
        let mt = Memtable::new();
        assert_eq!(mt.get(b"no-such-key", 100), None);
    }

    #[test]
    fn multiple_keys() {
        let mut mt = Memtable::new();
        mt.put(b"a".to_vec(), 1, b"aa".to_vec());
        mt.put(b"b".to_vec(), 2, b"bb".to_vec());
        mt.put(b"c".to_vec(), 3, b"cc".to_vec());

        assert_eq!(mt.get(b"b", 10), Some(Some(b"bb".to_vec())));
        assert_eq!(mt.get(b"d", 10), None);
    }

    #[test]
    fn approximate_size_grows() {
        let mut mt = Memtable::new();
        assert_eq!(mt.approximate_size(), 0);
        mt.put(b"key".to_vec(), 1, b"value".to_vec());
        assert!(mt.approximate_size() > 0);
    }
}
