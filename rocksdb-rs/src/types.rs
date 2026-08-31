/// A monotonically increasing version counter attached to every write.
/// The database advances this for every `Put` / `Delete` so that snapshots
/// and MVCC visibility can be implemented without locks on the read path.
pub type SequenceNumber = u64;

/// Sequence number handed out before any user write — no user data is visible.
pub const ZERO_SEQ: SequenceNumber = 0;
/// Largest usable sequence number (upper 8 bits encode the key kind).
pub const MAX_SEQ: SequenceNumber = u64::MAX >> 8;

// ─────────────────────────────────────────────────────────────────────────────
// KeyKind
// ─────────────────────────────────────────────────────────────────────────────

/// Discriminant stored in the low byte of the packed `(seq << 8) | kind` tag.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum KeyKind {
    /// A normal value entry.
    Value = 1,
    /// A deletion tombstone — the key should appear absent to readers.
    Deletion = 0,
}

impl KeyKind {
    pub fn from_u8(b: u8) -> Option<Self> {
        match b {
            1 => Some(KeyKind::Value),
            0 => Some(KeyKind::Deletion),
            _ => None,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// InternalKey
// ─────────────────────────────────────────────────────────────────────────────

/// The internal representation of a key stored on disk and in memory.
///
/// ## Ordering
/// Keys are first sorted by **user key ascending** (lexicographic), then by
/// **sequence number descending** (higher seq = more recent = earlier in
/// sorted order).  This guarantees that a forward scan starting at
/// `(user_key, read_seq)` yields the most-recent visible version of the key
/// as the very first match.
///
/// ## Wire encoding
/// `[user_key bytes][8-byte LE tag]` where `tag = (seq << 8) | kind as u8`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InternalKey {
    pub user_key: Vec<u8>,
    pub seq: SequenceNumber,
    pub kind: KeyKind,
}

impl InternalKey {
    pub fn new(user_key: Vec<u8>, seq: SequenceNumber, kind: KeyKind) -> Self {
        InternalKey { user_key, seq, kind }
    }

    /// Encode the internal key to its on-disk/in-memory byte representation.
    pub fn encode(&self) -> Vec<u8> {
        let tag: u64 = (self.seq << 8) | (self.kind as u64);
        let mut buf = Vec::with_capacity(self.user_key.len() + 8);
        buf.extend_from_slice(&self.user_key);
        buf.extend_from_slice(&tag.to_le_bytes());
        buf
    }

    /// Decode an internal key from its wire representation.
    pub fn decode(data: &[u8]) -> Option<Self> {
        if data.len() < 8 {
            return None;
        }
        let (key_bytes, tag_bytes) = data.split_at(data.len() - 8);
        let tag = u64::from_le_bytes(tag_bytes.try_into().ok()?);
        let seq = tag >> 8;
        let kind = KeyKind::from_u8((tag & 0xff) as u8)?;
        Some(InternalKey {
            user_key: key_bytes.to_vec(),
            seq,
            kind,
        })
    }

    /// Build a lookup key: same user key but with the maximum sequence number
    /// so that `BTreeMap::range(lookup_key..)` returns the most recent entry
    /// visible at `read_seq` as the first element.
    pub fn lookup_key(user_key: &[u8], read_seq: SequenceNumber) -> Self {
        InternalKey::new(user_key.to_vec(), read_seq, KeyKind::Value)
    }
}

impl PartialOrd for InternalKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for InternalKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // user_key ascending, seq descending (higher seq = comes first)
        self.user_key
            .cmp(&other.user_key)
            .then_with(|| other.seq.cmp(&self.seq))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_roundtrip() {
        let k = InternalKey::new(b"hello".to_vec(), 42, KeyKind::Value);
        let enc = k.encode();
        let dec = InternalKey::decode(&enc).unwrap();
        assert_eq!(dec.user_key, b"hello");
        assert_eq!(dec.seq, 42);
        assert_eq!(dec.kind, KeyKind::Value);
    }

    #[test]
    fn encode_decode_deletion() {
        let k = InternalKey::new(b"bye".to_vec(), 7, KeyKind::Deletion);
        let dec = InternalKey::decode(&k.encode()).unwrap();
        assert_eq!(dec.kind, KeyKind::Deletion);
    }

    #[test]
    fn ordering_higher_seq_first() {
        let k5 = InternalKey::new(b"foo".to_vec(), 5, KeyKind::Value);
        let k3 = InternalKey::new(b"foo".to_vec(), 3, KeyKind::Value);
        assert!(k5 < k3, "higher seq should sort first for the same user key");
    }

    #[test]
    fn ordering_user_key_asc() {
        let ka = InternalKey::new(b"aaa".to_vec(), 1, KeyKind::Value);
        let kb = InternalKey::new(b"bbb".to_vec(), 1, KeyKind::Value);
        assert!(ka < kb);
    }
}
