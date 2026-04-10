//! WriteBatch — atomic group of Put / Delete operations.
//!
//! All operations in a batch are assigned consecutive sequence numbers and
//! written atomically to the WAL as a single batch record before being applied
//! to the memtable.  Either every write in the batch is visible or none is.

use crate::types::SequenceNumber;

// ─────────────────────────────────────────────────────────────────────────────
// BatchOp
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum BatchOp {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

// ─────────────────────────────────────────────────────────────────────────────
// WriteBatch
// ─────────────────────────────────────────────────────────────────────────────

/// A collection of `Put` and `Delete` operations that are applied atomically.
///
/// Usage:
/// ```ignore
/// let mut batch = WriteBatch::new();
/// batch.put(b"key1", b"value1");
/// batch.delete(b"key2");
/// db.write(&batch, &WriteOptions::default())?;
/// ```
#[derive(Debug, Default, Clone)]
pub struct WriteBatch {
    pub(crate) ops: Vec<BatchOp>,
}

impl WriteBatch {
    pub fn new() -> Self {
        WriteBatch { ops: Vec::new() }
    }

    /// Add a `Put` operation.
    pub fn put(&mut self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) {
        self.ops.push(BatchOp::Put {
            key: key.into(),
            value: value.into(),
        });
    }

    /// Add a `Delete` operation.
    pub fn delete(&mut self, key: impl Into<Vec<u8>>) {
        self.ops.push(BatchOp::Delete { key: key.into() });
    }

    /// Number of operations in the batch.
    pub fn len(&self) -> usize {
        self.ops.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    /// Clear all operations.
    pub fn clear(&mut self) {
        self.ops.clear();
    }

    /// Approximate byte size of the batch (for write-buffer accounting).
    pub fn approximate_size(&self) -> usize {
        self.ops.iter().fold(0, |acc, op| match op {
            BatchOp::Put { key, value } => acc + key.len() + value.len() + 16,
            BatchOp::Delete { key } => acc + key.len() + 16,
        })
    }

    /// Convert the batch into WAL-compatible tuples:
    /// `(seq, key, value_opt)` where `value_opt = None` means Delete.
    pub fn to_wal_ops(
        &self,
        first_seq: SequenceNumber,
    ) -> Vec<(SequenceNumber, Vec<u8>, Option<Vec<u8>>)> {
        self.ops
            .iter()
            .enumerate()
            .map(|(i, op)| match op {
                BatchOp::Put { key, value } => {
                    (first_seq + i as u64, key.clone(), Some(value.clone()))
                }
                BatchOp::Delete { key } => (first_seq + i as u64, key.clone(), None),
            })
            .collect()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_and_delete() {
        let mut b = WriteBatch::new();
        b.put(b"k1".as_ref(), b"v1".as_ref());
        b.delete(b"k2".as_ref());
        assert_eq!(b.len(), 2);
    }

    #[test]
    fn wal_ops_get_consecutive_seqs() {
        let mut b = WriteBatch::new();
        b.put(b"a", b"1");
        b.put(b"b", b"2");
        b.delete(b"c");
        let ops = b.to_wal_ops(10);
        assert_eq!(ops[0].0, 10);
        assert_eq!(ops[1].0, 11);
        assert_eq!(ops[2].0, 12);
        assert!(ops[2].2.is_none());
    }

    #[test]
    fn approximate_size_nonzero() {
        let mut b = WriteBatch::new();
        b.put(b"hello", b"world");
        assert!(b.approximate_size() > 0);
    }
}
