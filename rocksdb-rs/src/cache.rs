//! LRU Block Cache
//!
//! An in-memory cache for uncompressed SST data blocks, keyed by
//! `(file_number, block_offset)`.  Hot blocks stay resident in memory,
//! eliminating repeated filesystem I/O for popular key ranges.
//!
//! ## Implementation
//! Uses a `HashMap` for O(1) lookup and a parallel `VecDeque` that tracks
//! access order (back = most-recently used, front = least-recently used).
//! Both structures are bounded by `capacity` entries.
//!
//! Time complexity:
//! * `get` — O(n) for the deque scan, acceptable for typical cache sizes
//!   (≤10 000 blocks).
//! * `insert` — same.
//!
//! A production implementation would use a doubly-linked list with intrusive
//! pointers or the `lru` crate; the trade-off here is simplicity with no
//! `unsafe` code.

use std::collections::{HashMap, VecDeque};

pub type CacheKey = (u64, u64); // (file_number, block_offset)
pub type CacheValue = Vec<u8>;  // raw block bytes

// ─────────────────────────────────────────────────────────────────────────────
// LruCache
// ─────────────────────────────────────────────────────────────────────────────

pub struct LruCache {
    capacity: usize,
    map: HashMap<CacheKey, CacheValue>,
    /// Keys in LRU order: front = oldest, back = most-recently used.
    order: VecDeque<CacheKey>,
}

impl LruCache {
    pub fn new(capacity: usize) -> Self {
        LruCache {
            capacity: capacity.max(1),
            map: HashMap::new(),
            order: VecDeque::new(),
        }
    }

    /// Look up a block.  Moves it to the MRU position on hit.
    pub fn get(&mut self, key: &CacheKey) -> Option<&CacheValue> {
        if !self.map.contains_key(key) {
            return None;
        }
        // Promote to MRU.
        if let Some(pos) = self.order.iter().position(|k| k == key) {
            let k = self.order.remove(pos).unwrap();
            self.order.push_back(k);
        }
        self.map.get(key)
    }

    /// Insert a block.  Evicts the LRU entry if at capacity.
    pub fn insert(&mut self, key: CacheKey, value: CacheValue) {
        if self.map.contains_key(&key) {
            // Update value and promote.
            self.map.insert(key, value);
            if let Some(pos) = self.order.iter().position(|k| k == &key) {
                let k = self.order.remove(pos).unwrap();
                self.order.push_back(k);
            }
            return;
        }

        // Evict LRU if full.
        if self.map.len() >= self.capacity {
            if let Some(lru) = self.order.pop_front() {
                self.map.remove(&lru);
            }
        }

        self.order.push_back(key);
        self.map.insert(key, value);
    }

    /// Evict all blocks belonging to `file_number` (called when a file is
    /// deleted after compaction).
    pub fn evict_file(&mut self, file_number: u64) {
        let keys: Vec<CacheKey> = self
            .order
            .iter()
            .filter(|(fn_, _)| *fn_ == file_number)
            .copied()
            .collect();
        for k in keys {
            self.map.remove(&k);
            if let Some(pos) = self.order.iter().position(|x| x == &k) {
                self.order.remove(pos);
            }
        }
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_insert_get() {
        let mut c = LruCache::new(3);
        c.insert((1, 0), vec![1, 2, 3]);
        assert_eq!(c.get(&(1, 0)), Some(&vec![1u8, 2, 3]));
        assert_eq!(c.get(&(9, 9)), None);
    }

    #[test]
    fn lru_eviction() {
        let mut c = LruCache::new(3);
        c.insert((0, 0), vec![0]);
        c.insert((1, 0), vec![1]);
        c.insert((2, 0), vec![2]);
        // (0,0) is now LRU — inserting a 4th should evict it.
        c.insert((3, 0), vec![3]);
        assert_eq!(c.get(&(0, 0)), None);
        assert_eq!(c.len(), 3);
    }

    #[test]
    fn access_promotes_to_mru() {
        let mut c = LruCache::new(3);
        c.insert((0, 0), vec![0]);
        c.insert((1, 0), vec![1]);
        c.insert((2, 0), vec![2]);
        // Access (0,0) to promote it; now (1,0) is LRU.
        c.get(&(0, 0));
        c.insert((3, 0), vec![3]);
        assert_eq!(c.get(&(1, 0)), None); // evicted
        assert!(c.get(&(0, 0)).is_some()); // still present
    }

    #[test]
    fn evict_file() {
        let mut c = LruCache::new(10);
        c.insert((7, 0), vec![0]);
        c.insert((7, 4096), vec![1]);
        c.insert((8, 0), vec![2]);
        c.evict_file(7);
        assert!(c.get(&(7, 0)).is_none());
        assert!(c.get(&(7, 4096)).is_none());
        assert!(c.get(&(8, 0)).is_some());
    }
}
