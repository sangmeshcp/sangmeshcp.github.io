//! Bloom filter
//!
//! A space-efficient probabilistic set membership test used to skip SST file
//! reads when a queried key is definitely absent.  False positives are
//! possible but false negatives are not.
//!
//! ## Algorithm
//! Uses the double-hashing technique to simulate *k* independent hash
//! functions from a single MurmurHash-inspired base hash.  For 10 bits/key the
//! false-positive rate is ≈ 1 %.
//!
//! ## On-disk format
//! `[num_hash_funcs: u8][filter_bits: &[u8]]`

// ─────────────────────────────────────────────────────────────────────────────
// BloomFilter
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct BloomFilter {
    bits: Vec<u8>,
    num_probes: u8,
}

impl BloomFilter {
    /// Construct a filter sized for `num_keys` expected insertions.
    ///
    /// `bits_per_key` trades memory for false-positive rate:
    /// * 10 → ~1 %
    /// * 12 → ~0.4 %
    /// * 14 → ~0.17 %
    pub fn new(num_keys: usize, bits_per_key: usize) -> Self {
        let num_bits = (num_keys * bits_per_key).max(64);
        let num_bytes = (num_bits + 7) / 8;
        // k = ln(2) × bits_per_key ≈ 0.693 × bpk, clamped to [1, 30].
        let num_probes = ((bits_per_key as f64 * 0.693).round() as u8).max(1).min(30);
        BloomFilter {
            bits: vec![0u8; num_bytes],
            num_probes,
        }
    }

    /// Deserialise from the on-disk representation.
    pub fn from_raw(num_probes: u8, bits: Vec<u8>) -> Self {
        BloomFilter { bits, num_probes }
    }

    /// Insert a key into the filter.
    pub fn insert(&mut self, key: &[u8]) {
        let num_bits = (self.bits.len() * 8) as u32;
        let h = bloom_hash(key);
        let delta = h.rotate_right(17);
        let mut h = h;
        for _ in 0..self.num_probes {
            let bit_pos = (h % num_bits) as usize;
            self.bits[bit_pos >> 3] |= 1 << (bit_pos & 7);
            h = h.wrapping_add(delta);
        }
    }

    /// Test membership.  Returns `false` if the key is *definitely* absent.
    pub fn may_contain(&self, key: &[u8]) -> bool {
        let num_bits = (self.bits.len() * 8) as u32;
        let h = bloom_hash(key);
        let delta = h.rotate_right(17);
        let mut h = h;
        for _ in 0..self.num_probes {
            let bit_pos = (h % num_bits) as usize;
            if self.bits[bit_pos >> 3] & (1 << (bit_pos & 7)) == 0 {
                return false;
            }
            h = h.wrapping_add(delta);
        }
        true
    }

    /// Number of probe positions per key.
    pub fn num_probes(&self) -> u8 {
        self.num_probes
    }

    /// The raw bit array.
    pub fn bits(&self) -> &[u8] {
        &self.bits
    }

    /// Encode to on-disk bytes: `[num_probes][bits...]`
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(1 + self.bits.len());
        out.push(self.num_probes);
        out.extend_from_slice(&self.bits);
        out
    }

    /// Decode from on-disk bytes.
    pub fn decode(data: &[u8]) -> Option<Self> {
        if data.is_empty() {
            return None;
        }
        let num_probes = data[0];
        if num_probes == 0 {
            return None; // sentinel for "no filter"
        }
        Some(BloomFilter {
            num_probes,
            bits: data[1..].to_vec(),
        })
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Hash function
// ─────────────────────────────────────────────────────────────────────────────

/// MurmurHash2-inspired 32-bit hash used for bloom probing.
/// Compatible with LevelDB's bloom filter hash so existing SST files remain
/// interoperable if needed.
fn bloom_hash(key: &[u8]) -> u32 {
    const SEED: u32 = 0xbc9f_1d34;
    const M: u32 = 0xc6a4_a793;
    const R: u32 = 24;

    let len = key.len();
    let mut h = SEED ^ ((len as u32).wrapping_mul(M));

    let mut chunks = key.chunks_exact(4);
    for chunk in chunks.by_ref() {
        let w = u32::from_le_bytes(chunk.try_into().unwrap());
        h = h.wrapping_add(w);
        h = h.wrapping_mul(M);
        h ^= h >> 16;
    }
    let rem = chunks.remainder();
    match rem.len() {
        3 => {
            h = h.wrapping_add((rem[2] as u32) << 16);
            h = h.wrapping_add((rem[1] as u32) << 8);
            h = h.wrapping_add(rem[0] as u32);
            h = h.wrapping_mul(M);
            h ^= h >> R;
        }
        2 => {
            h = h.wrapping_add((rem[1] as u32) << 8);
            h = h.wrapping_add(rem[0] as u32);
            h = h.wrapping_mul(M);
            h ^= h >> R;
        }
        1 => {
            h = h.wrapping_add(rem[0] as u32);
            h = h.wrapping_mul(M);
            h ^= h >> R;
        }
        _ => {}
    }
    h
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inserted_keys_always_match() {
        let keys: Vec<Vec<u8>> = (0u32..1000)
            .map(|i| format!("key-{i}").into_bytes())
            .collect();

        let mut bf = BloomFilter::new(keys.len(), 10);
        for k in &keys {
            bf.insert(k);
        }
        for k in &keys {
            assert!(bf.may_contain(k), "false negative for {k:?}");
        }
    }

    #[test]
    fn false_positive_rate_reasonable() {
        let mut bf = BloomFilter::new(1000, 10);
        for i in 0u32..1000 {
            bf.insert(format!("present-{i}").as_bytes());
        }

        let fps: usize = (0u32..10_000)
            .filter(|i| bf.may_contain(format!("absent-{i}").as_bytes()))
            .count();
        // With 10 bits/key the expected FP rate is ≈1 %; we allow ≤5 %.
        assert!(fps < 500, "too many false positives: {fps}/10000");
    }

    #[test]
    fn encode_decode_roundtrip() {
        let mut bf = BloomFilter::new(100, 10);
        bf.insert(b"hello");
        bf.insert(b"world");

        let enc = bf.encode();
        let bf2 = BloomFilter::decode(&enc).unwrap();
        assert!(bf2.may_contain(b"hello"));
        assert!(bf2.may_contain(b"world"));
    }

    #[test]
    fn empty_filter_has_no_false_negatives() {
        // num_probes = 0 is the "no filter" sentinel; decode returns None.
        let none = BloomFilter::decode(&[0u8, 0u8, 0u8]);
        assert!(none.is_none());
    }
}
