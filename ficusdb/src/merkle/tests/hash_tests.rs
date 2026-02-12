use super::eth_merkle::MPT;
use super::memstore::MemStore;
use crate::merkle::backend::Backend;
use crate::merkle::merkle::Merkle;
use crate::merkle::node::Value;
use crate::merkle::store::NodeStore;

use std::sync::{Arc, Mutex};

const TEST_CACHE_SIZE: usize = 1024;

/// A test-only backend wrapper that lets multiple `NodeStore`s share the same
/// underlying `MemStore` bytes, enabling "reopen" style tests.
struct SharedMemBackend(Arc<Mutex<MemStore>>);

impl Backend for SharedMemBackend {
    fn tail(&self) -> crate::merkle::CleanPtr {
        self.0.lock().unwrap().tail() as crate::merkle::CleanPtr
    }

    fn read(&mut self, ptr: crate::merkle::CleanPtr, len: usize) -> Vec<u8> {
        self.0.lock().unwrap().read(ptr as usize, len)
    }

    fn write(&mut self, ptr: crate::merkle::CleanPtr, data: &[u8]) {
        self.0.lock().unwrap().write(ptr as usize, data);
    }

    fn flush(&mut self) {
        self.0.lock().unwrap().flush();
    }

    #[cfg(feature = "stats")]
    fn print_stats(&mut self) {
        self.0.lock().unwrap().print_stats();
    }
}

fn new_merkle(shared: Arc<Mutex<MemStore>>, root_ptr: crate::merkle::CleanPtr) -> Merkle {
    let store = Arc::new(Mutex::new(NodeStore::new(
        Box::new(SharedMemBackend(shared)),
        TEST_CACHE_SIZE,
        None,
    )));
    Merkle::new(store, root_ptr)
}

#[test]
fn merkle_hash_empty_tree_matches_reference() {
    let shared = Arc::new(Mutex::new(MemStore::new()));
    let merkle = new_merkle(shared, 0);

    let got = merkle.hash();
    let expected = MPT::new().root_hash();
    assert_eq!(got, expected);
}

#[test]
fn merkle_hash_single_key_small_value_matches_reference() {
    let value = vec![0x11u8; 1];
    let key = b"k1";

    let shared = Arc::new(Mutex::new(MemStore::new()));
    let mut merkle = new_merkle(shared, 0);
    merkle.insert(key, Value::new(value.clone(), Vec::new()));
    merkle.commit();

    let got = merkle.hash();
    let mut mpt = MPT::new();
    mpt.insert(key, &value);
    let expected = mpt.root_hash();
    assert_eq!(got, expected);
}

#[test]
fn merkle_hash_single_key_large_value_matches_reference() {
    let value = vec![0x22u8; 64];
    let key = b"k2";

    let shared = Arc::new(Mutex::new(MemStore::new()));
    let mut merkle = new_merkle(shared, 0);
    merkle.insert(key, Value::new(value.clone(), Vec::new()));
    merkle.commit();

    let got = merkle.hash();
    let mut mpt = MPT::new();
    mpt.insert(key, &value);
    let expected = mpt.root_hash();
    assert_eq!(got, expected);
}

#[test]
fn merkle_hash_ignores_uncommitted_changes() {
    let value = vec![0x33u8; 8];
    let key = b"pending";

    let shared = Arc::new(Mutex::new(MemStore::new()));
    let mut merkle = new_merkle(shared, 0);

    // Apply an update but don't commit it.
    merkle.insert(key, Value::new(value.clone(), Vec::new()));

    // Hash must still be the previous committed root (empty trie).
    let got = merkle.hash();
    let expected = MPT::new().root_hash();
    assert_eq!(got, expected);

    // Once committed, it should match the reference trie.
    merkle.commit();
    let got = merkle.hash();
    let mut mpt = MPT::new();
    mpt.insert(key, &value);
    let expected = mpt.root_hash();
    assert_eq!(got, expected);
}

#[derive(Clone)]
struct XorShift64 {
    state: u64,
}

impl XorShift64 {
    fn new(seed: u64) -> Self {
        Self { state: seed.max(1) }
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }
}

fn rand_bytes(rng: &mut XorShift64, len: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(len);
    while out.len() < len {
        out.extend_from_slice(&rng.next_u64().to_le_bytes());
    }
    out.truncate(len);
    out
}

#[test]
fn merkle_hash_fuzzy_matches_mpt_reference() {
    // Keep this reasonably sized so debug builds finish quickly.
    const N: usize = 10_000;

    let shared = Arc::new(Mutex::new(MemStore::new()));
    let mut merkle = new_merkle(shared, 0);
    let mut mpt = MPT::new();

    let mut rng = XorShift64::new(0x9e37_79b9_7f4a_7c15);

    for i in 0..N {
        // Deterministic "random-looking" key bytes (varying length, non-empty).
        let klen = 4 + (rng.next_u64() as usize % 24); // 4..=27
        let mut key = rand_bytes(&mut rng, klen);
        key.extend_from_slice(&(i as u32).to_le_bytes());

        // Values with a mix of small and large sizes.
        let vlen = rng.next_u64() as usize % 96; // 0..=95
        let value = rand_bytes(&mut rng, vlen);

        merkle.insert(&key, Value::new(value.clone(), Vec::new()));
        mpt.insert(&key, &value);
    }

    merkle.commit();
    let got = merkle.hash();
    let expected = mpt.root_hash();
    assert_eq!(got, expected);
}

#[test]
fn merkle_hash_after_deletes_matches_mpt_reference() {
    const N: usize = 5_000;

    let shared = Arc::new(Mutex::new(MemStore::new()));
    let mut merkle = new_merkle(shared, 0);
    let mut mpt = MPT::new();

    let mut rng = XorShift64::new(0xfeed_beef_cafe_f00d);

    let mut keys: Vec<Vec<u8>> = Vec::with_capacity(N);
    for i in 0..N {
        let klen = 4 + (rng.next_u64() as usize % 24);
        let mut key = rand_bytes(&mut rng, klen);
        key.extend_from_slice(&(i as u32).to_le_bytes());
        keys.push(key);
    }

    for (i, key) in keys.iter().enumerate() {
        let vlen = (i * 37) % 96;
        let value = rand_bytes(&mut rng, vlen);
        merkle.insert(key, Value::new(value.clone(), Vec::new()));
        mpt.insert(key, &value);
    }

    merkle.commit();
    assert_eq!(merkle.hash(), mpt.root_hash());

    // Delete a deterministic subset (including some that won't exist later).
    for (i, key) in keys.iter().enumerate() {
        if i % 3 == 0 {
            assert_eq!(merkle.delete(key), mpt.delete(key));
        }
    }

    merkle.commit();
    assert_eq!(merkle.hash(), mpt.root_hash());

    // Deleting missing keys should not change the root.
    let before = merkle.hash();
    assert!(!merkle.delete(b"definitely-missing"));
    merkle.commit();
    assert_eq!(merkle.hash(), before);
}

#[test]
fn merkle_hash_delete_to_empty_matches_reference_empty() {
    let shared = Arc::new(Mutex::new(MemStore::new()));
    let mut merkle = new_merkle(shared, 0);
    let mut mpt = MPT::new();

    let keys = [
        b"a".as_slice(),
        b"ab".as_slice(),
        b"abc".as_slice(),
        b"b".as_slice(),
    ];
    for k in keys {
        merkle.insert(k, Value::new(vec![0x11u8], Vec::new()));
        mpt.insert(k, &[0x11u8]);
    }
    merkle.commit();
    assert_eq!(merkle.hash(), mpt.root_hash());

    for k in keys {
        assert_eq!(merkle.delete(k), mpt.delete(k));
    }
    merkle.commit();
    assert_eq!(merkle.hash(), mpt.root_hash());
    assert_eq!(merkle.hash(), MPT::new().root_hash());
}
