use super::memstore::MemStore;
use crate::merkle::backend::Backend;
use crate::merkle::merkle::Merkle;
use crate::merkle::node::Value;
use crate::merkle::store::NodeStore;

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

const TEST_CACHE_SIZE: usize = 1024;

/// A test-only backend wrapper that lets multiple `NodeStore`s share the same
/// underlying `MemStore` bytes, enabling "reopen" style tests.
struct SharedMemBackend(Arc<Mutex<MemStore>>);

impl Backend for SharedMemBackend {
    fn tail(&self) -> super::super::CleanPtr {
        self.0.lock().unwrap().tail() as super::super::CleanPtr
    }

    fn read(&mut self, ptr: super::super::CleanPtr, len: usize) -> Vec<u8> {
        let mut inner = self.0.lock().unwrap();
        inner.read(ptr as usize, len)
    }

    fn write(&mut self, ptr: super::super::CleanPtr, data: &[u8]) {
        let mut inner = self.0.lock().unwrap();
        inner.write(ptr as usize, data);
    }

    fn flush(&mut self) {
        self.0.lock().unwrap().flush();
    }

    #[cfg(feature = "stats")]
    fn print_stats(&mut self) {
        self.0.lock().unwrap().print_stats();
    }
}

fn new_merkle(shared: Arc<Mutex<MemStore>>, root_ptr: super::super::CleanPtr) -> Merkle {
    let store = Arc::new(Mutex::new(NodeStore::new(
        Box::new(SharedMemBackend(shared)),
        TEST_CACHE_SIZE,
        None,
    )));
    Merkle::new(store, root_ptr)
}

#[test]
fn merkle_insert_and_find() {
    let shared = Arc::new(Mutex::new(MemStore::new()));
    let mut merkle = new_merkle(shared, 0);

    assert!(merkle.find(b"missing").is_none());

    merkle.insert(b"dog", Value::new(b"puppy".to_vec(), Vec::new()));
    merkle.insert(b"doe", Value::new(b"deer".to_vec(), b"meta".to_vec()));
    merkle.insert(b"doge", Value::new(b"coin".to_vec(), Vec::new()));

    let v = merkle.find(b"dog").unwrap();
    assert_eq!(v.value, b"puppy".to_vec());
    assert_eq!(v.extra, Vec::<u8>::new());

    let v = merkle.find(b"doe").unwrap();
    assert_eq!(v.value, b"deer".to_vec());
    assert_eq!(v.extra, b"meta".to_vec());

    let v = merkle.find(b"doge").unwrap();
    assert_eq!(v.value, b"coin".to_vec());

    // overwrite existing key
    merkle.insert(b"dog", Value::new(b"hound".to_vec(), b"x".to_vec()));
    let v = merkle.find(b"dog").unwrap();
    assert_eq!(v.value, b"hound".to_vec());
    assert_eq!(v.extra, b"x".to_vec());
}

#[test]
fn merkle_delete_removes_key_and_preserves_others() {
    let shared = Arc::new(Mutex::new(MemStore::new()));
    let mut merkle = new_merkle(shared, 0);

    merkle.insert(b"dog", Value::new(b"puppy".to_vec(), Vec::new()));
    merkle.insert(b"doe", Value::new(b"deer".to_vec(), Vec::new()));
    merkle.insert(b"doge", Value::new(b"coin".to_vec(), Vec::new()));

    assert!(merkle.delete(b"doe"));
    assert!(merkle.find(b"doe").is_none());
    assert_eq!(merkle.find(b"dog").unwrap().value, b"puppy".to_vec());
    assert_eq!(merkle.find(b"doge").unwrap().value, b"coin".to_vec());

    // Deleting a missing key is a no-op.
    assert!(!merkle.delete(b"missing"));
}

#[test]
fn merkle_delete_then_commit_reopens_as_empty() {
    let shared = Arc::new(Mutex::new(MemStore::new()));

    let root_ptr = {
        let mut merkle = new_merkle(shared.clone(), 0);
        merkle.insert(b"k1", Value::new(b"v1".to_vec(), Vec::new()));
        assert!(merkle.delete(b"k1"));
        merkle.commit()
    };

    assert_eq!(
        root_ptr, 0,
        "committing deletion to empty should return root_ptr=0"
    );

    let merkle = new_merkle(shared, root_ptr);
    assert!(merkle.find(b"k1").is_none());
}

#[test]
fn merkle_delete_does_not_mutate_older_committed_versions() {
    let shared = Arc::new(Mutex::new(MemStore::new()));

    let key = b"time-travel-key";
    let val = Value::new(b"v1".to_vec(), b"meta1".to_vec());

    // Commit version 1 with the key present.
    let root_v1 = {
        let mut merkle = new_merkle(shared.clone(), 0);
        merkle.insert(key, val.clone());
        merkle.commit()
    };

    // Commit version 2 after deleting the key.
    let root_v2 = {
        let mut merkle = new_merkle(shared.clone(), root_v1);
        assert!(merkle.delete(key));
        merkle.commit()
    };

    // Reopen older version: key must still exist with the correct value.
    let merkle_v1 = new_merkle(shared.clone(), root_v1);
    let got = merkle_v1.find(key).expect("key missing in older snapshot");
    assert_eq!(got.value, val.value);
    assert_eq!(got.extra, val.extra);

    // Reopen newer version: key must be gone.
    let merkle_v2 = new_merkle(shared, root_v2);
    assert!(merkle_v2.find(key).is_none());
}

#[test]
fn merkle_persists_and_reopens_with_root_ptr() {
    let shared = Arc::new(Mutex::new(MemStore::new()));

    // First open and commit some writes.
    let root_ptr = {
        let mut merkle = new_merkle(shared.clone(), 0);
        merkle.insert(b"k1", Value::new(b"v1".to_vec(), Vec::new()));
        merkle.insert(b"k2", Value::new(b"v2".to_vec(), b"e2".to_vec()));
        merkle.insert(b"longer-key", Value::new(b"payload".to_vec(), Vec::new()));
        merkle.commit()
    };

    // Reopen with the previous root pointer (new store/cache, same backing bytes).
    let merkle = new_merkle(shared, root_ptr);

    let v = merkle.find(b"k1").unwrap();
    assert_eq!(v.value, b"v1".to_vec());
    assert_eq!(v.extra, Vec::<u8>::new());

    let v = merkle.find(b"k2").unwrap();
    assert_eq!(v.value, b"v2".to_vec());
    assert_eq!(v.extra, b"e2".to_vec());

    let v = merkle.find(b"longer-key").unwrap();
    assert_eq!(v.value, b"payload".to_vec());
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

#[test]
fn merkle_fuzzy_batch_commits_and_reopen_by_root_ptr() {
    const N_BATCHES: usize = 16;
    const PER_BATCH: usize = 500;
    let shared = Arc::new(Mutex::new(MemStore::new()));

    // Build multiple committed snapshots.
    let (roots, snapshots): (Vec<super::super::CleanPtr>, Vec<HashMap<Vec<u8>, Value>>) = {
        let mut merkle = new_merkle(shared.clone(), 0);
        let mut rng = XorShift64::new(0x1234_5678_9abc_def0);

        let mut state: HashMap<Vec<u8>, Value> = HashMap::new();
        let mut roots: Vec<super::super::CleanPtr> = Vec::with_capacity(N_BATCHES);
        let mut snapshots: Vec<HashMap<Vec<u8>, Value>> = Vec::with_capacity(N_BATCHES);

        for batch in 0..N_BATCHES {
            for i in 0..PER_BATCH {
                // Deterministic but "random-looking" key/value bytes.
                let mut key = Vec::with_capacity(16);
                key.extend_from_slice(&(batch as u32).to_le_bytes());
                key.extend_from_slice(&(i as u32).to_le_bytes());
                key.extend_from_slice(&rng.next_u64().to_le_bytes());

                let mut val = Vec::with_capacity(32);
                val.extend_from_slice(&rng.next_u64().to_le_bytes());
                val.extend_from_slice(&rng.next_u64().to_le_bytes());
                val.extend_from_slice(&rng.next_u64().to_le_bytes());
                val.extend_from_slice(&rng.next_u64().to_le_bytes());

                let extra = (rng.next_u64() & 1 == 0)
                    .then(|| rng.next_u64().to_le_bytes().to_vec())
                    .unwrap_or_default();

                let v = Value::new(val, extra);
                merkle.insert(&key, v.clone());
                state.insert(key, v);
            }

            let root = merkle.commit();
            if let Some(prev) = roots.last() {
                assert_ne!(*prev, root, "commit root_ptr should change across batches");
            }
            roots.push(root);
            snapshots.push(state.clone());
        }
        (roots, snapshots)
    };

    // Roots should be unique.
    let mut set = HashSet::new();
    for r in &roots {
        assert!(set.insert(*r), "duplicate root_ptr detected: {r}");
    }

    // Reopen each root in a fresh Merkle instance and validate snapshot contents.
    for (idx, root) in roots.iter().enumerate() {
        let merkle = new_merkle(shared.clone(), *root);
        for (k, expected) in snapshots[idx].iter() {
            let got = merkle
                .find(k.as_slice())
                .expect("missing key in reopened snapshot");
            assert_eq!(got.value, expected.value);
            assert_eq!(got.extra, expected.extra);
        }
    }
}
