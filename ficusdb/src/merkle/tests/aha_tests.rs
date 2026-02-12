use super::memstore::MemStore;
use crate::merkle::aha::AggregatedHashArray;
use crate::merkle::backend::Backend;
use crate::merkle::node::{Branch, Child, Node, NodePtr, NodeType};
use crate::merkle::store::NodeStore;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

/// Wrap `MemStore` so tests can observe backend tails.
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

fn make_hash(seed: u8, len: usize) -> Vec<u8> {
    (0..len).map(|i| seed.wrapping_add(i as u8)).collect()
}

#[test]
fn aha_selects_backend_by_array_len() {
    // tiers: <=8, 9..=12, 13..=16
    let b0 = Arc::new(Mutex::new(MemStore::new()));
    let b1 = Arc::new(Mutex::new(MemStore::new()));
    let b2 = Arc::new(Mutex::new(MemStore::new()));

    let mut aha = AggregatedHashArray::new(vec![
        (8, Box::new(SharedMemBackend(b0.clone()))),
        (12, Box::new(SharedMemBackend(b1.clone()))),
        (16, Box::new(SharedMemBackend(b2.clone()))),
    ]);

    let tails = || {
        (
            b0.lock().unwrap().tail(),
            b1.lock().unwrap().tail(),
            b2.lock().unwrap().tail(),
        )
    };

    // len=8 goes to tier0
    let _ = aha.write_aha((0..8).map(|i| make_hash(i, 32)).collect(), 0, 0);
    assert_eq!(tails(), (8 * (33 + 1), 0, 0));

    // len=9 goes to tier1
    let _ = aha.write_aha((0..9).map(|i| make_hash(i, 32)).collect(), 0, 0);
    assert_eq!(tails(), (8 * (33 + 1), 12 * (33 + 1), 0));

    // len=13 goes to tier2
    let _ = aha.write_aha((0..13).map(|i| make_hash(i, 32)).collect(), 0, 0);
    assert_eq!(tails(), (8 * (33 + 1), 12 * (33 + 1), 16 * (33 + 1)));
}

#[test]
fn aha_roundtrips_hash_arrays() {
    let b0 = Arc::new(Mutex::new(MemStore::new()));
    let b1 = Arc::new(Mutex::new(MemStore::new()));
    let b2 = Arc::new(Mutex::new(MemStore::new()));

    let mut aha = AggregatedHashArray::new(vec![
        (8, Box::new(SharedMemBackend(b0))),
        (12, Box::new(SharedMemBackend(b1))),
        (16, Box::new(SharedMemBackend(b2))),
    ]);

    // Mix variable hash byte-lengths (<=32) to validate the length-prefix encoding.
    let hashes: Vec<Vec<u8>> = vec![make_hash(0x10, 0), make_hash(0x20, 7), make_hash(0x30, 32)];
    let ptr = aha.write_aha(hashes.clone(), 0, 0);
    let got = aha.read_aha(hashes.len() as u8, ptr);
    assert_eq!(got, hashes);
}

#[test]
fn aha_recycles_after_commit() {
    let b0 = Arc::new(Mutex::new(MemStore::new()));

    let mut aha = AggregatedHashArray::new(vec![(8, Box::new(SharedMemBackend(b0)))]);
    let hashes1: Vec<Vec<u8>> = (0..8).map(|i| make_hash(i, 32)).collect();
    let hashes2: Vec<Vec<u8>> = (8..16).map(|i| make_hash(i, 32)).collect();

    // First write allocates at ptr=0.
    let p0 = aha.write_aha(hashes1, 0, 0);
    assert_eq!(p0, 0);

    // Second write moves old ptr into pending recycle, allocates at tail.
    let p1 = aha.write_aha(hashes2.clone(), 8, p0);
    assert_ne!(p1, p0);

    // Commit makes the old ptr available for reuse.
    aha.commit();

    // Third write should be able to reuse p0.
    let p2 = aha.write_aha(hashes2, 8, p1);
    assert_eq!(p2, p0);
}

#[test]
fn aha_returns_zero_when_array_len_exceeds_max() {
    let b0 = Arc::new(Mutex::new(MemStore::new()));
    let mut aha = AggregatedHashArray::new(vec![(8, Box::new(SharedMemBackend(b0)))]);
    let hashes: Vec<Vec<u8>> = (0..9).map(|i| make_hash(i, 32)).collect();
    assert_eq!(aha.write_aha(hashes, 0, 0), 0);
}

/// Backend wrapper that counts reads/writes, backed by `MemStore`.
struct CountingMemBackend {
    inner: MemStore,
    reads: Arc<AtomicUsize>,
    writes: Arc<AtomicUsize>,
}

impl CountingMemBackend {
    fn new(reads: Arc<AtomicUsize>, writes: Arc<AtomicUsize>) -> Self {
        Self {
            inner: MemStore::new(),
            reads,
            writes,
        }
    }
}

impl Backend for CountingMemBackend {
    fn tail(&self) -> crate::merkle::CleanPtr {
        self.inner.tail() as crate::merkle::CleanPtr
    }

    fn read(&mut self, ptr: crate::merkle::CleanPtr, len: usize) -> Vec<u8> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.inner.read(ptr as usize, len)
    }

    fn write(&mut self, ptr: crate::merkle::CleanPtr, data: &[u8]) {
        self.writes.fetch_add(1, Ordering::Relaxed);
        self.inner.write(ptr as usize, data);
    }

    fn flush(&mut self) {
        self.inner.flush();
    }

    #[cfg(feature = "stats")]
    fn print_stats(&mut self) {
        self.inner.print_stats();
    }
}

fn rlp_child_ref(seed: u8) -> Vec<u8> {
    // Produce a valid RLP item for embedding into a branch.
    rlp::encode(&vec![seed]).to_vec()
}

#[test]
fn store_load_children_hash_uses_aha_single_read() {
    // Arrange: create a store with AHA enabled and counters for reads.
    let node_reads = Arc::new(AtomicUsize::new(0));
    let node_writes = Arc::new(AtomicUsize::new(0));
    let aha_reads = Arc::new(AtomicUsize::new(0));
    let aha_writes = Arc::new(AtomicUsize::new(0));

    let node_backend: Box<dyn Backend> =
        Box::new(CountingMemBackend::new(node_reads.clone(), node_writes));
    let aha_backend: Box<dyn Backend> =
        Box::new(CountingMemBackend::new(aha_reads.clone(), aha_writes));

    let aha = AggregatedHashArray::new(vec![(17, aha_backend)]);
    let mut store = NodeStore::new(node_backend, 0, Some(aha));

    // Build a branch node with 17 child reference items already loaded (Child::Hash).
    let mut b = Branch::new();
    let mut expected_child_hash_items: Vec<Vec<u8>> = Vec::new();
    for i in 0..17 {
        let cptr = i as crate::merkle::CleanPtr + 1;
        let h = rlp_child_ref(i as u8);
        expected_child_hash_items.push(h.clone());
        b.children[i] = Some(Child::Hash(cptr, h));
    }
    let mut node = Node(NodeType::Branch(b));

    // Compute the branch hash item and store the AHA blob.
    node.calc_hash().unwrap();
    store.write_aha(&mut node);

    // Simulate a decoded-on-disk branch: child pointers are Clean ptrs; hashes absent.
    let NodeType::Branch(mut persisted_bnode) = node.get_inner().clone() else {
        unreachable!();
    };
    for i in 0..17 {
        let cptr = i as crate::merkle::CleanPtr + 1;
        persisted_bnode.children[i] = Some(Child::Ptr(NodePtr::Clean(cptr)));
    }
    let mut persisted = Node(NodeType::Branch(persisted_bnode));

    // Act: load children hashes. It should read from AHA (once) and not touch node backend.
    store.load_aha(&mut persisted);

    // Assert: AHA was used; node backend not used.
    assert_eq!(
        node_reads.load(Ordering::Relaxed),
        0,
        "should not read child nodes when AHA present"
    );
    assert_eq!(
        aha_reads.load(Ordering::Relaxed),
        1,
        "should read AHA blob once"
    );

    // Assert: children are now `Child::Hash` with the expected embedded hash items.
    let NodeType::Branch(out_bnode) = persisted.get_inner() else {
        unreachable!();
    };
    let mut got: Vec<Vec<u8>> = Vec::new();
    for i in 0..17 {
        match &out_bnode.children[i] {
            Some(Child::Hash(_, h)) => got.push(h.clone()),
            _ => panic!("child {i} not loaded to hash"),
        }
    }
    assert_eq!(got, expected_child_hash_items);
}

#[test]
fn store_write_aha_does_not_recycle_on_first_write() {
    // If `NodeStore::write_aha` incorrectly treats the first write as an "update",
    // it will recycle ptr=0 on commit and the next write will wrongly reuse it.
    let node_backend: Box<dyn Backend> = Box::new(MemStore::new());
    let aha_backend: Box<dyn Backend> = Box::new(MemStore::new());
    let aha = AggregatedHashArray::new(vec![(17, aha_backend)]);
    let mut store = NodeStore::new(node_backend, 0, Some(aha));

    let mut b = Branch::new();
    for i in 0..17 {
        b.children[i] = Some(Child::Hash(
            i as crate::merkle::CleanPtr + 1,
            rlp_child_ref(i as u8),
        ));
    }
    let mut node = Node(NodeType::Branch(b));
    node.calc_hash().unwrap();

    store.write_aha(&mut node);
    let NodeType::Branch(b0) = node.get_inner() else {
        unreachable!()
    };
    let first_ptr = b0.aha_ptr;
    let first_len = b0.aha_len;
    assert_eq!(first_ptr, 0);
    assert_eq!(first_len, 17);

    // Commit should NOT recycle ptr=0 from the first write.
    store.commit();

    // Second write should allocate a new pointer (not reuse 0).
    store.write_aha(&mut node);
    let NodeType::Branch(b1) = node.get_inner() else {
        unreachable!()
    };
    assert_ne!(
        b1.aha_ptr, first_ptr,
        "should not reuse first AHA pointer after initial commit"
    );
}
