#![allow(dead_code)]

use super::node::*;
#[cfg(feature = "stats")]
use super::stats::MerkleStats;
use super::store::NodeStore;
use super::utils;
use super::{CleanPtr, DirtyPtr, NBRANCH};
#[cfg(feature = "stats")]
use std::time::Instant;

use sha3::{Digest, Keccak256};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub struct Merkle {
    store: Arc<Mutex<NodeStore>>,
    root_cptr: CleanPtr,
    root_dptr: Option<DirtyPtr>,
    #[cfg(feature = "stats")]
    stats: Arc<Mutex<MerkleStats>>,
}

impl Merkle {
    pub fn new(store: Arc<Mutex<NodeStore>>, root_ptr: CleanPtr) -> Self {
        Self {
            store,
            root_cptr: root_ptr,
            root_dptr: None,
            #[cfg(feature = "stats")]
            stats: Arc::new(Mutex::new(MerkleStats::new())),
        }
    }

    pub fn root_cptr(&self) -> CleanPtr {
        self.root_cptr
    }

    pub fn hash(&self) -> Vec<u8> {
        let mut store = self.store.lock().unwrap();
        if self.root_cptr == 0 {
            return Keccak256::digest(&[0x80u8]).to_vec();
        }
        // Ethereum-style root hash is Keccak256(RLP(root_node_canonical)).
        let mut root_node = store.get_clean(self.root_cptr).clone();
        store.load_children_hash(&mut root_node);
        let root_rlp = root_node
            .rlp_encode()
            .expect("canonical root RLP encoding must succeed");
        Keccak256::digest(&root_rlp).to_vec()
    }

    pub fn find(&self, key: &[u8]) -> Option<Value> {
        if self.root_cptr == 0 && self.root_dptr.is_none() {
            return None;
        }
        #[cfg(feature = "stats")]
        let timer = Instant::now();
        let mut cur_ptr = match self.root_dptr {
            Some(dptr) => NodePtr::Dirty(dptr),
            None => NodePtr::Clean(self.root_cptr),
        };
        let mut store = self.store.lock().unwrap();
        let path = utils::to_path(key);
        let mut i = 0;
        let mut ptrs = Vec::new();
        while i <= path.len() {
            let cur_node = match cur_ptr {
                NodePtr::Clean(cptr) => {
                    ptrs.push(cptr);
                    store.get_clean(cptr)
                }
                NodePtr::Dirty(dptr) => match store.get_dirty(dptr) {
                    Some(n) => n,
                    None => break,
                },
            };
            match cur_node.get_inner() {
                NodeType::Branch(bnode) => {
                    assert!(i < path.len());
                    let bidx = path[i] as usize;
                    cur_ptr = match &bnode.children[bidx] {
                        Some(Child::Ptr(ptr)) => *ptr,
                        Some(Child::Hash(cptr, _)) => NodePtr::Clean(*cptr),
                        None => break,
                    };
                    i += 1;
                }
                NodeType::Short(snode) => {
                    assert!(i < path.len());
                    let shared_len = snode.common_prefix_len(&path[i..]);
                    if shared_len == snode.path.len() {
                        cur_ptr = match &snode.child {
                            Child::Ptr(ptr) => *ptr,
                            Child::Hash(cptr, _) => NodePtr::Clean(*cptr),
                        };
                        i += shared_len;
                    } else {
                        break;
                    }
                }
                NodeType::Value(vnode) => {
                    assert!(i == path.len());
                    #[cfg(feature = "stats")]
                    {
                        let mut stats = self.stats.lock().unwrap();
                        stats.get += 1;
                        stats.t_get += timer.elapsed().as_secs_f64();
                    }
                    return Some(vnode.clone());
                }
            }
        }
        #[cfg(not(feature = "lru"))]
        while let Some(cptr) = ptrs.pop() {
            store.get_clean(cptr);
        }
        #[cfg(feature = "stats")]
        {
            let mut stats = self.stats.lock().unwrap();
            stats.get += 1;
            stats.t_get += timer.elapsed().as_secs_f64();
        }
        None
    }

    pub fn insert(&mut self, key: &[u8], val: Value) {
        #[cfg(feature = "stats")]
        let timer = Instant::now();
        let mut store = self.store.lock().unwrap();
        let root_dptr = match &self.root_dptr {
            Some(dptr) => *dptr,
            None => {
                if self.root_cptr == 0 {
                    store.add_dirty(None)
                } else {
                    store.cow_clean(self.root_cptr)
                }
            }
        };
        // Track the dirty root so reads/commit see uncommitted changes.
        self.root_dptr = Some(root_dptr);

        let mut cur_dptr = root_dptr;
        let path = utils::to_path(key);
        let mut i = 0;

        let val_dptr = store.add_dirty(Some(Node(NodeType::Value(val))));

        while i < path.len() {
            match store.take_dirty(cur_dptr) {
                None => {
                    // reach a empty pointer with non-empty remaining path
                    // insert a short node for path compression and its value
                    let subpath = path[i..].to_vec();
                    assert!(subpath.len() > 0);
                    let snode = Short::new(subpath, Child::Ptr(NodePtr::Dirty(val_dptr)));
                    store.put_dirty(cur_dptr, Some(Node(NodeType::Short(snode))));
                    break;
                }
                Some(mut cur_node) => match &mut cur_node.get_inner_mut() {
                    NodeType::Value(_) => unreachable!(),
                    NodeType::Branch(bnode) => {
                        let bidx = path[i] as usize;
                        i += 1;
                        // after the branch index, path reaches the end
                        // insert the value to the current branch node
                        if i == path.len() {
                            // the last index of the path must be NBRANCH
                            assert!(bidx == NBRANCH);
                            bnode.children[bidx] = Some(Child::Ptr(NodePtr::Dirty(val_dptr)));
                            store.put_dirty(cur_dptr, Some(cur_node));
                            break;
                        } else {
                            // get the next node pointer (DirtyPtr)
                            let child_dptr = match &bnode.children[bidx] {
                                Some(Child::Ptr(NodePtr::Dirty(dptr))) => *dptr,
                                Some(Child::Ptr(NodePtr::Clean(cptr))) => store.cow_clean(*cptr),
                                Some(Child::Hash(cptr, _)) => store.cow_clean(*cptr),
                                None => store.add_dirty(None),
                            };
                            bnode.children[bidx] = Some(Child::Ptr(NodePtr::Dirty(child_dptr)));
                            store.put_dirty(cur_dptr, Some(cur_node));
                            cur_dptr = child_dptr;
                        }
                    }
                    NodeType::Short(snode) => {
                        let shared_len = snode.common_prefix_len(&path[i..]);
                        i += shared_len;
                        if i == path.len() && shared_len == snode.path.len() {
                            // the short node path is exact the remaining path
                            snode.child = Child::Ptr(NodePtr::Dirty(val_dptr));
                            store.put_dirty(cur_dptr, Some(cur_node));
                            break;
                        } else if i < path.len() && shared_len == snode.path.len() {
                            // the short node path matches a prefix of remaining
                            // path, get the next node pointer and continue the
                            // tree traversal
                            assert!(i + 1 < path.len());
                            let child_dptr = match snode.child {
                                Child::Ptr(NodePtr::Dirty(dptr)) => dptr,
                                Child::Ptr(NodePtr::Clean(cptr)) => {
                                    // the node is not loaded nor CoW yet
                                    store.cow_clean(cptr)
                                }
                                Child::Hash(cptr, _) => {
                                    // the hash is loaded, but the node is not CoW
                                    store.cow_clean(cptr)
                                }
                            };
                            snode.child = Child::Ptr(NodePtr::Dirty(child_dptr));
                            store.put_dirty(cur_dptr, Some(cur_node));
                            cur_dptr = child_dptr;
                        } else {
                            // partial short node path matches a partial prefix of
                            // remaining path, a branch node (maybe and a short
                            // node) is created BEFORE the current short node
                            assert!(i < path.len() && shared_len < snode.path.len());
                            let shared_prefix = snode.path[..shared_len].to_vec();
                            // branch index of the short node after the branch node
                            let bidx_snode = snode.path[shared_len] as usize;
                            // branch index of remaining path
                            let bidx_path = path[i] as usize;

                            let mut bnode = Branch::new();
                            bnode.children[bidx_snode] =
                                Some(if shared_len + 1 < snode.path.len() {
                                    // remaining path of the short node > 0, keep
                                    // the current short node
                                    snode.path = snode.path[shared_len + 1..].to_vec();
                                    Child::Ptr(NodePtr::Dirty(store.add_dirty(Some(cur_node))))
                                } else {
                                    // the remaining path of the short node is empty
                                    // remove the current short node and attach the
                                    // child to the branch node directly
                                    snode.child.clone()
                                });
                            bnode.children[bidx_path] = Some(if i + 1 < path.len() {
                                let new_snode = Node(NodeType::Short(Short::new(
                                    path[i + 1..].to_vec(),
                                    Child::Ptr(NodePtr::Dirty(val_dptr)),
                                )));
                                Child::Ptr(NodePtr::Dirty(store.add_dirty(Some(new_snode))))
                            } else {
                                // the remaining path reaches the NBRANCH
                                // attach the value to the branch node
                                assert!(bidx_path == NBRANCH);
                                Child::Ptr(NodePtr::Dirty(val_dptr))
                            });
                            let branch = Node(NodeType::Branch(bnode));
                            // the short node has a shared prefix with the remaining
                            // path, create a short node before the branch node
                            if shared_len > 0 {
                                let branch_ptr = store.add_dirty(Some(branch));
                                let prefix_snode = Node(NodeType::Short(Short::new(
                                    shared_prefix,
                                    Child::Ptr(NodePtr::Dirty(branch_ptr)),
                                )));
                                store.put_dirty(cur_dptr, Some(prefix_snode));
                            } else {
                                // no prefix short node is needed, set the current
                                // node to the branch node
                                store.put_dirty(cur_dptr, Some(branch));
                            }

                            break;
                        }
                    }
                },
            }
        }
        #[cfg(feature = "stats")]
        {
            let mut stats = self.stats.lock().unwrap();
            stats.put += 1;
            stats.t_put += timer.elapsed().as_secs_f64();
        }
    }

    /// Delete a key from the trie.
    ///
    /// Returns `true` if the key existed and was removed, `false` otherwise.
    pub fn delete(&mut self, key: &[u8]) -> bool {
        // Fast path: nothing committed and nothing dirty.
        if self.root_cptr == 0 && self.root_dptr.is_none() {
            return false;
        }

        #[cfg(feature = "stats")]
        let timer = Instant::now();

        let path = utils::to_path(key);

        // If we were clean, remember that so we can avoid "dirtying" the tree
        // when the key does not exist.
        let prev_root_dptr = self.root_dptr;

        let mut store = self.store.lock().unwrap();
        let root_dptr = match self.root_dptr {
            Some(dptr) => dptr,
            None => {
                if self.root_cptr == 0 {
                    // Create a placeholder dirty root; it will be populated by delete_rec
                    // or left as None if nothing gets deleted.
                    store.add_dirty(None)
                } else {
                    store.cow_clean(self.root_cptr)
                }
            }
        };

        // Tentatively track a dirty root so reads see in-flight changes.
        self.root_dptr = Some(root_dptr);

        let (new_root_opt, removed) =
            Self::delete_rec(&mut store, NodePtr::Dirty(root_dptr), &path, 0);

        if !removed {
            // Revert to prior state if this delete was a no-op on a clean tree.
            // (If we were already dirty, keep the dirty root.)
            if prev_root_dptr.is_none() {
                self.root_dptr = None;
            }
            #[cfg(feature = "stats")]
            {
                let mut stats = self.stats.lock().unwrap();
                stats.del += 1;
                stats.t_del += timer.elapsed().as_secs_f64();
            }
            return false;
        }

        match new_root_opt {
            None => {
                // Tree is now empty (uncommitted). Keep a dirty marker so `find`
                // sees emptiness; `commit` handles persisting to root_cptr=0.
                store.put_dirty(root_dptr, None);
                self.root_dptr = Some(root_dptr);
            }
            Some(NodePtr::Dirty(new_dptr)) => {
                self.root_dptr = Some(new_dptr);
            }
            Some(NodePtr::Clean(cptr)) => {
                // Should be rare, but keep representation consistent: move to dirty.
                let new_dptr = store.cow_clean(cptr);
                self.root_dptr = Some(new_dptr);
            }
        }

        #[cfg(feature = "stats")]
        {
            let mut stats = self.stats.lock().unwrap();
            stats.del += 1;
            stats.t_del += timer.elapsed().as_secs_f64();
        }
        true
    }

    fn delete_rec(
        store: &mut NodeStore,
        ptr: NodePtr,
        path: &[u8],
        depth: usize,
    ) -> (Option<NodePtr>, bool) {
        let NodePtr::Dirty(dptr) = ptr else {
            unreachable!("delete_rec must only be called with dirty pointers");
        };

        let Some(mut node) = store.take_dirty(dptr) else {
            // Empty subtree.
            return (None, false);
        };

        match node.get_inner_mut() {
            NodeType::Value(_) => {
                if depth == path.len() {
                    // Remove this value node.
                    store.put_dirty(dptr, None);
                    return (None, true);
                }
                store.put_dirty(dptr, Some(node));
                (Some(NodePtr::Dirty(dptr)), false)
            }
            NodeType::Short(snode) => {
                let remain = &path[depth..];
                let shared = snode.common_prefix_len(remain);
                if shared != snode.path.len() {
                    store.put_dirty(dptr, Some(node));
                    return (Some(NodePtr::Dirty(dptr)), false);
                }

                let new_depth = depth + shared;
                let child_ptr = match snode.child.clone() {
                    Child::Ptr(NodePtr::Dirty(cdptr)) => NodePtr::Dirty(cdptr),
                    Child::Ptr(NodePtr::Clean(cptr)) => NodePtr::Dirty(store.cow_clean(cptr)),
                    Child::Hash(cptr, _) => NodePtr::Dirty(store.cow_clean(cptr)),
                };

                // Ensure the short node points to the dirty child we will traverse/mutate.
                snode.child = Child::Ptr(child_ptr);

                let (new_child_opt, removed) = Self::delete_rec(store, child_ptr, path, new_depth);
                if !removed {
                    store.put_dirty(dptr, Some(node));
                    return (Some(NodePtr::Dirty(dptr)), false);
                }

                let Some(new_child_ptr) = new_child_opt else {
                    // Child removed => this short node is removed as well.
                    store.put_dirty(dptr, None);
                    return (None, true);
                };

                snode.child = Child::Ptr(new_child_ptr);

                // If this is an extension (no terminator nibble), merge consecutive shorts:
                // extension + (extension|leaf) => single short with concatenated path.
                if snode.path.last().copied() != Some(NBRANCH as u8) {
                    let child_node = match new_child_ptr {
                        NodePtr::Dirty(cdptr) => store.get_dirty(cdptr).cloned(),
                        NodePtr::Clean(cptr) => Some(store.get_clean(cptr).clone()),
                    };

                    if let Some(Node(NodeType::Short(child_snode))) = child_node {
                        let mut merged_path = snode.path.clone();
                        merged_path.extend_from_slice(&child_snode.path);
                        snode.path = merged_path;
                        snode.child = child_snode.child;
                    }
                }

                store.put_dirty(dptr, Some(node));
                (Some(NodePtr::Dirty(dptr)), true)
            }
            NodeType::Branch(bnode) => {
                if depth >= path.len() {
                    store.put_dirty(dptr, Some(node));
                    return (Some(NodePtr::Dirty(dptr)), false);
                }

                let bidx = path[depth] as usize;
                let next_depth = depth + 1;

                let Some(child) = bnode.children[bidx].take() else {
                    store.put_dirty(dptr, Some(node));
                    return (Some(NodePtr::Dirty(dptr)), false);
                };

                // Special-case the branch "value slot" (index 16) when we're at end of key.
                let (child_for_restore, new_child_opt, removed) =
                    if next_depth == path.len() && bidx == NBRANCH {
                        // Removing the value element is just clearing the slot.
                        (child, None, true)
                    } else {
                        // Ensure the child is dirty before descending.
                        let (child_ptr, child_updated) = match child {
                            Child::Ptr(NodePtr::Dirty(cdptr)) => {
                                (NodePtr::Dirty(cdptr), Child::Ptr(NodePtr::Dirty(cdptr)))
                            }
                            Child::Ptr(NodePtr::Clean(cptr)) => {
                                let cdptr = store.cow_clean(cptr);
                                (NodePtr::Dirty(cdptr), Child::Ptr(NodePtr::Dirty(cdptr)))
                            }
                            Child::Hash(cptr, _) => {
                                let cdptr = store.cow_clean(cptr);
                                (NodePtr::Dirty(cdptr), Child::Ptr(NodePtr::Dirty(cdptr)))
                            }
                        };
                        let (new_child_opt, removed) =
                            Self::delete_rec(store, child_ptr, path, next_depth);
                        (child_updated, new_child_opt, removed)
                    };

                if !removed {
                    // Put the child back unchanged.
                    bnode.children[bidx] = Some(child_for_restore);
                    store.put_dirty(dptr, Some(node));
                    return (Some(NodePtr::Dirty(dptr)), false);
                }

                bnode.children[bidx] = new_child_opt.map(|p| Child::Ptr(p));

                // Collapse rules (Ethereum MPT standard):
                // - Empty branch => remove it
                // - Single entry:
                //   - only value => leaf with empty path (represented as short path [16])
                //   - only one child and no value => short node (possibly merged)
                let mut present: Vec<usize> = Vec::new();
                for i in 0..NBRANCH + 1 {
                    if bnode.children[i].is_some() {
                        present.push(i);
                    }
                }

                match present.len() {
                    0 => {
                        store.put_dirty(dptr, None);
                        (None, true)
                    }
                    1 => {
                        let only = present[0];
                        let only_child = bnode.children[only].clone().unwrap();

                        // If the only entry is the value slot, collapse to a leaf short node.
                        let mut new_path: Vec<u8> = vec![only as u8];
                        let mut new_child: Child = only_child;

                        if only != NBRANCH {
                            // For non-value branches, attempt to merge [only] with a child short.
                            let child_ptr = match &new_child {
                                Child::Ptr(p) => *p,
                                Child::Hash(cptr, _) => NodePtr::Clean(*cptr),
                            };
                            let child_node = match child_ptr {
                                NodePtr::Dirty(cdptr) => store.get_dirty(cdptr).cloned(),
                                NodePtr::Clean(cptr) => Some(store.get_clean(cptr).clone()),
                            };
                            if let Some(Node(NodeType::Short(child_snode))) = child_node {
                                new_path.extend_from_slice(&child_snode.path);
                                new_child = child_snode.child;
                            }
                        }

                        let new_snode = Short::new(new_path, new_child);
                        store.put_dirty(dptr, Some(Node(NodeType::Short(new_snode))));
                        (Some(NodePtr::Dirty(dptr)), true)
                    }
                    _ => {
                        store.put_dirty(dptr, Some(node));
                        (Some(NodePtr::Dirty(dptr)), true)
                    }
                }
            }
        }
    }

    pub fn commit(&mut self) -> CleanPtr {
        #[cfg(feature = "stats")]
        let commit_timer = Instant::now();
        let root_dptr = match &self.root_dptr {
            Some(dptr) => *dptr,
            None => return self.root_cptr,
        };

        // If the dirty root is explicitly empty, this commit is deleting the trie to empty.
        let mut store = self.store.lock().unwrap();
        if store.get_dirty(root_dptr).is_none() {
            self.root_cptr = 0;
            self.root_dptr = None;
            store.commit();
            #[cfg(feature = "stats")]
            {
                let mut stats = self.stats.lock().unwrap();
                stats.t_commit += commit_timer.elapsed().as_secs_f64();
            }
            return 0;
        }

        let mut ptr_map: HashMap<DirtyPtr, (CleanPtr, Vec<u8>)> = HashMap::new();
        let mut nodes = Self::commit_order(&mut store, root_dptr);
        #[cfg(feature = "stats")]
        let mut stats = self.stats.lock().unwrap();
        #[cfg(feature = "stats")]
        let tc_node = Instant::now();
        while let Some((dptr, mut node)) = nodes.pop() {

            match &mut node.get_inner_mut() {
                
                NodeType::Branch(bnode) => {
                    for i in 0..NBRANCH + 1 {
                        if let Some(Child::Ptr(NodePtr::Dirty(child_dptr))) = &bnode.children[i] {
                            let (cptr, hash) = ptr_map.remove(child_dptr).unwrap();
                            bnode.children[i] = Some(Child::Hash(cptr, hash));
                        }
                    }
                }
                NodeType::Short(snode) => {
                    if let Child::Ptr(NodePtr::Dirty(child_dptr)) = snode.child.clone() {
                        let (cptr, hash) = ptr_map.remove(&child_dptr).unwrap();
                        snode.child = Child::Hash(cptr, hash);
                    }
                }
                NodeType::Value(_) => {}
            }

            #[cfg(feature = "stats")]
            let hash_timer = Instant::now();

            store.load_children_hash(&mut node);
            let hash = node.calc_hash().unwrap();

            #[cfg(feature = "stats")]
            {
                stats.tcn_hash += hash_timer.elapsed().as_secs_f64();
            }

            #[cfg(feature = "stats")]
            let add_timer = Instant::now();

            store.write_aha(&mut node);

            #[cfg(feature = "stats")] {
                stats.tcn_add += add_timer.elapsed().as_secs_f64();
            }

            #[cfg(feature = "stats")]
            let store_timer = Instant::now();

            let cptr = store.add_node(node);

            #[cfg(feature = "stats")] {
                stats.tcn_store += store_timer.elapsed().as_secs_f64();
            }   
            
            ptr_map.insert(dptr, (cptr, hash));
        }
        

        let (cptr, _hash) = ptr_map.remove(&root_dptr).unwrap();
        self.root_cptr = cptr;
        self.root_dptr = None;

        #[cfg(feature = "stats")] {
            stats.tc_node += tc_node.elapsed().as_secs_f64();
        }

        #[cfg(feature = "stats")]
        let tc_store = Instant::now();
        store.commit();
        #[cfg(feature = "stats")]
        {
            stats.tc_store += tc_store.elapsed().as_secs_f64();
            stats.t_commit += commit_timer.elapsed().as_secs_f64();
        }
        cptr
    }

    fn commit_order(store: &mut NodeStore, root_dptr: DirtyPtr) -> Vec<(DirtyPtr, Node)> {
        let mut nodes = Vec::new();
        nodes.push((root_dptr, store.take_dirty(root_dptr).unwrap()));

        let mut i = 0;
        while i < nodes.len() {
            // Collect child pointers first so we don't hold an immutable borrow
            // of `nodes[i]` while pushing into `nodes`.
            let dirty_children: Vec<DirtyPtr> = {
                let (_, cur_node) = &nodes[i];
                match cur_node.get_inner() {
                    NodeType::Branch(bnode) => {
                        let mut out = Vec::new();
                        for idx in 0..NBRANCH + 1 {
                            if let Some(Child::Ptr(NodePtr::Dirty(dptr))) = &bnode.children[idx] {
                                out.push(*dptr);
                            }
                        }
                        out
                    }
                    NodeType::Short(snode) => match &snode.child {
                        Child::Ptr(NodePtr::Dirty(dptr)) => vec![*dptr],
                        _ => Vec::new(),
                    },
                    NodeType::Value(_) => Vec::new(),
                }
            };

            i += 1;
            for dptr in dirty_children {
                nodes.push((dptr, store.take_dirty(dptr).unwrap()));
            }
        }
        nodes
    }

    #[cfg(feature = "stats")]
    pub fn print_stats(&mut self) {
        let mut stats = self.stats.lock().unwrap();
        stats.print_stats();
        stats.reset();
        self.store.lock().unwrap().print_stats();
    }
}
