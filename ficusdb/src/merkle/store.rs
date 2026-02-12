#![allow(dead_code)]

use super::aha::AggregatedHashArray;
use super::backend::Backend;
use super::node::{Child, Node, NodePtr, NodeType};
use super::{CleanPtr, DirtyPtr, NBRANCH};

#[cfg(feature = "stats")]
use super::stats::StoreStats;
use lru_mem::LruCache;
use std::io::{Error, ErrorKind};
use std::mem::size_of;
#[cfg(feature = "stats")]
use std::time::Instant;

type EncodedLen = u16;

pub struct NodeStore {
    dirty: Vec<Option<Node>>,
    clean: LruCache<CleanPtr, Node>,

    backend: Box<dyn Backend>,
    aha: Option<AggregatedHashArray>,
    #[cfg(feature = "stats")]
    stats: StoreStats,
}

impl NodeStore {
    pub fn new(
        backend: Box<dyn Backend>,
        cache_size: usize,
        aha: Option<AggregatedHashArray>,
    ) -> Self {
        Self {
            dirty: Vec::new(),
            clean: LruCache::new(cache_size),
            backend,
            aha,
            #[cfg(feature = "stats")]
            stats: StoreStats::new(),
        }
    }

    // ===== store =====
    fn get_node(&mut self, ptr: CleanPtr) -> Result<Node, Error> {
        let len_buf = self.backend.read(ptr, size_of::<EncodedLen>());
        if len_buf.len() != size_of::<EncodedLen>() {
            return Err(Error::new(ErrorKind::Other, "Invalid encoded length"));
        }
        let len = u16::from_le_bytes(len_buf.try_into().unwrap());
        let data = self
            .backend
            .read(ptr + size_of::<EncodedLen>() as CleanPtr, len as usize);
        Node::decode(&data)
    }

    pub fn add_node(&mut self, node: Node) -> CleanPtr {
        #[cfg(feature = "stats")]   
        let encode_timer = Instant::now();
        let encoded = node.encode();
        #[cfg(feature = "stats")] {
            self.stats.t_encode += encode_timer.elapsed().as_secs_f64();
        }
        let mut buf = (encoded.len() as EncodedLen).to_le_bytes().to_vec();
        buf.extend(encoded);
        let cptr = self.backend.tail();
        self.backend.write(cptr, &buf);
        let _ = self.clean.insert(cptr, node);
        cptr
    }

    // ===== cache =====
    pub fn get_clean(&mut self, cptr: CleanPtr) -> &Node {
        if !self.clean.contains(&cptr) {
            #[cfg(feature = "stats")]
            let load_timer = Instant::now();
            let node = self.get_node(cptr).unwrap();
            let _ = self.clean.insert(cptr, node);
            #[cfg(feature = "stats")]
            {
                self.stats.node_miss += 1;
                self.stats.node_load += load_timer.elapsed().as_secs_f64();
            }
        } else {
            #[cfg(feature = "stats")]
            {
                self.stats.node_hit += 1;
            }
        }
        self.clean.get(&cptr).unwrap()
    }

    pub fn take_clean(&mut self, cptr: CleanPtr) -> Node {
        match self.clean.remove(&cptr) {
            Some(node) => {
                #[cfg(feature = "stats")]
                {
                    self.stats.node_hit += 1;
                }
                node
            }
            None => {
                #[cfg(feature = "stats")]
                let load_timer = Instant::now();
                let node = self.get_node(cptr).unwrap();
                #[cfg(feature = "stats")]
                {
                    self.stats.node_miss += 1;
                    self.stats.node_load += load_timer.elapsed().as_secs_f64();
                }
                node
            }
        }
    }

    pub fn get_dirty(&mut self, dptr: DirtyPtr) -> Option<&Node> {
        match &self.dirty[dptr] {
            Some(node) => Some(node),
            None => None,
        }
    }

    pub fn take_dirty(&mut self, dptr: DirtyPtr) -> Option<Node> {
        self.dirty[dptr].take()
    }

    pub fn add_dirty(&mut self, n: Option<Node>) -> DirtyPtr {
        self.dirty.push(n);
        self.dirty.len() - 1
    }

    pub fn put_dirty(&mut self, dptr: DirtyPtr, n: Option<Node>) {
        self.dirty[dptr] = n;
    }

    pub fn cow_clean(&mut self, cptr: CleanPtr) -> DirtyPtr {
        #[cfg(not(feature = "lru"))] 
        let mut node = self.take_clean(cptr);
        #[cfg(feature = "lru")]
        let mut node = self.get_clean(cptr).clone();
        self.load_aha(&mut node);
        self.add_dirty(Some(node))
    }

    pub fn commit(&mut self) {
        #[cfg(feature = "stats")]
        let timer = Instant::now();
        self.dirty.clear();
        let cap = self.dirty.capacity();
        self.dirty.shrink_to(cap / 2);
        if let Some(aha) = &mut self.aha {
            aha.commit();
        }
        #[cfg(feature = "stats")]
        {
            self.stats.node_commit += timer.elapsed().as_secs_f64();
        }
    }

    pub fn flush(&mut self) {
        if let Some(aha) = &mut self.aha {
            aha.flush();
        }
        self.backend.flush();
    }

    // ===== node operations =====
    pub fn load_children_hash(&mut self, node: &mut Node) {
        #[cfg(feature = "stats")]
        let timer = Instant::now();
        match &mut node.get_inner_mut() {
            NodeType::Branch(bnode) => {
                for i in 0..NBRANCH + 1 {
                    if let Some(Child::Ptr(NodePtr::Clean(cptr))) = &bnode.children[i] {
                        let node = self.get_clean(*cptr);
                        let h = node.hash();
                        bnode.children[i] = Some(Child::Hash(*cptr, h));
                    }
                }
            }
            NodeType::Short(snode) => {
                if let Child::Ptr(NodePtr::Clean(cptr)) = snode.child.clone() {
                    let node = self.get_clean(cptr);
                    let h = node.hash();
                    snode.child = Child::Hash(cptr, h);
                }
            }
            NodeType::Value(_) => {}
        }
        #[cfg(feature = "stats")]
        {
            self.stats.t_hash_load += timer.elapsed().as_secs_f64();
        }
    }

    pub fn load_aha(&mut self, node: &mut Node) {
        #[cfg(feature = "stats")]
        let timer = Instant::now();
        if let Some(aha) = &mut self.aha {
            if let NodeType::Branch(bnode) = node.get_inner_mut() {
                let cnt_needed = bnode
                    .children
                    .iter()
                    .filter(|c| matches!(c, None | Some(Child::Ptr(NodePtr::Clean(_)))))
                    .count();
                if bnode.aha_len > 0 && cnt_needed > 0 {
                    let mut hashs = aha.read_aha(bnode.aha_len, bnode.aha_ptr);
                    assert!(hashs.len() == bnode.aha_len as usize);
                    let mut validate_bnode = bnode.clone();

                    for i in 0..NBRANCH + 1 {
                        if let Some(Child::Ptr(NodePtr::Clean(cptr))) = &validate_bnode.children[i]
                        {
                            let h = hashs.remove(0);
                            validate_bnode.children[i] = Some(Child::Hash(*cptr, h));
                        } else if let Some(Child::Hash(_, _)) = &validate_bnode.children[i] {
                            //panic!("child is already loaded");
                            let _ = hashs.remove(0);
                        }
                    }
                    assert!(hashs.is_empty());
                    // validate the children hashes are valid
                    if bnode.hash == validate_bnode.calc_hash().unwrap() {
                        bnode.children = validate_bnode.children.clone();
                        #[cfg(feature = "stats")]
                        {
                            self.stats.aha_hit += 1;
                            self.stats.t_hash_load += timer.elapsed().as_secs_f64();
                        }
                        return;
                    }
                    // if validation failed, fallback to load children hash from backend
                    #[cfg(feature = "stats")]
                    {
                        self.stats.aha_miss += 1;
                    }
                }
            }
        }
        #[cfg(feature = "stats")]
        {
            self.stats.t_hash_load += timer.elapsed().as_secs_f64();
        }
    }

    pub fn write_aha(&mut self, node: &mut Node) {
        if let Some(aha) = &mut self.aha {
            if let NodeType::Branch(bnode) = node.get_inner_mut() {
                let mut hashs = Vec::new();
                for i in 0..NBRANCH + 1 {
                    if let Some(Child::Hash(_, h)) = &bnode.children[i] {
                        hashs.push(h.clone());
                    } else if let Some(Child::Ptr(NodePtr::Clean(_))) = &bnode.children[i] {
                        panic!("child is not loaded");
                    }
                }
                let old_len = bnode.aha_len;
                let old_ptr = bnode.aha_ptr;
                bnode.aha_len = hashs.len() as u8;
                #[cfg(feature = "stats")]
                let write_timer = Instant::now();
                bnode.aha_ptr = aha.write_aha(hashs, old_len, old_ptr);
                #[cfg(feature = "stats")]
                {
                    self.stats.t_aha_write += write_timer.elapsed().as_secs_f64();
                }
            }
        }
    }

    #[cfg(feature = "stats")]
    pub fn print_stats(&mut self) {
        self.stats.cache_size = self.clean.current_size();
        self.stats.print_stats();
        self.stats.reset();
        if let Some(aha) = &mut self.aha {
            aha.print_stats();
        }
        println!("[store backend]");
        self.backend.print_stats();
    }
}
