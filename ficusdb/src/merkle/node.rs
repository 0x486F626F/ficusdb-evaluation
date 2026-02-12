#![allow(dead_code)]

use super::utils;
use super::{CleanPtr, DirtyPtr, NBRANCH};

use lru_mem::HeapSize;
use sha3::{Digest, Keccak256};
use std::io::{Error, ErrorKind};
use std::mem::size_of;

const HASH_SIZE: usize = 32;

const BRANCH_NODE_TYPE: u8 = 0x0;
const SHORT_NODE_TYPE: u8 = 0x1;
const VALUE_NODE_TYPE: u8 = 0x2;

#[derive(Copy, Clone)]
pub enum NodePtr {
    // on-disk
    Clean(CleanPtr),
    // in-memory
    Dirty(DirtyPtr),
}

#[derive(Clone)]
pub enum Child {
    Ptr(NodePtr),
    Hash(CleanPtr, Vec<u8>),
}

#[derive(Clone)]
pub struct Value {
    pub value: Vec<u8>,
    pub extra: Vec<u8>,
}

#[derive(Clone)]
pub struct Branch {
    pub hash: Vec<u8>,
    pub children: [Option<Child>; NBRANCH + 1],
    pub aha_len: u8,
    pub aha_ptr: CleanPtr,
}

#[derive(Clone)]
pub struct Short {
    pub hash: Vec<u8>,
    pub path: Vec<u8>,
    pub child: Child,
}

#[derive(Clone)]
pub enum NodeType {
    Branch(Branch),
    Short(Short),
    Value(Value),
}

#[derive(Clone)]
pub struct Node(pub NodeType);

//=============== Implementations ===============

impl Node {
    pub fn get_inner(&self) -> &NodeType {
        &self.0
    }

    pub fn get_inner_mut(&mut self) -> &mut NodeType {
        &mut self.0
    }

    /// Trie reference item for embedding into a parent:
    /// - Value: RLP(value_bytes)
    /// - Branch/Short: stored reference item (raw RLP if <32, else RLP(hash))
    pub fn hash(&self) -> Vec<u8> {
        match &self.0 {
            NodeType::Value(v) => rlp::encode(&v.value).to_vec(),
            NodeType::Branch(b) => b.hash.clone(),
            NodeType::Short(s) => s.hash.clone(),
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        rlp::encode(&self.0).to_vec()
    }

    pub fn decode(data: &[u8]) -> Result<Self, Error> {
        if let Ok(inner) = rlp::decode::<NodeType>(data) {
            Ok(Self(inner))
        } else {
            Err(Error::new(ErrorKind::Other, "Invalid RLP"))
        }
    }

    /// Canonical trie RLP used for hashing.
    pub fn rlp_encode(&self) -> Result<Vec<u8>, Error> {
        match &self.0 {
            NodeType::Branch(b) => b.rlp_encode(),
            NodeType::Short(s) => s.rlp_encode(),
            NodeType::Value(v) => Ok(rlp::encode(&v.value).to_vec()),
        }
    }

    /// Calculates and stores the trie reference item for this node.
    pub fn calc_hash(&mut self) -> Result<Vec<u8>, Error> {
        match &mut self.0 {
            NodeType::Value(v) => Ok(rlp::encode(&v.value).to_vec()),
            NodeType::Branch(b) => b.calc_hash(),
            NodeType::Short(s) => s.calc_hash(),
        }
    }
}

impl Value {
    pub fn new(value: Vec<u8>, extra: Vec<u8>) -> Self {
        Self { value, extra }
    }
}

impl Branch {
    pub fn new() -> Self {
        Self {
            hash: Vec::new(),
            children: std::array::from_fn(|_| None),
            aha_len: 0,
            aha_ptr: 0,
        }
    }

    pub fn rlp_encode(&self) -> Result<Vec<u8>, Error> {
        let mut encoder = rlp::RlpStream::new_list(NBRANCH + 1);
        for i in 0..NBRANCH + 1 {
            match &self.children[i] {
                None => encoder.append_empty_data(),
                // `h` is already the RLP encoding of the child reference item.
                Some(Child::Hash(_, h)) => encoder.append_raw(h, 1),
                _ => return Err(Error::new(ErrorKind::Other, "Child hash is not loaded")),
            };
        }
        Ok(encoder.out().to_vec())
    }

    pub fn calc_hash(&mut self) -> Result<Vec<u8>, Error> {
        let raw = self.rlp_encode()?;
        let out = if raw.len() < HASH_SIZE {
            raw
        } else {
            let hash = Keccak256::digest(&raw);
            rlp::encode(&hash.as_slice()).to_vec()
        };
        self.hash = out.clone();
        Ok(out)
    }
}

impl Short {
    pub fn new(path: Vec<u8>, child: Child) -> Self {
        Self {
            hash: Vec::new(),
            path,
            child,
        }
    }

    pub fn common_prefix_len(&self, nibbles: &[u8]) -> usize {
        let len = std::cmp::min(self.path.len(), nibbles.len());
        let mut matched = 0;
        while matched < len && nibbles[matched] == self.path[matched] {
            matched += 1;
        }
        matched
    }

    pub fn rlp_encode(&self) -> Result<Vec<u8>, Error> {
        let compact = utils::to_compact(&self.path);
        let mut s = rlp::RlpStream::new_list(2);
        match &self.child {
            Child::Hash(_, h) => {
                s.append(&compact);
                // `h` is already the RLP encoding of the child reference item.
                s.append_raw(h, 1);
            }
            _ => return Err(Error::new(ErrorKind::Other, "Child hash is not loaded")),
        }
        Ok(s.out().to_vec())
    }

    pub fn calc_hash(&mut self) -> Result<Vec<u8>, Error> {
        let raw = self.rlp_encode()?;
        let out = if raw.len() < HASH_SIZE {
            raw
        } else {
            let hash = Keccak256::digest(&raw);
            rlp::encode(&hash.as_slice()).to_vec()
        };
        self.hash = out.clone();
        Ok(out)
    }
}

// Provide a conservative heap size estimate for LRU admission/eviction.
impl HeapSize for Value {
    fn heap_size(&self) -> usize {
        self.value.len() + self.extra.len()
    }
}

impl HeapSize for Branch {
    fn heap_size(&self) -> usize {
        let mut sz = self.hash.len();
        sz += size_of::<u8>();
        sz += size_of::<CleanPtr>();
        for child in &self.children {
            sz += child.heap_size();
        }
        sz
    }
}

impl HeapSize for Short {
    fn heap_size(&self) -> usize {
        self.hash.len() + self.path.len() + self.child.heap_size()
    }
}

impl HeapSize for Child {
    fn heap_size(&self) -> usize {
        match self {
            Child::Ptr(_) => 0,
            Child::Hash(_, h) => h.len(),
        }
    }
}

impl HeapSize for Node {
    fn heap_size(&self) -> usize {
        match &self.0 {
            NodeType::Branch(b) => b.heap_size(),
            NodeType::Short(s) => s.heap_size(),
            NodeType::Value(v) => v.heap_size(),
        }
    }
}

//=============== Encode/Decode ===============

use rlp::{Decodable, DecoderError, Encodable, Rlp, RlpStream};

impl Encodable for Value {
    fn rlp_append(&self, s: &mut RlpStream) {
        s.begin_list(2)
            .append_list(&self.value)
            .append_list(&self.extra);
    }
}

impl Decodable for Value {
    fn decode(s: &Rlp) -> Result<Self, DecoderError> {
        Ok(Self {
            value: s.list_at(0)?,
            extra: s.list_at(1)?,
        })
    }
}

impl Encodable for Short {
    fn rlp_append(&self, s: &mut RlpStream) {
        s.begin_list(3)
            .append_list(&self.hash)
            .append_list(&self.path)
            .append(&self.child);
    }
}

impl Decodable for Short {
    fn decode(s: &Rlp) -> Result<Self, DecoderError> {
        Ok(Self {
            hash: s.list_at(0)?,
            path: s.list_at(1)?,
            child: s.val_at(2)?,
        })
    }
}

impl Encodable for Branch {
    fn rlp_append(&self, s: &mut RlpStream) {
        s.begin_list(4)
            .append_list(&self.hash)
            .append_list(&self.children)
            .append(&self.aha_len)
            .append(&self.aha_ptr);
    }
}

impl Decodable for Branch {
    fn decode(s: &Rlp) -> Result<Self, DecoderError> {
        let hash = s.list_at(0)?;
        let children: [Option<Child>; NBRANCH + 1] = s.list_at(1)?.try_into().unwrap_or_default();
        let aha_len = s.val_at(2)?;
        let aha_ptr = s.val_at(3)?;
        Ok(Self {
            hash,
            children,
            aha_len,
            aha_ptr,
        })
    }
}

impl Encodable for Child {
    fn rlp_append(&self, s: &mut RlpStream) {
        match self {
            Child::Ptr(NodePtr::Clean(p)) => s.append(p),
            Child::Hash(p, _) => s.append(p),
            _ => unreachable!(),
        };
    }
}

impl Decodable for Child {
    fn decode(s: &Rlp) -> Result<Self, DecoderError> {
        Ok(Child::Ptr(NodePtr::Clean(s.as_val()?)))
    }
}

impl Encodable for NodeType {
    fn rlp_append(&self, s: &mut RlpStream) {
        match self {
            NodeType::Branch(branch) => s.begin_list(2).append(&BRANCH_NODE_TYPE).append(branch),
            NodeType::Short(short) => s.begin_list(2).append(&SHORT_NODE_TYPE).append(short),
            NodeType::Value(value) => s.begin_list(2).append(&VALUE_NODE_TYPE).append(value),
        };
    }
}

impl Decodable for NodeType {
    fn decode(s: &Rlp) -> Result<Self, DecoderError> {
        let node_type: u8 = s.val_at(0)?;
        Ok(match node_type {
            BRANCH_NODE_TYPE => NodeType::Branch(s.val_at(1)?),
            SHORT_NODE_TYPE => NodeType::Short(s.val_at(1)?),
            VALUE_NODE_TYPE => NodeType::Value(s.val_at(1)?),
            _ => unreachable!(),
        })
    }
}
