#![allow(dead_code)]
use crate::backend::PageCachedFile;
use crate::merkle::{AggregatedHashArray, Backend, CleanPtr, Merkle, NodeStore, Value};
use lru_mem::{HeapSize, LruCache};
use num_bigint::BigUint;
use rlp::{Decodable, DecoderError, Encodable, Rlp, RlpStream};
use sha3::{Digest, Keccak256};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use typed_builder::TypedBuilder;

#[cfg(feature = "stats")]
use crate::stats::StateDBStats;
#[cfg(feature = "stats")]
use std::time::Instant;

#[derive(TypedBuilder)]
pub struct StateDBConfig {
    #[builder(default = false)]
    pub truncate: bool,
    #[builder(default = 4096 * 1024 * 1024)]
    pub cache_size: usize,
    #[builder(default = 64 * 1024 * 1024)]
    pub page_cache_size: usize,
    #[builder(default = 16 * 1024 * 1024)]
    pub aha_cache_size: usize,
    #[builder(default = vec![4, 8, 12, 16])]
    pub aha_lens: Vec<u8>,
    #[builder(default = 16 * 1024 * 1024)]
    pub obj_cache_size: usize,
}

#[derive(Clone)]
struct Account {
    nonce: u64,
    balance: BigUint,
    roothash: Vec<u8>,
    codehash: Vec<u8>,
}

impl HeapSize for Account {
    fn heap_size(&self) -> usize {
        self.nonce.heap_size() + 32 + self.roothash.heap_size() + self.codehash.heap_size()
    }
}

impl Account {
    fn new() -> Self {
        Self {
            nonce: 0,
            balance: BigUint::from_bytes_be(&[0]),
            roothash: Keccak256::digest(&[0x80u8]).to_vec(),
            codehash: Keccak256::digest(b"").to_vec(),
        }
    }
}

impl Encodable for Account {
    fn rlp_append(&self, s: &mut RlpStream) {
        let balance = if self.balance > BigUint::from_bytes_be(&[0]) {
            self.balance.to_bytes_be()
        } else {
            Vec::new()
        };
        s.begin_list(4)
            .append(&self.nonce)
            .append(&balance)
            .append(&self.roothash.to_vec())
            .append(&self.codehash.to_vec());
    }
}

impl Decodable for Account {
    fn decode(s: &Rlp) -> Result<Self, DecoderError> {
        let balance: Vec<u8> = s.val_at(1)?;
        Ok(Self {
            nonce: s.val_at(0)?,
            balance: BigUint::from_bytes_be(&balance),
            roothash: s.val_at::<Vec<u8>>(2)?.try_into().unwrap(),
            codehash: s.val_at::<Vec<u8>>(3)?.try_into().unwrap(),
        })
    }
}

#[derive(Clone)]
struct StateObject {
    account: Account,
    rootptr: CleanPtr,
    state_dirty: HashMap<Vec<u8>, Vec<u8>>,
    deleted: bool,
}

impl StateObject {
    fn new(account: Account, rootptr: CleanPtr) -> Self {
        Self {
            account,
            rootptr,
            state_dirty: HashMap::new(),
            deleted: false,
        }
    }

    fn set_state(&mut self, key: &[u8], val: &[u8]) {
        self.state_dirty.insert(key.to_vec(), val.to_vec());
    }
}

impl HeapSize for StateObject {
    fn heap_size(&self) -> usize {
        self.account.heap_size() + self.rootptr.heap_size() + self.deleted.heap_size()
    }
}

struct StateDBRoots {
    roots: LruCache<Vec<u8>, CleanPtr>,
    root_file: PageCachedFile,
    cur_cptr: u64,
}

impl StateDBRoots {
    fn new(mut root_file: PageCachedFile, cache_size: usize) -> (Self, CleanPtr) {
        let mut roots = LruCache::new(cache_size);
        let (latest, cur_cptr) = if root_file.tail() < 40 {
            (0, 0)
        } else {
            let buf = root_file.read(root_file.tail() - 40, 40);
            let hash = buf[..32].to_vec();
            let cptr = CleanPtr::from_le_bytes(buf[32..40].try_into().unwrap());
            let _ = roots.insert(hash.clone(), cptr);
            (cptr, root_file.tail() - 40)
        };
        (
            Self {
                roots,
                root_file,
                cur_cptr,
            },
            latest,
        )
    }

    fn get_root_ptr(&mut self, root_hash: &Vec<u8>) -> Option<CleanPtr> {
        if !self.roots.contains(root_hash) {
            while self.cur_cptr > 0 {
                self.cur_cptr -= 40;
                let buf = self.root_file.read(self.cur_cptr, 40);
                let hash = buf[..32].to_vec();
                let cptr = CleanPtr::from_le_bytes(buf[32..40].try_into().unwrap());
                let _ = self.roots.insert(hash.clone(), cptr);
                if hash == *root_hash {
                    break;
                }
            }
        }
        if self.cur_cptr == 0 {
            self.cur_cptr = self.root_file.tail();
        }
        self.roots.get(root_hash).cloned()
    }

    fn add_root_ptr(&mut self, root_hash: Vec<u8>, cptr: CleanPtr) {
        let mut buf = root_hash.clone();
        buf.resize(32, 0);
        buf.extend(&cptr.to_le_bytes());
        let file_tail = self.root_file.tail();
        self.root_file.write(file_tail, &buf);
        self.root_file.flush();
        let _ = self.roots.insert(root_hash.clone(), cptr);
    }
}

pub struct StateDB {
    roots: StateDBRoots,
    store: Arc<Mutex<NodeStore>>,
    merkle: Arc<Mutex<Merkle>>,

    obj_clean: LruCache<Vec<u8>, StateObject>,
    obj_dirty: HashMap<Vec<u8>, StateObject>,
    state_clean: LruCache<Vec<u8>, Vec<u8>>,
    deltas: Vec<HashMap<Vec<u8>, Option<StateObject>>>,
    #[cfg(feature = "stats")]
    stats: Arc<Mutex<StateDBStats>>,
}

impl StateDB {
    pub fn open(path: &str, cfg: StateDBConfig) -> Self {
        if cfg.truncate {
            let _ = std::fs::remove_file(path);
        }
        let _ = std::fs::create_dir_all(path);
        let node_path = format!("{}/node", path);
        let node_file = PageCachedFile::new(&node_path, cfg.page_cache_size);
        let aha = if cfg.aha_lens.is_empty() {
            None
        } else {
            let mut ahas: Vec<(u8, Box<dyn Backend>)> = Vec::new();
            for len in cfg.aha_lens {
                let aha_path = format!("{}/aha_{}", path, len);
                let aha_file = PageCachedFile::new(&aha_path, cfg.aha_cache_size);
                ahas.push((len, Box::new(aha_file)));
            }
            Some(AggregatedHashArray::new(ahas))
        };
        let node_store = Arc::new(Mutex::new(NodeStore::new(
            Box::new(node_file),
            cfg.cache_size,
            aha,
        )));

        let root_path = format!("{}/root", path);
        let root_file = PageCachedFile::new(&root_path, cfg.aha_cache_size);
        let (roots, root_cptr) = StateDBRoots::new(root_file, cfg.aha_cache_size / 1024);
        let merkle = Merkle::new(node_store.clone(), root_cptr);
        let obj_clean = LruCache::new(cfg.obj_cache_size);
        let obj_dirty = HashMap::new();
        let state_clean = LruCache::new(cfg.obj_cache_size);
        let deltas = Vec::new();
        Self {
            roots,
            store: node_store,
            merkle: Arc::new(Mutex::new(merkle)),
            obj_clean,
            obj_dirty,
            state_clean,
            deltas,
            #[cfg(feature = "stats")]
            stats: Arc::new(Mutex::new(StateDBStats::new())),
        }
    }

    /// Switch the StateDB view to a different committed root pointer.
    pub fn open_root(&mut self, root: CleanPtr) {
        if self.merkle.lock().unwrap().root_cptr() == root {
            return;
        }
        *self.merkle.lock().unwrap() = Merkle::new(self.store.clone(), root);
        self.obj_clean.clear();
        self.obj_dirty.clear();
        self.state_clean.clear();
        self.deltas.clear();
    }

    pub fn open_root_hash(&mut self, root_hash: &Vec<u8>) {
        if *root_hash == self.hash() {
            return;
        }
        if let Some(cptr) = self.roots.get_root_ptr(root_hash) {
            self.open_root(cptr);
        }
    }

    fn get_obj(&mut self, addr: &[u8]) -> Option<&StateObject> {
        match self.obj_dirty.get(addr) {
            Some(obj) => Some(obj),
            None => {
                if !self.obj_clean.contains(addr) {
                    let merkle = self.merkle.lock().unwrap();
                    if let Some(val) = merkle.find(addr) {
                        let _ = self.obj_clean.insert(
                            addr.to_vec(),
                            StateObject::new(
                                rlp::decode(&val.value).unwrap(),
                                rlp::decode(&val.extra).unwrap(),
                            ),
                        );
                    }
                }
                self.obj_clean.get(addr)
            }
        }
    }

    fn ensure_dirty_obj(&mut self, addr: &[u8]) -> &mut StateObject {
        if !self.obj_dirty.contains_key(addr) {
            let obj = match self.obj_clean.remove(addr) {
                Some(obj) => Some(obj),
                None => {
                    let merkle = self.merkle.lock().unwrap();
                    if let Some(val) = merkle.find(addr) {
                        Some(StateObject::new(
                            rlp::decode(&val.value).unwrap(),
                            rlp::decode(&val.extra).unwrap(),
                        ))
                    } else {
                        None
                    }
                }
            };
            match obj {
                Some(obj) => {
                    if let Some(delta) = self.deltas.last_mut() {
                        delta.entry(addr.to_vec()).or_insert(Some(obj.clone()));
                    }
                    self.obj_dirty.insert(addr.to_vec(), obj);
                }
                None => {
                    if let Some(delta) = self.deltas.last_mut() {
                        delta.entry(addr.to_vec()).or_insert(None);
                    }
                    self.obj_dirty
                        .insert(addr.to_vec(), StateObject::new(Account::new(), 0));
                }
            }
        }
        let obj = self.obj_dirty.get_mut(addr).unwrap();
        if let Some(delta) = self.deltas.last_mut() {
            delta.entry(addr.to_vec()).or_insert(Some(obj.clone()));
        }
        obj
    }

    pub fn add_balance(&mut self, addr: &[u8], amount: BigUint) {
        let obj = self.ensure_dirty_obj(addr);
        obj.account.balance += amount;
    }

    pub fn sub_balance(&mut self, addr: &[u8], amount: BigUint) {
        let obj = self.ensure_dirty_obj(addr);
        if amount <= obj.account.balance {
            obj.account.balance -= amount;
        }
    }

    pub fn get_balance(&mut self, addr: &[u8]) -> BigUint {
        match self.get_obj(addr) {
            Some(obj) => obj.account.balance.clone(),
            None => BigUint::from_bytes_be(&[0]),
        }
    }

    pub fn set_nonce(&mut self, addr: &[u8], nonce: u64) {
        let obj = self.ensure_dirty_obj(addr);
        obj.account.nonce = nonce;
    }

    pub fn get_nonce(&mut self, addr: &[u8]) -> u64 {
        match self.get_obj(addr) {
            Some(obj) => obj.account.nonce,
            None => 0,
        }
    }

    pub fn set_codehash(&mut self, addr: &[u8], codehash: Vec<u8>) {
        let obj = self.ensure_dirty_obj(addr);
        obj.account.codehash = codehash;
    }

    pub fn get_codehash(&mut self, addr: &[u8]) -> Vec<u8> {
        match self.get_obj(addr) {
            Some(obj) => obj.account.codehash.clone(),
            None => Vec::new(),
        }
    }

    pub fn set_state(&mut self, addr: &[u8], key: &[u8], val: &[u8]) {
        let obj = self.ensure_dirty_obj(addr);
        obj.set_state(key, val);
    }

    pub fn get_state(&mut self, addr: &[u8], key: &[u8]) -> Vec<u8> {
        let ckey = [addr, key].concat();
        if !self.state_clean.contains(&ckey) {
            let rootptr = if let Some(obj) = self.get_obj(addr) {
                obj.rootptr
            } else {
                return Vec::new();
            };
            let subtree = Merkle::new(self.store.clone(), rootptr);
            let val = subtree.find(key).map(|v| v.value).unwrap_or_default();
            let _ = self.state_clean.insert(ckey.to_vec(), val);
        }
        self.state_clean.get(&ckey).unwrap().to_vec()
    }

    pub fn create_account(&mut self, addr: &[u8]) {
        self.ensure_dirty_obj(addr);
        let obj = self.obj_dirty.get_mut(addr).unwrap();
        obj.account = Account::new();
        obj.state_dirty.clear();
        obj.deleted = false;
    }

    pub fn remove_account(&mut self, addr: &[u8]) {
        if let Some(mut obj) = self.obj_dirty.remove(addr) {
            obj.deleted = true;
            obj.account.balance = BigUint::from_bytes_be(&[0]);
            self.obj_dirty.insert(addr.to_vec(), obj);
            return;
        }
        if let Some(mut obj) = self.obj_clean.remove(addr) {
            obj.deleted = true;
            obj.account.balance = BigUint::from_bytes_be(&[0]);
            self.obj_dirty.insert(addr.to_vec(), obj);
            return;
        }
    }

    pub fn snapshot(&mut self) -> usize {
        self.deltas.push(HashMap::new());
        self.deltas.len() - 1
    }

    pub fn revert(&mut self, sid: usize) {
        for idx in (sid..self.deltas.len()).rev() {
            for (addr, obj) in self.deltas[idx].drain() {
                match obj {
                    Some(o) => {
                        self.obj_dirty.insert(addr, o);
                    }
                    None => {
                        self.obj_dirty.remove(&addr);
                    }
                };
            }
        }
    }

    pub fn commit(&mut self) -> CleanPtr {
        #[cfg(feature = "stats")]
        let timer = Instant::now();
        let mut merkle = self.merkle.lock().unwrap();
        for (addr, obj) in &mut self.obj_dirty {
            if obj.state_dirty.len() > 0 && !obj.deleted {
                #[cfg(feature = "stats")]
                let merkle_write_timer = Instant::now();
                let mut subtree = Merkle::new(self.store.clone(), obj.rootptr);
                for (key, val) in obj.state_dirty.drain() {
                    let mut ckey = addr.to_vec();
                    ckey.extend(&key.to_vec());
                    if val.len() > 0 {
                        // Ethereum storage trie stores RLP(value_bytes) as the leaf value.
                        let enc = rlp::encode(&val).to_vec();
                        let _ = self.state_clean.insert(ckey, enc.clone());
                        subtree.insert(&key, Value::new(enc, Vec::new()));
                    } else {
                        self.state_clean.remove(&ckey);
                        subtree.delete(&key);
                    }
                }
                #[cfg(feature = "stats")]
                {
                    let mut stats = self.stats.lock().unwrap();
                    stats.t_merkle_write += merkle_write_timer.elapsed().as_secs_f64();
                }
                #[cfg(feature = "stats")]
                let merkle_timer = Instant::now();
                let cptr = subtree.commit();
                #[cfg(feature = "stats")]
                {
                    let mut stats = self.stats.lock().unwrap();
                    stats.t_merkle_commit += merkle_timer.elapsed().as_secs_f64();
                }
                obj.rootptr = cptr;
                let h = subtree.hash();
                obj.account.roothash = h.as_slice().try_into().unwrap();
            }
        }

        #[cfg(feature = "stats")]
        let merkle_write_timer = Instant::now();
        for (addr, obj) in self.obj_dirty.drain() {
            if obj.deleted {
                merkle.delete(&addr);
            } else {
                let value = Value {
                    value: rlp::encode(&obj.account).to_vec(),
                    extra: rlp::encode(&obj.rootptr).to_vec(),
                };
                merkle.insert(&addr, value);
                assert!(obj.state_dirty.len() == 0);
                let _ = self.obj_clean.insert(addr, obj);
            }
        }
        #[cfg(feature = "stats")]
        {
            let mut stats = self.stats.lock().unwrap();
            stats.t_merkle_write += merkle_write_timer.elapsed().as_secs_f64();
        }
        #[cfg(feature = "stats")]
        let merkle_timer = Instant::now();
        let cptr = merkle.commit();
        #[cfg(feature = "stats")]
        {
            let mut stats = self.stats.lock().unwrap();
            stats.t_merkle_commit += merkle_timer.elapsed().as_secs_f64();
        }
        self.deltas.clear();
        self.roots.add_root_ptr(merkle.hash(), cptr);
        self.store.lock().unwrap().flush();
        #[cfg(feature = "stats")]
        {
            let mut stats = self.stats.lock().unwrap();
            stats.t_commit += timer.elapsed().as_secs_f64();
        }
        cptr
    }

    pub fn finalise(&mut self) {
        self.deltas.clear();
    }

    pub fn hash(&self) -> Vec<u8> {
        self.merkle
            .lock()
            .unwrap()
            .hash()
            .as_slice()
            .try_into()
            .unwrap()
    }

    #[cfg(feature = "stats")]
    pub fn print_stats(&self) {
        let mut stats = self.stats.lock().unwrap();
        stats.print_stats();
        stats.reset();
        self.merkle.lock().unwrap().print_stats();
    }
}

impl Drop for StateDB {
    fn drop(&mut self) {
        self.store.lock().unwrap().flush();
    }
}
