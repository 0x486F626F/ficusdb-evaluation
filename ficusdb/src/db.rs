#![allow(dead_code)]

use crate::backend::PageCachedFile;
use crate::merkle::{AggregatedHashArray, Backend, CleanPtr, Merkle, NodeStore, Value};
use lru_mem::LruCache;
use std::collections::HashMap;
use std::mem::size_of;
use std::sync::{Arc, Mutex};
use typed_builder::TypedBuilder;

#[derive(TypedBuilder)]
pub struct DBConfig {
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
    pub db_value_cache_size: usize,
}

pub struct DB {
    node_store: Arc<Mutex<NodeStore>>,
    merkle: Arc<Mutex<Merkle>>,
    root_file: Arc<Mutex<PageCachedFile>>,
    db_value_cache: Option<Arc<Mutex<LruCache<Vec<u8>, Option<Vec<u8>>>>>>,
}

impl DB {
    pub fn open(path: &str, cfg: DBConfig) -> Self {
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
        let mut root_file = PageCachedFile::new(&root_path, cfg.aha_cache_size);
        let root_cptr = if root_file.tail() as u64 >= size_of::<CleanPtr>() as u64 {
            let buf = root_file.read(
                root_file.tail() - size_of::<CleanPtr>() as u64,
                size_of::<CleanPtr>(),
            );
            CleanPtr::from_le_bytes(buf.try_into().unwrap())
        } else {
            0
        };
        let merkle = Merkle::new(node_store.clone(), root_cptr);
        Self {
            node_store,
            merkle: Arc::new(Mutex::new(merkle)),
            root_file: Arc::new(Mutex::new(root_file)),
            db_value_cache: if cfg.db_value_cache_size > 0 {
                Some(Arc::new(Mutex::new(LruCache::new(cfg.db_value_cache_size))))
            } else {
                None
            },
        }
    }

    pub fn open_root(&mut self, root_cptr: CleanPtr) {
        if self.merkle.lock().unwrap().root_cptr() == root_cptr {
            return;
        }
        *self.merkle.lock().unwrap() = Merkle::new(self.node_store.clone(), root_cptr);
        // Prevent stale values from a different root snapshot.
        if let Some(cache) = &self.db_value_cache {
            cache.lock().unwrap().clear();
        }
    }

    pub fn hash(&self) -> Vec<u8> {
        self.merkle.lock().unwrap().hash()
    }

    pub fn get(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        if let Some(cache) = &self.db_value_cache {
            let mut cache = cache.lock().unwrap();
            if let Some(v) = cache.get(key) {
                return v.clone();
            }

            let computed = self.merkle.lock().unwrap().find(key).map(|v| v.value);
            let _ = cache.insert(key.to_vec(), computed.clone());
            return computed;
        }

        self.merkle.lock().unwrap().find(key).map(|v| v.value)
    }

    pub fn new_writebatch(&self) -> WriteBatch {
        WriteBatch {
            merkle: self.merkle.clone(),
            staging: HashMap::new(),
            root_file: self.root_file.clone(),
            node_store: self.node_store.clone(),
            committed: false,
            db_value_cache: if let Some(cache) = &self.db_value_cache {
                Some(cache.clone())
            } else {
                None
            },
        }
    }

    pub fn flush(&mut self) {
        self.root_file.lock().unwrap().flush();
        self.node_store.lock().unwrap().flush();
    }

    #[cfg(feature = "stats")]
    pub fn print_stats(&self) {
        self.merkle.lock().unwrap().print_stats();
    }
}

impl Drop for DB {
    fn drop(&mut self) {
        self.flush();
    }
}

pub struct WriteBatch {
    merkle: Arc<Mutex<Merkle>>,
    staging: HashMap<Vec<u8>, Vec<u8>>,
    root_file: Arc<Mutex<PageCachedFile>>,
    node_store: Arc<Mutex<NodeStore>>,
    db_value_cache: Option<Arc<Mutex<LruCache<Vec<u8>, Option<Vec<u8>>>>>>,
    committed: bool,
}

impl WriteBatch {
    pub fn insert(&mut self, key: &[u8], value: &[u8]) {
        self.staging.insert(key.to_vec(), value.to_vec());
    }

    pub fn commit(&mut self) -> CleanPtr {
        let root_cptr = {
            let mut merkle = self.merkle.lock().unwrap();
            if let Some(cache) = &self.db_value_cache {
                let mut cache = cache.lock().unwrap();
                for (key, value) in self.staging.drain() {
                    merkle.insert(&key, Value::new(value.clone(), Vec::new()));
                    let _ = cache.insert(key, Some(value));
                }
            } else {
                for (key, value) in self.staging.drain() {
                    merkle.insert(&key, Value::new(value.clone(), Vec::new()));
                }
            }
            merkle.commit()
        };

        // Ensure node bytes are durable before publishing the new root pointer.
        self.node_store.lock().unwrap().flush();

        let mut root_file = self.root_file.lock().unwrap();
        let tail = root_file.tail() as u64;
        root_file.write(tail, &root_cptr.to_le_bytes());
        root_file.flush();

        self.committed = true;
        root_cptr
    }
}
