use crate::merkle::CleanPtr;
use crate::merkle::backend::Backend;

pub struct MemStore {
    data: Vec<u8>,
}

impl MemStore {
    pub fn new() -> Self {
        Self { data: Vec::new() }
    }

    pub fn tail(&self) -> usize {
        self.data.len()
    }

    pub fn read(&mut self, ptr: usize, len: usize) -> Vec<u8> {
        self.data[ptr..ptr + len].to_vec()
    }

    pub fn write(&mut self, ptr: usize, data: &[u8]) {
        self.data.resize(ptr + data.len(), 0);
        self.data[ptr..ptr + data.len()].copy_from_slice(data);
    }

    pub fn flush(&mut self) {
        // no-op
    }

    #[cfg(feature = "stats")]
    pub fn print_stats(&mut self) {
        // no-op
    }
}

impl Backend for MemStore {
    fn tail(&self) -> CleanPtr {
        MemStore::tail(self) as CleanPtr
    }

    fn read(&mut self, ptr: CleanPtr, len: usize) -> Vec<u8> {
        MemStore::read(self, ptr as usize, len)
    }

    fn write(&mut self, ptr: CleanPtr, data: &[u8]) {
        MemStore::write(self, ptr as usize, data);
    }

    fn flush(&mut self) {
        MemStore::flush(self);
    }

    #[cfg(feature = "stats")]
    fn print_stats(&mut self) {
        MemStore::print_stats(self);
    }
}
