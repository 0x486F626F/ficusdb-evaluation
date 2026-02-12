mod backend;
mod db;
mod merkle;
mod statedb;
#[cfg(feature = "stats")]
mod stats;

pub use db::{DB, DBConfig, WriteBatch};
pub use statedb::{StateDB, StateDBConfig};

use crate::backend::PageCachedFile;
use crate::merkle::CleanPtr;

impl merkle::Backend for PageCachedFile {
    fn tail(&self) -> CleanPtr {
        PageCachedFile::tail(self) as CleanPtr
    }

    fn read(&mut self, ptr: CleanPtr, len: usize) -> Vec<u8> {
        PageCachedFile::read(self, ptr as u64, len)
    }

    fn write(&mut self, ptr: CleanPtr, data: &[u8]) {
        PageCachedFile::write(self, ptr as u64, data);
    }

    fn flush(&mut self) {
        PageCachedFile::flush(self);
    }

    #[cfg(feature = "stats")]
    fn print_stats(&mut self) {
        PageCachedFile::print_stats(self);
    }
}
