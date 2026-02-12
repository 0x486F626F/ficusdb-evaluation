#![allow(dead_code)]
use super::{PAGE_BITS, PAGE_SIZE};

use lru::LruCache;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::num::NonZeroUsize;
use std::os::unix::fs::FileExt;

type Page = [u8; PAGE_SIZE];

pub struct PageCachedFile {
    file: File,
    file_tail: u64,
    buff_tail: u64,
    clean: LruCache<u64, Page>,
    dirty: HashMap<u64, Page>,
    #[cfg(feature = "stats")]
    stats: PageCachedFileStats,
}

impl PageCachedFile {
    pub fn new(path: &str, cache_size: usize) -> Self {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
            .unwrap();
        let file_tail = file.metadata().unwrap().len();
        Self {
            file,
            file_tail,
            buff_tail: file_tail,
            clean: LruCache::new(NonZeroUsize::new((cache_size / PAGE_SIZE).max(1)).unwrap()),
            dirty: HashMap::new(),
            #[cfg(feature = "stats")]
            stats: PageCachedFileStats::new(),
        }
    }

    fn load_page(&mut self, pid: u64) -> Page {
        let ptr = pid << PAGE_BITS;
        let mut page = [0u8; PAGE_SIZE];
        if ptr < self.file_tail {
            let size = PAGE_SIZE.min((self.file_tail - ptr) as usize);
            self.file.read_at(&mut page[..size], ptr).unwrap();
        }
        page
    }

    fn get_page(&mut self, pid: u64) -> &Page {
        if self.dirty.contains_key(&pid) {
            #[cfg(feature = "stats")]
            {
                self.stats.hit += 1;
            }
            return self.dirty.get(&pid).unwrap();
        }
        if !self.clean.contains(&pid) {
            #[cfg(feature = "stats")]
            let load_timer = std::time::Instant::now();
            let page = self.load_page(pid);
            let _ = self.clean.put(pid, page);
            #[cfg(feature = "stats")]
            {
                self.stats.miss += 1;
                self.stats.load += load_timer.elapsed().as_secs_f64();
            }
        } else {
            #[cfg(feature = "stats")]
            {
                self.stats.hit += 1;
            }
        }
        self.clean.get(&pid).unwrap()
    }

    fn ensure_dirty_page(&mut self, pid: u64) -> &mut Page {
        if !self.dirty.contains_key(&pid) {
            let page = match self.clean.pop(&pid) {
                Some(page) => {
                    #[cfg(feature = "stats")]
                    {
                        self.stats.hit += 1;
                    }
                    page
                }
                None => {
                    #[cfg(feature = "stats")]
                    let load_timer = std::time::Instant::now();
                    let page = self.load_page(pid);
                    #[cfg(feature = "stats")]
                    {
                        self.stats.miss += 1;
                        self.stats.load += load_timer.elapsed().as_secs_f64();
                    }
                    page
                }
            };
            self.dirty.insert(pid, page);
        }
        self.dirty.get_mut(&pid).unwrap()
    }

    pub fn read(&mut self, ptr: u64, len: usize) -> Vec<u8> {
        let mut buf = Vec::new();
        let end = (ptr + len as u64).min(self.buff_tail);
        let mut cur = ptr;
        while cur < end {
            let page_start = (cur >> PAGE_BITS) << PAGE_BITS;
            let page_end = page_start + PAGE_SIZE as u64;
            let copy_end = end.min(page_end);
            let copy_len = (copy_end - cur) as usize;
            let page_off = (cur - page_start) as usize;

            let page = self.get_page(page_start >> PAGE_BITS);
            buf.extend_from_slice(&page[page_off..page_off + copy_len]);
            cur += copy_len as u64;
        }
        buf
    }

    pub fn write(&mut self, ptr: u64, data: &[u8]) {
        let mut off = 0;
        let end = ptr + data.len() as u64;
        while off < data.len() {
            let page_start = (ptr + off as u64) >> PAGE_BITS << PAGE_BITS;
            let page_end = page_start + PAGE_SIZE as u64;
            let copy_end = end.min(page_end);
            let copy_len = (copy_end - (ptr + off as u64)) as usize;
            let page_off = ((ptr + off as u64) - page_start) as usize;

            let page = self.ensure_dirty_page(page_start >> PAGE_BITS);
            page[page_off..page_off + copy_len].copy_from_slice(&data[off..off + copy_len]);
            off += copy_len as usize;
        }
        self.buff_tail = (ptr + data.len() as u64).max(self.buff_tail);
    }

    pub fn flush(&mut self) {
        #[cfg(feature = "stats")]
        let flush_timer = std::time::Instant::now();
        for (pid, page) in self.dirty.drain() {
            let ptr = pid << PAGE_BITS;
            self.file.write_at(&page, ptr).unwrap();
            let _ = self.clean.put(pid, page);
        }
        self.dirty.clear();
        // Keep on-disk length consistent with logical tail.
        self.file.set_len(self.buff_tail).unwrap();
        self.file_tail = self.buff_tail;
        #[cfg(feature = "stats")]
        {
            self.stats.write += flush_timer.elapsed().as_secs_f64();
        }
    }

    pub fn tail(&self) -> u64 {
        self.buff_tail
    }

    #[cfg(feature = "stats")]
    pub fn print_stats(&mut self) {
        self.stats.cache_size = self.clean.len() * PAGE_SIZE;
        self.stats.print_stats();
        self.stats.reset();
    }
}

#[cfg(test)]
mod tests {
    use super::{PAGE_SIZE, PageCachedFile};
    use std::fs;
    use std::path::PathBuf;

    fn unique_temp_path(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        let pid = std::process::id();
        let n = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        p.push(format!("ficusdb-{name}-{pid}-{n}.dat"));
        p
    }

    #[test]
    fn read_empty_returns_empty_and_tail_zero() {
        let path = unique_temp_path("empty");
        let mut f = PageCachedFile::new(path.to_str().unwrap(), PAGE_SIZE * 2);
        assert_eq!(f.tail(), 0);
        assert_eq!(f.read(0, 10), Vec::<u8>::new());
        drop(f);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn write_then_read_without_flush() {
        let path = unique_temp_path("rw");
        let mut f = PageCachedFile::new(path.to_str().unwrap(), PAGE_SIZE * 2);
        f.write(0, b"hello");
        assert_eq!(f.tail(), 5);
        assert_eq!(f.read(0, 5), b"hello".to_vec());
        assert_eq!(f.read(0, 100), b"hello".to_vec()); // clamped to tail
        drop(f);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn write_and_read_across_page_boundary() {
        let path = unique_temp_path("page");
        let mut f = PageCachedFile::new(path.to_str().unwrap(), PAGE_SIZE * 2);
        let mut data = vec![0u8; PAGE_SIZE + 10];
        for (i, b) in data.iter_mut().enumerate() {
            *b = (i as u8).wrapping_mul(31).wrapping_add(7);
        }
        f.write(0, &data);
        assert_eq!(f.tail(), (PAGE_SIZE + 10) as u64);
        assert_eq!(f.read(0, data.len()), data);
        drop(f);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn sparse_write_fills_gaps_with_zeros() {
        let path = unique_temp_path("sparse");
        let mut f = PageCachedFile::new(path.to_str().unwrap(), PAGE_SIZE * 2);
        let off = (PAGE_SIZE as u64) * 2 + 3;
        f.write(off, b"xyz");
        assert_eq!(f.tail(), off + 3);

        // Unwritten gap reads as zeros.
        let gap = f.read((PAGE_SIZE as u64) * 2, 3);
        assert_eq!(gap, vec![0, 0, 0]);
        assert_eq!(f.read(off, 3), b"xyz".to_vec());

        drop(f);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn flush_persists_and_reopen_reads_same_bytes_and_tail() {
        let path = unique_temp_path("persist");
        {
            let mut f = PageCachedFile::new(path.to_str().unwrap(), PAGE_SIZE * 2);
            f.write(0, b"abc");
            f.flush();
            assert_eq!(f.tail(), 3);
            let meta_len = fs::metadata(&path).unwrap().len();
            assert_eq!(meta_len, 3);
        }
        {
            let mut f2 = PageCachedFile::new(path.to_str().unwrap(), PAGE_SIZE * 2);
            assert_eq!(f2.tail(), 3);
            assert_eq!(f2.read(0, 3), b"abc".to_vec());
            assert_eq!(f2.read(3, 10), Vec::<u8>::new());
        }
        let _ = fs::remove_file(path);
    }

    #[test]
    fn overwrite_then_flush_persists_overwrite() {
        let path = unique_temp_path("overwrite");
        {
            let mut f = PageCachedFile::new(path.to_str().unwrap(), PAGE_SIZE * 2);
            f.write(0, b"hello world");
            f.flush();
        }
        {
            let mut f2 = PageCachedFile::new(path.to_str().unwrap(), PAGE_SIZE * 2);
            f2.write(6, b"rust");
            f2.flush();
        }
        {
            let mut f3 = PageCachedFile::new(path.to_str().unwrap(), PAGE_SIZE * 2);
            assert_eq!(f3.read(0, 11), b"hello rustd".to_vec());
        }
        let _ = fs::remove_file(path);
    }
}

#[cfg(feature = "stats")]
struct PageCachedFileStats {
    pub miss: usize,
    pub hit: usize,
    pub load: f64,
    pub write: f64,
    pub cache_size: usize,
}

#[cfg(feature = "stats")]
impl PageCachedFileStats {
    pub fn new() -> Self {
        Self {
            miss: 0,
            hit: 0,
            load: 0.0,
            write: 0.0,
            cache_size: 0,
        }
    }

    pub fn print_stats(&mut self) {
        let cache_size = self.cache_size as f64 / 1024.0 / 1024.0;
        println!("hit\tmiss\tratio\tt_load\tt_write\tcache");
        let ratio = self.hit as f64 / (self.hit + self.miss) as f64;
        println!(
            "{}\t{}\t{:.2}\t{:.2}\t{:.2}\t{:.2}",
            self.hit, self.miss, ratio, self.load, self.write, cache_size
        );
    }

    pub fn reset(&mut self) {
        self.miss = 0;
        self.hit = 0;
        self.load = 0.0;
        self.write = 0.0;
        self.cache_size = 0;
    }
}
