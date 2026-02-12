#![allow(dead_code)]
use super::CleanPtr;
use super::backend::Backend;
#[cfg(feature = "stats")]
use super::stats::AHAStats;
#[cfg(feature = "stats")]
use std::time::Instant;

pub struct AggregatedHashArray {
    backends: Vec<Box<dyn Backend>>,
    aha_len: Vec<u8>,
    recycled: Vec<Vec<CleanPtr>>,
    pending_recycle: Vec<Vec<CleanPtr>>,
    #[cfg(feature = "stats")]
    stats: AHAStats,
}

impl AggregatedHashArray {
    pub fn new(mut ahas: Vec<(u8, Box<dyn Backend>)>) -> Self {
        let mut backends = Vec::new();
        let mut aha_len = Vec::new();
        let mut recycled = Vec::new();
        let mut pending_recycle = Vec::new();
        for (len, backend) in ahas.drain(..) {
            backends.push(backend);
            aha_len.push(len);
            recycled.push(Vec::new());
            pending_recycle.push(Vec::new());
        }
        Self {
            backends,
            aha_len,
            recycled,
            pending_recycle,
            #[cfg(feature = "stats")]
            stats: AHAStats::new(),
        }
    }

    #[inline(always)]
    fn aha_index(&self, len: u8) -> usize {
        for i in 0..self.aha_len.len() {
            if self.aha_len[i] >= len {
                return i;
            }
        }
        self.aha_len.len()
    }

    #[inline(always)]
    fn new_cptr(&mut self, idx: usize) -> CleanPtr {
        match self.recycled[idx].pop() {
            Some(cptr) => {
                #[cfg(feature = "stats")]
                {
                    self.stats.reused += 1;
                }
                cptr
            }
            None => {
                #[cfg(feature = "stats")]
                {
                    self.stats.new += 1;
                }
                self.backends[idx].tail()
            }
        }
    }

    pub fn read_aha(&mut self, aha_len: u8, aha_ptr: CleanPtr) -> Vec<Vec<u8>> {
        let idx = self.aha_index(aha_len);
        // Each stored item is: [u8 length] + [reference-item bytes].
        // Reference items are either:
        // - inlined canonical RLP (<32 bytes)  => at most 31 bytes
        // - RLP(keccak256(rlp))               => 33 bytes (0xa0 + 32-byte hash)
        // So worst-case is 1 + 33 = 34 bytes per entry.
        let max_bytes = (self.aha_len[idx] as usize) * (33 + 1);
        let backend = &mut self.backends[idx];
        let buf = backend.read(aha_ptr, max_bytes);
        let mut off = 0;
        let mut hashs = Vec::new();
        for _ in 0..aha_len as usize {
            let len = u8::from_le_bytes(buf[off..off + 1].try_into().unwrap());
            let hash = buf[off + 1..off + 1 + len as usize].to_vec();
            off += 1 + len as usize;
            hashs.push(hash);
        }
        hashs
    }

    pub fn write_aha(
        &mut self,
        mut hashs: Vec<Vec<u8>>,
        old_len: u8,
        old_cptr: CleanPtr,
    ) -> CleanPtr {

        if old_len > 0 {
            let idx = self.aha_index(old_len);
            self.pending_recycle[idx].push(old_cptr);
        }
        if hashs.is_empty() {
            return old_cptr;
        }
        // Select backend tier by *array length* (number of hashes), not by hash byte length.
        let idx = self.aha_index(hashs.len() as u8);
        if idx >= self.aha_len.len() {
            return 0;
        }
        // Each stored item is: [u8 length] + [reference-item bytes].
        // Reference items are either:
        // - inlined canonical RLP (<32 bytes)  => at most 31 bytes
        // - RLP(keccak256(rlp))               => 33 bytes (0xa0 + 32-byte hash)
        // So worst-case is 1 + 33 = 34 bytes per entry.
        let max_bytes = (self.aha_len[idx] as usize) * (33 + 1);
        let new_cptr = self.new_cptr(idx);
        
        let mut encoded = Vec::new();
        for hash in hashs.drain(..) {
            encoded.extend((hash.len() as u8).to_le_bytes());
            encoded.extend(hash);
        }
        debug_assert!(encoded.len() <= max_bytes);
        encoded.resize(max_bytes, 0);

        debug_assert!(new_cptr % (max_bytes as CleanPtr) == 0);
        let backend = &mut self.backends[idx];
        #[cfg(feature = "stats")]
        let timer = Instant::now();
        backend.write(new_cptr, &encoded);
        #[cfg(feature = "stats")]
        {
            self.stats.t_write += timer.elapsed().as_secs_f64();
        }
        
        new_cptr
    }

    pub fn commit(&mut self) {
        for i in 0..self.aha_len.len() {
            self.recycled[i].append(&mut self.pending_recycle[i]);
            self.pending_recycle[i].clear();
            let cap = self.pending_recycle[i].capacity();
            self.pending_recycle[i].shrink_to(cap / 2);
            assert!(self.pending_recycle[i].is_empty());
        }
    }

    pub fn flush(&mut self) {
        for backend in &mut self.backends {
            backend.flush();
        }
    }

    #[cfg(feature = "stats")]
    pub fn print_stats(&mut self) {
        self.stats.recycled = self.recycled.iter().map(|v| v.len()).sum();
        self.stats.print_stats();
        self.stats.reset();
    }
}
