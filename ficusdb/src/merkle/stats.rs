pub struct StoreStats {
    pub node_miss: usize,
    pub node_hit: usize,
    pub node_load: f64,
    pub node_commit: f64,
    pub cache_size: usize,
    pub aha_hit: usize,
    pub aha_miss: usize,
    pub t_aha_commit: f64,
    pub t_aha_write: f64,
    pub t_hash_load: f64,   
    pub t_encode: f64,
}

impl StoreStats {
    pub fn new() -> Self {
        Self {
            node_miss: 0,
            node_hit: 0,
            node_load: 0.0,
            node_commit: 0.0,
            cache_size: 0,
            aha_hit: 0,
            aha_miss: 0,
            t_aha_commit: 0.0,
            t_aha_write: 0.0,
            t_hash_load: 0.0,
            t_encode: 0.0
        }
    }
    pub fn print_stats(&mut self) {
        println!("node:\thit\tmiss\tratio\tt_load\tt_commit\tt_hash_load\tt_encode\tcache_size");
        let ratio = self.node_hit as f64 / (self.node_hit + self.node_miss) as f64;
        println!(
            "\t{}\t{}\t{:.2}\t{:.2}\t{:.2}\t{:.2}\t{:.2}\t{:.2}",
            self.node_hit,
            self.node_miss,
            ratio,
            self.node_load,
            self.node_commit,
            self.t_hash_load,
            self.t_encode,        
            self.cache_size as f64 / 1024.0 / 1024.0
        );
        let aha_ratio = if self.aha_hit + self.aha_miss > 0 {
            self.aha_hit as f64 / (self.aha_hit + self.aha_miss) as f64
        } else {
            0.0
        };
        println!("aha:\thit\tmiss\tratio\tt_write\tt_commit");
        println!(
            "\t{}\t{}\t{:.2}\t{:.2}\t{:.2}",
            self.aha_hit, self.aha_miss, aha_ratio, self.t_aha_write, self.t_aha_commit, 
        );
    }
    pub fn reset(&mut self) {
        self.node_miss = 0;
        self.node_hit = 0;
        self.node_load = 0.0;
        self.node_commit = 0.0;
        self.aha_hit = 0;
        self.aha_miss = 0;
        self.t_aha_write = 0.0;
        self.t_aha_commit = 0.0;
        self.t_hash_load = 0.0;
        self.cache_size = 0;
        self.t_encode = 0.0;
    }
}

pub struct AHAStats {
    pub reused: usize,
    pub new: usize,
    pub recycled: usize,
    pub t_write: f64,
}

impl AHAStats {
    pub fn new() -> Self {
        Self {
            reused: 0,
            new: 0,
            recycled: 0,
            t_write: 0.0,
        }
    }

    pub fn print_stats(&mut self) {
        println!("aha:\treused\tnew\trecycled\tt_write");
        println!("\t{}\t{}\t{}\t{:.2}", self.reused, self.new, self.recycled, self.t_write);
    }
    pub fn reset(&mut self) {
        self.reused = 0;
        self.new = 0;
        self.recycled = 0;
        self.t_write = 0.0;
    }
}

pub struct MerkleStats {
    pub get: usize,
    pub put: usize,
    pub del: usize,
    pub t_get: f64,
    pub t_put: f64,
    pub t_del: f64,
    pub t_commit: f64,
    
    pub tc_node: f64,
    pub tc_store: f64,

    pub tcn_hash: f64,
    pub tcn_add: f64,
    pub tcn_store: f64,
}

impl MerkleStats {
    pub fn new() -> Self {
        Self {
            get: 0,
            put: 0,
            del: 0,
            t_del: 0.0,
            t_get: 0.0,
            t_put: 0.0,
            t_commit: 0.0,      
            tcn_hash: 0.0,
            tcn_add: 0.0,
            tc_node: 0.0,
            tc_store: 0.0,
            tcn_store: 0.0,
        }
    }

    pub fn print_stats(&mut self) {
        println!("merkle:\tget\tput\tdel\tt_get\tt_put\tt_del\tt_cm\ttc_n\ttcn_hash\ttcn_add\ttcn_store");
        println!(
            "\t{}\t{}\t{}\t{:.2}\t{:.2}\t{:.2}\t{:.2}\t{:.2}\t{:.2}\t{:.2}\t{:.2}"  ,
            self.get,
            self.put,
            self.del,
            self.t_get,
            self.t_put,
            self.t_del,
            self.t_commit,
            self.tc_node,
            self.tcn_hash,
            self.tcn_add,   
            self.tcn_store,
        );
    }

    pub fn reset(&mut self) {
        self.get = 0;
        self.put = 0;
        self.del = 0;
        self.t_del = 0.0;
        self.t_get = 0.0;
        self.t_put = 0.0;
        self.t_commit = 0.0;
        self.tc_node = 0.0;
        self.tc_store = 0.0;
        self.tcn_hash = 0.0;
        self.tcn_add = 0.0;
        self.tcn_store = 0.0;
    }
}
