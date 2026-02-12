use ficusdb::{DB, DBConfig};

use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

fn unique_temp_dir(name: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    let pid = std::process::id();
    let n = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    p.push(format!("ficusdb-dbtests-{name}-{pid}-{n}"));
    p
}

fn default_cfg(truncate: bool, db_value_cache_size: usize) -> DBConfig {
    DBConfig::builder()
        .truncate(truncate)
        // Keep these small so tests are fast.
        .cache_size(1024)
        .page_cache_size(1 << 20)
        .aha_cache_size(1 << 20)
        .db_value_cache_size(db_value_cache_size)
        // Disable AHA here; DB tests focus on root/versioning/cache semantics.
        .aha_lens(vec![])
        .build()
}

#[test]
fn db_opens_latest_committed_root() {
    let dir = unique_temp_dir("latest");
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();

    let root1: u64;
    {
        let cfg = default_cfg(true, 1024);
        let db = DB::open(dir.to_str().unwrap(), cfg);
        let mut wb = db.new_writebatch();
        wb.insert(b"a", b"1");
        wb.insert(b"b", b"2");
        root1 = wb.commit();
    }

    // Reopen should automatically load the last root pointer and see the data.
    {
        let cfg = default_cfg(false, 1024);
        let mut db2 = DB::open(dir.to_str().unwrap(), cfg);
        assert_eq!(db2.get(b"a"), Some(b"1".to_vec()));
        assert_eq!(db2.get(b"b"), Some(b"2".to_vec()));

        // Committed root pointer should produce a non-empty hash.
        let h = db2.hash();
        assert_eq!(h.len(), 32);
        assert_ne!(root1, 0);
    }

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn db_can_open_historical_roots_and_cache_does_not_poison() {
    let dir = unique_temp_dir("history");
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();

    let (root1, root2, root3): (u64, u64, u64);
    {
        let cfg = default_cfg(true, 1024);
        let db = DB::open(dir.to_str().unwrap(), cfg);

        // Commit 1
        let mut wb = db.new_writebatch();
        wb.insert(b"k", b"v1");
        root1 = wb.commit();

        // Commit 2: overwrite k
        let mut wb = db.new_writebatch();
        wb.insert(b"k", b"v2");
        root2 = wb.commit();

        // Commit 3: add another key
        let mut wb = db.new_writebatch();
        wb.insert(b"x", b"xx");
        root3 = wb.commit();
    }

    // Reopen and exercise historical lookups.
    {
        let cfg = default_cfg(false, 1024);
        let mut db = DB::open(dir.to_str().unwrap(), cfg);

        // Warm cache with latest value.
        assert_eq!(db.get(b"k"), Some(b"v2".to_vec()));

        // Open historical root1: must return v1, not cached v2.
        db.open_root(root1);
        assert_eq!(db.get(b"k"), Some(b"v1".to_vec()));
        assert_eq!(db.get(b"x"), None);

        // Open root2: k=v2, x missing.
        db.open_root(root2);
        assert_eq!(db.get(b"k"), Some(b"v2".to_vec()));
        assert_eq!(db.get(b"x"), None);

        // Open root3: k=v2 and x present.
        db.open_root(root3);
        assert_eq!(db.get(b"k"), Some(b"v2".to_vec()));
        assert_eq!(db.get(b"x"), Some(b"xx".to_vec()));
    }

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn db_value_cache_eviction_does_not_change_results() {
    let dir = unique_temp_dir("cache");
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();

    let cfg = default_cfg(true, 1); // tiny cache to force evictions
    let mut db = DB::open(dir.to_str().unwrap(), cfg);

    let mut wb = db.new_writebatch();
    wb.insert(b"a", b"va");
    wb.insert(b"b", b"vb");
    wb.insert(b"c", b"vc");
    let _ = wb.commit();

    // Interleave reads to force evictions; results must always be correct.
    assert_eq!(db.get(b"a"), Some(b"va".to_vec()));
    assert_eq!(db.get(b"b"), Some(b"vb".to_vec()));
    assert_eq!(db.get(b"a"), Some(b"va".to_vec()));
    assert_eq!(db.get(b"c"), Some(b"vc".to_vec()));
    assert_eq!(db.get(b"b"), Some(b"vb".to_vec()));

    // Negative caching should also remain correct across evictions.
    assert_eq!(db.get(b"missing"), None);
    assert_eq!(db.get(b"missing"), None);

    let _ = fs::remove_dir_all(&dir);
}

#[derive(Clone)]
struct XorShift64 {
    state: u64,
}

impl XorShift64 {
    fn new(seed: u64) -> Self {
        Self { state: seed.max(1) }
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }
}

fn rand_bytes(rng: &mut XorShift64, len: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(len);
    while out.len() < len {
        out.extend_from_slice(&rng.next_u64().to_le_bytes());
    }
    out.truncate(len);
    out
}

#[test]
fn db_fuzzy_large_scale_historical_and_latest() {
    // Keep it “large scale” but still practical for debug CI runs.
    const N_COMMITS: usize = 20;
    const PER_COMMIT: usize = 2000;
    const CHECK_PER_ROOT: usize = 64;

    let dir = unique_temp_dir("fuzzy");
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();

    let mut roots: Vec<u64> = Vec::with_capacity(N_COMMITS);
    let mut samples: Vec<Vec<(Vec<u8>, Vec<u8>)>> = Vec::with_capacity(N_COMMITS);

    {
        let cfg = default_cfg(true, 4096);
        let db = DB::open(dir.to_str().unwrap(), cfg);
        let mut rng = XorShift64::new(0x1234_5678_9abc_def0);

        // Track latest values (for sampling), but don’t snapshot everything per root.
        let mut latest: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();

        for c in 0..N_COMMITS {
            let mut wb = db.new_writebatch();
            let mut touched: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(CHECK_PER_ROOT);

            for i in 0..PER_COMMIT {
                // Make keys collide sometimes to create overwrites.
                let klen = 8 + (rng.next_u64() as usize % 16);
                let mut key = rand_bytes(&mut rng, klen);
                key.extend_from_slice(&((i % 500) as u32).to_le_bytes());

                let vlen = 4 + (rng.next_u64() as usize % 64);
                let mut value = rand_bytes(&mut rng, vlen);
                value.extend_from_slice(&(c as u32).to_le_bytes());

                wb.insert(&key, &value);
                latest.insert(key.clone(), value.clone());

                if touched.len() < CHECK_PER_ROOT && (i % (PER_COMMIT / CHECK_PER_ROOT).max(1) == 0)
                {
                    touched.push((key, value));
                }
            }

            let root = wb.commit();
            roots.push(root);
            samples.push(touched);
        }

        // Spot-check latest values in the same process.
        let mut db_mut = DB::open(dir.to_str().unwrap(), default_cfg(false, 4096));
        for (k, v) in latest.iter().take(200) {
            assert_eq!(db_mut.get(k), Some(v.clone()));
        }
    }

    // Reopen and validate historical roots by samples taken at each commit.
    {
        let cfg = default_cfg(false, 4096);
        let mut db = DB::open(dir.to_str().unwrap(), cfg);

        // Latest root should satisfy last commit samples.
        for (k, v) in &samples[N_COMMITS - 1] {
            assert_eq!(db.get(k), Some(v.clone()));
        }

        // Historical checks.
        for (idx, root) in roots.iter().enumerate() {
            db.open_root(*root);
            for (k, v) in &samples[idx] {
                assert_eq!(db.get(k), Some(v.clone()));
            }
        }
    }

    let _ = fs::remove_dir_all(&dir);
}
