#![allow(dead_code)]
use ficusdb::{DB, DBConfig};
use rand::Rng;
use rand_distr::{Distribution, Exp};
use std::env;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Write, Seek, SeekFrom};
use std::time::Instant;

fn open_db(dbpath: &str, cachesize: usize) -> DB {
    let cfg = DBConfig::builder()
        .truncate(false)
        .cache_size(cachesize * 1024 * 1024)
        .db_value_cache_size(0)
        .build();
    DB::open(dbpath, cfg)
}

fn random_bytes(len: usize) -> Vec<u8> {
    let mut bytes = vec![0u8; len];
    rand::rng().fill_bytes(&mut bytes);
    bytes
}

fn bench_init(db: &mut DB, wlpath: &str, verpath: &str, batch_size: usize, val_size: usize) {
    let workload_buf = BufReader::new(File::open(wlpath).unwrap());
    let mut wb = db.new_writebatch();
    let mut in_batch = 0usize;
    let mut timer = Instant::now();
    let mut total_ops = 0usize;
    let mut final_root = 0;
    for line in workload_buf.lines() {
        let line = line.unwrap();
        let parts: Vec<&str> = line.trim().split_whitespace().collect();
        if parts.len() < 1 {
            continue;
        }
        let key = parts[0];
        if key.is_empty() {
            continue;
        }
        let val = random_bytes(val_size);
        wb.insert(key.as_bytes(), &val);
        in_batch += 1;

        if in_batch >= batch_size {
            final_root = wb.commit();
            wb = db.new_writebatch();
            in_batch = 0;
            let elapsed = timer.elapsed().as_secs_f64();
            let trpt = batch_size as f64 / elapsed;
            total_ops += batch_size;
            timer = Instant::now();
            println!("init:\t{}\t{:.3}\t{:.3}", total_ops, elapsed, trpt);
            #[cfg(feature = "stats")]
            db.print_stats();
        }
    }
    if in_batch > 0 {
        final_root = wb.commit();
        println!("final_root: {}", final_root);
    }
    let mut verfile = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(verpath)
        .unwrap();
    verfile.write_all(&final_root.to_le_bytes()).unwrap();
    verfile.flush().unwrap();
}

fn bench_put(db: &mut DB, wlpath: &str, verpath: &str, batch_size: usize, val_size: usize, versions: usize) {
    let mut verfile = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(verpath)
        .unwrap();
    let workload_buf = BufReader::new(File::open(wlpath).unwrap());
    let mut wb = db.new_writebatch();
    let mut in_batch = 0usize;
    let mut total_ops = 0usize;
    let mut n_batch = 0usize;
    let mut t_ops = 0.0;
    for line in workload_buf.lines() {
        let line = line.unwrap();
        let parts: Vec<&str> = line.trim().split_whitespace().collect();
        if parts.len() < 1 {
            continue;
        }
        let key = parts[0];
        if key.is_empty() {
            continue;
        }
        let val = random_bytes(val_size);
        let t_start = Instant::now();
        wb.insert(key.as_bytes(), &val);
        t_ops += t_start.elapsed().as_secs_f64();
        in_batch += 1;

        if in_batch >= batch_size {
            let t_commit = Instant::now();
            let root = wb.commit();
            t_ops += t_commit.elapsed().as_secs_f64();
            let trpt = batch_size as f64 / t_ops;
            total_ops += batch_size;
            println!("put:\t{}\t{:.3}\t{:.3}", total_ops, t_ops, trpt);

            verfile.seek(SeekFrom::End(0)).unwrap();
            verfile.write_all(&root.to_le_bytes()).unwrap();
            verfile.flush().unwrap();
            wb = db.new_writebatch();
            in_batch = 0;
            t_ops = 0.0;
            
            #[cfg(feature = "stats")]
            db.print_stats();
            n_batch += 1;
            if n_batch >= versions {
                break;
            }
        }
    }
    if in_batch > 0 {
        let root = wb.commit();
        verfile.seek(SeekFrom::End(0)).unwrap();
        verfile.write_all(&root.to_le_bytes()).unwrap();
        verfile.flush().unwrap();
    }
}

fn bench_get(db: &mut DB, wlpath: &str, batch_size: usize) {
    let workload_buf = BufReader::new(File::open(wlpath).unwrap());
    let mut in_batch = 0usize;
    let mut t_ops = 0.0;
    let mut total_ops = 0usize;
    for line in workload_buf.lines() {
        let line = line.unwrap();
        let parts: Vec<&str> = line.trim().split_whitespace().collect();
        if parts.len() < 1 {
            continue;
        }
        let key = parts[0];
        if key.is_empty() {
            continue;
        }
        let t_start = Instant::now();
        let _val = db.get(key.as_bytes());
        t_ops += t_start.elapsed().as_secs_f64();
        in_batch += 1;

        if in_batch >= batch_size {
            in_batch = 0;
            let trpt = batch_size as f64 / t_ops;
            total_ops += batch_size;
            t_ops = 0.0;
            println!("get:\t{}\t{:.3}\t{:.3}", total_ops, t_ops, trpt);
            #[cfg(feature = "stats")]
            db.print_stats();
        }
    }
}

fn load_versions(verpath: &str) -> Vec<u64> {
    let mut versions = Vec::new();
    let mut verfile = OpenOptions::new().read(true).open(verpath).unwrap();
    let len = verfile.metadata().unwrap().len() / 8;
    for _ in 0..len {
        let mut buf = [0u8; 8];
        verfile.read_exact(&mut buf).unwrap();
        versions.push(u64::from_le_bytes(buf));
    }
    versions.reverse();
    versions
}

fn bench_vget(db: &mut DB, wlpath: &str, verpath: &str, batch_size: usize) {
    let vers = load_versions(verpath);
    let exp = Exp::new(10.0).unwrap();
    let mut rng = rand::rng();
    let workload_buf = BufReader::new(File::open(wlpath).unwrap());
    let mut in_batch = 0usize;
    let mut t_ops = 0.0;
    let mut total_ops = 0usize;
    for line in workload_buf.lines() {
        let line = line.unwrap();
        let parts: Vec<&str> = line.trim().split_whitespace().collect();
        if parts.len() < 1 {
            continue;
        }
        let key = parts[0];
        if key.is_empty() {
            continue;
        }
        let veridx = exp.sample(&mut rng) as usize % vers.len();
        let t_start = Instant::now();
        db.open_root(vers[veridx]);
        let _val = db.get(key.as_bytes());
        t_ops += t_start.elapsed().as_secs_f64();
        in_batch += 1;

        if in_batch >= batch_size {
            in_batch = 0;
            let trpt = batch_size as f64 / t_ops;
            total_ops += batch_size;
            t_ops = 0.0;
            println!("get:\t{}\t{:.3}\t{:.3}", total_ops, t_ops, trpt);
            #[cfg(feature = "stats")]
            db.print_stats();
        }
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 5 {
        eprintln!(
            "usage: {} <init|get|vget|put> <dbpath> <workload_path> <cache_mb> [batch_size] [val_size]",
            args.get(0).map(|s| s.as_str()).unwrap_or("micro-bench")
        );
        std::process::exit(2);
    }

    let op = &args[1];
    let dbpath = &args[2];
    let wlpath = &args[3];
    let verpath = &args[4];
    let cache_str = &args[5];

    let cache_size = cache_str.parse::<usize>().unwrap_or(2048);
    let batch_size = args
        .get(6)
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(10000);
    

    let mut db = open_db(dbpath, cache_size);

    if op == "init" {
        let val_size = args
        .get(7)
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(200);
        bench_init(&mut db, wlpath, verpath, batch_size, val_size);
    } else if op == "get" {
        bench_get(&mut db, wlpath, batch_size);
    } else if op == "vget" {
        bench_vget(&mut db, wlpath, verpath, batch_size);
    } else if op == "put" {
        let val_size = args
        .get(7)
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(200);
        let versions = args
        .get(8)
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(10);
        bench_put(&mut db, wlpath, verpath, batch_size, val_size, versions);
    } else {
        eprintln!("unknown op: {}", op);
        std::process::exit(2);
    }
}
