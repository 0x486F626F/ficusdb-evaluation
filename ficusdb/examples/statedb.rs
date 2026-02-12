#![allow(dead_code)]
use ficusdb::{StateDB, StateDBConfig};
use num_bigint::BigUint;
use sha3::{Digest, Keccak256};

use std::env;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::process::Command;
use std::str;
use std::time::Instant;

struct BenchmarkStats {
    pub timer: Instant,
    pub blknum: usize,
    pub opget: usize,
    pub opput: usize,
    pub dbpath: String,

    pub t_get: f64,
    pub t_put: f64,
    pub t_snap: f64,
    pub t_commit: f64,
    pub t_ops: f64,
}

impl BenchmarkStats {
    pub fn new(dbpath: &str) -> Self {
        Self {
            timer: Instant::now(),
            blknum: 0,
            opget: 0,
            opput: 0,

            dbpath: dbpath.to_string(),

            t_get: 0.0,
            t_put: 0.0,
            t_snap: 0.0,
            t_commit: 0.0,
            t_ops: 0.0,
        }
    }

    pub fn reset(&mut self) {
        self.timer = Instant::now();
        self.opget = 0;
        self.opput = 0;
        self.t_get = 0.0;
        self.t_put = 0.0;
        self.t_snap = 0.0;
        self.t_commit = 0.0;
        self.t_ops = 0.0;
    }

    pub fn print_stats(&mut self) {
        let time_elapsed = self.timer.elapsed().as_secs_f64();
        let cmd_out = Command::new("du")
            .args(["-s", &self.dbpath])
            .output()
            .expect("cmd err")
            .stdout;
        let cmd_str = str::from_utf8(&cmd_out).unwrap();
        let split_p = cmd_str.find('\t').unwrap();
        let (footprint, _) = cmd_str.split_at(split_p);
        let opscnt = self.opget + self.opput;
        let trpt = opscnt as f64 / self.t_ops;
        println!("blocknum\ttime\tfootprint");
        println!(
            "{}\t{:.3}\t{}\t{:.3}",
            self.blknum, self.t_ops, footprint, time_elapsed
        );
        println!("opget\topput\topcnt\ttrpt");
        println!("{}\t{}\t{}\t{:.3}", self.opget, self.opput, opscnt, trpt);
        println!("t_get\tt_put\tt_commit\tt_snap\tt_ops");
        println!(
            "{:.3}\t{:.3}\t{:.3}\t{:.3}\t{:.3}",
            self.t_get, self.t_put, self.t_commit, self.t_snap, self.t_ops
        );
        self.reset();
    }
}

fn benchmark(dbpath: &str, wlpath: &str, cachesize: usize) {
    let workload_buf = BufReader::new(File::open(wlpath).unwrap());

    let cfg = StateDBConfig::builder()
        .truncate(false)
        .cache_size(cachesize * 1024 * 1024)
        .build();
    let mut statedb = StateDB::open(dbpath, cfg);
    let mut stats = BenchmarkStats::new(dbpath);

    for line in workload_buf.lines() {
        let l = line.unwrap();
        let l = l.trim();
        if l.is_empty() {
            continue;
        }
        let parts: Vec<&str> = l.split_whitespace().collect();
        let op = parts[0];

        let timer = Instant::now();
        match op {
            "addbalance" => {
                let addr: [u8; 32] =
                    Keccak256::digest(&hex::decode(&parts[1][2..]).unwrap()).into();
                let amount = if parts[2].starts_with("0x") {
                    BigUint::parse_bytes(parts[2][2..].as_bytes(), 16).unwrap()
                } else {
                    BigUint::parse_bytes(parts[2].as_bytes(), 10).unwrap()
                };
                statedb.add_balance(&addr, amount);
                stats.t_put += timer.elapsed().as_secs_f64();
                stats.opput += 1;
            }
            "subbalance" => {
                let addr: [u8; 32] =
                    Keccak256::digest(&hex::decode(&parts[1][2..]).unwrap()).into();
                let amount = if parts[2].starts_with("0x") {
                    BigUint::parse_bytes(parts[2][2..].as_bytes(), 16).unwrap()
                } else {
                    BigUint::parse_bytes(parts[2].as_bytes(), 10).unwrap()
                };
                statedb.sub_balance(&addr, amount);
                stats.t_put += timer.elapsed().as_secs_f64();
                stats.opput += 1;
            }
            "setnonce" => {
                let addr: [u8; 32] =
                    Keccak256::digest(&hex::decode(&parts[1][2..]).unwrap()).into();
                let nonce = parts[2].parse::<u64>().unwrap();
                statedb.set_nonce(&addr, nonce);
                stats.t_put += timer.elapsed().as_secs_f64();
                stats.opput += 1;
            }
            "setcode" => {
                let addr: [u8; 32] =
                    Keccak256::digest(&hex::decode(&parts[1][2..]).unwrap()).into();
                let code: Vec<u8> = parts
                    .get(2)
                    .map(|s| hex::decode(&s).unwrap())
                    .unwrap_or_default();
                let code_hash: [u8; 32] = Keccak256::digest(&code).into();
                statedb.set_codehash(&addr, code_hash.to_vec());
                stats.t_put += timer.elapsed().as_secs_f64();
                stats.opput += 1;
            }
            "setstate" => {
                let addr: [u8; 32] =
                    Keccak256::digest(&hex::decode(&parts[1][2..]).unwrap()).into();
                let key: [u8; 32] = Keccak256::digest(&hex::decode(&parts[2][2..]).unwrap()).into();
                let val = BigUint::parse_bytes(parts[3][2..].as_bytes(), 16).unwrap();
                if val > BigUint::from(0u8) {
                    statedb.set_state(&addr, &key, &val.to_bytes_be());
                } else {
                    statedb.set_state(&addr, &key, b"");
                }
                stats.t_put += timer.elapsed().as_secs_f64();
                stats.opput += 1;
            }
            "createaccount" => {
                let addr: [u8; 32] =
                    Keccak256::digest(&hex::decode(&parts[1][2..]).unwrap()).into();
                statedb.create_account(&addr);
                stats.t_put += timer.elapsed().as_secs_f64();
                stats.opput += 1;
            }
            "removeaccount" => {
                let addr: [u8; 32] =
                    Keccak256::digest(&hex::decode(&parts[1][2..]).unwrap()).into();
                statedb.remove_account(&addr);
                stats.t_put += timer.elapsed().as_secs_f64();
                stats.opput += 1;
            }
            "snapshot" => {
                let _sid = statedb.snapshot() as u64;
                //assert!(sid == strs[1].parse::<u64>().unwrap());
                stats.t_snap += timer.elapsed().as_secs_f64();
            }
            "revertsnapshot" => {
                let sid = parts[1].parse::<u64>().unwrap();
                statedb.revert(sid as usize);
                stats.t_snap += timer.elapsed().as_secs_f64();
            }
            "commit" => {
                let _ver = statedb.commit();
                stats.t_commit += timer.elapsed().as_secs_f64();

                // If the workload provides an expected hash in the 4th column, validate it.
                // if let Some(expected_hex) = parts.get(3) {
                //     if expected_hex.starts_with("0x") {
                //         let expected = hex::decode(&expected_hex[2..]).unwrap();
                //         if statedb.hash().to_vec() != expected {
                //             println!("blocknum: {}", stats.blknum);
                //             println!("expected: {}", hex::encode(&expected));
                //             println!("actual: {}", hex::encode(&statedb.hash().to_vec()));
                //         }

                //         assert_eq!(statedb.hash().to_vec(), expected);
                //     }
                // }

                stats.blknum += 1;
                #[cfg(feature = "stats")]
                if stats.blknum % 1000 == 0 {
                    stats.print_stats();
                    statedb.print_stats();
                }
            }
            "finalise" => {
                statedb.finalise();
                stats.t_snap += timer.elapsed().as_secs_f64();
            }
            "blocknum" => {
                stats.blknum = match parts[1].parse::<usize>() {
                    Ok(i) => i,
                    Err(_) => 0,
                }
            }

            "getcodehash" => {
                let addr: [u8; 32] =
                    Keccak256::digest(&hex::decode(&parts[1][2..]).unwrap()).into();
                let _codehash = statedb.get_codehash(&addr);
                stats.t_get += timer.elapsed().as_secs_f64();
                stats.opget += 1;
            }

            "getnonce" => {
                let addr: [u8; 32] =
                    Keccak256::digest(&hex::decode(&parts[1][2..]).unwrap()).into();
                let _nonce = statedb.get_nonce(&addr);
                stats.t_get += timer.elapsed().as_secs_f64();
                stats.opget += 1;
            }

            "getbalance" => {
                let addr: [u8; 32] =
                    Keccak256::digest(&hex::decode(&parts[1][2..]).unwrap()).into();
                let _balance = statedb.get_balance(&addr);
                stats.t_get += timer.elapsed().as_secs_f64();
                stats.opget += 1;
            }

            "getstate" => {
                let addr: [u8; 32] =
                    Keccak256::digest(&hex::decode(&parts[1][2..]).unwrap()).into();
                let key: [u8; 32] = Keccak256::digest(&hex::decode(&parts[2][2..]).unwrap()).into();
                let _state = statedb.get_state(&addr, &key);
                stats.t_get += timer.elapsed().as_secs_f64();
                stats.opget += 1;
            }

            _ => {}
        }
        stats.t_ops += timer.elapsed().as_secs_f64();
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let dbpath = &args[1];
    let wlpath = &args[2];
    let cache_str = &args[3];

    let cachesize = match cache_str.parse::<usize>() {
        Ok(i) => i,
        Err(_) => 2048,
    };
    benchmark(dbpath, wlpath, cachesize);
}
