use ficusdb::{StateDB, StateDBConfig};
use num_bigint::BigUint;
use sha3::{Digest, Keccak256};

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new(prefix: &str) -> Self {
        let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push("target");
        path.push("test-tmp");
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time went backwards")
            .as_nanos();
        let pid = std::process::id();
        path.push(format!("{prefix}_{pid}_{now}"));
        let _ = std::fs::create_dir_all(path.parent().unwrap());
        Self { path }
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

fn keccak32(bytes: &[u8]) -> [u8; 32] {
    Keccak256::digest(bytes).into()
}

fn parse_hex_prefixed(s: &str) -> Vec<u8> {
    if s.starts_with("0x") || s.starts_with("0X") {
        hex::decode(&s[2..]).unwrap()
    } else {
        hex::decode(s).unwrap()
    }
}

fn parse_biguint(s: &str) -> BigUint {
    if s.starts_with("0x") || s.starts_with("0X") {
        BigUint::parse_bytes(s[2..].as_bytes(), 16).unwrap()
    } else {
        BigUint::parse_bytes(s.as_bytes(), 10).unwrap()
    }
}

#[test]
fn statedb_genesis_block() {
    let dir = TempDir::new("prunusdb_statedb_genesis");
    let ops_path = {
        let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        p.push("tests");
        p.push("genesis.ops");
        p
    };

    let cfg = StateDBConfig::builder().truncate(true).build();
    let mut statedb = StateDB::open(dir.path.to_str().unwrap(), cfg);

    let f = BufReader::new(File::open(ops_path).unwrap());
    for line in f.lines() {
        let l = line.unwrap();
        let l = l.trim();
        if l.is_empty() {
            continue;
        }
        let parts: Vec<&str> = l.split_whitespace().collect();
        let op = parts[0];

        match op {
            "newstatedb" => {
                // newstatedb <expected_hash>
                let expected = parse_hex_prefixed(parts[1]);
                assert_eq!(statedb.hash().to_vec(), expected);
            }
            "addbalance" => {
                let addr = keccak32(&parse_hex_prefixed(parts[1]));
                let amount = parse_biguint(parts[2]);
                statedb.add_balance(&addr, amount);
            }
            "subbalance" => {
                let addr = keccak32(&parse_hex_prefixed(parts[1]));
                let amount = parse_biguint(parts[2]);
                statedb.sub_balance(&addr, amount);
            }
            "setnonce" => {
                let addr = keccak32(&parse_hex_prefixed(parts[1]));
                let nonce = parts[2].parse::<u64>().unwrap();
                statedb.set_nonce(&addr, nonce);
            }
            "setcode" => {
                // setcode <addr> [<code_hex>]
                let addr = keccak32(&parse_hex_prefixed(parts[1]));
                let code = parts
                    .get(2)
                    .map(|s| parse_hex_prefixed(s))
                    .unwrap_or_default();
                let code_hash: Vec<u8> = Keccak256::digest(&code).to_vec();
                statedb.set_codehash(&addr, code_hash);
            }
            "setstate" => {
                let addr = keccak32(&parse_hex_prefixed(parts[1]));
                let key = keccak32(&parse_hex_prefixed(parts[2]));
                let val = parse_biguint(parts[3]);
                if val > BigUint::from(0u8) {
                    statedb.set_state(&addr, &key, &val.to_bytes_be());
                } else {
                    statedb.set_state(&addr, &key, b"");
                }
            }
            "createaccount" => {
                let addr = keccak32(&parse_hex_prefixed(parts[1]));
                statedb.create_account(&addr);
            }
            "removeaccount" => {
                let addr = keccak32(&parse_hex_prefixed(parts[1]));
                statedb.remove_account(&addr);
            }
            "snapshot" => {
                let _ = statedb.snapshot();
            }
            "revertsnapshot" => {
                let sid = parts[1].parse::<usize>().unwrap();
                statedb.revert(sid);
            }
            "commit" => {
                // commit <blknum> <something> <expected_hash>
                let _root = statedb.commit();
                let expected = parse_hex_prefixed(parts[3]);
                assert_eq!(
                    statedb.hash().to_vec(),
                    expected,
                    "hash mismatch after commit line: {l}"
                );
            }
            "blocknum" => {
                // ignored (workload bookkeeping)
            }
            "getcodehash" => {
                let addr = keccak32(&parse_hex_prefixed(parts[1]));
                let _ = statedb.get_codehash(&addr);
            }
            "getnonce" => {
                let addr = keccak32(&parse_hex_prefixed(parts[1]));
                let _ = statedb.get_nonce(&addr);
            }
            "getbalance" => {
                let addr = keccak32(&parse_hex_prefixed(parts[1]));
                let _ = statedb.get_balance(&addr);
            }
            "getstate" => {
                let addr = keccak32(&parse_hex_prefixed(parts[1]));
                let key = keccak32(&parse_hex_prefixed(parts[2]));
                let _ = statedb.get_state(&addr, &key);
            }
            _ => {
                // Ignore unknown lines for now.
            }
        }
    }
}
