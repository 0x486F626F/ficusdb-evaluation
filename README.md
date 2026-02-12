# FicusDB Experiments

## Overview

This repository contains the scripts and data pipeline used to evaluate **FicusDB** against **Geth StateDB**.
It includes:

- **Microbenchmarks**: point reads/writes on synthetic traces that preserve real Ethereum access skew.
- **Ethereum StateDB evaluation**: end-to-end replay of StateDB operation traces extracted from Ethereum blocks.

**Data provenance**: all workloads are processed from the **public Ethereum blockchain**. Generating the raw traces
from scratch is extremely time- and SSD-space-intensive, so for artifact evaluation we provide **preprocessed**
trace shards (`block_XXm_ops.zip`) and microbenchmark key-frequency files (`micro-bench-keys.zip`) via AWS S3.
The extraction code for StateDB operation traces is included in `statedb-ops-extract/`.

Repository structure:
- [`ficusdb/`](ficusdb/): (expected sibling repo) FicusDB implementation
- [`geth-statedb/`](geth-statedb/): Geth StateDB baseline
- [`statedb-ops-extract/`](statedb-ops-extract/): tooling to extract StateDB ops from raw blocks
- [`scripts/`](scripts/): experiment drivers and plotting scripts
- [`db/`](db/): populated databases
- [`data/`](data/): downloaded traces and generated workloads
- [`logs/`](logs/): experiment logs and outputs

## System Requirements

This artifact is intended to be evaluated on a Linux machine with enough RAM and SSD space for archival workloads.

Paper evaluation machine (for reference): **Dell R340**, **6-core Intel Xeon E-2176 @ 3.70 GHz**, **64 GB DDR4**, **8 TB WD Black SN850X NVMe SSD**, **Ubuntu 24.04.4 LTS (ext4)**.

Hardware Requirements (minimum / recommended):
- **CPU**: x86_64, 4+ cores / 6+ cores (NVMe-friendly single-node evaluation).
- **Memory**: 16 GB / 64 GB.
- **Storage**: SSD required; **NVMe strongly recommended** for throughput experiments.
  - **Free disk space**: **hundreds of GB** for microbenchmarks (often ~1 TB); **multiple TB** for the Ethereum StateDB evaluation (we recommend **~4 TB** free).
- **Network**: Internet access recommended to download traces/datasets (e.g., via `curl`).

Dependencies:
- **OS**: Linux (tested on **Ubuntu 24.04.4 LTS** with the **ext4** filesystem, matching the paper’s evaluation environment).
- **Go**:
  - `micro-bench-go/` uses the Go toolchain directive `go1.24.7` (see `micro-bench-go/go.mod`).
  - `statedb-ops-extract/` uses Go `1.22.x` (see `statedb-ops-extract/go.mod`).
  - `geth-statedb/` is an older fork pinned to Go `1.17` in `go.mod` (newer Go versions may still build, but are not guaranteed).
- **Rust**: required to build and run FicusDB binaries used by the benchmark scripts (the scripts expect a sibling repo at `../ficusdb`); tested with **Rust 1.93.0**.
- **Python**: Python 3.x for trace/workload generation scripts in `scripts/` (e.g., `generate-bench-trace.py`).
- **System utilities**: `git`, `curl`, `unzip` (for downloading and unpacking traces), plus a C toolchain (`gcc`/`clang`) for building some dependencies.
- **Docker (optional)**: only required if you build the `statedb-ops-extract/` tooling via its `Dockerfile`.

## Microbenchmark

The microbenchmark evaluates point reads/writes on synthetic workloads derived from real Ethereum access skew.
It compares **FicusDB** against two baselines:

- **Geth-TrieDB**
- **ChainKV**

### 1) Download key-frequency data, populate databases, and generate workload traces

Run:

```bash
cd scripts
./micro-bench-init.sh
```

This will download and unzip `micro-bench-keys.zip` into `data/micro/`, containing:
- **`micro-20m.keys`**: 20,000,000 keys
- **`micro-100m.keys`**: 100,000,000 keys

Each line in `*.keys` is `key frequency`, where the frequency is computed from real Ethereum block execution traces.

It then populates **three** different databases:
- **FicusDB**
- **Geth-TrieDB**
- **ChainKV**

Each database is populated at **two** different dataset sizes using the two key files:
- **20M keys** (`micro-20m.keys`)
- **100M keys** (`micro-100m.keys`)

The populated databases are written under `db/` (with backups under `db/backup-*`).

Finally, it generates two workload traces (50,000,000 operations each) by sampling keys according to the provided frequencies:
- **`data/micro/micro-20m-50m.ops`**
- **`data/micro/micro-100m-50m.ops`**

**Expected runtime / space**: a few hours end-to-end and **hundreds of GB** of free disk space (often **~0.5--1 TB**, especially for the 100M-key dataset and backups).

### 2) Run the microbenchmarks

Run:

```bash
cd scripts
./micro-bench.sh
```

This script executes the point read/write microbenchmarks across the populated databases and writes logs under `logs/`.

**Expected runtime**: this step can take **multiple hours** to complete.

### 3) Plot the results

Run:

```bash
cd scripts
python3 micro-plot.py
```

This generates `scripts/micro-plot.png` from the logs in `logs/statedb/`.

## Ethereum StateDB Evaluation

This evaluation replays a large, preprocessed Ethereum StateDB workload trace to compare **FicusDB** against **Geth StateDB** at scale.

**Disk space warning**: this experiment consumes a **huge** amount of storage. We recommend **at least ~4 TB of free disk space** before starting.

### 1) Download the preprocessed Ethereum StateDB trace

Run:

```bash
cd scripts
./download-all-blocks.sh
```

This downloads the preprocessed StateDB operation traces into `data/statedb-ops/` as `block_XXm_ops.zip` shards (via `curl` from an AWS S3 bucket). The total download is **~1 TB** of compressed data.

### 2) Populate FicusDB and Geth StateDB (10M blocks)

Run the initialization for each backend:

```bash
cd scripts
./ficus-statedb-init.sh
```

```bash
cd scripts
./geth-statedb-init.sh
```

These scripts replay the first **10 million blocks** of the trace (`block_01m_ops.zip` through `block_10m_ops.zip`) to build the corresponding databases under `db/` (with logs written to `logs/statedb/`).

**Expected runtime**: initialization can take **multiple days** to complete, depending on your CPU, SSD/NVMe performance, and available memory.

### 3) Run the evaluation workload (next 2M blocks)

After the databases are populated, run:

```bash
cd scripts
./statedb-eval.sh
```

This script replays the **next 2 million blocks** using the trace shards `block_11m_ops.zip` and `block_12m_ops.zip`. It evaluates **both** backends (FicusDB and Geth StateDB) under two cache limits: 4GB and 32GB.

To keep the populated databases unchanged, the script first copies them to `db/ficus-statedb-eval/` and `db/geth-statedb-eval/`. Logs are written to `logs/statedb/` (e.g., `ficus-statedb-4096.log`, `geth-statedb-32768.log`).

### 4) Plot the results

Run:

```bash
cd scripts
python3 statedb-plot.py
```

This generates `scripts/statedb-plot.png` from the logs in `logs/statedb/`.