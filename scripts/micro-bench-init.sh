#!/bin/bash
ficusdb_dir=../ficusdb
ficusdb=$ficusdb_dir/target/release/examples/micro-bench
db_dir=../db
data_dir=../data/micro

go_bench_dir=../micro-bench-go
go_bench=$go_bench_dir/micro-bench

# PrunusDB lives in the sibling arbora repo; override with PRUNUSDB_DIR if elsewhere.
prunusdb_dir=${PRUNUSDB_DIR:-../../arbora/prunus/prunusdb}
prunusdb=$prunusdb_dir/target/release/examples/micro-bench

cache_size=${1:-16384}   # MB
batch_size=${2:-2000}
val_size=${3:-200}       # bytes
# Systems to populate (space-separated), e.g. SYSTEMS=prunus ./micro-bench-init.sh
systems=${SYSTEMS:-"ficus geth chainkv prunus"}
# Dataset sizes to run (space-separated), e.g. KEY_SIZES=20m
key_sizes=${KEY_SIZES:-"20m 100m"}

has_key_size() {
    [[ " $key_sizes " == *" $1 "* ]]
}

has_system() {
    [[ " $systems " == *" $1 "* ]]
}

ensure_keys() {
    mkdir -p $data_dir/
    if [ -f "$data_dir/micro-20m.keys" ] && [ -f "$data_dir/micro-100m.keys" ]; then
        return
    fi
    echo "Downloading micro-bench key-frequency files..."
    curl -o $data_dir/micro-bench-keys.zip https://geth-statedb-ops.s3.us-east-2.amazonaws.com/statedb-ops/micro-bench-keys.zip
    unzip $data_dir/micro-bench-keys.zip -d $data_dir/
    rm -f $data_dir/micro-bench-keys.zip
}

compile_ficus() {
    cd $ficusdb_dir
    cargo build --release --features stats --example micro-bench
    cd -
}

compile_prunus() {
    cd $prunusdb_dir
    cargo build --release --features stats --example micro-bench
    cd -
}

compile_go_bench() {
    cd $go_bench_dir
    go build
    cd -
}

populate_ficus() {
    key_size=$1
    cache_size=$2
    batch_size=$3
    val_size=$4
    dbpath=$db_dir/micro-ficus-$key_size
    wlpath=$data_dir/micro-$key_size.keys
    verpath=$dbpath/vers

    rm -rf $dbpath
    mkdir -p $dbpath
    echo "init $dbpath $wlpath $verpath $cache_size $batch_size $val_size"
    $ficusdb init $dbpath $wlpath $verpath $cache_size $batch_size $val_size

    backup_path=$db_dir/backup-ficus-$key_size
    rm -rf $backup_path
    cp -r $dbpath $backup_path
}

populate_prunus() {
    key_size=$1
    cache_size=$2
    batch_size=$3
    val_size=$4
    dbpath=$db_dir/micro-prunus-$key_size
    wlpath=$data_dir/micro-$key_size.keys
    verpath=$dbpath/vers

    rm -rf $dbpath
    mkdir -p $dbpath
    echo "prunus init $dbpath $wlpath $verpath $cache_size $batch_size $val_size"
    $prunusdb init $dbpath $wlpath $verpath $cache_size $batch_size $val_size

    backup_path=$db_dir/backup-prunus-$key_size
    rm -rf $backup_path
    cp -r $dbpath $backup_path
}

populate_go_bench() {
    dbname=$1
    key_size=$2
    cache_size=$3
    batch_size=$4
    val_size=$5
    dbpath=$db_dir/micro-$dbname-$key_size
    backup_path=$db_dir/backup-$dbname-$key_size
    wlpath=$data_dir/micro-$key_size.keys
    verpath=$dbpath/vers

    rm -rf $dbpath
    mkdir -p $dbpath
    echo "$dbname init $dbpath $wlpath $verpath $cache_size $batch_size $val_size"
    $go_bench $dbname init $dbpath $wlpath $verpath $cache_size $batch_size $val_size

    rm -rf $backup_path
    cp -r $dbpath $backup_path
}

populate_all() {
    for key_size in $key_sizes; do
        has_system ficus   && populate_ficus $key_size $cache_size $batch_size $val_size
        has_system geth    && populate_go_bench geth $key_size $cache_size $batch_size $val_size
        has_system chainkv && populate_go_bench chainkv $key_size $cache_size $batch_size $val_size
        has_system prunus  && populate_prunus $key_size $cache_size $batch_size $val_size
    done
}

generate_trace() {
    nops=50000000
    for key_size in 20m 100m; do
        out=$data_dir/micro-$key_size-50m.ops
        if [ -f "$out" ]; then
            echo "$out exists, skipping"
            continue
        fi
        echo "Generating micro-$key_size-50m.ops"
        python3 generate-bench-trace.py $data_dir/micro-$key_size.keys $nops $out
    done
}

has_system ficus && compile_ficus
{ has_system geth || has_system chainkv; } && compile_go_bench
has_system prunus && compile_prunus
ensure_keys
populate_all
generate_trace
