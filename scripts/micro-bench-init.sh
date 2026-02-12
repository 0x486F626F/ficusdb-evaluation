ficusdb_dir=../ficusdb
ficusdb=$ficusdb_dir/target/release/examples/micro-bench
db_dir=../db
data_dir=../data/micro

go_bench_dir=../micro-bench-go
go_bench=$go_bench_dir/micro-bench

cache_size=${1:-16384}   # MB
batch_size=${2:-2000}
val_size=${3:-200}       # bytes

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
    populate_ficus 20m $cache_size $batch_size $val_size
    populate_go_bench geth 20m $cache_size $batch_size $val_size
    populate_go_bench chainkv 20m $cache_size $batch_size $val_size

    populate_ficus 100m $cache_size $batch_size $val_size
    populate_go_bench geth 100m $cache_size $batch_size $val_size
    populate_go_bench chainkv 100m $cache_size $batch_size $val_size
}

generate_trace() {
    nops=50000000
    echo "Generating micro-20m-50m.ops"
    python3 generate-bench-trace.py $data_dir/micro-20m.keys $nops $data_dir/micro-20m-50m.ops
    echo "Generating micro-100m-50m.ops"
    python3 generate-bench-trace.py $data_dir/micro-100m.keys $nops $data_dir/micro-100m-50m.ops
}

compile_ficus
compile_go_bench
ensure_keys
populate_all
generate_trace
