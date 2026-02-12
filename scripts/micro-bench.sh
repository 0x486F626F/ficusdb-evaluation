ficusdb_dir=../ficusdb
ficusdb=$ficusdb_dir/target/release/examples/micro-bench
go_bench_dir=../micro-bench-go
go_bench=$go_bench_dir/micro-bench
db_dir=../db
data_dir=../data/micro
log_dir=../logs/

mkdir -p $log_dir/get
mkdir -p $log_dir/put
mkdir -p $log_dir/vget
mkdir -p $log_dir/lru

versions=2000

ficus_compile() {
    cd $ficusdb_dir
    cargo build --release --features stats --example micro-bench
    cd -
}

compile_go_bench() {
    cd $go_bench_dir
    go build
    cd -
}

ficus_get() {
    key_size=$1
    ops_size=$2
    cache_size=$3
    batch_size=$4
    dbpath=$db_dir/micro-ficus-$key_size
    wlpath=$data_dir/micro-$key_size-$ops_size.ops
    verpath=$dbpath/vers
    logpath=$log_dir/get/micro-get-ficus-$key_size-$ops_size-$cache_size.log

    $ficusdb get $dbpath $wlpath $verpath $cache_size $batch_size > $logpath
}

ficus_put() {
    key_size=$1
    ops_size=$2
    cache_size=$3
    batch_size=$4
    val_size=$5
    versions=$6
    dbpath=$db_dir/micro-ficus-$key_size
    wlpath=$data_dir/micro-$key_size-$ops_size.ops
    verpath=$dbpath/vers
    logpath=$log_dir/put/micro-put-ficus-$key_size-$ops_size-$cache_size-$val_size-$batch_size.log

    $ficusdb put $dbpath $wlpath $verpath $cache_size $batch_size $val_size $versions > $logpath

    backup_path=$db_dir/backup-ficus-$key_size
    rm -rf $dbpath
    cp -r  $backup_path $dbpath
}

ficus_vget() {
    key_size=$1
    ops_size=$2
    cache_size=$3
    batch_size=$4
    dbpath=$db_dir/micro-ficus-$key_size
    wlpath=$data_dir/micro-$key_size-$ops_size.ops
    verpath=$dbpath/vers
    logpath=$log_dir/vget/micro-vget-ficus-$key_size-$ops_size-$cache_size.log

    $ficusdb vget $dbpath $wlpath $verpath $cache_size $batch_size > $logpath
}

go_get() {
    dbname=$1
    key_size=$2
    ops_size=$3
    cache_size=$4
    batch_size=$5
    dbpath=$db_dir/micro-$dbname-$key_size
    wlpath=$data_dir/micro-$key_size-$ops_size.ops
    verpath=$dbpath/vers
    logpath=$log_dir/get/micro-get-$dbname-$key_size-$ops_size-$cache_size.log

    $go_bench $dbname get $dbpath $wlpath $verpath $cache_size $batch_size > $logpath
}

go_vget() {
    dbname=$1
    key_size=$2
    ops_size=$3
    cache_size=$4
    batch_size=$5
    dbpath=$db_dir/micro-$dbname-$key_size
    wlpath=$data_dir/micro-$key_size-$ops_size.ops
    verpath=$dbpath/vers
    logpath=$log_dir/vget/micro-vget-$dbname-$key_size-$ops_size-$cache_size.log

    $go_bench $dbname vget $dbpath $wlpath $verpath $cache_size $batch_size > $logpath
}

go_put() {
    dbname=$1
    key_size=$2
    ops_size=$3
    cache_size=$4
    batch_size=$5
    val_size=$6
    versions=$7
    dbpath=$db_dir/micro-$dbname-$key_size
    wlpath=$data_dir/micro-$key_size-$ops_size.ops
    verpath=$dbpath/vers
    logpath=$log_dir/put/micro-put-$dbname-$key_size-$ops_size-$cache_size-$val_size-$batch_size.log

    $go_bench $dbname put $dbpath $wlpath $verpath $cache_size $batch_size $val_size $versions > $logpath

    backup_path=$db_dir/backup-$dbname-$key_size
    rm -rf $dbpath
    cp -r  $backup_path $dbpath
}

ficus_expr() {
    ficus_compile
    key_size=$1

    dbpath=$db_dir/micro-ficus-$key_size
    backup_path=$db_dir/backup-ficus-$key_size
    rm -rf $dbpath
    cp -r  $backup_path $dbpath

    for cache_size in 1024 2048 4096 8192 16384; do
       echo "ficus_put $key_size 50m $cache_size 100000"
       ficus_put $key_size 50m $cache_size 2000 200 $versions
    done

    for cache_size in 1024 2048 4096 8192 16384; do
       echo "ficus_get $key_size 50m $cache_size 2000 200 $versions"
       ficus_get $key_size 50m $cache_size 100000
    done

    wlpath=$data_dir/micro-$key_size-50m.ops
    verpath=$dbpath/vers
    echo "populating versions..."
    $ficusdb put $dbpath $wlpath $verpath 16384 2000 200 $versions > /dev/null

    for cache_size in 1024 2048 4096 8192 16384; do
        echo "ficus_vget $key_size 50m $cache_size 2000 200 $versions"
        ficus_vget $key_size 50m $cache_size 10000
    done
}

go_expr() {
    compile_go_bench
    dbname=$1
    key_size=$2
    dbpath=$db_dir/micro-$dbname-$key_size
    backup_path=$db_dir/backup-$dbname-$key_size
    rm -rf $dbpath
    cp -r  $backup_path $dbpath

    for cache_size in 1024 2048 4096 8192 16384; do
        echo "go_put $dbname $key_size 50m $cache_size 2000 200 $versions"
        go_put $dbname $key_size 50m $cache_size 2000 200 $versions
    done

    for cache_size in 1024 2048 4096 8192 16384; do
        echo "go_get $dbname $key_size 50m $cache_size 100000"
        go_get $dbname $key_size 50m $cache_size 100000
    done

    wlpath=$data_dir/micro-$key_size-50m.ops
    verpath=$dbpath/vers
    echo "populating versions..."
    $go_bench $dbname put $dbpath $wlpath $verpath 16384 2000 200 $versions > /dev/null

    for cache_size in 1024 2048 4096 8192 16384; do
        echo "go_vget $dbname $key_size 50m $cache_size 10000"
        go_vget $dbname $key_size 50m $cache_size 10000
    done
}

ficus_batch() {
    ficus_compile
    key_size=$1

    dbpath=$db_dir/micro-ficus-$key_size
    backup_path=$db_dir/backup-ficus-$key_size
    rm -rf $dbpath
    cp -r  $backup_path $dbpath

    for batch_size in 500 2000 8000 32000; do
        for val_size in 50 200 800 3200; do
            echo "ficus_put $key_size 50m 16384 $batch_size $val_size $versions"
            ficus_put $key_size 50m 16384 $batch_size $val_size $versions
        done
    done
}

ficus_lru() {
    cd $ficusdb_dir
    cargo build --release --features stats --features lru --example micro-bench
    cd -

    ops_size=50m
    batch_size=100000
    
    for cache_size in 1024 2048 4096 8192 16384; do
        logpath=$log_dir/lru/micro-lru-ficus-20m-50m-$cache_size.log
        wlpath=$data_dir/micro-20m-50m.ops
        dbpath=$db_dir/micro-ficus-20m
        verpath=$dbpath/vers
        $ficusdb vget $dbpath $wlpath $verpath $cache_size $batch_size > $logpath
    done

    for cache_size in 1024 2048 4096 8192 16384; do
        logpath=$log_dir/lru/micro-lru-ficus-100m-50m-$cache_size.log
        wlpath=$data_dir/micro-100m-50m.ops
        dbpath=$db_dir/micro-ficus-100m
        verpath=$dbpath/vers
        $ficusdb vget $dbpath $wlpath $verpath $cache_size $batch_size > $logpath
    done
}

ficus_expr 20m
ficus_expr 100m
go_expr geth 20m
go_expr geth 100m
go_expr chainkv 20m
go_expr chainkv 100m
ficus_batch 20m
ficus_lru