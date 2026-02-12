datapath=../data/statedb-ops
logpath=../logs/statedb
ficusdb_path=../ficusdb/
mkdir -p $logpath

cd $ficusdb_path
cargo build --release --features stats --example statedb
cd -

echo "Unzipping block 11m and 12m"
unzip $datapath/block_11m_ops.zip -d $datapath
unzip $datapath/block_12m_ops.zip -d $datapath

eval_ficus_statedb() {
    cache_size=$1
    dbpath=../db/ficus-statedb
    ficusdb_path=../ficusdb/
    ficusdb=$ficusdb_path/target/release/examples/statedb
    eval_dbpath=../db/ficus-statedb-eval

    rm -rf $eval_dbpath
    cp -r $dbpath $eval_dbpath

    echo "Running FicusDB with $cache_size cache limit"
    rm -f /tmp/statedb-ops-fifo
    mkfifo /tmp/statedb-ops-fifo
    cat $datapath/block_11m.ops $datapath/block_12m.ops > /tmp/statedb-ops-fifo &
    $ficusdb $eval_dbpath /tmp/statedb-ops-fifo $cache_size > $logpath/ficus-statedb-$cache_size.log

    rm /tmp/statedb-ops-fifo
}

eval_geth_statedb() {
    cache_size=$1
    geth_path=../geth-statedb/
    geth=$geth_path/go-ethereum
    datapath=../data/statedb-ops
    dbpath=../db/geth-statedb
    eval_dbpath=../db/geth-statedb-eval

    rm -rf $eval_dbpath
    cp -r $dbpath $eval_dbpath

    echo "Running Geth with $cache_size cache limit"
    rm -f /tmp/statedb-ops-fifo
    mkfifo /tmp/statedb-ops-fifo
    cat $datapath/block_11m.ops $datapath/block_12m.ops > /tmp/statedb-ops-fifo &
    $geth $eval_dbpath /tmp/statedb-ops-fifo $cache_size > $logpath/geth-statedb-$cache_size.log

    rm /tmp/statedb-ops-fifo
}

eval_ficus_statedb 4096
eval_geth_statedb 4096
eval_ficus_statedb 32768
eval_geth_statedb 32768