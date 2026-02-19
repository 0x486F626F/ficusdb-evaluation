datapath=../data/statedb-ops
dbpath=../db/ficus-statedb
ficusdb_path=../ficusdb/
ficusdb=$ficusdb_path/target/release/examples/statedb
logpath=../logs/statedb
mkdir -p $dbpath
mkdir -p $logpath

cd $ficusdb_path
cargo build --release --features stats --example statedb
cd -

for i in {01..10}; do
    echo block ${i}
    unzip $datapath/block_${i}m_ops.zip -d $datapath
    $ficusdb $dbpath $datapath/block_${i}m.ops 16384 > $logpath/ficus-statedb-init-${i}.log
    rm $datapath/block_${i}m.ops
done
