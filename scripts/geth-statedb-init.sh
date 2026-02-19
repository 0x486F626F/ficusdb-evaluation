geth_path=../geth-statedb/
geth=$geth_path/go-ethereum
datapath=../data/statedb-ops
dbpath=../db/geth-statedb
logpath=../logs/statedb
mkdir -p $dbpath
mkdir -p $logpath

cd $geth_path
go build
cd -

for i in {01..10}; do
    echo "block ${i}"
    unzip $datapath/block_${i}m_ops.zip -d $datapath
    $geth $dbpath $datapath/block_${i}m.ops 16384 > $logpath/geth-statedb-init-${i}.log
    rm $datapath/block_${i}m.ops
done
