datadir=../data/statedb-ops/
mkdir -p ${datadir}
blocknum=$1
blockfile=$datadir/block_${1}m_ops.zip

if [ -f $blockfile ]; then
    echo "Block file already exists: $blockfile"
    exit 0
fi

echo "Downloading block_${blocknum}m_ops.zip"
curl https://geth-statedb-ops.s3.us-east-2.amazonaws.com/statedb-ops/block_${blocknum}m_ops.zip -o $blockfile