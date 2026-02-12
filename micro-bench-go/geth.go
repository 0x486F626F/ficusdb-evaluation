package main

import (
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/trie"
)

type CacheConfig struct {
	TrieCleanLimit      int           // Memory allowance (MB) to use for caching trie nodes in memory
	TrieCleanJournal    string        // Disk journal for saving clean cache entries.
	TrieCleanRejournal  time.Duration // Time interval to dump clean cache to disk periodically
	TrieCleanNoPrefetch bool          // Whether to disable heuristic state prefetching for followup blocks
	TrieDirtyLimit      int           // Memory limit (MB) at which to start flushing dirty trie nodes to disk
	TrieDirtyDisabled   bool          // Whether to disable trie write caching and GC altogether (archive node)
	TrieTimeLimit       time.Duration // Time limit after which to flush the current in-memory trie to disk
	SnapshotLimit       int           // Memory allowance (MB) to use for caching snapshot entries in memory
	Preimages           bool          // Whether to store preimage of trie key to the disk

	SnapshotWait bool // Wait for snapshot construction on startup. TODO(karalabe): This is a dirty hack for testing, nuke it
}

// defaultCacheConfig are the default caching values if none are specified by the
// user (also used during testing).
var defaultCacheConfig = &CacheConfig{
	TrieCleanLimit:   16 * 1024,
	TrieDirtyLimit:   4 * 1024,
	TrieTimeLimit:    1 * time.Second,
	TrieCleanJournal: "triejournal",
	SnapshotLimit:    256,
	SnapshotWait:     true,
}
var CacheSize = common.StorageSize(defaultCacheConfig.TrieDirtyLimit * 1024 * 1024)

type Geth struct {
	level      ethdb.Database
	stateCache state.Database
	trie       state.Trie
	path       string
	cachesize  int
}

func NewGeth(dbpath string, cachesize int) *Geth {
	level, err := rawdb.NewLevelDBDatabase(dbpath, cachesize/2, 0, "", false)
	if err != nil {
		panic(err)
	}
	stateCache := state.NewDatabaseWithConfig(level, &trie.Config{
		Cache:     cachesize / 2,
		Journal:   defaultCacheConfig.TrieCleanJournal,
		Preimages: defaultCacheConfig.Preimages,
	})
	trie, err := stateCache.OpenTrie(common.Hash{})
	if err != nil {
		panic(err)
	}
	return &Geth{level: level, stateCache: stateCache, trie: trie, path: dbpath, cachesize: cachesize}
}

func (g *Geth) Open(root common.Hash) {
	trie, err := g.stateCache.OpenTrie(root)
	if err != nil {
		panic(err)
	}
	g.trie = trie
}

func (g *Geth) Commit() common.Hash {
	root, err := g.trie.Commit(nil)
	if err != nil {
		panic(err)
	}
	if err := g.stateCache.TrieDB().Commit(root, false, nil); err != nil {
		panic(err)
	}
	return root
}

func (g *Geth) Get(key []byte) []byte {
	val, err := g.trie.TryGet(key)
	if err != nil {
		panic(err)
	}
	return val
}

func (g *Geth) Set(key []byte, value []byte) {
	g.trie.TryUpdate(key, value)
}

func (g *Geth) Reopen() {
	g.level.Close()
	level, err := rawdb.NewLevelDBDatabase(g.path, g.cachesize/2, 0, "", false)
	if err != nil {
		panic(err)
	}
	g.level = level
}

func (g *Geth) Version() common.Hash {
	return g.trie.Hash()
}

func (g *Geth) PrintStats() {
	g.trie.PrintStats()
}
