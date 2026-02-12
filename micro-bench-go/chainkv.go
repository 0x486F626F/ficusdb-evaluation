package main

import (
	"chainkv/ethdb"
	"chainkv/trie"

	"github.com/ethereum/go-ethereum/common"
)

type ChainKV struct {
	db        *ethdb.LDBDatabase
	trie      *trie.Trie
	path      string
	cachesize int
}

func NewChainKV(dbpath string, cachesize int) *ChainKV {
	db, err := ethdb.NewLDBDatabase(dbpath, cachesize, 128)
	if err != nil {
		panic(err)
	}
	tr, err := trie.New(common.Hash{}, db)
	if err != nil {
		panic(err)
	}
	return &ChainKV{db: db, trie: tr, path: dbpath, cachesize: cachesize}
}

func (c *ChainKV) Open(root common.Hash) {
	tr, err := trie.New(root, c.db)
	if err != nil {
		panic(err)
	}
	c.trie = tr
}

func (c *ChainKV) Reopen() {
	c.db.Close()
	db, err := ethdb.NewLDBDatabase(c.path, c.cachesize, 128)
	if err != nil {
		panic(err)
	}
	c.db = db
}

func (c *ChainKV) Commit() common.Hash {
	root, _, err := c.trie.Commit()
	if err != nil {
		panic(err)
	}
	c.Open(root)
	return root
}

func (c *ChainKV) Get(key []byte) []byte {
	return c.trie.Get(key)
}

func (c *ChainKV) Set(key []byte, value []byte) {
	c.trie.Update(key, value)
}

func (c *ChainKV) Version() common.Hash {
	return c.trie.Hash()
}

func (c *ChainKV) PrintStats() {
}
