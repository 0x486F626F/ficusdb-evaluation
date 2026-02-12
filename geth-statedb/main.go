package main

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"fmt"
	"math"
	"math/big"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus/misc"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/state"
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

type StateDBStats struct {
	objCacheHit   int
	objCacheMiss  int
	valCacheDirty int
	valCacheHit   int
	valCacheMiss  int
}

func (s *StateDBStats) Update(statedb *state.StateDB) {
	s.objCacheHit += statedb.ObjCacheHit
	s.objCacheMiss += statedb.ObjCacheMiss
	s.valCacheHit += statedb.ValCacheHit
	s.valCacheMiss += statedb.ValCacheMiss
	s.valCacheDirty += statedb.ValCacheDirty
}

func (s *StateDBStats) PrintStats() {
	objCacheHitRatio := float64(s.objCacheHit) / math.Max(float64(s.objCacheHit+s.objCacheMiss), 1)
	valCacheHitRatio := float64(s.valCacheHit+s.valCacheDirty) /
		math.Max(float64(s.valCacheHit+s.valCacheMiss+s.valCacheDirty), 1)
	fmt.Print("StateDB:\t")
	fmt.Printf("%d\t%d\t%.3f\t", s.objCacheHit, s.objCacheMiss, objCacheHitRatio)
	fmt.Printf("%d\t%d\t%d\t%.3f\t", s.valCacheHit, s.valCacheDirty, s.valCacheMiss, valCacheHitRatio)
	fmt.Println()
}

func (s *StateDBStats) Reset() {
	s.objCacheHit = 0
	s.objCacheMiss = 0
	s.valCacheHit = 0
	s.valCacheHit = 0
	s.valCacheDirty = 0
}

func statedb_benchmark(dbpath, wlpath, hash string, cachesize int) {
	level, err := rawdb.NewLevelDBDatabase(dbpath, cachesize/2, 0, "", false)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer level.Close()
	stateCache := state.NewDatabaseWithConfig(level, &trie.Config{
		Cache:     cachesize / 2,
		Journal:   defaultCacheConfig.TrieCleanJournal,
		Preimages: defaultCacheConfig.Preimages,
	})

	statedb, err := state.New(common.HexToHash(hash), stateCache, nil)
	if err != nil {
		fmt.Println(err)
		return
	}
	statedbStats := StateDBStats{}

	file, _ := os.Open(wlpath)
	defer file.Close()
	scanner := bufio.NewScanner(file)

	timer := time.Now()
	opget := 0
	opput := 0
	opcnt := 0
	t_get := float64(0)
	t_put := float64(0)
	t_commit := float64(0)
	t_trie_commit := float64(0)
	blocknum := 0
	for scanner.Scan() {
		s := strings.Split(scanner.Text(), " ")

		if s[0] == "blockid" {
			blocknum, _ = strconv.Atoi(s[1])
			if blocknum == 1920000 {
				misc.ApplyDAOHardFork(statedb)
			}
		}

		if s[0] == "newstatedb" {
			hash := common.HexToHash(s[1])
			statedb, err = state.New(hash, stateCache, nil)
			if err != nil {
				fmt.Println(err)
				return
			}
		}

		if s[0] == "snapshot" {
			_ = statedb.Snapshot()
			/*
				id := statedb.Snapshot()
				fmt.Println("snapshot", id)
					if strconv.Itoa(id) != s[1] {
						fmt.Println("snaperror", blocknum, id, s[1])
						return
					}
			*/
		}

		if s[0] == "revertsnapshot" {
			revid, _ := strconv.Atoi(s[1])
			statedb.RevertToSnapshot(revid)
		}

		if s[0] == "commit" {
			substart := time.Now()
			deleteEmptyObject := false
			if s[1] == "true" {
				deleteEmptyObject = true
			}
			statedbStats.Update(statedb)
			root, err := statedb.Commit(deleteEmptyObject)
			if err != nil {
				fmt.Println("statedb commit err", err)
				return
			}
			t_commit += time.Since(substart).Seconds()
			substart = time.Now()

			/*
				fmt.Println("commit", blocknum, hex.EncodeToString(root.Bytes()))
				if hex.EncodeToString(root.Bytes()) != s[3][2:66] {
					fmt.Println("hash", blocknum, hex.EncodeToString(root.Bytes()), s[3])
					return
				}
			*/

			triedb := stateCache.TrieDB()
			if err := triedb.Commit(root, false, nil); err != nil {
				fmt.Println(err)
			}
			t_trie_commit += time.Since(substart).Seconds()

			blocknum += 1

			if blocknum%10000 == 0 {
				elapsed := time.Since(timer)
				cmd := exec.Command("du", "-s", dbpath)
				var out bytes.Buffer
				cmd.Stdout = &out
				cmd.Run()
				dbsize := strings.Split(out.String(), "\t")[0]
				opcnt = opget + opput
				fmt.Printf("%d\t%.3f\t%d\t%d\t%.3f\t%s\n",
					blocknum,
					elapsed.Seconds(), opget, opput, float64(opcnt)/elapsed.Seconds(), dbsize)
				fmt.Printf("time %.3f\t%.3f\t%.3f\t%.3f\n", t_get, t_put, t_commit, t_trie_commit)

				statedbStats.PrintStats()
				statedbStats.Reset()
				stateCache.PrintStats()

				opcnt = 0
				opget = 0
				opput = 0
				t_get = 0
				t_put = 0
				t_commit = 0
				t_trie_commit = 0
				timer = time.Now()
			}
		}

		if s[0] == "addbalance" || s[0] == "subbalance" {
			substart := time.Now()
			addr := common.HexToAddress(s[1])
			amount := new(big.Int)
			amount.SetString(s[2], 10)
			if s[0] == "addbalance" {
				//fmt.Println("addbalance", hex.EncodeToString(addr.Bytes()), amount)
				statedb.AddBalance(addr, amount)
			}
			if s[0] == "subbalance" {
				//fmt.Println("subbalance", hex.EncodeToString(addr.Bytes()), amount)
				statedb.SubBalance(addr, amount)
			}
			t_put += time.Since(substart).Seconds()
			opput += 1
		}

		if s[0] == "createaccount" || s[0] == "removeaccount" {
			addr := common.HexToAddress(s[1])
			if s[0] == "createaccount" {
				//fmt.Println("createaccount", hex.EncodeToString(addr.Bytes()))
				statedb.CreateAccount(addr)
			}
			if s[0] == "removeaccount" {
				//fmt.Println("suicide", hex.EncodeToString(addr.Bytes()))
				statedb.Suicide(addr)
			}
		}

		if s[0] == "setcode" {
			substart := time.Now()
			addr := common.HexToAddress(s[1])
			var code []byte
			if len(s) > 2 {
				code, _ = hex.DecodeString(s[2])
			}
			statedb.SetCode(addr, code)
			//fmt.Println("setcode", hex.EncodeToString(addr.Bytes()), hex.EncodeToString(code))
			t_put += time.Since(substart).Seconds()
			opput += 1
		}

		if s[0] == "setnonce" {
			substart := time.Now()
			addr := common.HexToAddress(s[1])
			nonce, _ := strconv.ParseUint(s[2], 10, 64)
			statedb.SetNonce(addr, nonce)
			//fmt.Println("setnonce", hex.EncodeToString(addr.Bytes()), nonce)
			t_put += time.Since(substart).Seconds()
			opput += 1
		}

		if s[0] == "setstate" {
			substart := time.Now()
			addr := common.HexToAddress(s[1])
			hash := common.HexToHash(s[2])
			value := common.HexToHash(s[3])

			statedb.SetState(addr, hash, value)
			t_put += time.Since(substart).Seconds()
			opput += 1
		}

		if s[0] == "getcodehash" {
			substart := time.Now()
			addr := common.HexToAddress(s[1])
			statedb.GetCodeHash(addr)
			t_get += time.Since(substart).Seconds()
			opget += 1
		}
		if s[0] == "getnonce" {
			substart := time.Now()
			addr := common.HexToAddress(s[1])
			statedb.GetNonce(addr)
			t_get += time.Since(substart).Seconds()
			opget += 1
		}
		if s[0] == "getbalance" {
			substart := time.Now()
			addr := common.HexToAddress(s[1])
			statedb.GetBalance(addr)
			t_get += time.Since(substart).Seconds()
			opget += 1
		}
		if s[0] == "getstate" {
			substart := time.Now()
			addr := common.HexToAddress(s[1])
			hash := common.HexToHash(s[2])
			statedb.GetState(addr, hash)
			t_get += time.Since(substart).Seconds()
			opget += 1
		}
		if s[0] == "finalise" {
			statedb.Finalise(s[1][:4] == "true")
		}
	}
}

func main() {
	if len(os.Args) >= 4 {
		dbpath := os.Args[1]
		wlpath := os.Args[2]
		cachesize, _ := strconv.Atoi(os.Args[3])
		hash := ""
		if len(os.Args) == 5 {
			hash = os.Args[4]
		}

		statedb_benchmark(dbpath, wlpath, hash, cachesize)
	}
}
