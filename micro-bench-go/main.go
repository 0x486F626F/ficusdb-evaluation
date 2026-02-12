package main

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"golang.org/x/exp/rand"
	"gonum.org/v1/gonum/stat/distuv"
)

type AuthDB interface {
	Open(root common.Hash)
	Commit() common.Hash
	Get(key []byte) []byte
	Set(key []byte, value []byte)
	Version() common.Hash
	Reopen()
	PrintStats()
}

// randomBytes returns a slice of random bytes of given length
func randomBytes(size int) []byte {
	b := make([]byte, size)
	_, err := rand.Read(b)
	if err != nil {
		panic(err)
	}
	return b
}

func load_versions(verpath string) []common.Hash {
	verfile, _ := os.Open(verpath)
	defer verfile.Close()
	hashes := make([]common.Hash, 0)
	for {
		hash := make([]byte, 32)
		if _, err := verfile.Read(hash); err != nil {
			break
		}
		hashes = append(hashes, common.BytesToHash(hash))
	}
	slices.Reverse(hashes)
	return hashes
}

func bench_init(db AuthDB, wlpath, verpath string, batch_size, val_size int) {
	verfile, _ := os.OpenFile(verpath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	defer verfile.Close()
	file, _ := os.Open(wlpath)
	defer file.Close()
	scanner := bufio.NewScanner(file)
	in_batch := 0
	total_ops := 0
	var final_root common.Hash
	timer := time.Now()
	for scanner.Scan() {
		line := strings.Split(scanner.Text(), " ")
		key, _ := hex.DecodeString(line[0][2:])
		val := make([]byte, 8)
		copy(val, []byte(strconv.FormatUint(uint64(0), 10)))
		val = append(val, randomBytes(val_size)...)
		db.Set(key, val)
		in_batch += 1

		if in_batch >= batch_size {
			final_root = db.Commit()
			fmt.Println("geth commit", final_root.Hex())
			in_batch = 0
			elapsed := time.Since(timer).Seconds()
			trpt := float64(batch_size) / elapsed
			total_ops += batch_size
			timer = time.Now()
			fmt.Println("init", total_ops, elapsed, trpt)
			db.PrintStats()
		}
	}
	if in_batch > 0 {
		final_root = db.Commit()
		fmt.Println("geth commit", final_root.Hex())
	}

	fmt.Println("final root", final_root.Hex())
	verfile.Write(final_root.Bytes())
	verfile.Sync()
}

func bench_put(db AuthDB, wlpath, verpath string, batch_size, val_size, versions int) {
	hs := load_versions(verpath)
	db.Open(hs[0])

	verfile, _ := os.OpenFile(verpath, os.O_APPEND|os.O_WRONLY, 0644)
	defer verfile.Close()
	file, _ := os.Open(wlpath)
	defer file.Close()
	scanner := bufio.NewScanner(file)
	in_batch := 0
	total_ops := 0
	n_batch := 0
	t_ops := 0.0
	for scanner.Scan() {
		line := strings.Split(scanner.Text(), " ")
		key, _ := hex.DecodeString(line[0][2:])
		val := make([]byte, 8)
		copy(val, []byte(strconv.FormatUint(uint64(0), 10)))
		val = append(val, randomBytes(val_size)...)
		t_start := time.Now()
		db.Set(key, val)
		in_batch += 1
		t_ops += time.Since(t_start).Seconds()

		if in_batch >= batch_size {
			t_commit := time.Now()
			root := db.Commit()
			t_ops += time.Since(t_commit).Seconds()
			verfile.Write(root.Bytes())
			in_batch = 0
			trpt := float64(batch_size) / t_ops
			total_ops += batch_size
			fmt.Println("put", total_ops, t_ops, trpt)
			t_ops = 0.0
			db.PrintStats()
			n_batch += 1
			if n_batch >= versions {
				break
			}
			db.Open(root)
		}
	}
	if in_batch > 0 {
		root := db.Commit()
		verfile.Write(root.Bytes())
	}
	verfile.Sync()
}

func bench_get(db AuthDB, wlpath, verpath string, batch_size int) {
	hs := load_versions(verpath)
	db.Open(hs[0])

	file, _ := os.Open(wlpath)
	defer file.Close()
	scanner := bufio.NewScanner(file)
	in_batch := 0
	total_ops := 0
	t_ops := 0.0
	for scanner.Scan() {
		line := strings.Split(scanner.Text(), " ")
		key, _ := hex.DecodeString(line[0][2:])
		t_start := time.Now()
		db.Get(key)
		t_ops += time.Since(t_start).Seconds()
		in_batch += 1

		if in_batch >= batch_size {
			in_batch = 0
			trpt := float64(batch_size) / t_ops
			total_ops += batch_size
			fmt.Println("get", total_ops, t_ops, trpt)
			t_ops = 0.0
			db.PrintStats()
			db.Open(hs[0])
		}
	}
}

func bench_vget(db AuthDB, wlpath, verpath string, batch_size int) {
	hs := load_versions(verpath)
	dist := distuv.Exponential{Rate: 10.0, Src: rand.NewSource(uint64(time.Now().UnixNano()))}

	file, _ := os.Open(wlpath)
	defer file.Close()
	scanner := bufio.NewScanner(file)
	in_batch := 0
	total_ops := 0
	t_ops := 0.0
	for scanner.Scan() {
		line := strings.Split(scanner.Text(), " ")
		key, _ := hex.DecodeString(line[0][2:])
		idx := int(dist.Rand())
		ver := hs[idx]
		t_start := time.Now()
		if ver != db.Version() {
			db.Open(ver)
		}
		db.Get(key)
		t_ops += time.Since(t_start).Seconds()
		in_batch += 1

		if in_batch >= batch_size {
			in_batch = 0
			trpt := float64(batch_size) / t_ops
			total_ops += batch_size
			fmt.Println("vget", total_ops, t_ops, trpt)
			t_ops = 0.0
			db.PrintStats()
		}
	}
}

func main() {
	if len(os.Args) < 8 {
		fmt.Println("usage: micro-bench-go <dbname> <bench> <dbpath> <wlpath> <verpath> <cache_size> <batch_size> [val_size] [versions]")
		os.Exit(1)
	}
	dbname := os.Args[1]
	bench := os.Args[2]
	dbpath := os.Args[3]
	wlpath := os.Args[4]
	verpath := os.Args[5]
	cache_size, _ := strconv.Atoi(os.Args[6])
	batch_size, _ := strconv.Atoi(os.Args[7])

	var db AuthDB
	if dbname == "geth" {
		db = NewGeth(dbpath, cache_size)
	} else if dbname == "chainkv" {
		db = NewChainKV(dbpath, cache_size)
	}

	if bench == "init" {
		val_size, _ := strconv.Atoi(os.Args[8])
		bench_init(db, wlpath, verpath, batch_size, val_size)
	} else if bench == "get" {
		bench_get(db, wlpath, verpath, batch_size)
	} else if bench == "vget" {
		bench_vget(db, wlpath, verpath, batch_size)
	} else if bench == "put" {
		val_size, _ := strconv.Atoi(os.Args[8])
		versions, _ := strconv.Atoi(os.Args[9])
		bench_put(db, wlpath, verpath, batch_size, val_size, versions)
	}
}
