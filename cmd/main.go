package main

import (
	"math/rand"
	"os"
	"strconv"

	"github.com/Fantom-foundation/lachesis-base/common/bigendian"
	"github.com/Fantom-foundation/lachesis-base/kvdb"
	"github.com/Fantom-foundation/lachesis-base/kvdb/leveldb"
	"github.com/Fantom-foundation/lachesis-base/kvdb/pebble"
)

func main() {
	p := "/tmp/111"
	err := os.RemoveAll(p)
	if err != nil {
		panic(err)
	}

	lastCh := os.Args[2][len(os.Args[2])-1]
	str := os.Args[2][:len(os.Args[2])-1]
	size, err := strconv.ParseInt(str, 10, 64)
	if err != nil {
		panic(err)
	}
	if lastCh == 'k' {
		size *= 1024
	} else if lastCh == 'm' {
		size *= 1024 * 1024
	} else if lastCh == 'g' {
		size *= 1024 * 1024 * 1024
	} else {
		panic("suffix")
	}
	println(os.Args[1], size)

	println("open")
	var db kvdb.Store
	if os.Args[1] == "ldb" {
		db, err = leveldb.New(p, int(size), 50000, nil, nil)
	} else {
		db, err = pebble.New(p, int(size), 50000, nil, nil)
	}
	println("opened")
	if err != nil {
		panic(err)
	}

	b := make([]byte, 64)
	for i := 0; ; i++ {
		rand.Read(b)
		db.Put(bigendian.Uint32ToBytes(uint32(i)), b)
		db.Get(bigendian.Uint32ToBytes(rand.Uint32() % (uint32(i) + 1)))
		it := db.NewIterator(nil, nil)
		for j := 0; j < 5; j++ {
			it.Next()
		}
		it.Release()
	}
}
