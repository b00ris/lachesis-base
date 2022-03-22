package backed

import (
	"io/ioutil"
	"math/big"
	"math/rand"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/assert"
	"github.com/syndtr/goleveldb/leveldb/opt"

	"github.com/Fantom-foundation/lachesis-base/kvdb"
	"github.com/Fantom-foundation/lachesis-base/kvdb/leveldb"
	"github.com/Fantom-foundation/lachesis-base/kvdb/memorydb"
	"github.com/Fantom-foundation/lachesis-base/kvdb/table"
)

func TestFlushable(t *testing.T) {
	assertar := assert.New(t)

	tries := 60            // number of test iterations
	opsPerIter := 0x140    // max number of put/delete ops per iteration
	dictSize := opsPerIter // number of different words

	disk := dbProducer("TestFlushable")

	// open raw databases
	leveldb1, _ := disk.OpenDB("1")
	defer leveldb1.Drop()
	defer leveldb1.Close()

	leveldb2, _ := disk.OpenDB("2")
	defer leveldb2.Drop()
	defer leveldb2.Close()

	leveldb3, _ := disk.OpenDB("3")
	defer leveldb3.Drop()
	defer leveldb3.Close()

	// create wrappers
	dbs := map[string]kvdb.Store{
		"leveldb": leveldb1,
		"memory":  memorydb.New(),
	}

	flushableDbs := map[string]*Store{
		"leveldb-over-leveldb": Wrap(leveldb3, leveldb2),
		"memory-over-memory":  Wrap(memorydb.New(), memorydb.New()),
	}

	baseLdb := table.New(dbs["leveldb"], []byte{})
	baseMem := table.New(dbs["memory"], []byte{})

	dbsTables := [][]kvdb.Store{
		{
			dbs["leveldb"],
			baseLdb.NewTable([]byte{0, 1}),
			baseLdb.NewTable([]byte{0}).NewTable(common.Hex2Bytes("ffffffffffffffffffffffffffffffffffff")),
		},
		{
			dbs["memory"],
			baseMem.NewTable([]byte{0, 1}),
			baseMem.NewTable([]byte{0}).NewTable(common.Hex2Bytes("ffffffffffffffffffffffffffffffffffff")),
		},
	}

	baseLdb = table.New(flushableDbs["leveldb-over-leveldb"], []byte{})
	baseMem = table.New(flushableDbs["memory-over-memory"], []byte{})
	flushableDbsTables := [][]kvdb.Store{
		{
			flushableDbs["leveldb-over-leveldb"],
			baseLdb.NewTable([]byte{0, 1}),
			baseLdb.NewTable([]byte{0}).NewTable(common.Hex2Bytes("ffffffffffffffffffffffffffffffffffff")),
		},
		{
			flushableDbs["memory-over-memory"],
			baseMem.NewTable([]byte{0, 1}),
			baseMem.NewTable([]byte{0}).NewTable(common.Hex2Bytes("ffffffffffffffffffffffffffffffffffff")),
		},
	}

	assertar.Equal(len(dbsTables), len(flushableDbsTables))
	assertar.Equal(len(dbsTables[0]), len(flushableDbsTables[0]))

	groupsNum := len(dbsTables)
	tablesNum := len(dbsTables[0])

	// use the same seed for determinism
	rand := rand.New(rand.NewSource(0))

	// words dictionary
	prefixes := [][]byte{
		{},
		{0},
		{0x1},
		{0x22},
		{0x33},
		{0x11},
		{0x11, 0x22},
		{0x11, 0x23},
		{0x11, 0x22, 0x33},
		{0x11, 0x22, 0x34},
	}
	dict := [][]byte{}
	for i := 0; i < dictSize; i++ {
		b := append(prefixes[i%len(prefixes)], big.NewInt(rand.Int63()).Bytes()...)
		dict = append(dict, b)
	}

	for try := 0; try < tries; try++ {

		// random put/delete operations
		putDeleteRandom := func() {
			for j := 0; j < tablesNum; j++ {
				var batches []kvdb.Batch
				for i := 0; i < groupsNum; i++ {
					batches = append(batches, dbsTables[i][j].NewBatch())
					batches = append(batches, flushableDbsTables[i][j].NewBatch())
				}

				ops := 1 + rand.Intn(opsPerIter)
				for p := 0; p < ops; p++ {
					var pair kv
					if rand.Intn(2) == 0 { // put
						pair = kv{
							k: dict[rand.Intn(len(dict))],
							v: dict[rand.Intn(len(dict))],
						}
					} else { // delete
						pair = kv{
							k: dict[rand.Intn(len(dict))],
							v: nil,
						}
					}

					for _, batch := range batches {
						if pair.v != nil {
							assertar.NoError(batch.Put(pair.k, pair.v))
						} else {
							assertar.NoError(batch.Delete(pair.k))
						}
					}
				}

				for _, batch := range batches {
					size := batch.ValueSize()
					assertar.NotEqual(0, size)
					assertar.NoError(batch.Write())
					assertar.Equal(size, batch.ValueSize())
					batch.Reset()
					assertar.Equal(0, batch.ValueSize())
				}
			}
		}
		// put/delete values
		putDeleteRandom()

		// flush
		for _, db := range flushableDbs {
			it := db.diff.NewIterator(nil, nil)
			for it.Next() {
				if it.Value()[0] != 0 {
					assertar.NoError(db.back.Put(it.Key(), it.Value()[1:]))
				} else {
					assertar.NoError(db.back.Delete(it.Key()))
				}
				assertar.NoError(db.diff.Delete(it.Key()))
			}
			it.Release()
		}

		// put/delete values (not flushed)
		putDeleteRandom()

		// try to ForEach random prefix
		prefix := prefixes[try%len(prefixes)]
		if try == 1 {
			prefix = []byte{0, 0, 0, 0, 0, 0} // not existing prefix
		}

		for j := 0; j < tablesNum; j++ {
			expectPairs := []kv{}

			testForEach := func(db kvdb.Store, first bool) {

				var it kvdb.Iterator
				if try%4 == 0 {
					it = db.NewIterator(nil, nil)
				} else if try%4 == 1 {
					it = db.NewIterator(prefix, nil)
				} else if try%4 == 2 {
					it = db.NewIterator(nil, prefix)
				} else {
					it = db.NewIterator(prefix[:len(prefix)/2], prefix[len(prefix)/2:])
				}
				defer it.Release()

				var got int

				for got = 0; it.Next(); got++ {
					if first {
						expectPairs = append(expectPairs, kv{
							k: common.CopyBytes(it.Key()),
							v: common.CopyBytes(it.Value()),
						})
					} else {
						assertar.NotEqual(len(expectPairs), got, try) // check that we've for the same num of values
						if t.Failed() {
							return
						}
						assertar.Equal(expectPairs[got].k, it.Key(), try)
						assertar.Equal(expectPairs[got].v, it.Value(), try)
					}
				}

				if !assertar.NoError(it.Error()) {
					return
				}

				assertar.Equal(len(expectPairs), got) // check that we've got the same num of pairs
			}

			// check that all groups return the same result
			for i := 0; i < groupsNum; i++ {
				testForEach(dbsTables[i][j], i == 0)
				if t.Failed() {
					return
				}
				testForEach(flushableDbsTables[i][j], false)
				if t.Failed() {
					return
				}
			}
		}

		// try to get random values
		ops := rand.Intn(opsPerIter)
		for p := 0; p < ops; p++ {
			key := dict[rand.Intn(len(dict))]

			for j := 0; j < tablesNum; j++ {
				// get values for first group, so we could check that all groups return the same result
				ok, _ := dbsTables[0][j].Has(key)
				vl, _ := dbsTables[0][j].Get(key)

				// check that all groups return the same result
				for i := 0; i < groupsNum; i++ {
					ok1, err := dbsTables[i][j].Has(key)
					assertar.NoError(err)
					vl1, err := dbsTables[i][j].Get(key)
					assertar.NoError(err)

					ok2, err := flushableDbsTables[i][j].Has(key)
					assertar.NoError(err)
					vl2, err := flushableDbsTables[i][j].Get(key)
					assertar.NoError(err)

					assertar.Equal(ok1, ok2)
					assertar.Equal(vl1, vl2)
					assertar.Equal(ok1, ok)
					assertar.Equal(vl1, vl)
				}
			}
		}

		if t.Failed() {
			return
		}
	}
}

func cache16mb(string) int {
	return 16 * opt.MiB
}

func dbProducer(name string) kvdb.DBProducer {
	dir, err := ioutil.TempDir("", name)
	if err != nil {
		panic(err)
	}
	return leveldb.NewProducer(dir, cache16mb)
}
