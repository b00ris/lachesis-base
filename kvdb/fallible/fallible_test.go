package fallible

import (
	"math/rand"
	"os"
	"testing"

	"github.com/syndtr/goleveldb/leveldb/opt"

	"github.com/Fantom-foundation/lachesis-base/common/bigendian"
	"github.com/Fantom-foundation/lachesis-base/kvdb/pebble"
	"github.com/Fantom-foundation/lachesis-base/utils/piecefunc"
)

var adjustCache = piecefunc.NewFunc([]piecefunc.Dot{
	{
		X: 0,
		Y: 16 * opt.KiB,
	},
	{
		X: 12 * opt.MiB,
		Y: 100 * opt.KiB,
	},
	{
		X: 15 * opt.MiB,
		Y: 1 * opt.MiB,
	},
	{
		X: 47 * opt.MiB,
		Y: 10 * opt.MiB,
	},
	{
		X: 69 * opt.MiB,
		Y: 14 * opt.MiB,
	},
	{
		X: 88 * opt.MiB,
		Y: 18 * opt.MiB,
	},
	{
		X: 190 * opt.MiB,
		Y: 40 * opt.MiB,
	},
	{
		X: 350 * opt.MiB,
		Y: 100 * opt.MiB,
	},
	{
		X: 930 * opt.MiB,
		Y: 300 * opt.MiB,
	},
	{
		X: 3300 * opt.MiB,
		Y: 1000 * opt.MiB,
	},
	{
		X: 6400000 * opt.MiB,
		Y: 2000000 * opt.MiB,
	},
})

func TestFallible(t *testing.T) {
	p := "/tmp/111"
	os.RemoveAll(p)
	db, _ := pebble.New(p, 46 * opt.MiB, 50000, nil, nil)
	b := make([]byte, 64)
	for i := 0; ; i++ {
		if i % 10000 == 0 {
			println(i)
		}
		rand.Read(b)
		db.Put(bigendian.Uint32ToBytes(uint32(i)), b)
		db.Get(bigendian.Uint32ToBytes(rand.Uint32() % (uint32(i) + 1)))
	}
}
