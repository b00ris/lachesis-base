package perfl

import (
	"time"

	"github.com/ethereum/go-ethereum/common"
)

type Entry struct {
	n    int
	t    time.Duration
	prev time.Time
}

var st = map[string]Entry{}

func Log(typ string, t time.Duration) {
	rec := st[typ]
	rec.n++
	rec.t += t
	if rec.n%10 == 0 && time.Since(rec.prev) >= time.Second {
		println("+++++", typ, common.PrettyDuration(rec.t).String(), common.PrettyDuration(rec.t/time.Duration(rec.n)).String())
		rec.prev = time.Now()
	}
	st[typ] = rec
}
