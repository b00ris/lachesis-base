package backed

import (
	"github.com/Fantom-foundation/lachesis-base/kvdb"
)

type Producer struct {
	diff    kvdb.IterableDBProducer
	back    kvdb.IterableDBProducer
	backDBs map[string]bool
}

// NewProducer of level db.
func NewProducer(diff, back kvdb.IterableDBProducer) kvdb.IterableDBProducer {
	backDBs := make(map[string]bool)
	for _, name := range back.Names() {
		backDBs[name] = true
	}
	return &Producer{
		diff:    diff,
		back:    back,
		backDBs: backDBs,
	}
}

// Names of existing databases.
func (p *Producer) Names() []string {
	return p.diff.Names()
}

// OpenDB or create db with name.
func (p *Producer) OpenDB(name string) (kvdb.DropableStore, error) {
	if !p.backDBs[name] {
		return p.diff.OpenDB(name)
	}
	diffDB, err := p.diff.OpenDB(name)
	if err != nil {
		return nil, err
	}
	backDB, err := p.back.OpenDB(name)
	if err != nil {
		return nil, err
	}
	return Wrap(diffDB, backDB), nil
}
