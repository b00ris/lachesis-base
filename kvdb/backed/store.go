package backed

import (
	"bytes"
	"errors"

	"github.com/ethereum/go-ethereum/common"

	"github.com/Fantom-foundation/lachesis-base/kvdb"
	"github.com/Fantom-foundation/lachesis-base/kvdb/devnulldb"
)

var (
	errClosed = errors.New("database closed")
)

type Store struct {
	reader
	onDrop func()
	back   kvdb.Store
	diff   kvdb.Store
}

type reader struct {
	back kvdb.IteratedReader

	diff kvdb.IteratedReader
}

// Wrap underlying db.
// All the writes into the cache won't be written in parent until .Flush() is called.
func Wrap(diff, back kvdb.DropableStore) *Store {
	if back == nil {
		panic("nil parent")
	}

	return WrapWithDrop(diff, back, diff.Drop)
}

// WrapWithDrop is the same as Wrap, but defines onDrop callback.
func WrapWithDrop(diff, back kvdb.Store, drop func()) *Store {
	if back == nil {
		panic("nil parent")
	}

	return &Store{
		reader: reader{
			back: back,
			diff: diff,
		},
		onDrop: drop,
		back:   back,
		diff:   diff,
	}
}

/*
 * Database interface implementation
 */

// Put puts key-value pair into the cache.
func (w *Store) Put(key []byte, value []byte) error {
	if w.diff == nil {
		return errClosed
	}
	return w.diff.Put(key, append([]byte{1}, value...))
}

// Has checks if key is in the exists. Looks in diff first, then - in backend.
func (w *reader) Has(key []byte) (bool, error) {
	if w.diff == nil {
		return false, errClosed
	}

	val, err := w.diff.Get(key)
	if err != nil {
		return false, err
	}
	if val != nil {
		return val[0] != 0, nil
	}

	return w.back.Has(key)
}

// Get returns key-value pair by key. Looks in diff first, then - in backend.
func (w *reader) Get(key []byte) ([]byte, error) {
	if w.diff == nil {
		return nil, errClosed
	}

	val, err := w.diff.Get(key)
	if err != nil {
		return nil, err
	}
	if val != nil {
		if val[0] != 0 {
			return val[1:], nil
		}
		return nil, nil
	}
	return w.back.Get(key)
}

// Delete removes key-value pair by key. In parent DB, key won't be deleted until .Flush() is called.
func (w *Store) Delete(key []byte) error {
	if w.diff == nil {
		return errClosed
	}
	return w.diff.Put(key, []byte{0})
}

// Close leaves underlying database.
func (w *Store) Close() error {
	if w.diff == nil {
		return errClosed
	}

	err := w.back.Close()
	if err != nil {
		return err
	}
	err = w.diff.Close()
	if err != nil {
		return err
	}
	w.diff = nil
	w.back = nil
	return nil
}

// Drop whole database.
func (w *Store) Drop() {
	if w.diff != nil {
		panic("close db first")
	}

	if w.onDrop != nil {
		w.onDrop()
	}
}

// Stat returns a particular internal stat of the database.
func (w *Store) Stat(property string) (string, error) {
	return w.diff.Stat(property)
}

// Compact flattens the modified data store for the given key range.
func (w *Store) Compact(start []byte, limit []byte) error {
	return w.diff.Compact(start, limit)
}

/*
 * Iterator
 */

type backedIterator struct {
	key, val []byte
	prevKey  []byte

	diffIt kvdb.Iterator
	diffOk bool

	parentIt kvdb.Iterator
	parentOk bool
}

func castToPair(key, val []byte) ([]byte, []byte) {
	if val == nil {
		return common.CopyBytes(key), nil
	}
	if val[0] != 0 {
		return common.CopyBytes(key), common.CopyBytes(val[1:])
	}
	return common.CopyBytes(key), nil
}

// init should be called once under lock
func (it *backedIterator) init() {
	it.parentOk = it.parentIt.Next()
	it.diffOk = it.diffIt.Next()
}

// Next scans key-value pair by key in lexicographic order. Looks in cache first,
// then - in DB.
func (it *backedIterator) Next() bool {
	if it.Error() != nil {
		return false
	}

	isSuitable := func(key, prevKey []byte) bool {
		return prevKey == nil || bytes.Compare(key, prevKey) > 0
	}

	for it.diffOk || it.parentOk {
		// tree has priority, so check it first
		if it.diffOk {
			diffKey, diffVal := castToPair(it.diffIt.Key(), it.diffIt.Value())
			for it.diffOk && (!it.parentOk || bytes.Compare(diffKey, it.parentIt.Key()) <= 0) {
				// it's not possible that diffKey isn't bigger than prevKey
				// diffVal may be nil (i.e. deleted). move to next tree's key if it is
				var ok bool
				if diffVal != nil {
					ok = isSuitable(diffKey, it.prevKey)
				} else {
					it.prevKey = diffKey // next key must be greater than deleted, even if from parent
				}

				if ok {
					it.key, it.val = diffKey, diffVal
					it.prevKey = it.key
				}
				if it.diffOk {
					it.diffOk = it.diffIt.Next()
					diffKey, diffVal = castToPair(it.diffIt.Key(), it.diffIt.Value())
				}
				if ok {
					return true
				}
			}
		}

		if it.parentOk {
			ok := isSuitable(it.parentIt.Key(), it.prevKey)

			if ok {
				it.key = common.CopyBytes(it.parentIt.Key()) // leveldb's iterator may use the same memory
				it.val = common.CopyBytes(it.parentIt.Value())
				it.prevKey = it.key
			}
			if it.parentOk {
				it.parentOk = it.parentIt.Next()
			}
			if ok {
				return true
			}
		}
	}

	return false
}

// Error returns any accumulated error. Exhausting all the key/value pairs
// is not considered to be an error. A memory iterator cannot encounter errors.
func (it *backedIterator) Error() error {
	err := it.parentIt.Error()
	if err != nil {
		return err
	}
	return it.diffIt.Error()
}

// Key returns the key of the current key/value pair, or nil if done. The caller
// should not modify the contents of the returned slice, and its contents may
// change on the next call to Next.
func (it *backedIterator) Key() []byte {
	return it.key
}

// Value returns the value of the current key/value pair, or nil if done. The
// caller should not modify the contents of the returned slice, and its contents
// may change on the next call to Next.
func (it *backedIterator) Value() []byte {
	return it.val
}

// Release releases associated resources. Release should always succeed and can
// be called multiple times without causing error.
func (it *backedIterator) Release() {
	if it.parentIt != nil {
		it.parentIt.Release()
		it.diffIt.Release()
		*it = backedIterator{}
	}
}

// GetSnapshot returns a latest snapshot of the underlying DB. A snapshot
// is a frozen snapshot of a DB state at a particular point in time. The
// content of snapshot are guaranteed to be consistent.
//
// The snapshot must be released after use, by calling Release method.
func (w *Store) GetSnapshot() (kvdb.Snapshot, error) {
	diffSnap, err := w.diff.GetSnapshot()
	if err != nil {
		return nil, err
	}
	return &Snapshot{
		reader: reader{
			back: w.back,
			diff: diffSnap,
		},
		diffSnap: diffSnap,
	}, nil
}

type errIterator struct {
	kvdb.Iterator
	err error
}

func (it *errIterator) Error() error {
	return it.err
}

var (
	devnull = devnulldb.New()
)

// NewIterator creates a binary-alphabetical iterator over a subset
// of database content with a particular key prefix, starting at a particular
// initial key (or after, if it does not exist).
func (w *reader) NewIterator(prefix []byte, start []byte) kvdb.Iterator {
	if w.diff == nil {
		return &errIterator{
			Iterator: devnull.NewIterator(nil, nil),
			err:      errClosed,
		}
	}

	it := &backedIterator{
		diffIt:   w.diff.NewIterator(prefix, start),
		parentIt: w.back.NewIterator(prefix, start),
	}
	it.init()
	return it
}

/*
 * Batch
 */

// NewBatch creates new batch.
func (w *Store) NewBatch() kvdb.Batch {
	return &cacheBatch{db: w}
}

type kv struct {
	k, v []byte
}

// cacheBatch is a batch structure.
type cacheBatch struct {
	db     *Store
	writes []kv
	size   int
}

// Put adds "add key-value pair" operation into batch.
func (b *cacheBatch) Put(key, value []byte) error {
	b.writes = append(b.writes, kv{common.CopyBytes(key), common.CopyBytes(value)})
	b.size += len(value) + len(key)
	return nil
}

// Delete adds "remove key" operation into batch.
func (b *cacheBatch) Delete(key []byte) error {
	b.writes = append(b.writes, kv{common.CopyBytes(key), nil})
	b.size += len(key)
	return nil
}

// Write writes batch into db. Not atomic.
func (b *cacheBatch) Write() error {
	for _, kv := range b.writes {
		var err error

		if kv.v == nil {
			err = b.db.Delete(kv.k)
		} else {
			err = b.db.Put(kv.k, kv.v)
		}

		if err != nil {
			return err
		}
	}
	return nil
}

// ValueSize returns key-values sizes sum.
func (b *cacheBatch) ValueSize() int {
	return b.size
}

// Reset cleans whole batch.
func (b *cacheBatch) Reset() {
	b.writes = b.writes[:0]
	b.size = 0
}

// Replay replays the batch contents.
func (b *cacheBatch) Replay(w kvdb.Writer) error {
	for _, kv := range b.writes {
		if kv.v == nil {
			if err := w.Delete(kv.k); err != nil {
				return err
			}
			continue
		}
		if err := w.Put(kv.k, kv.v); err != nil {
			return err
		}
	}
	return nil
}

// Snapshot is a DB snapshot.
type Snapshot struct {
	reader
	diffSnap kvdb.Snapshot
}

func (s *Snapshot) Release() {
	s.diffSnap.Release()
	s.diff = nil
}
