package KV

import (
	"KV/index"
	"bytes"
)

type Iterator struct {
	indexIter index.Iterator
	db        *DB
	option    IteratorOptions
}

func (db *DB) NewIterator(op IteratorOptions) *Iterator {
	indexIterator := db.index.Iterator(op.Reverse)
	return &Iterator{
		indexIter: indexIterator,
		db:        db,
		option:    op,
	}
}

func (it *Iterator) SkipToNext() {
	prefixLen := len(it.option.Prefix)
	if prefixLen == 0 {
		return
	}

	for ; it.indexIter.Valid(); it.indexIter.Next() {
		k := it.indexIter.Key()
		if prefixLen <= len(k) && bytes.Compare(it.option.Prefix, k[:prefixLen]) == 0 {
			break
		}
	}
}

func (it *Iterator) Rewind() {
	it.indexIter.Rewind()
	it.SkipToNext()
}

func (it *Iterator) Seek(key []byte) {
	it.indexIter.Seek(key)
	it.SkipToNext()
}

func (it *Iterator) Valid() bool {
	return it.indexIter.Valid()
}

func (it *Iterator) Next() {
	it.indexIter.Next()
	it.SkipToNext()
}

func (it *Iterator) Key() []byte {
	return it.indexIter.Key()
}

func (it *Iterator) Value() ([]byte, error) {
	logRecordPos := it.indexIter.Value()
	it.db.mu.RLock()
	defer it.db.mu.RUnlock()
	return it.db.getValueByPosition(logRecordPos)

}

func (it *Iterator) Close() {
	it.indexIter.Close()
}
