package index

import (
	"KV/data"
	"bytes"
	"github.com/google/btree"
)

type Indexer interface {
	Put(key []byte, pos *data.LogRecordPos) bool
	Get(key []byte) *data.LogRecordPos
	Delete(key []byte) bool
}

type Item struct {
	key []byte
	pos *data.LogRecordPos
}

func (i *Item) Less(other btree.Item) bool {
	return bytes.Compare(i.key, other.(*Item).key) == -1
}
