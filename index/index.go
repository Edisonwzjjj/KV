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

type IndexerType int8

const (
	Btree IndexerType = iota + 1

	ART
)

func NewIndexer(typ IndexerType) Indexer {
	switch typ {
	case Btree:
		return NewBTree()
	case ART:
		//todo()!
		return nil
	default:
		panic("unknown index type")
	}

}

type Item struct {
	key []byte
	pos *data.LogRecordPos
}

func (i *Item) Less(other btree.Item) bool {
	return bytes.Compare(i.key, other.(*Item).key) == -1
}
