package index

import (
	"KV/data"
	"bytes"
	"github.com/google/btree"
	"sort"
	"sync"
)

type BTree struct {
	tree *btree.BTree
	lock *sync.RWMutex
}

func NewBTree() *BTree {
	return &BTree{
		tree: btree.New(32),
		lock: new(sync.RWMutex),
	}
}

func (bt *BTree) Put(key []byte, pos *data.LogRecordPos) bool {
	it := &Item{key: key, pos: pos}
	bt.lock.Lock()
	bt.tree.ReplaceOrInsert(it)
	bt.lock.Unlock()
	return true
}

func (bt *BTree) Get(key []byte) *data.LogRecordPos {
	it := &Item{key: key}
	btreeItem := bt.tree.Get(it)
	if btreeItem == nil {
		return nil
	}
	return btreeItem.(*Item).pos
}

func (bt *BTree) Delete(key []byte) bool {
	it := &Item{key: key}
	bt.lock.Lock()
	oldItem := bt.tree.Delete(it)
	bt.lock.Unlock()
	if oldItem == nil {
		return false
	}
	return true
}

func (bt *BTree) Size() int {
	return bt.tree.Len()
}

type btreeIterator struct {
	curr    int
	reverse bool
	values  []*Item
}

func (bt *BTree) Iterator(reverse bool) Iterator {
	if bt.tree == nil {
		return nil
	}
	bt.lock.RLock()
	defer bt.lock.RUnlock()
	return newBTreeIterator(bt.tree, reverse)
}

func newBTreeIterator(tree *btree.BTree, reverse bool) *btreeIterator {
	var idx int
	values := make([]*Item, tree.Len())

	// 将所有的数据存放到数组中
	saveValues := func(it btree.Item) bool {
		values[idx] = it.(*Item)
		idx++
		return true
	}
	if reverse {
		tree.Descend(saveValues)
	} else {
		tree.Ascend(saveValues)
	}

	return &btreeIterator{
		curr:    0,
		reverse: reverse,
		values:  values,
	}
}

func (bti *btreeIterator) Rewind() {
	bti.curr = 0
}

func (bti *btreeIterator) Seek(key []byte) {
	if bti.reverse {
		bti.curr = sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(key, bti.values[i].key) <= 0
		})
	} else {
		bti.curr = sort.Search(len(bti.values), func(i int) bool {
			return bytes.Compare(key, bti.values[i].key) >= 0
		})
	}
}

func (bti *btreeIterator) Next() {
	bti.curr++
}

func (bti *btreeIterator) Valid() bool {
	return bti.curr < len(bti.values)
}

func (bti *btreeIterator) Key() []byte {
	return bti.values[bti.curr].key
}

func (bti *btreeIterator) Value() *data.LogRecordPos {
	return bti.values[bti.curr].pos
}

func (bti *btreeIterator) Close() {
	bti.values = nil
}
