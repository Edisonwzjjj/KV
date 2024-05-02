package index

import (
	"KV/data"
	"bytes"
	goart "github.com/plar/go-adaptive-radix-tree"
	"sort"
	"sync"
)

type AdaptiveRadixTree struct {
	tree goart.Tree
	lock *sync.RWMutex
}

func NewART() *AdaptiveRadixTree {
	return &AdaptiveRadixTree{
		tree: goart.New(),
		lock: new(sync.RWMutex),
	}
}

func (art *AdaptiveRadixTree) Put(key []byte, pos *data.LogRecordPos) bool {
	art.lock.Lock()
	defer art.lock.Unlock()
	art.tree.Insert(key, pos)
	return true
}

// Get 根据 key 取出对应的索引位置信息
func (art *AdaptiveRadixTree) Get(key []byte) *data.LogRecordPos {
	art.lock.RLock()
	defer art.lock.RUnlock()
	value, found := art.tree.Search(key)
	if !found {
		return nil
	}
	return value.(*data.LogRecordPos)
}

// Delete 根据 key 删除对应的索引位置信息
func (art *AdaptiveRadixTree) Delete(key []byte) bool {
	art.lock.Lock()
	defer art.lock.Unlock()
	_, deleted := art.tree.Delete(key)
	return deleted
}

func (art *AdaptiveRadixTree) Iterator(reverse bool) Iterator {
	art.lock.RLock()
	defer art.lock.RUnlock()
	return newArtIterator(art.tree, reverse)
}

func (art *AdaptiveRadixTree) Size() int {
	art.lock.RLock()
	defer art.lock.RUnlock()
	return art.tree.Size()
}

type artIterator struct {
	curr    int
	reverse bool
	values  []*Item
}

func newArtIterator(tree goart.Tree, reverse bool) *artIterator {
	var idx int
	if reverse {
		idx = tree.Size() - 1
	}
	values := make([]*Item, tree.Size())
	saveValues := func(node goart.Node) bool {
		item := &Item{
			key: node.Key(),
			pos: node.Value().(*data.LogRecordPos),
		}
		values[idx] = item
		if reverse {
			idx--
		} else {
			idx++
		}
		return true
	}
	tree.ForEach(saveValues)
	return &artIterator{
		curr:    0,
		reverse: reverse,
		values:  values,
	}
}

func (arti *artIterator) Rewind() {
	arti.curr = 0
}

func (arti *artIterator) Seek(key []byte) {
	if arti.reverse {
		arti.curr = sort.Search(len(arti.values), func(i int) bool {
			return bytes.Compare(key, arti.values[i].key) <= 0
		})
	} else {
		arti.curr = sort.Search(len(arti.values), func(i int) bool {
			return bytes.Compare(key, arti.values[i].key) >= 0
		})
	}
}

func (arti *artIterator) Next() {
	arti.curr++
}

func (arti *artIterator) Valid() bool {
	return arti.curr < len(arti.values)
}

func (arti *artIterator) Key() []byte {
	return arti.values[arti.curr].key
}

func (arti *artIterator) Value() *data.LogRecordPos {
	return arti.values[arti.curr].pos
}

func (arti *artIterator) Close() {
	arti.values = nil
}
