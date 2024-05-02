package index

import (
	"KV/data"
	bolt "go.etcd.io/bbolt"
	"path/filepath"
)

const (
	bptreeIndexFileName = "bptree-index"
)

var (
	indexBucketName = []byte("bitcask-index")
)

type BPlusTree struct {
	tree *bolt.DB
}

func NewBPTree(dirPath string, syncWrite bool) *BPlusTree {
	opt := bolt.DefaultOptions
	opt.NoSync = !syncWrite

	bpt, err := bolt.Open(filepath.Join(dirPath, bptreeIndexFileName), 0644, nil)
	if err != nil {
		panic("failed to open BPTree")
	}

	if err := bpt.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(indexBucketName)
		return err
	}); err != nil {
		panic("fail to create bucket")
	}
	return &BPlusTree{tree: bpt}
}

func (bpt *BPlusTree) Put(key []byte, pos *data.LogRecordPos) bool {
	if err := bpt.tree.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		return bucket.Put(key, data.EncodeLogRecordPos(pos))
	}); err != nil {
		panic("fail to put value in BPTree")
		return false
	}
	return true
}

// Get 根据 key 取出对应的索引位置信息
func (bpt *BPlusTree) Get(key []byte) *data.LogRecordPos {
	var pos *data.LogRecordPos

	if err := bpt.tree.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		val := bucket.Get(key)
		if len(val) != 0 {
			pos = data.DecodeLogRecordPos(val)
		}
		return nil
	}); err != nil {
		panic("fail to get value in BPTree")
	}

	return pos
}

// Delete 根据 key 删除对应的索引位置信息
func (bpt *BPlusTree) Delete(key []byte) bool {
	var ok = false

	if err := bpt.tree.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		if val := bucket.Get(key); len(val) != 0 {
			ok = true
			return bucket.Delete(key)
		}
		return nil
	}); err != nil {
		panic("fail to delete value in BPTree")
	}
	return ok
}

func (bpt *BPlusTree) Iterator(reverse bool) Iterator {
	return newBPtreeIterator(bpt.tree, reverse)
}

func (bpt *BPlusTree) Size() int {
	var size int

	if err := bpt.tree.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(indexBucketName)
		size = bucket.Stats().KeyN
		return nil
	}); err != nil {
		panic("fail to get bucket size")
	}

	return size
}

type bptreeIterator struct {
	tx        *bolt.Tx
	cursor    *bolt.Cursor
	reverse   bool
	currKey   []byte
	currValue []byte
}

func newBPtreeIterator(bpt *bolt.DB, reverse bool) *bptreeIterator {
	tx, err := bpt.Begin(false)
	if err != nil {
		panic("failed to begin a transaction")
	}
	bi := &bptreeIterator{
		tx:      tx,
		cursor:  tx.Bucket(indexBucketName).Cursor(),
		reverse: reverse,
	}
	bi.Rewind()
	return bi
}

func (bi *bptreeIterator) Rewind() {
	if bi.reverse {
		bi.currKey, bi.currValue = bi.cursor.Last()
	} else {
		bi.currKey, bi.currValue = bi.cursor.First()
	}
}

func (bi *bptreeIterator) Seek(key []byte) {
	bi.currKey, bi.currValue = bi.cursor.Seek(key)
}
func (bi *bptreeIterator) Next() {
	bi.currKey, bi.currValue = bi.cursor.Next()
}

func (bi *bptreeIterator) Valid() bool {
	return len(bi.currKey) != 0
}
func (bi *bptreeIterator) Key() []byte {
	return bi.currKey
}

func (bi *bptreeIterator) Value() *data.LogRecordPos {
	return data.DecodeLogRecordPos(bi.currValue)
}

func (bpt *bptreeIterator) Close() {
	_ = bpt.tx.Commit()
}
