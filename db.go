package KV

// 面向用户的操作接口
type DB struct {
}

func (db *DB) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

}
