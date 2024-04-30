package KV

import (
	"KV/data"
	"KV/index"
	"sync"
)

// DB 面向用户的操作接口
type DB struct {
	options Options
	mtx     *sync.RWMutex
	active  *data.DataFile            // 可以写的文件
	old     map[uint32]*data.DataFile //过期文件
	index   index.Indexer
}

func (db *DB) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	logRecord := &data.LogRecord{
		Key:   key,
		Value: value,
		Type:  data.LogRecordNormal,
	}

	pos, err := db.appendLogRecord(logRecord)
	if err != nil {
		return err
	}
	//更新内存索引
	if ok := db.index.Put(key, pos); !ok {
		return ErrIndexUpdateFailed
	}
	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	db.mtx.RLock()
	defer db.mtx.RUnlock()
	if len(key) == 0 {
		return nil, ErrKeyIsEmpty
	}
	pos := db.index.Get(key)
	if pos == nil {
		return nil, ErrKeyNotExists
	}

	//根据id找文件
	var dataFile *data.DataFile
	if db.active.FileId == pos.Fid {
		dataFile = db.active
	} else {
		dataFile = db.old[pos.Fid]
	}

	if dataFile == nil {
		return nil, ErrDataFileNotFound
	}

	logRecord, err := dataFile.ReadLogRecord(pos.Offset)
	if err != nil {
		return nil, err
	}

	if logRecord.Type != data.LogRecordNormal {
		return nil, ErrKeyNotExists
	}

	return logRecord.Value, nil
}

func (db *DB) appendLogRecord(logRecord *data.LogRecord) (*data.LogRecordPos, error) {
	db.mtx.Lock()
	defer db.mtx.Unlock()
	//初始化活跃
	if db.active == nil {
		if err := db.setActiveDatafile(); err != nil {
			return nil, err
		}
	}
	encRecord, size := data.EncodeLogRecord(logRecord)
	if size+db.active.WriteOffset > db.options.DataFileSize {
		if err := db.active.Sync(); err != nil {
			return nil, err
		}

		db.old[db.active.FileId] = db.active

		if err := db.setActiveDatafile(); err != nil {
			return nil, err
		}
	}

	if err := db.active.Write(encRecord); err != nil {
		return nil, err
	}

	if db.options.SyncWrite {
		if err := db.active.Sync(); err != nil {
			return nil, err
		}
	}

	pos := &data.LogRecordPos{
		Fid:    db.active.FileId,
		Offset: db.active.WriteOffset,
	}

	return pos, nil
}

func (db *DB) setActiveDatafile() error {
	var initialId uint32 = 0
	if db.active != nil {
		initialId = db.active.FileId + 1
	}

	file, err := data.OpenDataFIle(db.options.DirPath, initialId)
	if err != nil {
		return err
	}
	db.active = file
	return nil
}
