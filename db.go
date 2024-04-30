package KV

import (
	"KV/data"
	"KV/index"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// DB 面向用户的操作接口
type DB struct {
	options Options
	mtx     *sync.RWMutex
	fileIds []int                     // 只能在加载索引时用
	active  *data.DataFile            // 可以写的文件
	old     map[uint32]*data.DataFile //过期文件
	index   index.Indexer
}

func Open(op Options) (*DB, error) {
	//配置校验
	if err := checkOptions(op); err != nil {
		return nil, err
	}

	//目录检验，不存在需要创建
	if _, err := os.Stat(op.DirPath); os.IsNotExist(err) {
		if err := os.Mkdir(op.DirPath, os.ModePerm); err != nil {
			return nil, err
		}

	}
	db := &DB{
		options: op,
		mtx:     new(sync.RWMutex),
		active:  nil,
		old:     make(map[uint32]*data.DataFile),
		index:   index.NewIndexer(index.IndexerType(op.IndexType)),
	}

	//加载文件
	if err := db.LoadDataFiles(); err != nil {
		return nil, err
	}

	//加载索引

	if err := db.LoadIndexFromFiles(); err != nil {
		return nil, err
	}

	return db, nil
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

	logRecord, _, err := dataFile.ReadLogRecord(pos.Offset)
	if err != nil {
		return nil, err
	}

	if logRecord.Type != data.LogRecordNormal {
		return nil, ErrKeyNotExists
	}

	return logRecord.Value, nil
}

func (db *DB) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrKeyIsEmpty
	}

	if pos := db.index.Get(key); pos == nil {
		return nil
	}

	logRecord := &data.LogRecord{
		Key:  key,
		Type: data.LogRecordDelete,
	}

	_, err := db.appendLogRecord(logRecord)
	if err != nil {
		return nil
	}

	ok := db.index.Delete(key)
	if !ok {
		return ErrIndexUpdateFailed
	}
	return nil
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

func (db *DB) LoadDataFiles() error {
	dirEntries, err := os.ReadDir(db.options.DirPath)
	if err != nil {
		return err
	}

	var fileIds []int
	//遍历目录，后缀.data
	for _, entry := range dirEntries {
		if strings.HasSuffix(entry.Name(), data.DataFileNameSuffix) {
			splitNames := strings.Split(entry.Name(), ".")
			fileId, err := strconv.Atoi(splitNames[0])
			if err != nil {
				return ErrDataDirctoryCorrupt
			}
			fileIds = append(fileIds, fileId)

		}
	}

	sort.Ints(fileIds)
	db.fileIds = fileIds

	for idx, fileId := range fileIds {
		dataFile, err := data.OpenDataFIle(db.options.DirPath, uint32(fileId))
		if err != nil {
			return err
		}

		//最后一个是可写文件
		if idx == len(fileIds)-1 {
			db.active = dataFile
		} else {
			db.old[uint32(fileId)] = dataFile
		}
	}
	return nil
}

func (db *DB) LoadIndexFromFiles() error {
	if len(db.fileIds) == 0 {
		return nil
	}

	for idx, fid := range db.fileIds {
		var dataFile *data.DataFile
		var fileId = uint32(fid)
		if db.active.FileId == fileId {
			dataFile = db.active
		} else {
			dataFile = db.old[fileId]
		}

		var offSet uint64 = 0
		for {
			logRecord, size, err := dataFile.ReadLogRecord(offSet)
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			logRecordPos := &data.LogRecordPos{
				Fid:    fileId,
				Offset: offSet,
			}
			var ok bool
			if logRecord.Type != data.LogRecordNormal {
				ok = db.index.Delete(logRecord.Key)
			} else {
				ok = db.index.Put(logRecord.Key, logRecordPos)
			}
			if !ok {
				return ErrIndexUpdateFailed
			}
			offSet += size
		}

		if idx == len(db.fileIds)-1 {
			db.active.WriteOffset = offSet
		}
	}

	return nil
}

func checkOptions(option Options) error {
	if option.DirPath == "" {
		return ErrDirIsEmpty
	}

	if option.DataFileSize <= 0 {
		return ErrInvalidDataFileSize
	}

	return nil
}
