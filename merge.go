package KV

import (
	"KV/data"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
)

const (
	mergeDirName   = "-merge"
	mergeFinishKey = "merge.finished"
)

func (db *DB) Merge() error {
	if db.activeFile == nil {
		return nil
	}
	db.mu.Lock()

	if db.isMerging {
		db.mu.Unlock()
		return ErrMergeInProgress
	}

	db.isMerging = true

	defer func() {
		db.isMerging = false
	}()

	//处理活跃文件
	if err := db.activeFile.Sync(); err != nil {
		db.mu.Unlock()
		return err
	}
	db.olderFiles[db.activeFile.FileId] = db.activeFile

	if err := db.setActiveDataFile(); err != nil {
		db.mu.Unlock()
		return err
	}

	var mergeFiles []*data.DataFile
	for _, file := range db.olderFiles {
		mergeFiles = append(mergeFiles, file)

	}

	db.mu.Unlock()

	sort.Slice(mergeFiles, func(i, j int) bool {
		return mergeFiles[i].FileId < mergeFiles[j].FileId
	})
	mergePath := db.getMergePath()
	if _, err := os.Stat(mergePath); err != nil {
		if err := os.RemoveAll(mergePath); err != nil {
			return err
		}
	}

	if err := os.MkdirAll(mergePath, os.ModePerm); err != nil {
		return err
	}

	mergeOption := db.options
	mergeOption.DirPath = mergePath
	mergeOption.SyncWrites = false
	mergeDB, err := Open(mergeOption)
	if err != nil {
		return err
	}
	hintFile, err := data.OpenHintFile(mergePath)
	if err != nil {
		return err
	}

	for _, dataFile := range mergeFiles {
		var offset int64 = 0

		for {
			logRecord, size, err := dataFile.ReadLogRecord(offset)
			if err != nil {
				if err == io.EOF {
					return nil
				}
				return err
			}
			realKey, _ := parseLogRecordKey(logRecord.Key)
			logRecordPos := db.index.Get(realKey)
			if logRecordPos != nil && logRecordPos.Fid == dataFile.FileId && logRecordPos.Offset == offset {
				logRecord.Key = logRecordKeyWithSeqNo(realKey, nonTransactionSeqNo)
				pos, err := mergeDB.appendLogRecord(logRecord)
				if err != nil {
					return err
				}

				// 写到 hint 文件
				if err := dataFile.WriteHint(realKey, pos); err != nil {
					return err
				}

			}
			offset += size
		}
	}

	//持久化
	if err := hintFile.Sync(); err != nil {
		return err
	}

	if err := mergeDB.Sync(); err != nil {
		return err
	}

	mergeFinishFile, err := data.OpenMergeFinishFile(mergePath)
	if err != nil {
		return err
	}
	return nil
}

func (db *DB) getMergePath() string {
	dir := path.Dir(path.Clean(db.options.DirPath))
	base := path.Base(db.options.DirPath)

	return filepath.Join(dir, base+mergeDirName)
}
