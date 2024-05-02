package KV

import (
	"KV/data"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
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
		return ErrMergeIsProgress
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

	//记录当前文件，不参与 merge
	nonMergeFileId := db.activeFile.FileId

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

	mergeFinRecord := &data.LogRecord{
		Key:   []byte(mergeFinishKey),
		Value: []byte(strconv.Itoa(int(nonMergeFileId))),
	}

	encRecord, _ := data.EncodeLogRecord(mergeFinRecord)

	if err := mergeFinishFile.Write(encRecord); err != nil {
		return err
	}

	if err := mergeFinishFile.Sync(); err != nil {
		return err
	}

	return nil
}

func (db *DB) getMergePath() string {
	dir := path.Dir(path.Clean(db.options.DirPath))
	base := path.Base(db.options.DirPath)

	return filepath.Join(dir, base+mergeDirName)
}

func (db *DB) loadMergeFiles() error {
	mergePath := db.getMergePath()
	if _, err := os.Stat(mergePath); os.IsNotExist(err) {
		return err
	}

	defer func() {
		_ = os.RemoveAll(mergePath)
	}()
	dirEntries, _ := os.ReadDir(mergePath)

	//查找标识 merge
	var mergeFinished = false
	var mergeFileNames []string

	for _, entry := range dirEntries {
		if entry.Name() == data.MergeFinishFileName {
			mergeFinished = true
		}
		mergeFileNames = append(mergeFileNames, entry.Name())
	}
	//未完成
	if !mergeFinished {
		return nil
	}

	nonMergeFileId, err := db.getNonMergeFileId(mergePath)
	if err != nil {
		return err
	}

	//删除旧文件
	var fileId uint32 = 0
	for ; fileId < nonMergeFileId; fileId++ {
		fileName := data.GetDataFileName(mergePath, fileId)
		if _, err := os.Stat(fileName); err != nil {
			_ = os.Remove(fileName)
		}

	}

	//新的数据文件移动过来
	for _, fileName := range mergeFileNames {
		srcPath := path.Join(mergePath, fileName)
		dstPath := path.Join(db.options.DirPath, fileName)

		if err := os.Rename(srcPath, dstPath); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) getNonMergeFileId(dirPath string) (uint32, error) {
	dataFile, err := data.OpenMergeFinishFile(dirPath)
	if err != nil {
		return 0, err
	}
	logRecord, _, err := dataFile.ReadLogRecord(0)
	if err != nil {
		return 0, err
	}

	nonMergeFileId, err := strconv.Atoi(string(logRecord.Key))
	if err != nil {
		return 0, err
	}
	return uint32(nonMergeFileId), nil
}

func (db *DB) loadIndexFromHintFile() error {
	hintFileName := filepath.Join(db.options.DirPath, data.HintFileName)
	if _, err := os.Stat(hintFileName); os.IsNotExist(err) {
		return err
	}
	hintFile, err := data.OpenHintFile(hintFileName)
	if err != nil {
		return err
	}

	var offset int64 = 0
	for {
		logRecord, size, err := hintFile.ReadLogRecord(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		pos := data.DecodeLogRecordPos(logRecord.Value)
		db.index.Put(logRecord.Key, pos)
		offset += size
	}
	return nil
}
