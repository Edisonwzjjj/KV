package data

import "KV/fio"

const DataFileNameSuffix = ".data"

type DataFile struct {
	FileId      uint32
	WriteOffset uint64
	IoManager   fio.IOManager
}

func (f *DataFile) Sync() error {
	return nil
}

func OpenDataFIle(dir string, fileId uint32) (*DataFile, error) {
	return nil, nil
}

func (f *DataFile) Write(buf []byte) error {
	return nil
}

func (f *DataFile) ReadLogRecord(offset uint64) (*LogRecord, uint64, error) {
	return nil, 0, nil
}
