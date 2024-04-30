package data

import (
	"KV/fio"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"path/filepath"
)

const DataFileNameSuffix = ".data"

var (
	ErrCrcIncorrect = errors.New("crc32 check failed")
)

type DataFile struct {
	FileId      uint32
	WriteOffset uint64
	IoManager   fio.IOManager
}

func (f *DataFile) Sync() error {
	return f.IoManager.Sync()
}

func OpenDataFIle(dir string, fileId uint32) (*DataFile, error) {
	fileName := filepath.Join(dir, fmt.Sprint("%09d", fileId), DataFileNameSuffix)
	ioManager, err := fio.NewFileIOManager(fileName)
	if err != nil {
		return nil, err
	}
	return &DataFile{
		FileId:      fileId,
		WriteOffset: 0,
		IoManager:   ioManager,
	}, nil
}

func (f *DataFile) Write(buf []byte) error {
	n, err := f.IoManager.Write(buf)
	if err != nil {
		return err
	}
	f.WriteOffset += uint64(n)
	return nil
}

func (f *DataFile) ReadLogRecord(offset int64) (*LogRecord, uint64, error) {
	headerBuf, err := f.ReadNBytes(maxLogRecordHeaderSize, offset)
	if err != nil {
		return nil, 0, err
	}

	header, size := decodeLogRecordHeader(headerBuf)
	if header == nil {
		return nil, 0, io.EOF
	}

	if header.Crc == 0 && header.KeySize == 0 && header.ValueSize == 0 {
		return nil, 0, io.EOF
	}

	keySize := uint64(header.KeySize)
	valueSize := uint64(header.ValueSize)

	var recordSize = keySize + valueSize + size

	logRecord := &LogRecord{
		Type: header.Type,
	}

	if keySize > 0 || valueSize > 0 {
		buf, err := f.ReadNBytes(keySize+valueSize, offset+int64(size))
		if err != nil {
			return nil, 0, err
		}

		logRecord.Key = buf[:keySize]
		logRecord.Value = buf[keySize:]
	}

	//检验crc
	crc := getHeaderCrc(logRecord, headerBuf[crc32.Size:size])
	if crc != header.Crc {
		return nil, 0, ErrCrcIncorrect
	}

	return logRecord, recordSize, nil
}

func (f *DataFile) ReadNBytes(n uint64, offset int64) ([]byte, error) {
	b := make([]byte, n)
	_, err := f.IoManager.Read(b, offset)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func Close(f *DataFile) error {
	return f.IoManager.Close()
}
