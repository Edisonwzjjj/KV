package fio

import "os"

type FileIO struct {
	fd *os.File
}

func NewFileIOManager(filepath string) (*FileIO, error) {
	file, err := os.OpenFile(filepath,
		os.O_APPEND|os.O_CREATE|os.O_RDWR,
		DataFilePerm)
	if err != nil {
		return nil, err
	}
	return &FileIO{file}, nil
}

func (fio *FileIO) Read(b []byte, offset int64) (int, error) {
	return fio.fd.ReadAt(b, offset)
}

func (fio *FileIO) Write(b []byte) (int, error) {
	return fio.fd.Write(b)
}

func (fio *FileIO) Sync() error {
	return fio.fd.Sync()
}

func (fio *FileIO) Close() error {
	return fio.fd.Close()
}
