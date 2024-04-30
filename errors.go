package KV

import "errors"

var (
	ErrKeyIsEmpty          = errors.New("key is empty")
	ErrIndexUpdateFailed   = errors.New("index update failed")
	ErrKeyNotExists        = errors.New("key not exists")
	ErrDataFileNotFound    = errors.New("data file not found")
	ErrDirIsEmpty          = errors.New("dir is empty")
	ErrInvalidDataFileSize = errors.New("invalid data file size")
	ErrDataDirctoryCorrupt = errors.New("data dirctory corrupt")
)
