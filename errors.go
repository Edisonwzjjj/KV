package KV

import "errors"

var (
	ErrKeyIsEmpty        = errors.New("key is empty")
	ErrIndexUpdateFailed = errors.New("index update failed")
	ErrKeyNotExists      = errors.New("key not exists")
	ErrDataFileNotFound  = errors.New("data file not found")
)
