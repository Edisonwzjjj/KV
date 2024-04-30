package data

import "encoding/binary"

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDelete
)

// crc type keySize valueSize
const maxLogRecordHeaderSize = binary.MaxVarintLen32*2 + 5

// 写入的文件
type LogRecord struct {
	Key   []byte
	Value []byte
	Type  LogRecordType
}

// 磁盘上的位置
type LogRecordPos struct {
	Fid    uint32
	Offset uint64
}

// 序列化
func EncodeLogRecord(logRecord *LogRecord) ([]byte, uint64) {
	return nil, 0
}

type LogRecordHeader struct {
	Crc       uint32
	Type      LogRecordType
	KeySize   uint32
	ValueSize uint32
}

func decodeLogRecordHeader(buf []byte) (*LogRecordHeader, uint64) {
	return nil, 0
}

func getHeaderCrc(logRecord *LogRecord, header []byte) uint32 {
	return 0
}
