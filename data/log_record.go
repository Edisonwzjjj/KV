package data

type LogRecordType = byte

const (
	LogRecordNormal LogRecordType = iota
	LogRecordDelete
)

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
