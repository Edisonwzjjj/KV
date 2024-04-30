package KV

type Options struct {
	DirPath      string
	DataFileSize uint64
	SyncWrite    bool
	IndexType    IndexerType
}

type IndexerType int8

const (
	Btree IndexerType = iota + 1
	ART
)
