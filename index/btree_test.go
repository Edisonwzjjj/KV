package index

import (
	"KV/data"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBTree_Pet(t *testing.T) {
	bt := NewBTree()
	res1 := bt.Put(nil, &data.LogRecordPos{
		Fid:    0,
		Offset: 100,
	})
	assert.True(t, res1)

	res2 := bt.Put([]byte("a"), &data.LogRecordPos{
		Fid:    1,
		Offset: 100,
	})
	assert.True(t, res2)

}

func TestBTree_Get(t *testing.T) {
	bt := NewBTree()

	bt.Put([]byte("a"), &data.LogRecordPos{
		Fid:    0,
		Offset: 100,
	})

	res1 := bt.Get([]byte("a"))

	k1 := res1.Fid
	v1 := res1.Offset
	assert.Equal(t, int32(0), k1)
	assert.Equal(t, int64(100), v1)

	bt.Put([]byte("a"), &data.LogRecordPos{
		Fid:    1,
		Offset: 200,
	})

	res2 := bt.Get([]byte("a"))

	k2 := res2.Fid
	v2 := res2.Offset
	assert.Equal(t, int32(1), k2)
	assert.Equal(t, int64(200), v2)
	t.Log(k2, v2)
}

func TestBTree_Delete(t *testing.T) {
	bt := NewBTree()

	bt.Put([]byte("a"), &data.LogRecordPos{
		Fid:    0,
		Offset: 100,
	})

	res1 := bt.Delete([]byte("a"))
	assert.True(t, res1)

	res2 := bt.Get([]byte("a"))
	assert.Nil(t, res2)

}
