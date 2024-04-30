package fio

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func Destory(name string) {
	if err := os.RemoveAll(name); err != nil {
		panic(err)
	}
}

func TestFileIO_Read(t *testing.T) {
	fio, err := NewFileIOManager("test.txt")
	defer Destory("test.txt")
	assert.Nil(t, err)
	assert.NotNil(t, fio)

}

func TestFileIO_Write(t *testing.T) {
	fio, err := NewFileIOManager("test.txt")
	defer Destory("test.txt")
	assert.Nil(t, err)
	assert.NotNil(t, fio)

	n, err := fio.Write([]byte("hello world\n"))
	assert.Nil(t, err)
	assert.Equal(t, 12, n)
	t.Log(n, err)

	n, err = fio.Write([]byte("1233455\n"))
	assert.Nil(t, err)
	t.Log(n, err)
}
