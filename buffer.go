package nbd

import (
	"sync"

	"github.com/akmistry/go-util/bufferpool"
)

var (
	bufPool = sync.Pool{New: func() any { return new(Buffer) }}
)

type Buffer struct {
	buf []byte
}

func NewBuffer(size int) *Buffer {
	b := bufPool.Get().(*Buffer)
	b.buf = bufferpool.Get(size)
	return b
}

func (b *Buffer) Release() {
	if b.buf != nil {
		bufferpool.Put(b.buf)
	}
	b.buf = nil
	bufPool.Put(b)
}
