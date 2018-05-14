package nbd

import (
	"github.com/akmistry/go-util/bufferpool"
)

type Buffer struct {
	buf []byte
}

func NewBuffer(size int) *Buffer {
	return &Buffer{
		buf: bufferpool.Get(size),
	}
}

func (b *Buffer) Release() {
	if b.buf != nil {
		bufferpool.Put(b.buf)
	}
	b.buf = nil
}
