package nbd

import (
	"encoding/binary"
	"io"
	"sync"
)

const (
	replyHeaderSize = 16
)

type Reply struct {
	handle uint64
	err    uint32
	buf    []byte
}

func NewReply(handle uint64, dataSize int) *Reply {
	return &Reply{
		handle: handle,
		buf:    make([]byte, replyHeaderSize+dataSize),
	}
}

func (r *Reply) SetError(err uint32) {
	r.err = err
}

func (r *Reply) Buffer() []byte {
	return r.buf[replyHeaderSize:]
}

func (r *Reply) BufferSize() int {
	return len(r.buf) - replyHeaderSize
}

func (r *Reply) Send(w io.Writer) error {
	binary.BigEndian.PutUint32(r.buf, nbdReplyMagic)
	binary.BigEndian.PutUint32(r.buf[4:], r.err)
	binary.BigEndian.PutUint64(r.buf[8:], r.handle)
	_, err := w.Write(r.buf)
	return err
}

type ReplyPool struct {
	pools map[int]*sync.Pool
	lock  sync.Mutex
}

func (p *ReplyPool) getPool(size int) *sync.Pool {
	p.lock.Lock()
	defer p.lock.Unlock()

	if p.pools == nil {
		p.pools = make(map[int]*sync.Pool)
	}

	pool := p.pools[size]
	if pool == nil {
		pool = &sync.Pool{New: func() any {
			return NewReply(0, size)
		}}
		p.pools[size] = pool
	}
	return pool
}

func (p *ReplyPool) Get(handle uint64, size int) *Reply {
	r := p.getPool(size).Get().(*Reply)
	r.handle = handle
	r.err = 0
	return r
}

func (p *ReplyPool) Put(r *Reply) {
	size := r.BufferSize()
	p.getPool(size).Put(r)
}
