package nbd

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"sync"
)

const (
	requestHeaderSize = 28
)

var (
	nbo         = binary.BigEndian
	requestPool = sync.Pool{New: func() any { return new(Request) }}
)

type Request struct {
	flags  uint16
	cmd    uint16
	handle uint64
	offset uint64
	length uint32
	buf    *[]byte
}

func (r *Request) Buffer() []byte {
	if r.buf == nil {
		return nil
	}
	return *r.buf
}

func (r *Request) String() string {
	name := "<Unsupported>"
	args := ""
	switch r.cmd {
	case nbdCmdRead:
		name = "Read"
		args = fmt.Sprintf("offset: %d, length: %d", r.offset, r.length)
	case nbdCmdWrite:
		name = "Write"
		args = fmt.Sprintf("offset: %d, length: %d", r.offset, r.length)
	case nbdCmdFlush:
		name = "Flush"
	case nbdCmdTrim:
		name = "Trim"
		args = fmt.Sprintf("offset: %d, length: %d", r.offset, r.length)
	case nbdCmdCache:
		name = "Cache"
	case nbdCmdWriteZeroes:
		name = "WriteZeros"
	}
	return fmt.Sprintf("%s(%s)", name, args)
}

func (r *Request) readHeader(b *bufio.Reader) error {
	h, err := b.Peek(requestHeaderSize)
	if err != nil {
		return err
	}
	magic := nbo.Uint32(h)
	if magic != nbdRequestMagic {
		return fmt.Errorf("Unexpected request magic 0x%x", magic)
	}
	r.flags = nbo.Uint16(h[4:])
	r.cmd = nbo.Uint16(h[6:])
	r.handle = nbo.Uint64(h[8:])
	r.offset = nbo.Uint64(h[16:])
	r.length = nbo.Uint32(h[24:])
	r.buf = nil
	_, err = b.Discard(requestHeaderSize)
	return err
}

type RequestPool struct {
	pools map[int]*sync.Pool
	lock  sync.Mutex
}

func (p *RequestPool) getPool(size int) *sync.Pool {
	p.lock.Lock()
	defer p.lock.Unlock()

	if p.pools == nil {
		p.pools = make(map[int]*sync.Pool)
	}

	pool := p.pools[size]
	if pool == nil {
		pool = &sync.Pool{New: func() any {
			b := make([]byte, size)
			return &b
		}}
		p.pools[size] = pool
	}
	return pool
}

func (p *RequestPool) Recv(b *bufio.Reader) (*Request, error) {
	r := requestPool.Get().(*Request)
	err := r.readHeader(b)
	if err != nil {
		return nil, err
	}
	if r.cmd == nbdCmdWrite {
		r.buf = p.getPool(int(r.length)).Get().(*[]byte)
		_, err = io.ReadFull(b, *r.buf)
		if err != nil {
			return nil, err
		}
	}
	return r, nil
}

func (p *RequestPool) Put(r *Request) {
	if r.buf != nil {
		p.getPool(int(r.length)).Put(r.buf)
		r.buf = nil
	}

	requestPool.Put(r)
}
