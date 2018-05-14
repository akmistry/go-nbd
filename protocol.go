package nbd

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/akmistry/go-util/bufferpool"
)

type nbdRequest struct {
	flags  uint16
	cmd    uint16
	handle uint64
	offset uint64
	length uint32
	data   *Buffer
}

type nbdReply struct {
	handle uint64
	err    uint32
	data   *Buffer
}

var (
	nbo = binary.BigEndian
)

func readRequest(r io.Reader, req *nbdRequest) error {
	var h [28]byte
	_, err := io.ReadFull(r, h[:])
	if err != nil {
		return err
	}
	magic := nbo.Uint32(h[:])
	if magic != nbdRequestMagic {
		return fmt.Errorf("Unexpected request magic 0x%x", magic)
	}
	req.flags = nbo.Uint16(h[4:])
	req.cmd = nbo.Uint16(h[6:])
	req.handle = nbo.Uint64(h[8:])
	req.offset = nbo.Uint64(h[16:])
	req.length = nbo.Uint32(h[24:])
	req.data = nil
	if req.cmd == nbdCmdWrite {
		req.data = NewBuffer(int(req.length))
		_, err = io.ReadFull(r, req.data.buf)
		if err != nil {
			req.data.Release()
			return err
		}
	}
	return nil
}

func writeReply(w io.Writer, reply *nbdReply) error {
	var header [16]byte
	var b []byte
	if reply.data != nil {
		// TODO: Determine if the total cost of doing a single write is smaller than doing two
		// (without the alloc)
		b = bufferpool.Get(16 + len(reply.data.buf))
		defer bufferpool.Put(b)
	} else {
		b = header[:]
	}
	nbo.PutUint32(b, nbdReplyMagic)
	nbo.PutUint32(b[4:], reply.err)
	nbo.PutUint64(b[8:], reply.handle)
	if reply.data != nil {
		copy(b[16:], reply.data.buf)
	}
	_, err := w.Write(b)
	return err
}
