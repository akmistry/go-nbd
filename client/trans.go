package client

import (
	"encoding/binary"
	"fmt"
	"io"
)

type request struct {
	flags  uint16
	cmd    uint16
	handle uint64
	offset uint64
	length uint32
	data   []byte
}

type reply struct {
	err    uint32
	handle uint64
	data   []byte
}

var (
	nbo = binary.BigEndian
	bm  = newBufferManager(256*1024, 4)
)

func readRequest(r io.Reader) (*request, error) {
	req := new(request)
	return req, readRequestTo(r, req)
}

func readRequestTo(r io.Reader, req *request) error {
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
		req.data = bm.get(uint(req.length))
		_, err = io.ReadFull(r, req.data)
		if err != nil {
			return err
		}
	}
	return nil
}

func writeReply(w io.Writer, handle uint64, replyErr uint32, data []byte) error {
	var header [16]byte
	var b []byte
	if len(data) > 0 {
		// TODO: Determine if the total cost of doing a single write is smaller than doing two
		// (without the alloc)
		b = bm.get(uint(16 + len(data)))
		defer bm.put(b)
	} else {
		b = header[:]
	}
	nbo.PutUint32(b, nbdReplyMagic)
	nbo.PutUint32(b[4:], replyErr)
	nbo.PutUint64(b[8:], handle)
	if len(data) > 0 {
		copy(b[16:], data)
	}
	_, err := w.Write(b)
	return err
}
