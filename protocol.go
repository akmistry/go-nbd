package nbd

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
)

var (
	nbo = binary.BigEndian
)

type nbdRequest struct {
	flags  uint16
	cmd    uint16
	handle uint64
	offset uint64
	length uint32
	data   *Buffer
}

func (r *nbdRequest) String() string {
	name := "<Unsupported>"
	args := ""
	switch r.cmd {
	case nbdCmdRead:
		name = "Read"
		args = fmt.Sprintf("offset: %d, length: %d", r.offset, r.length)
	case nbdCmdWrite:
		name = "Write"
		args = fmt.Sprintf("offset: %d, length: %d", r.offset, len(*r.data.buf))
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

func readRequest(r *bufio.Reader, req *nbdRequest) error {
	h, err := r.Peek(28)
	if err != nil {
		return err
	}
	magic := nbo.Uint32(h)
	if magic != nbdRequestMagic {
		return fmt.Errorf("Unexpected request magic 0x%x", magic)
	}
	req.flags = nbo.Uint16(h[4:])
	req.cmd = nbo.Uint16(h[6:])
	req.handle = nbo.Uint64(h[8:])
	req.offset = nbo.Uint64(h[16:])
	req.length = nbo.Uint32(h[24:])
	req.data = nil
	_, err = r.Discard(28)
	if err != nil {
		panic(err)
	}
	if req.cmd == nbdCmdWrite {
		req.data = NewBuffer(int(req.length))
		_, err = io.ReadFull(r, *req.data.buf)
		if err != nil {
			req.data.Release()
			return err
		}
	}
	return nil
}
