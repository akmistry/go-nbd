package nbd

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"runtime"
	"sync"
	"syscall"

	"golang.org/x/sys/unix"
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

	reqHeaderPool = sync.Pool{New: func() any {
		b := make([]byte, 28)
		return &b
	}}

	replyHeaderPool = sync.Pool{New: func() any {
		b := make([]byte, 16)
		return &b
	}}
)

func readRequest(r io.Reader, req *nbdRequest) error {
	hp := reqHeaderPool.Get().(*[]byte)
	defer reqHeaderPool.Put(hp)

	_, err := io.ReadFull(r, *hp)
	if err != nil {
		return err
	}
	magic := nbo.Uint32(*hp)
	if magic != nbdRequestMagic {
		return fmt.Errorf("Unexpected request magic 0x%x", magic)
	}
	req.flags = nbo.Uint16((*hp)[4:])
	req.cmd = nbo.Uint16((*hp)[6:])
	req.handle = nbo.Uint64((*hp)[8:])
	req.offset = nbo.Uint64((*hp)[16:])
	req.length = nbo.Uint32((*hp)[24:])
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
	osf, fileOk := w.(*os.File)

	hp := replyHeaderPool.Get().(*[]byte)
	defer replyHeaderPool.Put(hp)

	var b []byte
	if reply.data != nil && !fileOk {
		// TODO: Determine if the total cost of doing a single write is smaller than doing two
		// (without the alloc)
		b = make([]byte, 16+len(reply.data.buf))
	} else {
		b = *hp
	}
	nbo.PutUint32(b, nbdReplyMagic)
	nbo.PutUint32(b[4:], reply.err)
	nbo.PutUint64(b[8:], reply.handle)
	if reply.data != nil {
		if fileOk {
			var iov [2][]byte
			iov[0] = b
			iov[1] = reply.data.buf
			for {
				_, err := unix.Writev(int(osf.Fd()), iov[:])
				runtime.KeepAlive(osf)
				if errno, ok := err.(syscall.Errno); ok && errno == unix.EINTR {
					// Try again
					continue
				}
				return err
			}
		}
		copy(b[16:], reply.data.buf)
	}
	_, err := w.Write(b)
	return err
}
