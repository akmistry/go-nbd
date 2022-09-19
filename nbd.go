package nbd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sync"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sys/unix"
)

const (
	DefaultMaxConcurrentOps = 16
)

var (
	ErrUnsupported = errors.New("nbd: unsupported operation")
)

type BlockDevice interface {
	io.ReaderAt
	io.WriterAt
	io.Closer

	Readonly() bool
	Size() int64
	BlockSize() int
	MaxConcurrentOps() int
}

type BlockDeviceTrimer interface {
	Trim(off int64, length uint32) error
}

type BlockDeviceFlusher interface {
	Flush() error
}

type NbdServer struct {
	devFd  int
	sockfd int
	block  BlockDevice

	// Netlink stuff
	nlConn *NetlinkConn
	index  int

	reqCh  chan *nbdRequest
	doneCh chan bool
}

func NewServer(dev string, block BlockDevice) (*NbdServer, error) {
	devFd, err := unix.Open(dev, unix.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	return NewServerFromFd(devFd, block)
}

func NewServerFromFd(devFd int, block BlockDevice) (*NbdServer, error) {
	return &NbdServer{
		devFd:  devFd,
		block:  block,
		reqCh:  make(chan *nbdRequest),
		doneCh: make(chan bool),
	}, nil
}

func NewServerWithNetlink(index int, block BlockDevice) (*NbdServer, error) {
	nl, err := NewNetlinkConn()
	if err != nil {
		return nil, err
	}

	return &NbdServer{
		block:  block,
		nlConn: nl,
		index:  index,
		reqCh:  make(chan *nbdRequest),
		doneCh: make(chan bool),
	}, nil
}

func (s *NbdServer) runNetlink(f *os.File, fd int) error {
	s.nlConn.SetFd(fd)
	s.nlConn.SetSize(uint64(s.block.Size()))
	s.nlConn.SetBlockSize(uint64(s.block.BlockSize()))

	err := s.nlConn.Connect()
	if err != nil {
		f.Close()
		log.Println("Error connecting to NBD: ", err)
		return err
	}

	go s.do(f)
	<-s.doneCh

	return nil
}

func (s *NbdServer) Run() error {
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
	if err != nil {
		log.Println("Error creating socket pair: ", err)
		return err
	}
	f := os.NewFile(uintptr(fds[1]), "nbd-sock")

	if s.nlConn != nil {
		return s.runNetlink(f, fds[0])
	}

	_, _, errno := unix.Syscall(unix.SYS_IOCTL, uintptr(s.devFd), nbdSetSock, uintptr(fds[0]))
	if errno != 0 {
		log.Println("Error setting NBD socket:", errno)
		return errno
	}
	_, _, errno = unix.Syscall(unix.SYS_IOCTL, uintptr(s.devFd), nbdSetBlkSize, uintptr(s.block.BlockSize()))
	if errno != 0 {
		log.Println("Error setting NBD block size:", errno)
		return errno
	}
	sizeBlocks := s.block.Size() / int64(s.block.BlockSize())
	if int64(uintptr(sizeBlocks)) != sizeBlocks {
		return fmt.Errorf("File size %d too big for arch, bs=%d, blocks=%d", s.block.Size(), s.block.BlockSize(), sizeBlocks)
	}
	_, _, errno = unix.Syscall(unix.SYS_IOCTL, uintptr(s.devFd), nbdSetSizeBlocks, uintptr(sizeBlocks))
	if errno != 0 {
		log.Println("Error setting NBD size blocks:", errno)
		return errno
	}

	var flags uint16
	if s.block.Readonly() {
		flags |= nbdFlagHasFlags | nbdFlagReadOnly
	}
	if _, ok := s.block.(BlockDeviceFlusher); ok {
		flags |= nbdFlagHasFlags | nbdFlagSendFlush
	}
	if _, ok := s.block.(BlockDeviceTrimer); ok {
		flags |= nbdFlagHasFlags | nbdFlagSendTrim
	}
	if flags != 0 {
		_, _, errno = unix.Syscall(unix.SYS_IOCTL, uintptr(s.devFd), nbdSetFlags, uintptr(flags))
		if errno != 0 {
			log.Println("Error setting NBD flags:", errno)
			return errno
		}
	}

	go s.do(f)
	_, _, errno = unix.Syscall(unix.SYS_IOCTL, uintptr(s.devFd), nbdDoIt, 0)
	if errno != 0 {
		return errno
	}

	return nil
}

func (s *NbdServer) Disconnect() error {
	if s.nlConn != nil {
		return s.nlConn.Disconnect()
	}

	_, _, errno := unix.Syscall(unix.SYS_IOCTL, uintptr(s.devFd), nbdDisconnect, 0)
	if errno != 0 {
		return errno
	}
	return nil
}

var (
	reqPool   = sync.Pool{New: func() interface{} { return new(nbdRequest) }}
	replyPool = sync.Pool{New: func() interface{} { return new(nbdReply) }}
)

func (s *NbdServer) doRequest(req *nbdRequest) (*nbdReply, error) {
	reply := replyPool.Get().(*nbdReply)
	reply.handle = req.handle
	reply.err = 0
	reply.data = nil

	var err error
	switch req.cmd {
	case nbdCmdRead:
		reply.data = NewBuffer(int(req.length))
		_, err = s.block.ReadAt(reply.data.buf, int64(req.offset))
	case nbdCmdWrite:
		_, err = s.block.WriteAt(req.data.buf, int64(req.offset))
	case nbdCmdFlush:
		err = s.block.(BlockDeviceFlusher).Flush()
	case nbdCmdTrim:
		log.Printf("Trim off: %d, len: %d", req.offset, req.length)
		err = s.block.(BlockDeviceTrimer).Trim(int64(req.offset), req.length)
	case nbdCmdCache:
		fallthrough
	case nbdCmdWriteZeroes:
		fallthrough
	default:
		log.Println("Unsupported operation", req.cmd)
		err = ErrUnsupported
	}
	if err != nil {
		log.Printf("request error: %v", err)
		reply.err = nbdEio
		//return nil, err
	}
	return reply, nil
}

func (s *NbdServer) do(f *os.File) {
	defer close(s.doneCh)
	defer f.Close()

	g, ctx := errgroup.WithContext(context.Background())

	var replyLock sync.Mutex

	workers := s.block.MaxConcurrentOps()
	if workers <= 0 {
		workers = DefaultMaxConcurrentOps
	}

	for i := 0; i < workers; i++ {
		g.Go(func() error {
			var req *nbdRequest
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case req = <-s.reqCh:
					if req == nil {
						return nil
					}
				}

				reply, err := s.doRequest(req)
				if err != nil {
					return err
				}
				if req.data != nil {
					req.data.Release()
					req.data = nil
				}
				reqPool.Put(req)

				replyLock.Lock()
				err = writeReply(f, reply)
				replyLock.Unlock()

				if reply.data != nil {
					reply.data.Release()
					reply.data = nil
				}
				if err != nil {
					log.Printf("Error writing NBD reply: %v", err)
					return err
				}
				replyPool.Put(reply)
			}
		})
	}

	go func() {
		err := g.Wait()
		if err != nil {
			s.Disconnect()
		}
	}()

	var err error
	for {
		req := reqPool.Get().(*nbdRequest)
		err = readRequest(f, req)
		if err != nil {
			break
		}

		if req.cmd == nbdCmdDisc {
			break
		}

		s.reqCh <- req
	}
	close(s.reqCh)

	if err != nil {
		log.Println("Error in main server loop", err)
	}

	g.Wait()
	if s.nlConn == nil {
		unix.Syscall(unix.SYS_IOCTL, uintptr(s.devFd), nbdClearSock, 0)
		unix.Close(s.devFd)
	}
	s.block.Close()
}
