package nbd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math/bits"
	"os"
	"sync"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sys/unix"
)

const (
	DefaultBlockSize     = 512
	DefaultConcurrentOps = 1

	// Maximum number of concurrent operations
	// (block device queue depth: https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git/tree/drivers/block/nbd.c?h=v5.15#n1692)
	MaxConcurrentOps = 128
)

var (
	ErrUnsupported = errors.New("nbd: unsupported operation")
)

type BlockDevice interface {
	io.ReaderAt
	io.WriterAt
}

type BlockDeviceTrimer interface {
	Trim(off int64, length uint32) error
}

type BlockDeviceFlusher interface {
	Flush() error
}

type BlockDeviceOptions struct {
	// BlockSize is the size of each block on the block device, in bytes.
	// Must be between 512 and the system page size (usually 4096 on x86).
	// If 0, the default value of DefaultBlockSize will be used.
	BlockSize int

	// ConcurrentOps is the number of operations (read, write, trim, flush)
	// which can be performed concurrently. Must be between 1 and 128.
	// If 0, the default value of DefaultConcurrentOps will be used.
	ConcurrentOps int

	// Readonly should be set to true if the block device is read-only.
	Readonly bool
}

type NbdServer struct {
	opts   BlockDeviceOptions
	size   int64
	devFd  int
	sockfd int
	block  BlockDevice

	// Netlink stuff
	nlConn *NetlinkConn
	index  int

	reqCh  chan *nbdRequest
	doneCh chan bool
}

func validateOptions(opts *BlockDeviceOptions, size int64) error {
	pageSize := os.Getpagesize()
	if opts.BlockSize == 0 {
		opts.BlockSize = DefaultBlockSize
	} else if opts.BlockSize < 512 || opts.BlockSize > pageSize {
		return fmt.Errorf("nbd: BlockSize must be between 512 and %d", pageSize)
	} else if bits.OnesCount(uint(opts.BlockSize)) != 1 {
		return errors.New("nbd: BlockSize must be a power-of-2")
	}

	if size <= 0 || size%int64(opts.BlockSize) != 0 {
		return errors.New("nbd: size must be a positive multiple of BlockSize")
	}

	if opts.ConcurrentOps == 0 {
		opts.ConcurrentOps = DefaultConcurrentOps
	} else if opts.ConcurrentOps < 0 || opts.ConcurrentOps > MaxConcurrentOps {
		return fmt.Errorf("nbd: ConcurrentOps must be between 1 and %d", MaxConcurrentOps)
	}

	return nil
}

func NewServer(dev string, block BlockDevice, size int64, opts BlockDeviceOptions) (*NbdServer, error) {
	devFd, err := unix.Open(dev, unix.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	return NewServerFromFd(devFd, block, size, opts)
}

func NewServerFromFd(devFd int, block BlockDevice, size int64, opts BlockDeviceOptions) (*NbdServer, error) {
	err := validateOptions(&opts, size)
	if err != nil {
		return nil, err
	}
	return &NbdServer{
		opts:   opts,
		size:   size,
		devFd:  devFd,
		block:  block,
		reqCh:  make(chan *nbdRequest),
		doneCh: make(chan bool),
	}, nil
}

func NewServerWithNetlink(index int, block BlockDevice, size int64, opts BlockDeviceOptions) (*NbdServer, error) {
	if index < 0 {
		return nil, errors.New("nbd: index must be non-negative")
	}

	err := validateOptions(&opts, size)
	if err != nil {
		return nil, err
	}

	nl, err := NewNetlinkConn()
	if err != nil {
		return nil, err
	}

	return &NbdServer{
		opts:   opts,
		size:   size,
		block:  block,
		nlConn: nl,
		index:  index,
		reqCh:  make(chan *nbdRequest),
		doneCh: make(chan bool),
	}, nil
}

func (s *NbdServer) runNetlink(f *os.File, fd int) error {
	s.nlConn.SetIndex(s.index)
	s.nlConn.SetFd(fd)
	s.nlConn.SetSize(uint64(s.size))
	s.nlConn.SetBlockSize(uint64(s.opts.BlockSize))

	if s.opts.Readonly {
		s.nlConn.SetReadonly(true)
	}
	if _, ok := s.block.(BlockDeviceFlusher); ok {
		s.nlConn.SetSupportsFlush(true)
	}
	if _, ok := s.block.(BlockDeviceTrimer); ok {
		s.nlConn.SetSupportsTrim(true)
	}

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
	_, _, errno = unix.Syscall(unix.SYS_IOCTL, uintptr(s.devFd), nbdSetBlkSize, uintptr(s.opts.BlockSize))
	if errno != 0 {
		log.Println("Error setting NBD block size:", errno)
		return errno
	}
	sizeBlocks := s.size / int64(s.opts.BlockSize)
	if int64(uintptr(sizeBlocks)) != sizeBlocks {
		return fmt.Errorf("File size %d too big for arch, bs=%d, blocks=%d", s.size, s.opts.BlockSize, sizeBlocks)
	}
	_, _, errno = unix.Syscall(unix.SYS_IOCTL, uintptr(s.devFd), nbdSetSizeBlocks, uintptr(sizeBlocks))
	if errno != 0 {
		log.Println("Error setting NBD size blocks:", errno)
		return errno
	}

	var flags uint16
	if s.opts.Readonly {
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

	workers := s.opts.ConcurrentOps
	if workers <= 0 {
		workers = DefaultConcurrentOps
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
}
