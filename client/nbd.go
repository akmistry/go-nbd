package client

import (
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"golang.org/x/sys/unix"
)

type BlockDevice interface {
	io.ReaderAt
	io.WriterAt
	io.Closer

	Readonly() bool
	Size() uint64
	BlockSize() uint32
}

type BlockDeviceTrimer interface {
	Trim(off uint64, length uint32) error
}

type BlockDeviceFlusher interface {
	Flush() error
}

type NbdServer struct {
	devFd  int
	sockfd int
	block  BlockDevice
}

func NewServer(dev string, block BlockDevice) (*NbdServer, error) {
	devFd, err := unix.Open(dev, unix.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	return NewServerFromFd(devFd, block)
}

func NewServerFromFd(devFd int, block BlockDevice) (*NbdServer, error) {
	return &NbdServer{devFd: devFd, block: block}, nil
}

func (s *NbdServer) Run() error {
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM, 0)
	if err != nil {
		log.Println("Error creating socket pair:", err)
		return err
	}
	f := os.NewFile(uintptr(fds[1]), "nbd-sock")
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
	sizeBlocks := s.block.Size() / uint64(s.block.BlockSize())
	if uint64(uintptr(sizeBlocks)) != sizeBlocks {
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
	_, _, errno := unix.Syscall(unix.SYS_IOCTL, uintptr(s.devFd), nbdDisconnect, 0)
	if errno != 0 {
		return errno
	}
	return nil
}

func (s *NbdServer) do(f *os.File) {
	var err error
	defer func() {
		unix.Syscall(unix.SYS_IOCTL, uintptr(s.devFd), nbdClearSock, 0)
		unix.Close(s.devFd)
		f.Close()
		s.block.Close()
		if err != nil {
			log.Println("Error in main server loop", err)
		}
	}()

	start := time.Now()
	var readBytes, writeBytes uint64
	var readCount, writeCount int
	for {
		var req request
		err = readRequestTo(f, &req)
		if err != nil {
			return
		}

		switch req.cmd {
		case nbdCmdRead:
			data := bm.get(uint(req.length))
			_, err = s.block.ReadAt(data, int64(req.offset))
			if err != nil {
				return
			}
			err = writeReply(f, req.handle, 0, data)
			bm.put(data)
			if err != nil {
				return
			}
			readBytes += uint64(req.length)
			readCount++
		case nbdCmdWrite:
			_, err = s.block.WriteAt(req.data, int64(req.offset))
			bm.put(req.data)
			if err != nil {
				return
			}
			err = writeReply(f, req.handle, 0, nil)
			if err != nil {
				return
			}
			writeBytes += uint64(len(req.data))
			writeCount++
		case nbdCmdDisc:
			return
		case nbdCmdFlush:
			err = s.block.(BlockDeviceFlusher).Flush()
			if err != nil {
				return
			}
			err = writeReply(f, req.handle, 0, nil)
			if err != nil {
				return
			}
		case nbdCmdTrim:
			err = s.block.(BlockDeviceTrimer).Trim(req.offset, req.length)
			if err != nil {
				return
			}
			err = writeReply(f, req.handle, 0, nil)
			if err != nil {
				return
			}
		case nbdCmdCache:
			fallthrough
		case nbdCmdWriteZeroes:
			fallthrough
		default:
			log.Panicln("Unsupported operation", req.cmd)
		}

		diff := time.Since(start)
		if diff > time.Second {
			td := float64(diff) / float64(time.Second)
			readBw := float64(readBytes) / td / (1024 * 1024)
			writeBw := float64(writeBytes) / td / (1024 * 1024)
			readOps := float64(readCount) / td
			writeOps := float64(writeCount) / td
			log.Printf("read BW %0.3fM (%0.1f ops), write BW %0.3fM (%0.1f ops)\n",
				readBw, readOps, writeBw, writeOps)
			readBytes, writeBytes = 0, 0
			readCount, writeCount = 0, 0
			start = time.Now()
		}
	}
}
