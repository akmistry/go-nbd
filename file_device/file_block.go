package main

import (
	"log"
	"os"
	"sync"
	"sync/atomic"
	"syscall"

	"golang.org/x/sys/unix"
)

var (
	enosysOnce    sync.Once
	eopnotsupOnce sync.Once

	punchHoleUnsupported atomic.Bool
)

type FileBlockDevice struct {
	*os.File
}

func NewFileBlockDevice(file *os.File) *FileBlockDevice {
	return &FileBlockDevice{File: file}
}

func (f *FileBlockDevice) Flush() error {
	return f.File.Sync()
}

func (f *FileBlockDevice) Trim(off int64, length uint32) error {
	if punchHoleUnsupported.Load() {
		return nil
	}

	log.Printf("Trim off: %d, len: %d", off, length)
	err := unix.Fallocate(int(f.Fd()), unix.FALLOC_FL_KEEP_SIZE|unix.FALLOC_FL_PUNCH_HOLE, off, int64(length))
	if err != nil {
		if errno, ok := err.(syscall.Errno); ok {
			switch errno {
			case unix.ENOSYS:
				punchHoleUnsupported.Store(true)
				enosysOnce.Do(func() {
					log.Println("fallocate() not supported")
				})
				return nil
			case unix.EOPNOTSUPP:
				punchHoleUnsupported.Store(true)
				eopnotsupOnce.Do(func() {
					log.Println("fallocate(FALLOC_FL_PUNCH_HOLE) not supported on this filesystem")
				})
				return nil
			}
		}
		log.Println("fallocate() error: ", err)
	}
	return err
}
