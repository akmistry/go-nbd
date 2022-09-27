package main

import (
	"os"
)

const (
	oneMeg = 1024 * 1024
)

var (
	zeroBuffer = make([]byte, oneMeg)
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
	// Writing zeros allows the underlying filesystem to make the file sparse,
	// or optimise the stored file data.
	// Alternatively, use fallocate(FALLOC_FL_PUNCH_HOLE).
	return f.writeZeros(off, length)
}

func (f *FileBlockDevice) writeZeros(off int64, length uint32) error {
	for i := int64(0); i < int64(length); i += oneMeg {
		start := off + i
		end := off + i + oneMeg
		if end > (off + int64(length)) {
			end = off + int64(length)
		}
		l := end - start
		_, err := f.WriteAt(zeroBuffer[:l], start)
		if err != nil {
			return err
		}
	}
	return nil
}
