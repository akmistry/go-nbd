package main

import (
	"log"
	"os"
)

type FileBlockDevice struct {
	file *os.File
}

func NewFileBlockDevice(file *os.File) *FileBlockDevice {
	return &FileBlockDevice{file: file}
}

func (f *FileBlockDevice) Readonly() bool {
	return false
}

func (f *FileBlockDevice) MaxConcurrentOps() int {
	return 1
}

func (f *FileBlockDevice) Size() int64 {
	info, err := f.file.Stat()
	if err != nil {
		log.Panicln("Error reading file info", err)
	}
	size := int64(info.Size())
	return size - (size % int64(f.BlockSize()))
}

func (f *FileBlockDevice) BlockSize() int {
	return 512
}

func (f *FileBlockDevice) ReadAt(p []byte, off int64) (n int, err error) {
	if off%int64(f.BlockSize()) != 0 {
		log.Panicln("Invalid offset", off)
	} else if len(p)%f.BlockSize() != 0 {
		log.Panicln("Invalid read length", len(p))
	}
	return f.file.ReadAt(p, off)
}

func (f *FileBlockDevice) WriteAt(p []byte, off int64) (n int, err error) {
	if off%int64(f.BlockSize()) != 0 {
		log.Panicln("Invalid offset", off)
	} else if len(p)%f.BlockSize() != 0 {
		log.Panicln("Invalid write length", len(p))
	}
	return f.file.WriteAt(p, off)
}

func (f *FileBlockDevice) Close() error {
	return f.file.Close()
}

func (f *FileBlockDevice) Flush() error {
	return f.file.Sync()
}

func (f *FileBlockDevice) Trim(off int64, length uint32) error {
	if off%int64(f.BlockSize()) != 0 {
		log.Panicln("Invalid offset", off)
	} else if int(length)%f.BlockSize() != 0 {
		log.Panicln("Invalid trim length", length)
	}
	// Writing zeros assumes the underlying filesystem can make the file sparse.
	return f.writeZeros(off, length)
}

func (f *FileBlockDevice) writeZeros(off int64, length uint32) error {
	const oneMeg = 1024 * 1024
	b := make([]byte, oneMeg)
	for i := int64(0); i < int64(length); i += oneMeg {
		start := off + i
		end := off + i + oneMeg
		if end > (off + int64(length)) {
			end = off + int64(length)
		}
		l := end - start
		_, err := f.file.WriteAt(b[:l], start)
		if err != nil {
			return err
		}
	}
	return nil
}
