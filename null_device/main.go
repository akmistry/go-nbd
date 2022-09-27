package main

import (
	"flag"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"

	"github.com/akmistry/go-nbd"
)

const (
	blockSize = 512
)

var (
	size = flag.Int64("size", 64*1024*1024*1024, "Device size, in bytes")
)

type nullDevice struct {
}

func (d *nullDevice) ReadAt(b []byte, off int64) (int, error) {
	for i := range b {
		b[i] = 0
	}
	return len(b), nil
}

func (d *nullDevice) WriteAt(b []byte, off int64) (int, error) {
	return len(b), nil
}

func main() {
	flag.Parse()

	go func() {
		log.Println("http: ", http.ListenAndServe("localhost:6060", nil))
	}()

	opts := nbd.BlockDeviceOptions{
		BlockSize:     blockSize,
		ConcurrentOps: 1,
	}
	nbdDevice, err := nbd.NewServerWithNetlink(0, &nullDevice{}, *size, opts)
	if err != nil {
		log.Panicln(err)
	}

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGQUIT, syscall.SIGTERM)
	go func() {
		<-ch
		nbdDevice.Disconnect()
	}()

	log.Println("nbd: ", nbdDevice.Run())
}
