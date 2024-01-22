package main

import (
	"flag"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/akmistry/go-nbd"
)

const (
	blockSize = 4096
	nbdPrefix = "/dev/nbd"
)

var (
	dev        = flag.String("device", "/dev/nbd0", "Path to /dev/nbdX device")
	size       = flag.Int64("size", 64*1024*1024*1024, "Size of device, in bytes")
	useNetlink = flag.Bool("use-netlink", true, "Use netlink to initiate nbd")
)

type nullDevice struct {
}

func (nullDevice) ReadAt(b []byte, offset int64) (int, error) {
	for i := range b {
		b[i] = 0
	}
	return len(b), nil
}

func (nullDevice) WriteAt(b []byte, offset int64) (int, error) {
	return len(b), nil
}

func main() {
	flag.Parse()

	go func() {
		log.Println("http: ", http.ListenAndServe("localhost:6060", nil))
	}()

	opts := nbd.BlockDeviceOptions{
		BlockSize:     blockSize,
		ConcurrentOps: 4,
	}

	var err error
	var nbdDevice *nbd.NbdServer
	if *useNetlink {
		index, err := strconv.ParseUint(strings.TrimPrefix(*dev, nbdPrefix), 10, 32)
		if err != nil {
			log.Panicln(err)
		}
		nbdDevice, err = nbd.NewServerWithNetlink(int(index), nullDevice{}, *size, opts)
	} else {
		nbdDevice, err = nbd.NewServer(*dev, nullDevice{}, *size, opts)
	}
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
