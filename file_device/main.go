package main

import (
	"flag"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"

	"github.com/akmistry/go-nbd"
)

var (
	dev  = flag.String("device", "/dev/nbd0", "Path to /deb/nbdX device.")
	file = flag.String("file", "", "Path to file to use as block device.")
)

func main() {
	flag.Parse()

	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	if *file == "" {
		log.Println("Block device file must be specified")
		return
	}

	f, err := os.OpenFile(*file, os.O_RDWR, 0)
	if err != nil {
		log.Panicln(err)
	}

	nbdDevice, err := nbd.NewServer(*dev, NewFileBlockDevice(f))
	if err != nil {
		log.Panicln(err)
	}

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt)
	go func() {
		<-ch
		nbdDevice.Disconnect()
	}()

	log.Println(nbdDevice.Run())
}
