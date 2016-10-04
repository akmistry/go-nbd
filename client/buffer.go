package client

type bufferManager struct {
	ch   chan []byte
	size uint
}

func newBufferManager(size uint, buffers uint) *bufferManager {
	return &bufferManager{ch: make(chan []byte, buffers), size: size}
}

func (m *bufferManager) get(size uint) []byte {
	if size > m.size {
		return make([]byte, size)
	}

	select {
	case b := <-m.ch:
		return b[:size]
	default:
		return make([]byte, size, m.size)
	}
}

func (m *bufferManager) put(buf []byte) {
	if uint(cap(buf)) != m.size {
		return
	}

	select {
	case m.ch <- buf[:cap(buf)]:
	default:
	}
}
