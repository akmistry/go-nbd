package nbd

import (
	"github.com/mdlayher/genetlink"
	"github.com/mdlayher/netlink"
)

type NetlinkConn struct {
	conn   *genetlink.Conn
	family genetlink.Family
	index  int

	fds            []int
	sizeBytes      uint64
	blockSizeBytes uint64
	readOnly       bool
	supportsTrim   bool
	supportsFlush  bool
}

func NewNetlinkConn(index int) (*NetlinkConn, error) {
	conn, err := genetlink.Dial(&netlink.Config{Strict: true})
	if err != nil {
		return nil, err
	}

	family, err := conn.GetFamily(nbdNlFamilyName)
	if err != nil {
		conn.Close()
		return nil, err
	}

	return &NetlinkConn{conn: conn, family: family, index: index}, nil
}

func (c *NetlinkConn) AddFd(fd int) {
	c.fds = append(c.fds, fd)
}

func (c *NetlinkConn) SetSize(size uint64) {
	c.sizeBytes = size
}

func (c *NetlinkConn) SetBlockSize(size uint64) {
	c.blockSizeBytes = size
}

func (c *NetlinkConn) SetReadonly(ro bool) {
	c.readOnly = ro
}

func (c *NetlinkConn) SetSupportsTrim(trim bool) {
	c.supportsTrim = trim
}

func (c *NetlinkConn) SetSupportsFlush(flush bool) {
	c.supportsFlush = flush
}

func (c *NetlinkConn) Connect() error {
	enc := netlink.NewAttributeEncoder()
	enc.Uint32(nbdNlAttrIndex, uint32(c.index))
	enc.Uint64(nbdNlAttrSizeBytes, c.sizeBytes)
	enc.Uint64(nbdNlAttrBlockSizeBytes, c.blockSizeBytes)
	enc.Uint64(nbdNlAttrClientFlags, nbdClientFlagDestroyOnDisconnect)
	var flags uint64
	if len(c.fds) > 1 {
		flags |= nbdFlagCanMultiConn
	}
	if c.readOnly {
		flags |= nbdFlagReadOnly
	}
	if c.supportsTrim {
		flags |= nbdFlagSendTrim
	}
	if c.supportsFlush {
		flags |= nbdFlagSendFlush
	}
	if flags != 0 {
		enc.Uint64(nbdNlAttrServerFlags, flags)
	}

	if len(c.fds) == 0 {
		panic("At least 1 FD must be added")
	}
	enc.Nested(nbdNlAttrSockets, func(nae *netlink.AttributeEncoder) error {
		for _, fd := range c.fds {
			nae.Nested(nbdNlSockItem, func(nae *netlink.AttributeEncoder) error {
				nae.Uint32(nbdNlSockFd, uint32(fd))
				return nil
			})
		}
		return nil
	})

	buf, err := enc.Encode()
	if err != nil {
		return err
	}

	req := genetlink.Message{
		Header: genetlink.Header{
			Command: nbdNlCmdConnect,
			Version: c.family.Version,
		},
		Data: buf,
	}
	_, err = c.conn.Execute(req, c.family.ID, netlink.Request)
	return err
}

func (c *NetlinkConn) Disconnect() error {
	enc := netlink.NewAttributeEncoder()
	// TODO: Use correct index if auto-discovered
	enc.Uint32(nbdNlAttrIndex, uint32(c.index))

	buf, err := enc.Encode()
	if err != nil {
		return err
	}

	req := genetlink.Message{
		Header: genetlink.Header{
			Command: nbdNlCmdDisconnect,
			Version: c.family.Version,
		},
		Data: buf,
	}
	_, err = c.conn.Send(req, c.family.ID, netlink.Request)
	return err
}
