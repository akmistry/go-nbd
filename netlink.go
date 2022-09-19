package nbd

import (
	"github.com/mdlayher/genetlink"
	"github.com/mdlayher/netlink"
)

type NetlinkConn struct {
	conn   *genetlink.Conn
	family genetlink.Family

	fd             int
	sizeBytes      uint64
	blockSizeBytes uint64
}

func NewNetlinkConn() (*NetlinkConn, error) {
	conn, err := genetlink.Dial(&netlink.Config{Strict: true})
	if err != nil {
		return nil, err
	}

	family, err := conn.GetFamily(nbdNlFamilyName)
	if err != nil {
		conn.Close()
		return nil, err
	}

	return &NetlinkConn{conn: conn, family: family}, nil
}

func (c *NetlinkConn) SetFd(fd int) {
	c.fd = fd
}

func (c *NetlinkConn) SetSize(size uint64) {
	c.sizeBytes = size
}

func (c *NetlinkConn) SetBlockSize(size uint64) {
	c.blockSizeBytes = size
}

func (c *NetlinkConn) Connect() error {
	enc := netlink.NewAttributeEncoder()
	enc.Uint32(nbdNlAttrIndex, 0)
	enc.Uint64(nbdNlAttrSizeBytes, c.sizeBytes)
	enc.Uint64(nbdNlAttrBlockSizeBytes, c.blockSizeBytes)

	enc.Nested(nbdNlAttrSockets, func(nae *netlink.AttributeEncoder) error {
		nae.Nested(nbdNlSockItem, func(nae *netlink.AttributeEncoder) error {
			nae.Uint32(nbdNlSockFd, uint32(c.fd))
			return nil
		})
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
	enc.Uint32(nbdNlAttrIndex, 0)

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
