package connthrottler

import "net"

type Conn struct {
	netConn net.Conn
}
