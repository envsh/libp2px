package asnutil

import "net"

var Store backwardCompat

type backwardCompat struct{}

func (backwardCompat) AsnForIPv6(ip net.IP) (string, error) {
	return "", nil
}

func AsnForIPv6(ip net.IP) uint32 {
	return 0
}

func AsnForIPv6Network(network uint64) uint32 {
	return 0
}
