package simulation

import (
	"encoding/base64"
	"net"
)

// IP is a string representation of net.IP
type IP string

func (ip IP) ToNetIP() net.IP {
	return net.ParseIP(string(ip))
}

func IPBase64(ip net.IP) string {
	// normalise IPv4
	ipn := ip.To4()
	if ipn == nil {
		ipn = ip
	}
	// convert to base 64 using URL encoding
	b64 := base64.URLEncoding.EncodeToString(ipn)
	return b64
}
