package simulation

type Protocol string

const (
	// User Datagram Protocol
	UDP Protocol = "udp"
	// Transmission Control Protocol
	TCP Protocol = "tcp"
	// Stream Control Transmission Protocol
	SCTP Protocol = "sctp"
	// Datagram Delivery Protocol
	DDP Protocol = "ddp"
	// Internet Control Message Protocol
	ICMP Protocol = "icmp"
)
