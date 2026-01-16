package simulation

// Direction designates the flow direction of a unidirectional flow from the point of view of the device processing the
// packet.
//
// The direction is either egress (an uplink packet leaving the session device), or
// ingress (a downlink packet entering the session device).
type Direction bool

const (
	// Uplink/Egress - packets being emitted from the device.
	EGRESS Direction = true
	// Downlink/Ingress - packets being absorbed by the device.
	INGRESS Direction = false
)
