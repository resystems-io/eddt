package contract

import (
	"time"
)

type Kind string

// CompilerRule defines a rule for generating relationship edge assertions based on subject names.
//
// The relationship edges are unidirectional, from source to destination.
// The payload data is not interpreted. Only the subject name is considered.
type CompilerRule struct {
	Description      string        `json:"description"`      // A human readable description.
	Match            string        `json:"match"`            // The subjects to match when applying this assertion rule.
	TTL              time.Duration `json:"duration"`         // The duration that the assertion is valid for, before expiring.
	TTLQuantisation  time.Duration `json:"quantisation"`     // Round to expiry to the closest TTL quanta e.g. 10 seconds
	SourceType       Kind          `json:"sourceType"`       // The type of identifier which forms part of the source portion of the edge key.
	DestinationType  Kind          `json:"destinationType"`  // The type of identifier which forms part of the destination portion of the edge key.
	SourceToken      int           `json:"sourceToken"`      // The position of the source identifier token in the subject.
	DestinationToken int           `json:"destinationToken"` // The position of the destination identifier token in the subject.
}
