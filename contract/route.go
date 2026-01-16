package contract

import ()

// RouteID defines a unique identifier for a given routing description.
type RouteID string

// Route defines a re-routing rule to be applied payloads with matching subjects.
//
// Re-routing never changes the payload.
// Re-routing never reads the payload contents.
//
// Payloads are re-routed by first transforming the subject. Unlike NATS subject mapping:
// - https://docs.nats.io/running-a-nats-service/configuration/configuring_subject_mapping
// domain re-routing is performed by leveraging relation-set knowledge.
//
// The re-routing rules define how to map incoming domain subjects to outgoing subjects
// based on leveraging subject tokens to either directly build a new subject, or first
// build a lookup key that is expanded via relation-sets.
//
// Tokens are extracted via straight-forward string splitting on `.`.
//
// Tokens are referenced by number. Relation-set look-up keys are referenced by name (starting with [a-z]).
// Note, transitive look-ups are not supported.
type Route struct {

	// ID provides a unique identifier for this routing rule.
	ID RouteID

	// Disabled routes are removed, where as enabled routes are compiled and actioned.
	Disabled bool

	// MatchA subject matching expression for domain notifications to be re-routed based
	// on this routing rule.
	Match string

	// Transform provides an expression that defines a new subject that is constructed through copying or
	// expansion of tokens or relation-set lookups.
	//
	// ${idx}  references a positional token in the incoming subject.
	// ${name} references a name in the relation-set look-up map.
	//
	// Note, relation-sets may result in zero or more values, therefore resulting in zero or more expanded versions.
	// Take care when referencing multiple relation-sets since the result may be a cross-product.
	Transform string

	// References provide a set of named expressions that are used to create keys used to look-up relation-sets.
	//
	// Each expression may contain positional token references, `${n}`, in order to build a look-up subject
	// based on the incoming domain notification subject name.
	//
	// The subject name constructed from substituting the token references will be used to fetch the latest
	// relation-set.
	References map[string]string
}

// Validate checks all the subject expressions in the routing rule and returns any expressions that are malformed.
func (r *Route) Validate() ([]string, bool) {
	return nil, false
}

func ValidateMatch(match string) bool {
	return false
}

func ValidateTransform(transform string, valid_references ...string) bool {
	return false
}

func ValidateReference(ref string) bool {
	return false
}
