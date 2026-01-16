package route

import (
	"go.resystems.io/eddt/internal/relate/relationset"
)

// RelationSetSource provides access to relation-sets given the domain key.
type RelationSetSource interface {

	// Get returns the last known `RelationSet` associated with a given domain key.
	//
	// e.g.
	// - resys.sol.test-solution.r.kv.imei.50886290034037.imsi.set
	// - resys.sol.test-solution.r.kv.imsi.702208948702285.ip.set
	// - resys.sol.test-solution.r.kv.ip.Cos-5w==.imsi.set
	Get(key string) (*relationset.RelationSet, bool)
}
