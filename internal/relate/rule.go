package relate

import (
	"fmt"
	"strings"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"

	"go.resystems.io/eddt/contract"
	"go.resystems.io/eddt/internal/relate/assertion"
)

// Example observations subjects
//
// resys.sol.test-solution.obs.ip.AQEBAQ==.ip.Cqmzgg==.packet
// resys.sol.%s.obs.3gpp.imei.%s.create
// resys.sol.%s.obs.3gpp.imei.%s.dispose
// resys.sol.%s.obs.3gpp.imei.%s.imsi.%s.attach
// resys.sol.%s.obs.3gpp.imei.%s.imsi.%s.detach
// resys.sol.%s.obs.3gpp.imsi.%s.ip.%s.session.create
// resys.sol.%s.obs.3gpp.imsi.%s.ip.%s.session.dispose
// resys.sol.%s.obs.3gpp.imsi.%s.flow.create
// resys.sol.%s.obs.3gpp.imsi.%s.flow.dispose
// resys.sol.%s.obs.ip.%s.ip.%s.packet

const SID = 2

func subject_to_assertion(r *contract.CompilerRule, sub string, estimate int) (string, []byte, error) {
	// tokenise the subject
	last := max(r.SourceToken, r.DestinationToken)
	// We don't need to split arbitrarily. But, we need to account for starting a zero,
	// and we need to extend beyond the last token need.
	required := last + 2
	tokens := strings.SplitN(sub, ".", required)
	if len(tokens) < last {
		// ignore mismatching subjects
		err := fmt.Errorf("Subject [%s] did not have enough tokens (dst=%d;src=%d)",
			sub, r.DestinationToken, r.SourceToken)
		return "", nil, err
	}

	// compute the expiry timestamp
	// (we round to the closest TTL quanta e.g. 10 seconds)
	now := time.Now()
	then := now.Add(r.TTL)
	then.Round(r.TTLQuantisation)

	// build an assertion
	builder := flatbuffers.NewBuilder(estimate)
	//   create strings
	sid := builder.CreateString(tokens[SID])
	si := builder.CreateString(tokens[r.SourceToken])
	di := builder.CreateString(tokens[r.DestinationToken])
	st := builder.CreateString(string(r.SourceType))
	dt := builder.CreateString(string(r.DestinationType))
	//   build the buffer
	assertion.AssertionStart(builder)
	assertion.AssertionAddSid(builder, sid)
	assertion.AssertionAddSi(builder, si)
	assertion.AssertionAddDi(builder, di)
	assertion.AssertionAddSt(builder, st)
	assertion.AssertionAddDt(builder, dt)
	assertion.AssertionAddTtl(builder, r.TTL.Milliseconds())
	assertion.AssertionAddExp(builder, then.UnixMilli())
	assert := assertion.AssertionEnd(builder)
	builder.Finish(assert)

	// extract the buffer slice
	buf := builder.FinishedBytes()
	return tokens[SID], buf, nil
}
