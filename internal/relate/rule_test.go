package relate

import (
	"testing"
	"time"

	"go.resystems.io/eddt/contract"
	"go.resystems.io/eddt/internal/relate/assertion"
)

func TestRuleAssert(t *testing.T) {

	rules := []contract.CompilerRule{
		{
			Match:            "resys.sol.*.obs.3gpp.imei.*.imsi.*.attach",
			TTL:              30 * time.Minute,
			SourceType:       "imei",
			DestinationType:  "imsi",
			SourceToken:      6,
			DestinationToken: 8,
		},
		{
			Match:            "resys.sol.*.obs.3gpp.imei.*.imsi.*.detach",
			TTL:              0 * time.Minute,
			SourceType:       "imei",
			DestinationType:  "imsi",
			SourceToken:      6,
			DestinationToken: 8,
		},
		{
			Match:            "resys.sol.*.obs.3gpp.imsi.*.ip.*.session.create",
			TTL:              15 * time.Minute,
			SourceType:       "imsi",
			DestinationType:  "ip",
			SourceToken:      6,
			DestinationToken: 8,
		},
		{
			Match:            "resys.sol.*.obs.3gpp.imsi.*.ip.*.session.dispose",
			TTL:              15 * time.Minute,
			SourceType:       "imsi",
			DestinationType:  "ip",
			SourceToken:      6,
			DestinationToken: 8,
		},
	}

	sid, buf, err := subject_to_assertion(&rules[0], "resys.sol.test-solution.obs.3gpp.imei.28712576071100.imsi.545591184331028.attach", 200)
	if err != nil {
		t.Errorf("Failed to apply rule: %v", err)
	}

	if sid != "test-solution" {
		t.Errorf("Bad SID: 'test-solution' != %s", sid)
	}

	// map buffer
	assertion := assertion.GetRootAsAssertion(buf, 0)

	if string(assertion.St()) != "imei" {
		t.Errorf("Incorrect ST type expected 'imei' got %s", assertion.St())
	}
	if string(assertion.Si()) != "28712576071100" {
		t.Errorf("Incorrect SI IMEI expected 28712576071100 got %s", assertion.Si())
	}
	if string(assertion.Dt()) != "imsi" {
		t.Errorf("Incorrect DT type expected 'imsi' got %s", assertion.Dt())
	}
	if string(assertion.Di()) != "545591184331028" {
		t.Errorf("Incorrect DI IMSI expected 545591184331028 got %s", assertion.Di())
	}
	if assertion.Ttl() != rules[0].TTL.Milliseconds() {
		t.Errorf("Incorrect DI IMSI expected %d got %d", rules[0].TTL.Milliseconds(), assertion.Ttl())
	}

}
