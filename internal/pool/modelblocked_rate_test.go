package pool

import (
	"testing"
	"time"

	"github.com/linguo2625469/workbuddy2api-panel/internal/auth"
)

func TestModelBlockedRateCountUsesRoutingEligibility(t *testing.T) {
	for _, tc := range []struct {
		name        string
		rateReason  string
		disabled    bool
		paused      bool
		auditOnly   bool
		expired     bool
		otherRealm  bool
		wantBlocked bool
		wantCount   int
		wantRates   int
	}{
		{"mixed", "6004 model rate limit", false, false, false, false, false, true, 2, 1},
		{"short_reason", "6004", false, false, false, false, false, true, 2, 1},
		{"disabled", "6004 model rate limit", true, false, false, false, false, true, 1, 0},
		{"paused", "6004 model rate limit", false, true, false, false, false, true, 1, 0},
		{"audit_only", "6004 model rate limit", false, false, true, false, false, false, 0, 0},
		{"expired", "6004 model rate limit", false, false, false, true, false, false, 0, 0},
		{"other_realm", "6004 model rate limit", false, false, false, false, true, true, 1, 0},
		{"not_6004", "60040 other error", false, false, false, false, false, true, 2, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := New("")
			p.Add(&auth.Auth{UID: "unavailable"})
			domain := ""
			if tc.otherRealm {
				domain = "www.workbuddy.ai"
			}
			p.Add(&auth.Auth{UID: "rate", Domain: domain})
			now := time.Now()
			unavailableUntil := now.Add(time.Minute)
			rateUntil := now.Add(2 * time.Hour)
			if tc.expired {
				rateUntil = now.Add(-time.Minute)
			}
			p.mu.Lock()
			p.byUID["unavailable"].modelCooldowns = map[string]modelCooldown{
				"m": {Until: unavailableUntil, Reason: "11102 model not available"},
			}
			rate := p.byUID["rate"]
			rate.disabled, rate.paused = tc.disabled, tc.paused
			rate.modelCooldowns = map[string]modelCooldown{
				"m": {Until: rateUntil, ResetAt: now.Add(6 * time.Hour), Reason: tc.rateReason, AuditOnly: tc.auditOnly},
			}
			p.mu.Unlock()
			for i := 0; i < 20; i++ {
				got := p.ModelBlockedForRealm("m", "cn")
				if got.Blocked != tc.wantBlocked || got.Count != tc.wantCount || got.RateLimitedCount != tc.wantRates {
					t.Fatalf("unexpected classification: %+v", got)
				}
				if got.Blocked && !got.Until.Equal(unavailableUntil) {
					t.Fatalf("earliest local retry changed to a later rate-limit/reset time: %+v", got)
				}
			}
		})
	}
}
