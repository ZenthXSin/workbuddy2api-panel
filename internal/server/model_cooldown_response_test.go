package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/linguo2625469/workbuddy2api-panel/internal/auth"
	"github.com/linguo2625469/workbuddy2api-panel/internal/pool"
)

func assertModelCooldownResponse(t *testing.T, rec *httptest.ResponseRecorder, status int, code string, retryAt time.Time, started, finished time.Time) {
	t.Helper()
	if rec.Code != status {
		t.Fatalf("status=%d want %d body=%s", rec.Code, status, rec.Body)
	}
	var response struct {
		Error struct {
			Code string `json:"code"`
			Hint string `json:"gateway_hint"`
		} `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Error.Code != code {
		t.Fatalf("error.code=%q want %q", response.Error.Code, code)
	}
	header := rec.Header().Get("Retry-After")
	if retryAt.IsZero() {
		if header != "" {
			t.Fatalf("model refusal must not advertise retry: %q", header)
		}
		return
	}
	seconds, err := strconv.ParseInt(header, 10, 64)
	if err != nil || seconds < 1 {
		t.Fatalf("invalid Retry-After %q: %v", header, err)
	}
	// The header must never invite an early retry, and may round up by less than a second.
	if finished.Add(time.Duration(seconds)*time.Second).Before(retryAt) || started.Add(time.Duration(seconds)*time.Second).After(retryAt.Add(time.Second)) {
		t.Fatalf("Retry-After=%d does not match local Until=%s (request %s..%s)", seconds, retryAt, started, finished)
	}
	if !strings.Contains(response.Error.Hint, "earliest local retry at "+retryAt.Format(time.RFC3339Nano)) {
		t.Fatalf("hint omits exact local retry time: %q", response.Error.Hint)
	}
}

func TestChatModelRateLimitFreshCachedAndRestored(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprintf("stream_%t", stream), func(t *testing.T) {
			statePath := filepath.Join(t.TempDir(), "state.json")
			p := pool.New(statePath)
			t.Cleanup(p.Close)
			p.SetSoftRateMax(2 * time.Hour)
			p.Add(&auth.Auth{UID: "u1", AccessToken: "at1", ExpiresAt: 9999999999})
			p.SetCredits("u1", 1000, 0)
			// Match the incident: upstream reset is later than the local two-hour cap.
			reset := time.Now().Add(6 * time.Hour).In(time.FixedZone("UTC+8", 8*60*60)).Format("2006-01-02 15:04:05")
			body, err := json.Marshal(map[string]any{"code": 6004, "msg": "模型用量达到上限，将在 " + reset + " 重置", "requestId": "rate-limit-test"})
			if err != nil {
				t.Fatal(err)
			}
			calls := 0
			up := newFakeUpstream(t, func(_ string) (int, string, bool) {
				calls++
				return http.StatusTooManyRequests, string(body), false
			})
			// One attempt also exercises final error mapping when MaxRotate is exhausted.
			h := NewHandler(Config{Pool: p, Upstream: up, MaxRotate: 1})
			request := fmt.Sprintf(`{"model":"glm-5.3","messages":[],"stream":%t}`, stream)
			post := func(handler *Handler) (*httptest.ResponseRecorder, time.Time, time.Time) {
				rec := httptest.NewRecorder()
				started := time.Now()
				handler.ServeHTTP(rec, httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(request)))
				return rec, started, time.Now()
			}
			rec, started, finished := post(h)
			block := p.ModelBlockedForRealm("glm-5.3", "cn")
			if !block.Blocked || block.RateLimitedCount != 1 {
				t.Fatalf("real 6004 was not recorded as model rate limit: %+v", block)
			}
			assertModelCooldownResponse(t, rec, 429, "rate_limit_exceeded", block.Until, started, finished)
			if !strings.Contains(rec.Body.String(), "rate-limit-test") || !strings.Contains(rec.Body.String(), "6004") {
				t.Fatalf("first upstream body was not preserved: %s", rec.Body)
			}
			if remaining := block.Until.Sub(started); remaining < 119*time.Minute || remaining > 121*time.Minute {
				t.Fatalf("retry must use local two-hour cap, not upstream six-hour reset: %s", remaining)
			}
			rec, started, finished = post(h)
			assertModelCooldownResponse(t, rec, 429, "rate_limit_exceeded", block.Until, started, finished)
			p.Close()
			restored := pool.New(statePath)
			t.Cleanup(restored.Close)
			h = NewHandler(Config{Pool: restored, Upstream: up, MaxRotate: 1})
			rec, started, finished = post(h)
			assertModelCooldownResponse(t, rec, 429, "rate_limit_exceeded", block.Until, started, finished)
			if calls != 1 {
				t.Fatalf("cached and restored cooldowns must not probe upstream: calls=%d", calls)
			}
		})
	}
}

func TestChatModelCooldownMixedCausesAndRefusals(t *testing.T) {
	for _, tc := range []struct {
		name   string
		reason string
		mixed  bool
	}{
		{"unavailable", "11102 model not available", false},
		{"forbidden", "11158 model not allowed for account type", false},
		{"mixed_unavailable", "11102 model not available", true},
		{"mixed_forbidden", "11158 model not allowed for account type", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := testPoolWith(
				&auth.Auth{UID: "a", AccessToken: "a", ExpiresAt: 9999999999},
				&auth.Auth{UID: "b", AccessToken: "b", ExpiresAt: 9999999999},
				&auth.Auth{UID: "other-realm", Domain: "www.workbuddy.ai", AccessToken: "x", ExpiresAt: 9999999999},
			)
			p.BlockModelBackoff("a", "glm-5.3", tc.reason)
			if tc.mixed {
				p.CooldownSoftForModel("b", time.Minute, time.Now().Add(time.Minute), "glm-5.3", "6004 model rate limit")
			} else {
				p.BlockModelBackoff("b", "glm-5.3", tc.reason)
			}
			calls := 0
			up := newFakeUpstream(t, func(_ string) (int, string, bool) {
				calls++
				return 200, sseOK, true
			})
			h := NewHandler(Config{Pool: p, Upstream: up})
			block := p.ModelBlockedForRealm("glm-5.3", "cn")
			status, code, retryAt := 400, "model_unavailable", time.Time{}
			if tc.mixed {
				status, code, retryAt = 429, "rate_limit_exceeded", block.Until
			}
			// Map iteration order cannot decide whether the same pool is retryable.
			for i := 0; i < 20; i++ {
				rec := httptest.NewRecorder()
				started := time.Now()
				h.ServeHTTP(rec, httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(`{"model":"cn:glm-5.3","messages":[]}`)))
				assertModelCooldownResponse(t, rec, status, code, retryAt, started, time.Now())
			}
			if calls != 0 {
				t.Fatalf("cooldown response contacted upstream: calls=%d", calls)
			}
		})
	}
}

func TestRetryAfterSecondsRoundsUp(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		remaining time.Duration
		seconds   int64
	}{
		{-time.Second, 1}, {0, 1}, {time.Nanosecond, 1},
		{time.Second, 1}, {time.Second + time.Nanosecond, 2}, {2 * time.Hour, 7200},
	} {
		if got := retryAfterSeconds(now.Add(tc.remaining), now); got != tc.seconds {
			t.Errorf("remaining=%s: seconds=%d want %d", tc.remaining, got, tc.seconds)
		}
	}
}
