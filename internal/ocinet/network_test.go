package ocinet

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/common"
)

type reply struct {
	status int
	code   string
}

// fakeEndpoint answers ListPrivateIps with the given replies in order and then
// with a single private IP. It returns the server and its request counter.
func fakeEndpoint(t *testing.T, replies ...reply) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		i := int(n.Add(1)) - 1
		w.Header().Set("Content-Type", "application/json")
		if i < len(replies) {
			w.WriteHeader(replies[i].status)
			fmt.Fprintf(w, `{"code":%q,"message":"test"}`, replies[i].code)
			return
		}
		fmt.Fprint(w, `[{"id":"p1","ipAddress":"10.0.0.5","vnicId":"vnic1","isPrimary":false}]`)
	}))
	t.Cleanup(srv.Close)
	return srv, &n
}

func testNetwork(t *testing.T, endpoint string) *sdkNetwork {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	pemKey := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	provider := common.NewRawConfigurationProvider(
		"ocid1.tenancy.oc1..test", "ocid1.user.oc1..test", "us-ashburn-1", "aa:bb", string(pemKey), nil)

	// Same retry conditions and attempt limit as production, without the sleep.
	policy := RetryPolicy()
	policy.NextDuration = func(common.OCIOperationResponse) time.Duration { return time.Millisecond }

	n, err := newWithProvider(provider, endpoint, policy)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestRetryPolicyBounds(t *testing.T) {
	p := RetryPolicy()
	if p.MaximumNumberAttempts != retryMaxAttempts {
		t.Fatalf("attempts = %d, want %d", p.MaximumNumberAttempts, retryMaxAttempts)
	}
	if got := p.GetMaximumCumulativeBackoffWithoutJitter(); got != 15*time.Second {
		t.Fatalf("cumulative backoff = %s, want 15s", got)
	}
}

func TestRetriesTransientErrors(t *testing.T) {
	srv, calls := fakeEndpoint(t,
		reply{http.StatusTooManyRequests, "TooManyRequests"},
		reply{http.StatusConflict, "IncorrectState"},
		reply{http.StatusServiceUnavailable, "ServiceUnavailable"},
	)
	p, err := testNetwork(t, srv.URL).GetPrivateIP(context.Background(), "subnet1", "10.0.0.5")
	if err != nil {
		t.Fatal(err)
	}
	if p == nil || p.VnicID != "vnic1" {
		t.Fatalf("unexpected result: %+v", p)
	}
	if calls.Load() != 4 {
		t.Fatalf("requests = %d, want 4", calls.Load())
	}
}

func TestDoesNotRetryPermanentErrors(t *testing.T) {
	for _, r := range []reply{
		{http.StatusConflict, "Conflict"},
		{http.StatusNotFound, "NotAuthorizedOrNotFound"},
		{http.StatusBadRequest, "InvalidParameter"},
	} {
		t.Run(r.code, func(t *testing.T) {
			srv, calls := fakeEndpoint(t, r)
			if _, err := testNetwork(t, srv.URL).GetPrivateIP(context.Background(), "subnet1", "10.0.0.5"); err == nil {
				t.Fatal("expected an error")
			}
			if calls.Load() != 1 {
				t.Fatalf("requests = %d, want 1", calls.Load())
			}
		})
	}
}

func TestGivesUpAfterMaxAttempts(t *testing.T) {
	var replies []reply
	for range retryMaxAttempts + 1 {
		replies = append(replies, reply{http.StatusInternalServerError, "InternalServerError"})
	}
	srv, calls := fakeEndpoint(t, replies...)
	if _, err := testNetwork(t, srv.URL).GetPrivateIP(context.Background(), "subnet1", "10.0.0.5"); err == nil {
		t.Fatal("expected an error")
	}
	if calls.Load() != retryMaxAttempts {
		t.Fatalf("requests = %d, want %d", calls.Load(), retryMaxAttempts)
	}
}
