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
	"strings"
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

func testProvider(t *testing.T, region string) common.ConfigurationProvider {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	pemKey := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	return common.NewRawConfigurationProvider(
		"ocid1.tenancy.oc1..test", "ocid1.user.oc1..test", region, "aa:bb", string(pemKey), nil)
}

func testNetwork(t *testing.T, endpoint string) *sdkNetwork {
	t.Helper()
	// Same retry conditions and attempt limit as production, without the sleep.
	policy := RetryPolicy()
	policy.NextDuration = func(common.OCIOperationResponse) time.Duration { return time.Millisecond }

	n, err := newWithProvider(testProvider(t, "us-ashburn-1"), endpoint, policy)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestCheckEndpoint(t *testing.T) {
	for _, tc := range []struct {
		name, region, endpoint, defaultRealm string
		wantErr                              bool
	}{
		{name: "known region", region: "us-ashburn-1"},
		{name: "unknown region without endpoint", region: "xx-dedicated-1", wantErr: true},
		{name: "unknown region with endpoint", region: "xx-dedicated-1", endpoint: "https://iaas.xx-dedicated-1.example.com"},
		{name: "unknown region with default realm", region: "xx-dedicated-1", defaultRealm: "example.com"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.defaultRealm != "" {
				t.Setenv(defaultRealmEnv, tc.defaultRealm)
			}
			err := checkEndpoint(testProvider(t, tc.region), tc.endpoint)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
			if err != nil && !strings.Contains(err.Error(), "OCI_SERVICE_ENDPOINT") {
				t.Fatalf("error does not name the setting to fix: %v", err)
			}
		})
	}
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
