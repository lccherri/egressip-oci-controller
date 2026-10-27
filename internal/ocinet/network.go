// Package ocinet wraps the OCI Networking API calls used by the controller.
// The Network interface allows the reconcile loop to be tested without OCI.
package ocinet

import (
	"context"
	"fmt"
	"net/netip"
	"time"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/common/auth"
	"github.com/oracle/oci-go-sdk/v65/core"
)

// PrivateIP is the subset of core.PrivateIp used by the controller.
type PrivateIP struct {
	ID      string
	Address string
	VnicID  string
	Primary bool
	Tags    map[string]string
}

// Network is the subset of the OCI Networking API used by the reconcile loop.
type Network interface {
	SubnetCIDR(ctx context.Context, subnetID string) (netip.Prefix, error)
	// GetPrivateIP returns nil, nil when the address does not exist in the subnet.
	GetPrivateIP(ctx context.Context, subnetID, address string) (*PrivateIP, error)
	ListPrivateIPs(ctx context.Context, subnetID string) ([]PrivateIP, error)
	CreatePrivateIP(ctx context.Context, vnicID, address, displayName string, tags map[string]string) error
	MovePrivateIP(ctx context.Context, privateIPID, vnicID string, tags map[string]string) error
	DeletePrivateIP(ctx context.Context, privateIPID string) error
}

// AuthMode selects how the controller authenticates to OCI.
type AuthMode string

const (
	// AuthAPIKey uses an OCI configuration file (mounted from a Secret).
	AuthAPIKey AuthMode = "api_key"
	// AuthInstancePrincipal uses the identity of the instance running the pod.
	AuthInstancePrincipal AuthMode = "instance_principal"
)

// Options configures the OCI client.
type Options struct {
	Auth            AuthMode
	ConfigFile      string // used with AuthAPIKey
	ConfigProfile   string // used with AuthAPIKey; empty means DEFAULT
	ServiceEndpoint string // overrides the endpoint (needed in DRCC realms)
}

// Retry settings for transient errors: 409 IncorrectState/LockConflict, 429,
// 5xx and network errors. The SDK applies no retry to the Networking operations
// used here. Five attempts with exponential backoff capped at 8 s add up to
// about 15 s (plus up to 1 s of jitter per attempt), which keeps a failover
// within one resync interval; anything beyond that is handled by the
// controller-runtime requeue.
const (
	retryMaxAttempts = 5
	retryMaxSleep    = 8 * time.Second
	retryBackoffBase = 2.0
)

// RetryPolicy is the retry policy applied to every Networking call.
func RetryPolicy() common.RetryPolicy {
	return common.NewRetryPolicyWithOptions(
		common.ReplaceWithValuesFromRetryPolicy(common.DefaultRetryPolicyWithoutEventualConsistency()),
		common.WithMaximumNumberAttempts(retryMaxAttempts),
		common.WithExponentialBackoff(retryMaxSleep, retryBackoffBase),
	)
}

type sdkNetwork struct {
	vcn core.VirtualNetworkClient
}

// New creates the oci-go-sdk implementation of Network.
func New(opts Options) (Network, error) {
	var provider common.ConfigurationProvider
	var err error
	switch opts.Auth {
	case AuthInstancePrincipal:
		provider, err = auth.InstancePrincipalConfigurationProvider()
	case AuthAPIKey, "":
		profile := opts.ConfigProfile
		if profile == "" {
			profile = "DEFAULT"
		}
		provider, err = common.ConfigurationProviderFromFileWithProfile(opts.ConfigFile, profile, "")
	default:
		return nil, fmt.Errorf("unknown OCI authentication mode: %q", opts.Auth)
	}
	if err != nil {
		return nil, fmt.Errorf("configuring OCI authentication: %w", err)
	}
	return newWithProvider(provider, opts.ServiceEndpoint, RetryPolicy())
}

func newWithProvider(provider common.ConfigurationProvider, endpoint string, policy common.RetryPolicy) (*sdkNetwork, error) {
	vcn, err := core.NewVirtualNetworkClientWithConfigurationProvider(provider)
	if err != nil {
		return nil, fmt.Errorf("creating OCI Networking client: %w", err)
	}
	if endpoint != "" {
		vcn.Host = endpoint
	}
	// Only the retry policy is replaced; SetCustomClientConfiguration would also
	// drop the default circuit breaker.
	vcn.Configuration.RetryPolicy = &policy
	return &sdkNetwork{vcn: vcn}, nil
}

func (n *sdkNetwork) SubnetCIDR(ctx context.Context, subnetID string) (netip.Prefix, error) {
	resp, err := n.vcn.GetSubnet(ctx, core.GetSubnetRequest{SubnetId: &subnetID})
	if err != nil {
		return netip.Prefix{}, err
	}
	if resp.CidrBlock == nil {
		return netip.Prefix{}, fmt.Errorf("subnet %s has no cidrBlock", subnetID)
	}
	return netip.ParsePrefix(*resp.CidrBlock)
}

func (n *sdkNetwork) GetPrivateIP(ctx context.Context, subnetID, address string) (*PrivateIP, error) {
	resp, err := n.vcn.ListPrivateIps(ctx, core.ListPrivateIpsRequest{
		SubnetId:  &subnetID,
		IpAddress: &address,
	})
	if err != nil {
		return nil, err
	}
	if len(resp.Items) == 0 {
		return nil, nil
	}
	p := convert(resp.Items[0])
	return &p, nil
}

func (n *sdkNetwork) ListPrivateIPs(ctx context.Context, subnetID string) ([]PrivateIP, error) {
	var out []PrivateIP
	req := core.ListPrivateIpsRequest{SubnetId: &subnetID}
	for {
		resp, err := n.vcn.ListPrivateIps(ctx, req)
		if err != nil {
			return nil, err
		}
		for _, item := range resp.Items {
			out = append(out, convert(item))
		}
		if resp.OpcNextPage == nil {
			return out, nil
		}
		req.Page = resp.OpcNextPage
	}
}

func (n *sdkNetwork) CreatePrivateIP(ctx context.Context, vnicID, address, displayName string, tags map[string]string) error {
	// The SDK sets an opc-retry-token, so a retried create is not duplicated.
	_, err := n.vcn.CreatePrivateIp(ctx, core.CreatePrivateIpRequest{
		CreatePrivateIpDetails: core.CreatePrivateIpDetails{
			VnicId:       &vnicID,
			IpAddress:    &address,
			DisplayName:  &displayName,
			FreeformTags: tags,
		},
	})
	return err
}

func (n *sdkNetwork) MovePrivateIP(ctx context.Context, privateIPID, vnicID string, tags map[string]string) error {
	_, err := n.vcn.UpdatePrivateIp(ctx, core.UpdatePrivateIpRequest{
		PrivateIpId: &privateIPID,
		UpdatePrivateIpDetails: core.UpdatePrivateIpDetails{
			VnicId:       &vnicID,
			FreeformTags: tags,
		},
	})
	return err
}

func (n *sdkNetwork) DeletePrivateIP(ctx context.Context, privateIPID string) error {
	_, err := n.vcn.DeletePrivateIp(ctx, core.DeletePrivateIpRequest{PrivateIpId: &privateIPID})
	return err
}

func convert(p core.PrivateIp) PrivateIP {
	return PrivateIP{
		ID:      deref(p.Id),
		Address: deref(p.IpAddress),
		VnicID:  deref(p.VnicId),
		Primary: p.IsPrimary != nil && *p.IsPrimary,
		Tags:    p.FreeformTags,
	}
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
