package controller

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"sort"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/lccherri/egressip-oci-controller/internal/ocinet"
)

// ---------------------------------------------------------------- fake OCI

type fakeOCI struct {
	ips    map[string]*ocinet.PrivateIP // by address
	nextID int
	calls  []string
	failOn string // "create" | "move" | "delete" | "get"
}

func newFakeOCI(ips ...ocinet.PrivateIP) *fakeOCI {
	f := &fakeOCI{ips: map[string]*ocinet.PrivateIP{}}
	for i := range ips {
		p := ips[i]
		f.ips[p.Address] = &p
	}
	return f
}

func (f *fakeOCI) SubnetCIDR(context.Context, string) (netip.Prefix, error) {
	return netip.MustParsePrefix("10.0.0.0/24"), nil
}

func (f *fakeOCI) GetPrivateIP(_ context.Context, _ string, addr string) (*ocinet.PrivateIP, error) {
	if f.failOn == "get" {
		return nil, errors.New("boom")
	}
	if p, ok := f.ips[addr]; ok {
		c := *p
		return &c, nil
	}
	return nil, nil
}

func (f *fakeOCI) ListPrivateIPs(context.Context, string) ([]ocinet.PrivateIP, error) {
	var out []ocinet.PrivateIP
	for _, p := range f.ips {
		out = append(out, *p)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Address < out[j].Address })
	return out, nil
}

func (f *fakeOCI) CreatePrivateIP(_ context.Context, vnic, addr, _ string, tags map[string]string) error {
	f.calls = append(f.calls, "create "+addr+" "+vnic)
	if f.failOn == "create" {
		return errors.New("409 conflict")
	}
	f.nextID++
	f.ips[addr] = &ocinet.PrivateIP{ID: fmt.Sprintf("new%d", f.nextID), Address: addr, VnicID: vnic, Tags: tags}
	return nil
}

func (f *fakeOCI) MovePrivateIP(_ context.Context, id, vnic string, tags map[string]string) error {
	f.calls = append(f.calls, "move "+id+" "+vnic)
	if f.failOn == "move" {
		return errors.New("409 conflict")
	}
	for _, p := range f.ips {
		if p.ID == id {
			p.VnicID, p.Tags = vnic, tags
		}
	}
	return nil
}

func (f *fakeOCI) DeletePrivateIP(_ context.Context, id string) error {
	f.calls = append(f.calls, "delete "+id)
	for a, p := range f.ips {
		if p.ID == id {
			delete(f.ips, a)
		}
	}
	return nil
}

// ---------------------------------------------------------------- fixtures

const cluster = "c1"

var managed = map[string]string{TagManagedBy: ManagedByVal, TagCluster: cluster}

func baseIPs() []ocinet.PrivateIP {
	return []ocinet.PrivateIP{
		{ID: "n1", Address: "10.0.0.10", VnicID: "vnicA", Primary: true},
		{ID: "n2", Address: "10.0.0.11", VnicID: "vnicB", Primary: true},
	}
}

func node(name, ip string, viaAnnotation bool) *corev1.Node {
	n := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}}
	if viaAnnotation {
		n.Annotations = map[string]string{nodePrimaryIfAddrAnnotation: fmt.Sprintf(`{"ipv4":"%s/24"}`, ip)}
	} else {
		n.Status.Addresses = []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: ip}}
	}
	return n
}

func egressIP(name string, spec []string, status map[string]string) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(EgressIPGVK)
	u.SetName(name)
	specIPs := make([]any, 0, len(spec))
	for _, s := range spec {
		specIPs = append(specIPs, s)
	}
	_ = unstructured.SetNestedSlice(u.Object, specIPs, "spec", "egressIPs")
	var items []any
	for ip, n := range status {
		items = append(items, map[string]any{"egressIP": ip, "node": n})
	}
	if items != nil {
		_ = unstructured.SetNestedSlice(u.Object, items, "status", "items")
	}
	return u
}

func fencing(active string) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: "egressip-oci", Name: "egressip-oci-active"},
		Data:       map[string]string{"active": active},
	}
}

func newReconciler(t *testing.T, oci *fakeOCI, objs ...client.Object) (*Reconciler, *record.FakeRecorder) {
	t.Helper()
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	s.AddKnownTypeWithName(EgressIPGVK, &unstructured.Unstructured{})
	s.AddKnownTypeWithName(EgressIPGVK.GroupVersion().WithKind("EgressIPList"), &unstructured.UnstructuredList{})

	rec := record.NewFakeRecorder(20)
	return &Reconciler{
		Client:          fake.NewClientBuilder().WithScheme(s).WithObjects(objs...).Build(),
		OCI:             oci,
		Recorder:        rec,
		Subnets:         []Subnet{{ID: "subnet1", CIDR: netip.MustParsePrefix("10.0.0.0/24")}},
		ClusterID:       cluster,
		ActiveConfigMap: types.NamespacedName{Namespace: "egressip-oci", Name: "egressip-oci-active"},
	}, rec
}

func run(t *testing.T, r *Reconciler) error {
	t.Helper()
	_, err := r.Reconcile(context.Background(), reconcile.Request{})
	return err
}

func cluster2Nodes() []client.Object {
	return []client.Object{node("w1", "10.0.0.10", true), node("w2", "10.0.0.11", false), fencing("true")}
}

// ---------------------------------------------------------------- tests

func TestCreateWhenMissing(t *testing.T) {
	oci := newFakeOCI(baseIPs()...)
	r, rec := newReconciler(t, oci, append(cluster2Nodes(),
		egressIP("app1", []string{"10.0.0.140"}, map[string]string{"10.0.0.140": "w1"}))...)

	if err := run(t, r); err != nil {
		t.Fatal(err)
	}
	got := oci.ips["10.0.0.140"]
	if got == nil || got.VnicID != "vnicA" || got.Tags[TagName] != "app1" || got.Tags[TagCluster] != cluster {
		t.Fatalf("IP not created correctly: %+v", got)
	}
	if ev := <-rec.Events; !strings.Contains(ev, "OCIPrivateIPCreated") {
		t.Fatalf("unexpected event: %s", ev)
	}
}

func TestMoveAfterFailover(t *testing.T) {
	oci := newFakeOCI(append(baseIPs(),
		ocinet.PrivateIP{ID: "e1", Address: "10.0.0.140", VnicID: "vnicA", Tags: managed})...)
	// OVN moved the EgressIP to w2 (InternalIP, no OVN annotation).
	r, _ := newReconciler(t, oci, append(cluster2Nodes(),
		egressIP("app1", []string{"10.0.0.140"}, map[string]string{"10.0.0.140": "w2"}))...)

	if err := run(t, r); err != nil {
		t.Fatal(err)
	}
	if oci.ips["10.0.0.140"].VnicID != "vnicB" {
		t.Fatalf("IP not moved: %+v", oci.ips["10.0.0.140"])
	}
}

func TestAdoptsManuallyAssignedIP(t *testing.T) {
	// An untagged IP assigned manually to the correct VNIC results in no calls.
	oci := newFakeOCI(append(baseIPs(),
		ocinet.PrivateIP{ID: "m1", Address: "10.0.0.140", VnicID: "vnicA"})...)
	r, _ := newReconciler(t, oci, append(cluster2Nodes(),
		egressIP("app1", []string{"10.0.0.140"}, map[string]string{"10.0.0.140": "w1"}))...)

	if err := run(t, r); err != nil {
		t.Fatal(err)
	}
	if len(oci.calls) != 0 {
		t.Fatalf("unexpected OCI calls: %v", oci.calls)
	}
}

func TestIdempotent(t *testing.T) {
	oci := newFakeOCI(baseIPs()...)
	r, _ := newReconciler(t, oci, append(cluster2Nodes(),
		egressIP("app1", []string{"10.0.0.140"}, map[string]string{"10.0.0.140": "w1"}))...)
	for i := 0; i < 3; i++ {
		if err := run(t, r); err != nil {
			t.Fatal(err)
		}
	}
	if len(oci.calls) != 1 {
		t.Fatalf("want 1 call (create), got %v", oci.calls)
	}
}

func TestGarbageCollectOnlyOwnOrphans(t *testing.T) {
	oci := newFakeOCI(append(baseIPs(),
		ocinet.PrivateIP{ID: "own", Address: "10.0.0.99", VnicID: "vnicA", Tags: managed},
		ocinet.PrivateIP{ID: "other", Address: "10.0.0.98", VnicID: "vnicA",
			Tags: map[string]string{TagManagedBy: ManagedByVal, TagCluster: "other-cluster"}},
		ocinet.PrivateIP{ID: "manual", Address: "10.0.0.97", VnicID: "vnicA"},
		// Declared in spec but not yet assigned to a node: must not be deleted.
		ocinet.PrivateIP{ID: "pending", Address: "10.0.0.142", VnicID: "vnicA", Tags: managed},
	)...)
	r, _ := newReconciler(t, oci, append(cluster2Nodes(),
		egressIP("app1", []string{"10.0.0.142"}, nil))...)

	if err := run(t, r); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(oci.calls) != "[delete own]" {
		t.Fatalf("unexpected GC calls: %v", oci.calls)
	}
}

func TestPrimaryIPConflict(t *testing.T) {
	oci := newFakeOCI(baseIPs()...)
	// 10.0.0.11 is the primary IP of w2.
	r, rec := newReconciler(t, oci, append(cluster2Nodes(),
		egressIP("bad", []string{"10.0.0.11"}, map[string]string{"10.0.0.11": "w1"}))...)

	if err := run(t, r); err == nil || !strings.Contains(err.Error(), "primary IP") {
		t.Fatalf("want conflict error, got %v", err)
	}
	if len(oci.calls) != 0 {
		t.Fatalf("unexpected OCI calls: %v", oci.calls)
	}
	if ev := <-rec.Events; !strings.Contains(ev, "OCIAddressConflict") {
		t.Fatalf("unexpected event: %s", ev)
	}
}

func TestFencingInactive(t *testing.T) {
	for _, tc := range []struct {
		name string
		cm   []client.Object
	}{
		{"active=false", []client.Object{fencing("false")}},
		{"ConfigMap missing", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			oci := newFakeOCI(baseIPs()...)
			objs := append([]client.Object{node("w1", "10.0.0.10", true),
				egressIP("app1", []string{"10.0.0.140"}, map[string]string{"10.0.0.140": "w1"})}, tc.cm...)
			r, _ := newReconciler(t, oci, objs...)
			if err := run(t, r); err != nil {
				t.Fatal(err)
			}
			if len(oci.calls) != 0 {
				t.Fatalf("inactive cluster changed OCI: %v", oci.calls)
			}
		})
	}
}

func TestDryRun(t *testing.T) {
	oci := newFakeOCI(append(baseIPs(),
		ocinet.PrivateIP{ID: "e1", Address: "10.0.0.140", VnicID: "vnicA", Tags: managed},
		ocinet.PrivateIP{ID: "own", Address: "10.0.0.99", VnicID: "vnicA", Tags: managed})...)
	r, _ := newReconciler(t, oci, append(cluster2Nodes(),
		egressIP("app1", []string{"10.0.0.140", "10.0.0.141"},
			map[string]string{"10.0.0.140": "w2", "10.0.0.141": "w1"}))...)
	r.DryRun = true

	if err := run(t, r); err != nil {
		t.Fatal(err)
	}
	if len(oci.calls) != 0 {
		t.Fatalf("dry-run changed OCI: %v", oci.calls)
	}
}

func TestPartialFailureContinuesAndReturnsError(t *testing.T) {
	oci := newFakeOCI(append(baseIPs(),
		ocinet.PrivateIP{ID: "e1", Address: "10.0.0.140", VnicID: "vnicA", Tags: managed})...)
	oci.failOn = "move"
	r, _ := newReconciler(t, oci, append(cluster2Nodes(),
		egressIP("a", []string{"10.0.0.140"}, map[string]string{"10.0.0.140": "w2"}),
		egressIP("b", []string{"10.0.0.141"}, map[string]string{"10.0.0.141": "w1"}))...)

	err := run(t, r)
	if err == nil {
		t.Fatal("want an error so the request is requeued with backoff")
	}
	// The CREATE for the other EgressIP still happens after the MOVE fails.
	if oci.ips["10.0.0.141"] == nil {
		t.Fatalf("a failing EgressIP blocked the others: %v", oci.calls)
	}
}

func TestIPOutsideSubnetsIgnored(t *testing.T) {
	oci := newFakeOCI(baseIPs()...)
	r, _ := newReconciler(t, oci, append(cluster2Nodes(),
		egressIP("ext", []string{"192.168.1.5"}, map[string]string{"192.168.1.5": "w1"}))...)
	if err := run(t, r); err != nil {
		t.Fatal(err)
	}
	if len(oci.calls) != 0 {
		t.Fatalf("IP outside the subnets caused OCI calls: %v", oci.calls)
	}
}

func TestUnknownNodeReturnsError(t *testing.T) {
	oci := newFakeOCI(baseIPs()...)
	r, _ := newReconciler(t, oci, append(cluster2Nodes(),
		egressIP("app1", []string{"10.0.0.140"}, map[string]string{"10.0.0.140": "w9"}))...)
	if err := run(t, r); err == nil {
		t.Fatal("unknown node must return an error so the request is retried")
	}
}
