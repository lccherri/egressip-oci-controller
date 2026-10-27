// Package controller reconciles OVN-Kubernetes EgressIPs (k8s.ovn.org/v1) with
// secondary private IPs on OCI VNICs.
//
// The model is level-triggered: every reconciliation reads the full state of the
// cluster and of OCI and corrects the differences. All events (EgressIP, Node,
// fencing ConfigMap) are therefore collapsed into a single queue key, so a burst
// of events results in a single reconciliation.
//
//	desired state = EgressIP.status.items[]  (egressIP -> node chosen by OVN)
//	actual state  = PrivateIp in OCI         (ipAddress -> vnicId)
//	actions       = CREATE | MOVE | DELETE   (DELETE only for IPs managed by this controller)
package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/lccherri/egressip-oci-controller/internal/ocinet"
)

const (
	// Freeform tags set on the private IPs managed by this controller.
	TagManagedBy = "egressip-oci-managed-by"
	TagCluster   = "egressip-oci-cluster"
	TagName      = "egressip-oci-name"
	ManagedByVal = "egressip-oci-controller"

	nodePrimaryIfAddrAnnotation = "k8s.ovn.org/node-primary-ifaddr"
)

// EgressIPGVK identifies the OVN-Kubernetes EgressIP resource. It is handled as
// unstructured to avoid depending on the ovn-kubernetes module.
var EgressIPGVK = schema.GroupVersionKind{Group: "k8s.ovn.org", Version: "v1", Kind: "EgressIP"}

// singletonKey is the only queue key: any event triggers a global reconciliation.
var singletonKey = reconcile.Request{NamespacedName: types.NamespacedName{Name: "egressip-oci-global"}}

// Subnet maps a subnet OCID to its CIDR.
type Subnet struct {
	ID   string
	CIDR netip.Prefix
}

// Reconciler keeps EgressIPs registered on OCI VNICs.
type Reconciler struct {
	Client   client.Client
	OCI      ocinet.Network
	Recorder record.EventRecorder

	Subnets   []Subnet
	ClusterID string
	// ActiveConfigMap is the fencing ConfigMap (key "active").
	ActiveConfigMap types.NamespacedName
	Resync          time.Duration
	DryRun          bool
}

// assignment is an EgressIP that OVN-Kubernetes has assigned to a node.
type assignment struct {
	IP       string
	Node     string
	EgressIP string // EgressIP object name
}

// Reconcile ignores the request: the whole state is recomputed on every run.
func (r *Reconciler) Reconcile(ctx context.Context, _ reconcile.Request) (reconcile.Result, error) {
	log := logf.FromContext(ctx)

	active, err := r.clusterIsActive(ctx)
	if err != nil {
		return reconcile.Result{}, err
	}
	if !active {
		log.V(1).Info("cluster inactive (fencing); no OCI changes")
		return reconcile.Result{RequeueAfter: r.Resync}, nil
	}

	eips := &unstructured.UnstructuredList{}
	eips.SetGroupVersionKind(EgressIPGVK.GroupVersion().WithKind("EgressIPList"))
	if err := r.Client.List(ctx, eips); err != nil {
		return reconcile.Result{}, fmt.Errorf("listing EgressIPs: %w", err)
	}
	nodes := &corev1.NodeList{}
	if err := r.Client.List(ctx, nodes); err != nil {
		return reconcile.Result{}, fmt.Errorf("listing nodes: %w", err)
	}

	assigned, inSpec, objs := desiredState(eips.Items)
	nodeIPs := nodeIPMap(nodes.Items)

	var errs []error
	for _, a := range assigned {
		if err := r.reconcileOne(ctx, a, nodeIPs, objs[a.EgressIP]); err != nil {
			errs = append(errs, err)
			actionErrors.Inc()
		}
	}
	if err := r.garbageCollect(ctx, inSpec); err != nil {
		errs = append(errs, err)
		actionErrors.Inc()
	}

	if len(errs) > 0 {
		// A returned error makes controller-runtime requeue with exponential backoff.
		return reconcile.Result{}, errors.Join(errs...)
	}
	return reconcile.Result{RequeueAfter: r.Resync}, nil
}

func (r *Reconciler) reconcileOne(ctx context.Context, a assignment, nodeIPs map[string]string,
	obj *unstructured.Unstructured) error {
	log := logf.FromContext(ctx).WithValues("egressIP", a.IP, "node", a.Node, "object", a.EgressIP)

	subnetID, ok := r.subnetFor(a.IP)
	if !ok {
		log.Info("EgressIP outside the configured subnets; ignored")
		return nil
	}
	nodeIP := nodeIPs[a.Node]
	nodeSubnet, ok := r.subnetFor(nodeIP)
	if !ok {
		return fmt.Errorf("node %s (IP %q) is outside the configured subnets", a.Node, nodeIP)
	}
	nodePIP, err := r.OCI.GetPrivateIP(ctx, nodeSubnet, nodeIP)
	if err != nil {
		return fmt.Errorf("looking up VNIC of node %s: %w", a.Node, err)
	}
	if nodePIP == nil {
		return fmt.Errorf("IP %s of node %s not found in OCI", nodeIP, a.Node)
	}
	targetVnic := nodePIP.VnicID

	current, err := r.OCI.GetPrivateIP(ctx, subnetID, a.IP)
	if err != nil {
		return fmt.Errorf("looking up %s in OCI: %w", a.IP, err)
	}
	tags := map[string]string{TagManagedBy: ManagedByVal, TagCluster: r.ClusterID, TagName: a.EgressIP}

	switch {
	case current == nil:
		log.Info("CREATE", "vnic", targetVnic, "dryRun", r.DryRun)
		if r.DryRun {
			return nil
		}
		if err := r.OCI.CreatePrivateIP(ctx, targetVnic, a.IP, "egressip-"+a.EgressIP, tags); err != nil {
			return fmt.Errorf("creating %s: %w", a.IP, err)
		}
		actionsTotal.WithLabelValues("create").Inc()
		r.event(obj, corev1.EventTypeNormal, "OCIPrivateIPCreated",
			"IP %s registered on the VNIC of node %s", a.IP, a.Node)

	case current.Primary:
		r.event(obj, corev1.EventTypeWarning, "OCIAddressConflict",
			"IP %s is the primary IP of another VNIC in OCI", a.IP)
		return fmt.Errorf("%s is the primary IP of another VNIC; address conflict", a.IP)

	case current.VnicID != targetVnic:
		log.Info("MOVE", "from", current.VnicID, "to", targetVnic, "dryRun", r.DryRun)
		if r.DryRun {
			return nil
		}
		if err := r.OCI.MovePrivateIP(ctx, current.ID, targetVnic, tags); err != nil {
			return fmt.Errorf("moving %s: %w", a.IP, err)
		}
		actionsTotal.WithLabelValues("move").Inc()
		r.event(obj, corev1.EventTypeNormal, "OCIPrivateIPMoved",
			"IP %s moved to the VNIC of node %s", a.IP, a.Node)

	default:
		log.V(1).Info("already on the target VNIC")
	}
	return nil
}

// garbageCollect deletes private IPs tagged by this controller for this cluster
// that no longer appear in the spec of any EgressIP. Untagged IPs are never touched.
func (r *Reconciler) garbageCollect(ctx context.Context, inSpec map[string]bool) error {
	log := logf.FromContext(ctx)
	var errs []error
	for _, s := range r.Subnets {
		pips, err := r.OCI.ListPrivateIPs(ctx, s.ID)
		if err != nil {
			errs = append(errs, fmt.Errorf("listing private IPs of subnet %s: %w", s.ID, err))
			continue
		}
		for _, p := range pips {
			if p.Primary || p.Tags[TagManagedBy] != ManagedByVal || p.Tags[TagCluster] != r.ClusterID {
				continue
			}
			if inSpec[p.Address] {
				continue
			}
			log.Info("DELETE orphan", "ip", p.Address, "dryRun", r.DryRun)
			if r.DryRun {
				continue
			}
			if err := r.OCI.DeletePrivateIP(ctx, p.ID); err != nil {
				errs = append(errs, fmt.Errorf("deleting %s: %w", p.Address, err))
				continue
			}
			actionsTotal.WithLabelValues("delete").Inc()
		}
	}
	return errors.Join(errs...)
}

// clusterIsActive implements DR fencing: only a cluster with active=true changes
// private IPs. A missing ConfigMap means inactive (fail-safe).
func (r *Reconciler) clusterIsActive(ctx context.Context) (bool, error) {
	cm := &corev1.ConfigMap{}
	err := r.Client.Get(ctx, r.ActiveConfigMap, cm)
	if apierrors.IsNotFound(err) {
		logf.FromContext(ctx).Info("fencing ConfigMap not found; treating cluster as INACTIVE",
			"configMap", r.ActiveConfigMap.String())
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("reading fencing ConfigMap: %w", err)
	}
	return cm.Data["active"] == "true", nil
}

func (r *Reconciler) subnetFor(ip string) (string, bool) {
	addr, err := netip.ParseAddr(ip)
	if err != nil {
		return "", false
	}
	for _, s := range r.Subnets {
		if s.CIDR.Contains(addr) {
			return s.ID, true
		}
	}
	return "", false
}

func (r *Reconciler) event(obj *unstructured.Unstructured, eventType, reason, msgFmt string, args ...any) {
	if r.Recorder != nil && obj != nil {
		r.Recorder.Eventf(obj, eventType, reason, msgFmt, args...)
	}
}

// desiredState returns the OVN assignments, the IPs declared in spec and a
// name -> object index used to emit Events.
func desiredState(items []unstructured.Unstructured) ([]assignment, map[string]bool, map[string]*unstructured.Unstructured) {
	var assigned []assignment
	inSpec := map[string]bool{}
	objs := map[string]*unstructured.Unstructured{}

	for i := range items {
		e := &items[i]
		objs[e.GetName()] = e

		ips, _, _ := unstructured.NestedStringSlice(e.Object, "spec", "egressIPs")
		for _, ip := range ips {
			inSpec[ip] = true
		}
		statusItems, _, _ := unstructured.NestedSlice(e.Object, "status", "items")
		for _, raw := range statusItems {
			m, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			ip, _ := m["egressIP"].(string)
			node, _ := m["node"].(string)
			if ip != "" && node != "" {
				assigned = append(assigned, assignment{IP: ip, Node: node, EgressIP: e.GetName()})
			}
		}
	}
	sort.Slice(assigned, func(i, j int) bool { return assigned[i].IP < assigned[j].IP })
	return assigned, inSpec, objs
}

// nodeIPMap returns node name -> primary IP (OVN annotation, falling back to InternalIP).
func nodeIPMap(nodes []corev1.Node) map[string]string {
	out := make(map[string]string, len(nodes))
	for _, n := range nodes {
		out[n.Name] = nodePrimaryIP(&n)
	}
	return out
}

func nodePrimaryIP(n *corev1.Node) string {
	if raw, ok := n.Annotations[nodePrimaryIfAddrAnnotation]; ok {
		var v struct {
			IPv4 string `json:"ipv4"`
		}
		if json.Unmarshal([]byte(raw), &v) == nil && v.IPv4 != "" {
			if p, err := netip.ParsePrefix(v.IPv4); err == nil {
				return p.Addr().String()
			}
		}
	}
	for _, a := range n.Status.Addresses {
		if a.Type == corev1.NodeInternalIP {
			return a.Address
		}
	}
	return ""
}

// SetupWithManager registers the watches. Every event maps to the same key.
func (r *Reconciler) SetupWithManager(mgr ctrl.Manager) error {
	toSingleton := handler.EnqueueRequestsFromMapFunc(
		func(context.Context, client.Object) []reconcile.Request { return []reconcile.Request{singletonKey} })

	egressIP := &unstructured.Unstructured{}
	egressIP.SetGroupVersionKind(EgressIPGVK)

	// Only node changes that affect the node -> IP/VNIC mapping are relevant.
	nodeChanged := predicate.Funcs{
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldN, ok1 := e.ObjectOld.(*corev1.Node)
			newN, ok2 := e.ObjectNew.(*corev1.Node)
			return !ok1 || !ok2 || nodePrimaryIP(oldN) != nodePrimaryIP(newN)
		},
	}
	fencingCM := predicate.NewPredicateFuncs(func(o client.Object) bool {
		return o.GetNamespace() == r.ActiveConfigMap.Namespace && o.GetName() == r.ActiveConfigMap.Name
	})

	return ctrl.NewControllerManagedBy(mgr).
		Named("egressip-oci").
		Watches(egressIP, toSingleton, builder.WithPredicates(predicate.Or(
			predicate.GenerationChangedPredicate{}, // spec
			statusChanged{},                        // status.items (assigned node)
		))).
		Watches(&corev1.Node{}, toSingleton, builder.WithPredicates(nodeChanged)).
		Watches(&corev1.ConfigMap{}, toSingleton, builder.WithPredicates(fencingCM)).
		Complete(r)
}

// statusChanged fires when the EgressIP status changes (OVN moved the IP).
type statusChanged struct{ predicate.Funcs }

func (statusChanged) Update(e event.UpdateEvent) bool {
	oldU, ok1 := e.ObjectOld.(*unstructured.Unstructured)
	newU, ok2 := e.ObjectNew.(*unstructured.Unstructured)
	if !ok1 || !ok2 {
		return true
	}
	o, _, _ := unstructured.NestedFieldNoCopy(oldU.Object, "status")
	n, _, _ := unstructured.NestedFieldNoCopy(newU.Object, "status")
	ob, _ := json.Marshal(o)
	nb, _ := json.Marshal(n)
	return string(ob) != string(nb)
}

func (statusChanged) Create(event.CreateEvent) bool   { return true }
func (statusChanged) Delete(event.DeleteEvent) bool   { return true }
func (statusChanged) Generic(event.GenericEvent) bool { return true }
