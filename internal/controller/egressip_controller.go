// Package controller reconciles OVN-Kubernetes EgressIPs (k8s.ovn.org/v1)
// with secondary private IPs on dedicated OCI VNICs.
//
// The controller prefers a dedicated secondary interface on each eligible
// infrastructure node to discover the target OCI VNIC. If the annotation is
// absent, it falls back to the node primary interface (OVN annotation, then
// NodeInternalIP).
//
// Desired state = EgressIP.status.items[] (egressIP -> node chosen by OVN)
// Actual state  = PrivateIp in OCI (ipAddress -> vnicId)
// Actions       = CREATE | MOVE | DELETE
//
// Only Private IPs managed by this controller and belonging to this cluster
// are eligible for MOVE and DELETE.
//
// All events are collapsed into a single queue key. Every reconciliation
// reads the full state of Kubernetes and OCI and corrects the differences.

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

	// Annotation containing the primary IP of the secondary interface
	// dedicated to EgressIP on each infrastructure node.
	//
	// Example:
	// egressip.oci.dataprev.gov.br/interface-ip: "192.168.2.20"
	egressIPInterfaceIPAnnotation = "egressip.oci.dataprev.gov.br/interface-ip"
)

// EgressIPGVK identifies the OVN-Kubernetes EgressIP resource.
// It is handled as unstructured to avoid depending on ovn-kubernetes.
var EgressIPGVK = schema.GroupVersionKind{
	Group:   "k8s.ovn.org",
	Version: "v1",
	Kind:    "EgressIP",
}

// singletonKey is the only queue key.
// Any relevant event triggers a global reconciliation.
var singletonKey = reconcile.Request{
	NamespacedName: types.NamespacedName{
		Name: "egressip-oci-global",
	},
}

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

	Resync time.Duration
	DryRun bool
}

// assignment is an EgressIP assigned by OVN-Kubernetes to a node.
type assignment struct {
	IP       string
	Node     string
	EgressIP string
}

// Reconcile ignores the request: the whole state is recomputed on every run.
func (r *Reconciler) Reconcile(
	ctx context.Context,
	_ reconcile.Request,
) (reconcile.Result, error) {

	log := logf.FromContext(ctx)

	// 1. Check DR fencing.
	active, err := r.clusterIsActive(ctx)
	if err != nil {
		return reconcile.Result{}, err
	}

	if !active {
		log.V(1).Info(
			"cluster inactive (fencing); no OCI changes",
		)

		return reconcile.Result{
			RequeueAfter: r.Resync,
		}, nil
	}

	// 2. Read all EgressIP resources.
	eips := &unstructured.UnstructuredList{}
	eips.SetGroupVersionKind(
		EgressIPGVK.GroupVersion().WithKind("EgressIPList"),
	)

	if err := r.Client.List(ctx, eips); err != nil {
		return reconcile.Result{}, fmt.Errorf(
			"listing EgressIPs: %w", err,
		)
	}

	// 3. Read all Kubernetes Nodes.
	nodes := &corev1.NodeList{}

	if err := r.Client.List(ctx, nodes); err != nil {
		return reconcile.Result{}, fmt.Errorf(
			"listing nodes: %w", err,
		)
	}

	// 4. Build desired state and EgressIP interface mapping.
	assigned, inSpec, objs := desiredState(eips.Items)

	nodeEgressIPs := nodeEgressIPInterfaceMap(nodes.Items)

	// 5. Reconcile each assigned EgressIP.
	var errs []error

	for _, a := range assigned {
		if err := r.reconcileOne(
			ctx,
			a,
			nodeEgressIPs,
			objs[a.EgressIP],
		); err != nil {

			errs = append(errs, err)
			actionErrors.Inc()
		}
	}

	// 6. Garbage collection.
	if err := r.garbageCollect(ctx, inSpec); err != nil {
		errs = append(errs, err)
		actionErrors.Inc()
	}

	if len(errs) > 0 {
		return reconcile.Result{}, errors.Join(errs...)
	}

	return reconcile.Result{
		RequeueAfter: r.Resync,
	}, nil
}

// reconcileOne ensures that an EgressIP is associated with the OCI VNIC
// corresponding to the EgressIP interface resolved for its assigned node.
func (r *Reconciler) reconcileOne(
	ctx context.Context,
	a assignment,
	nodeEgressIPs map[string]string,
	obj *unstructured.Unstructured,
) error {

	log := logf.FromContext(ctx).WithValues(
		"egressIP", a.IP,
		"node", a.Node,
		"object", a.EgressIP,
	)

	// Identify the subnet containing the EgressIP.
	egressSubnetID, ok := r.subnetFor(a.IP)

	if !ok {
		log.Info(
			"EgressIP outside configured subnets; ignored",
		)
		return nil
	}

	// Get the EgressIP interface IP of the assigned node (secondary if
	// annotated; otherwise the primary interface).
	egressInterfaceIP := nodeEgressIPs[a.Node]

	if egressInterfaceIP == "" {
		return fmt.Errorf(
			"node %s has no valid EgressIP interface IP (checked annotation %q, OVN primary interface and NodeInternalIP)",
			a.Node,
			egressIPInterfaceIPAnnotation,
		)
	}

	// Identify the subnet containing the resolved node interface.
	nodeSubnetID, ok := r.subnetFor(egressInterfaceIP)

	if !ok {
		return fmt.Errorf(
			"EgressIP interface IP %q of node %s is outside configured subnets",
			egressInterfaceIP,
			a.Node,
		)
	}

	// OCI secondary private IPs must belong to the same subnet
	// as the destination VNIC.
	if egressSubnetID != nodeSubnetID {
		return fmt.Errorf(
			"EgressIP %s belongs to subnet %s, but node %s EgressIP interface %s belongs to subnet %s",
			a.IP,
			egressSubnetID,
			a.Node,
			egressInterfaceIP,
			nodeSubnetID,
		)
	}

	// Discover the OCI VNIC using the resolved node interface IP.
	nodePIP, err := r.OCI.GetPrivateIP(
		ctx,
		nodeSubnetID,
		egressInterfaceIP,
	)

	if err != nil {
		return fmt.Errorf(
			"looking up EgressIP VNIC of node %s (IP %s): %w",
			a.Node,
			egressInterfaceIP,
			err,
		)
	}

	if nodePIP == nil {
		return fmt.Errorf(
			"EgressIP interface IP %s of node %s not found in OCI",
			egressInterfaceIP,
			a.Node,
		)
	}

	targetVnic := nodePIP.VnicID

	// Find the current location of the EgressIP in OCI.
	current, err := r.OCI.GetPrivateIP(
		ctx,
		egressSubnetID,
		a.IP,
	)

	if err != nil {
		return fmt.Errorf(
			"looking up EgressIP %s in OCI: %w",
			a.IP,
			err,
		)
	}

	tags := map[string]string{
		TagManagedBy: ManagedByVal,
		TagCluster:   r.ClusterID,
		TagName:      a.EgressIP,
	}

	switch {

	// CREATE: EgressIP does not exist in OCI.
	case current == nil:

		log.Info(
			"CREATE",
			"vnic", targetVnic,
			"interfaceIP", egressInterfaceIP,
			"dryRun", r.DryRun,
		)

		if r.DryRun {
			return nil
		}

		if err := r.OCI.CreatePrivateIP(
			ctx,
			targetVnic,
			a.IP,
			"egressip-"+a.EgressIP,
			tags,
		); err != nil {

			return fmt.Errorf(
				"creating EgressIP %s: %w",
				a.IP,
				err,
			)
		}

		actionsTotal.WithLabelValues("create").Inc()

		r.event(
			obj,
			corev1.EventTypeNormal,
			"OCIPrivateIPCreated",
			"IP %s registered on the EgressIP VNIC of node %s",
			a.IP,
			a.Node,
		)

	// CONFLICT: IP is a primary address.
	case current.Primary:

		r.event(
			obj,
			corev1.EventTypeWarning,
			"OCIAddressConflict",
			"IP %s is the primary IP of another VNIC in OCI",
			a.IP,
		)

		return fmt.Errorf(
			"%s is the primary IP of another VNIC; address conflict",
			a.IP,
		)

	// MOVE: IP exists but belongs to another VNIC.
	case current.VnicID != targetVnic:

		// Never move an IP that is not managed by this controller
		// and owned by this cluster.
		if current.Tags[TagManagedBy] != ManagedByVal ||
			current.Tags[TagCluster] != r.ClusterID {

			r.event(
				obj,
				corev1.EventTypeWarning,
				"OCIAddressOwnershipConflict",
				"IP %s exists in OCI but is not managed by this controller and cluster",
				a.IP,
			)

			return fmt.Errorf(
				"IP %s exists on VNIC %s but is not managed by this controller/cluster",
				a.IP,
				current.VnicID,
			)
		}

		log.Info(
			"MOVE",
			"from", current.VnicID,
			"to", targetVnic,
			"interfaceIP", egressInterfaceIP,
			"dryRun", r.DryRun,
		)

		if r.DryRun {
			return nil
		}

		if err := r.OCI.MovePrivateIP(
			ctx,
			current.ID,
			targetVnic,
			tags,
		); err != nil {

			return fmt.Errorf(
				"moving EgressIP %s: %w",
				a.IP,
				err,
			)
		}

		actionsTotal.WithLabelValues("move").Inc()

		r.event(
			obj,
			corev1.EventTypeNormal,
			"OCIPrivateIPMoved",
			"IP %s moved to the EgressIP VNIC of node %s",
			a.IP,
			a.Node,
		)

	// NO ACTION: IP already belongs to the target VNIC.
	default:

		log.V(1).Info(
			"EgressIP already on target VNIC",
			"vnic", targetVnic,
		)
	}

	return nil
}

// garbageCollect deletes private IPs tagged by this controller for this cluster
// that no longer appear in the spec of any EgressIP.
//
// Untagged IPs and IPs belonging to other clusters are never touched.
func (r *Reconciler) garbageCollect(
	ctx context.Context,
	inSpec map[string]bool,
) error {

	log := logf.FromContext(ctx)

	var errs []error

	for _, s := range r.Subnets {

		pips, err := r.OCI.ListPrivateIPs(
			ctx,
			s.ID,
		)

		if err != nil {
			errs = append(
				errs,
				fmt.Errorf(
					"listing private IPs of subnet %s: %w",
					s.ID,
					err,
				),
			)
			continue
		}

		for _, p := range pips {

			// Never touch primary IPs or IPs not owned by this controller.
			if p.Primary ||
				p.Tags[TagManagedBy] != ManagedByVal ||
				p.Tags[TagCluster] != r.ClusterID {

				continue
			}

			// IP is still declared in an EgressIP spec.
			if inSpec[p.Address] {
				continue
			}

			log.Info(
				"DELETE orphan",
				"ip", p.Address,
				"subnet", s.ID,
				"dryRun", r.DryRun,
			)

			if r.DryRun {
				continue
			}

			if err := r.OCI.DeletePrivateIP(
				ctx,
				p.ID,
			); err != nil {

				errs = append(
					errs,
					fmt.Errorf(
						"deleting EgressIP %s: %w",
						p.Address,
						err,
					),
				)

				continue
			}

			actionsTotal.WithLabelValues("delete").Inc()
		}
	}

	return errors.Join(errs...)
}

// clusterIsActive implements DR fencing.
//
// Only a cluster with active=true can change private IPs.
// A missing ConfigMap means inactive (fail-safe).
func (r *Reconciler) clusterIsActive(
	ctx context.Context,
) (bool, error) {

	cm := &corev1.ConfigMap{}

	err := r.Client.Get(
		ctx,
		r.ActiveConfigMap,
		cm,
	)

	if apierrors.IsNotFound(err) {

		logf.FromContext(ctx).Info(
			"fencing ConfigMap not found; treating cluster as INACTIVE",
			"configMap", r.ActiveConfigMap.String(),
		)

		return false, nil
	}

	if err != nil {
		return false, fmt.Errorf(
			"reading fencing ConfigMap: %w",
			err,
		)
	}

	return cm.Data["active"] == "true", nil
}

// subnetFor returns the subnet OCID containing the given IP.
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

// event emits a Kubernetes Event associated with the EgressIP object.
func (r *Reconciler) event(
	obj *unstructured.Unstructured,
	eventType string,
	reason string,
	msgFmt string,
	args ...any,
) {

	if r.Recorder != nil && obj != nil {
		r.Recorder.Eventf(
			obj,
			eventType,
			reason,
			msgFmt,
			args...,
		)
	}
}

// desiredState returns:
//
//   - OVN assignments from status.items
//   - All declared IPs from spec.egressIPs
//   - EgressIP object index used to emit Events
func desiredState(
	items []unstructured.Unstructured,
) (
	[]assignment,
	map[string]bool,
	map[string]*unstructured.Unstructured,
) {

	var assigned []assignment

	inSpec := map[string]bool{}

	objs := map[string]*unstructured.Unstructured{}

	for i := range items {

		e := &items[i]

		objs[e.GetName()] = e

		ips, _, _ := unstructured.NestedStringSlice(
			e.Object,
			"spec",
			"egressIPs",
		)

		for _, ip := range ips {
			inSpec[ip] = true
		}

		statusItems, _, _ := unstructured.NestedSlice(
			e.Object,
			"status",
			"items",
		)

		for _, raw := range statusItems {

			m, ok := raw.(map[string]any)

			if !ok {
				continue
			}

			ip, _ := m["egressIP"].(string)
			node, _ := m["node"].(string)

			if ip != "" && node != "" {

				assigned = append(
					assigned,
					assignment{
						IP:       ip,
						Node:     node,
						EgressIP: e.GetName(),
					},
				)
			}
		}
	}

	sort.Slice(
		assigned,
		func(i, j int) bool {
			return assigned[i].IP < assigned[j].IP
		},
	)

	return assigned, inSpec, objs
}

// nodeEgressIPInterfaceIP returns the IP address used to discover the OCI VNIC
// for a Node. The dedicated EgressIP interface annotation has priority. If it
// is absent, the function falls back to the OVN primary-interface annotation
// and finally to NodeInternalIP.
//
// The custom annotation is expected to contain a plain IPv4 address, for
// example: "192.168.2.20".
func nodeEgressIPInterfaceIP(n *corev1.Node) string {
	if n == nil {
		return ""
	}

	// 1. Prefer the dedicated secondary interface when explicitly configured.
	if raw := n.Annotations[egressIPInterfaceIPAnnotation]; raw != "" {
		addr, err := netip.ParseAddr(raw)
		if err != nil || !addr.Is4() {
			// Do not silently select another interface when an explicit but
			// invalid EgressIP interface annotation is present.
			return ""
		}
		return addr.String()
	}

	// 2. No dedicated interface annotation: use the primary interface
	// reported by OVN, falling back to Kubernetes NodeInternalIP.
	return nodePrimaryIP(n)
}

// nodePrimaryIP extracts the IPv4 address from the OVN annotation
// k8s.ovn.org/node-primary-ifaddr. OVN commonly stores this annotation as a
// JSON object whose "ipv4" value is an address/prefix (for example,
// "192.168.1.20/24"). If it is unavailable or cannot be parsed, use the
// NodeInternalIP address as a fallback.
func nodePrimaryIP(n *corev1.Node) string {
	if n == nil {
		return ""
	}

	const ovnPrimaryIfAddrAnnotation = "k8s.ovn.org/node-primary-ifaddr"

	if raw := n.Annotations[ovnPrimaryIfAddrAnnotation]; raw != "" {
		var ifaddr struct {
			IPv4 string `json:"ipv4"`
		}

		if err := json.Unmarshal([]byte(raw), &ifaddr); err == nil && ifaddr.IPv4 != "" {
			// The OVN value is normally a CIDR prefix. Accept a plain IP too,
			// to remain tolerant of format variations.
			if prefix, err := netip.ParsePrefix(ifaddr.IPv4); err == nil {
				addr := prefix.Addr()
				if addr.Is4() {
					return addr.String()
				}
			} else if addr, err := netip.ParseAddr(ifaddr.IPv4); err == nil && addr.Is4() {
				return addr.String()
			}
		}
	}

	return nodeInternalIP(n)
}

// nodeInternalIP returns the first valid IPv4 NodeInternalIP address.
func nodeInternalIP(n *corev1.Node) string {
	if n == nil {
		return ""
	}

	for _, address := range n.Status.Addresses {
		if address.Type != corev1.NodeInternalIP {
			continue
		}

		addr, err := netip.ParseAddr(address.Address)
		if err == nil && addr.Is4() {
			return addr.String()
		}
	}

	return ""
}

// nodeEgressIPInterfaceMap returns Node name -> resolved EgressIP interface IP.
// Resolution prefers the dedicated interface annotation and falls back to
// the node primary interface when no dedicated interface is configured.
func nodeEgressIPInterfaceMap(nodes []corev1.Node) map[string]string {
	out := make(map[string]string, len(nodes))

	for i := range nodes {
		n := &nodes[i]
		out[n.Name] = nodeEgressIPInterfaceIP(n)
	}

	return out
}

// SetupWithManager registers the watches.
//
// Every relevant event maps to the same global reconciliation key.
func (r *Reconciler) SetupWithManager(
	mgr ctrl.Manager,
) error {

	toSingleton := handler.EnqueueRequestsFromMapFunc(
		func(
			context.Context,
			client.Object,
		) []reconcile.Request {

			return []reconcile.Request{
				singletonKey,
			}
		},
	)

	egressIP := &unstructured.Unstructured{}

	egressIP.SetGroupVersionKind(EgressIPGVK)

	// Only node changes affecting the EgressIP interface mapping
	// are relevant.
	nodeChanged := predicate.Funcs{

		UpdateFunc: func(e event.UpdateEvent) bool {

			oldN, ok1 := e.ObjectOld.(*corev1.Node)
			newN, ok2 := e.ObjectNew.(*corev1.Node)

			if !ok1 || !ok2 {
				return true
			}

			// Reconcile when the effective interface IP changes. This covers:
			// - the dedicated EgressIP interface annotation;
			// - OVN's primary-interface annotation;
			// - NodeInternalIP when OVN's annotation is absent/invalid.
			return nodeEgressIPInterfaceIP(oldN) !=
				nodeEgressIPInterfaceIP(newN)
		},
	}

	// Only the configured fencing ConfigMap triggers reconciliation.
	fencingCM := predicate.NewPredicateFuncs(
		func(o client.Object) bool {

			return o.GetNamespace() ==
				r.ActiveConfigMap.Namespace &&
				o.GetName() ==
					r.ActiveConfigMap.Name
		},
	)

	return ctrl.NewControllerManagedBy(mgr).
		Named("egressip-oci").
		Watches(
			egressIP,
			toSingleton,
			builder.WithPredicates(
				predicate.Or(
					predicate.GenerationChangedPredicate{},
					statusChanged{},
				),
			),
		).
		Watches(
			&corev1.Node{},
			toSingleton,
			builder.WithPredicates(nodeChanged),
		).
		Watches(
			&corev1.ConfigMap{},
			toSingleton,
			builder.WithPredicates(fencingCM),
		).
		Complete(r)
}

// statusChanged fires when EgressIP status changes.
// This is important when OVN moves an EgressIP to another Node.
type statusChanged struct {
	predicate.Funcs
}

func (statusChanged) Update(e event.UpdateEvent) bool {

	oldU, ok1 := e.ObjectOld.(*unstructured.Unstructured)
	newU, ok2 := e.ObjectNew.(*unstructured.Unstructured)

	if !ok1 || !ok2 {
		return true
	}

	oldStatus, _, _ := unstructured.NestedFieldNoCopy(
		oldU.Object,
		"status",
	)

	newStatus, _, _ := unstructured.NestedFieldNoCopy(
		newU.Object,
		"status",
	)

	oldJSON, _ := json.Marshal(oldStatus)
	newJSON, _ := json.Marshal(newStatus)

	return string(oldJSON) != string(newJSON)
}

func (statusChanged) Create(event.CreateEvent) bool {
	return true
}

func (statusChanged) Delete(event.DeleteEvent) bool {
	return true
}

func (statusChanged) Generic(event.GenericEvent) bool {
	return true
}
