package controller

import (
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

var (
	actionsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "egressip_oci_actions_total",
		Help: "Actions performed in OCI, by type (create, move, delete).",
	}, []string{"action"})

	actionErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "egressip_oci_errors_total",
		Help: "Errors while reconciling EgressIPs with OCI.",
	})
)

func init() {
	metrics.Registry.MustRegister(actionsTotal, actionErrors)
}
