// egressip-oci-controller keeps OpenShift (OVN-Kubernetes) EgressIPs registered
// as secondary private IPs on the VNICs of OCI / DRCC instances.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/lccherri/egressip-oci-controller/internal/controller"
	"github.com/lccherri/egressip-oci-controller/internal/ocinet"
)

func main() {
	var (
		subnetIDs       = flag.String("subnet-ids", env("SUBNET_OCIDS", ""), "comma-separated OCIDs of the node subnets")
		clusterID       = flag.String("cluster-id", env("CLUSTER_ID", ""), "cluster identifier (stored as a freeform tag)")
		activeCM        = flag.String("active-configmap", env("ACTIVE_CONFIGMAP", "egressip-oci/egressip-oci-active"), "namespace/name of the fencing ConfigMap")
		resync          = flag.Duration("resync", envDuration("RESYNC", 60*time.Second), "full reconciliation interval")
		dryRun          = flag.Bool("dry-run", env("DRY_RUN", "false") == "true", "log actions without changing OCI")
		ociAuth         = flag.String("oci-auth", env("OCI_AUTH", string(ocinet.AuthAPIKey)), "api_key | instance_principal")
		ociConfigFile   = flag.String("oci-config-file", env("OCI_CONFIG_FILE", "/etc/oci/config"), "OCI configuration file (api_key)")
		ociProfile      = flag.String("oci-config-profile", env("OCI_CONFIG_PROFILE", "DEFAULT"), "OCI configuration file profile")
		ociEndpoint     = flag.String("oci-service-endpoint", env("OCI_SERVICE_ENDPOINT", ""), "Networking service endpoint (DRCC realms)")
		leaderElect     = flag.Bool("leader-elect", true, "enable leader election (recommended with 2+ replicas)")
		leaderNamespace = flag.String("leader-election-namespace", env("POD_NAMESPACE", ""), "namespace of the leader election Lease")
		metricsAddr     = flag.String("metrics-bind-address", ":8080", "Prometheus metrics bind address")
		probeAddr       = flag.String("health-probe-bind-address", ":8081", "health probe bind address")
	)
	opts := zap.Options{}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))
	log := ctrl.Log.WithName("setup")

	if err := run(*subnetIDs, *clusterID, *activeCM, *resync, *dryRun,
		ocinet.Options{
			Auth:            ocinet.AuthMode(*ociAuth),
			ConfigFile:      *ociConfigFile,
			ConfigProfile:   *ociProfile,
			ServiceEndpoint: *ociEndpoint,
		},
		*leaderElect, *leaderNamespace, *metricsAddr, *probeAddr); err != nil {
		log.Error(err, "fatal error")
		os.Exit(1)
	}
}

func run(subnetIDs, clusterID, activeCM string, resync time.Duration, dryRun bool,
	ociOpts ocinet.Options, leaderElect bool, leaderNS, metricsAddr, probeAddr string) error {
	log := ctrl.Log.WithName("setup")

	if subnetIDs == "" || clusterID == "" {
		return fmt.Errorf("SUBNET_OCIDS and CLUSTER_ID are required")
	}
	cmNS, cmName, ok := strings.Cut(activeCM, "/")
	if !ok || cmNS == "" || cmName == "" {
		return fmt.Errorf("ACTIVE_CONFIGMAP must be namespace/name, got %q", activeCM)
	}

	oci, err := ocinet.New(ociOpts)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var subnets []controller.Subnet
	for _, id := range strings.Split(subnetIDs, ",") {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		cidr, err := oci.SubnetCIDR(ctx, id)
		if err != nil {
			return fmt.Errorf("reading subnet %s from OCI: %w", id, err)
		}
		log.Info("subnet configured", "id", id, "cidr", cidr.String())
		subnets = append(subnets, controller.Subnet{ID: id, CIDR: cidr})
	}

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		return err
	}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                  scheme,
		Metrics:                 metricsserver.Options{BindAddress: metricsAddr},
		HealthProbeBindAddress:  probeAddr,
		LeaderElection:          leaderElect,
		LeaderElectionID:        "egressip-oci-controller",
		LeaderElectionNamespace: leaderNS,
		// The ConfigMap cache is limited to the fencing ConfigMap (minimal RBAC and memory).
		Cache: cache.Options{
			ByObject: map[client.Object]cache.ByObject{
				&corev1.ConfigMap{}: {
					Namespaces: map[string]cache.Config{cmNS: {}},
					Field:      fields.OneTermEqualSelector("metadata.name", cmName),
				},
			},
		},
	})
	if err != nil {
		return fmt.Errorf("creating manager: %w", err)
	}

	r := &controller.Reconciler{
		Client:          mgr.GetClient(),
		OCI:             oci,
		Recorder:        mgr.GetEventRecorderFor("egressip-oci-controller"),
		Subnets:         subnets,
		ClusterID:       clusterID,
		ActiveConfigMap: types.NamespacedName{Namespace: cmNS, Name: cmName},
		Resync:          resync,
		DryRun:          dryRun,
	}
	if err := r.SetupWithManager(mgr); err != nil {
		return fmt.Errorf("registering controller: %w", err)
	}
	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		return err
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		return err
	}

	log.Info("starting", "clusterID", clusterID, "dryRun", dryRun, "resync", resync.String())
	return mgr.Start(ctrl.SetupSignalHandler())
}

func env(key, def string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return def
}

func envDuration(key string, def time.Duration) time.Duration {
	if v, ok := os.LookupEnv(key); ok {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
