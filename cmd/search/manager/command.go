package manager

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"go.miloapis.net/search/internal/indexer"
	"go.miloapis.net/search/internal/tenant"
	"go.miloapis.net/search/pkg/apis/search/install"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/dynamic"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	policycontroller "go.miloapis.net/search/internal/controllers/policy"

	"go.miloapis.net/search/pkg/meilisearch"

	"go.miloapis.net/search/internal/cel"
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	install.Install(scheme)
}

// ControllerManagerOptions contains configuration for the controller manager
type ControllerManagerOptions struct {
	MetricsAddr                string
	EnableLeaderElection       bool
	LeaderElectionNamespace    string
	ProbeAddr                  string
	SecureMetrics              bool
	EnableHTTP2                bool
	MaxCELDepth                int
	MeilisearchDomain          string
	MeilisearchChunkSize       int
	MeilisearchTaskWaitTimeout time.Duration
	MeilisearchHTTPTimeout     time.Duration

	// NATS settings for publishing per-resource re-index messages.
	NatsURL            string
	NatsReindexSubject string
	NatsTLSCA          string
	NatsTLSCert        string
	NatsTLSKey         string

	// Multi-tenancy settings.
	EnableMultiTenancy   bool
	ProjectLabelSelector string
}

// NewControllerManagerOptions creates a new ControllerManagerOptions with default values
func NewControllerManagerOptions() *ControllerManagerOptions {
	return &ControllerManagerOptions{
		MetricsAddr:                ":8080",
		ProbeAddr:                  ":8081",
		EnableLeaderElection:       true,
		LeaderElectionNamespace:    "",
		SecureMetrics:              false,
		EnableHTTP2:                false,
		MaxCELDepth:                50,
		MeilisearchChunkSize:       1000,
		MeilisearchTaskWaitTimeout: 30 * time.Second,
		MeilisearchHTTPTimeout:     60 * time.Second,
		MeilisearchDomain:          "http://meilisearch.meilisearch-system.svc.cluster.local:7700",
		NatsURL:                    "nats://nats.nats-system.svc.cluster.local:4222",
		NatsReindexSubject:         "reindex.all",
		EnableMultiTenancy:         false,
	}
}

// AddFlags adds flags to the specified FlagSet
func (o *ControllerManagerOptions) AddFlags(fs *pflag.FlagSet) {
	fs.StringVar(&o.MetricsAddr, "metrics-bind-address", o.MetricsAddr, "The address the metric endpoint binds to.")
	fs.StringVar(&o.ProbeAddr, "health-probe-bind-address", o.ProbeAddr, "The address the probe endpoint binds to.")
	fs.BoolVar(&o.EnableLeaderElection, "leader-elect", o.EnableLeaderElection,
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")
	fs.StringVar(&o.LeaderElectionNamespace, "leader-elect-resource-namespace", o.LeaderElectionNamespace,
		"The namespace in which the leader election resource will be created.")
	fs.BoolVar(&o.SecureMetrics, "metrics-secure", o.SecureMetrics,
		"If set the metrics endpoint is served securely")
	fs.BoolVar(&o.EnableHTTP2, "enable-http2", o.EnableHTTP2,
		"If set, HTTP/2 will be enabled for the metrics and webhook servers")
	fs.IntVar(&o.MaxCELDepth, "max-cel-depth", o.MaxCELDepth, "Maximum recursion depth allowed for CEL expressions in policies.")

	// Meilisearch
	fs.StringVar(&o.MeilisearchDomain, "meilisearch-domain", o.MeilisearchDomain, "Domain of the Meilisearch instance.")
	fs.DurationVar(&o.MeilisearchTaskWaitTimeout, "meilisearch-task-wait-timeout", o.MeilisearchTaskWaitTimeout, "Timeout for waiting for Meilisearch tasks to complete.")
	fs.DurationVar(&o.MeilisearchHTTPTimeout, "meilisearch-http-timeout", o.MeilisearchHTTPTimeout, "Timeout for HTTP requests to Meilisearch.")
	fs.IntVar(&o.MeilisearchChunkSize, "meilisearch-chunk-size", o.MeilisearchChunkSize, "The number of documents to process in a single chunk.")

	// NATS
	fs.StringVar(&o.NatsURL, "nats-url", o.NatsURL, "The URL of the NATS server used to publish re-index messages.")
	fs.StringVar(&o.NatsReindexSubject, "nats-reindex-subject", o.NatsReindexSubject, "The NATS subject to publish per-resource re-index messages to.")
	fs.StringVar(&o.NatsTLSCA, "nats-tls-ca", o.NatsTLSCA, "The path to the NATS TLS CA file.")
	fs.StringVar(&o.NatsTLSCert, "nats-tls-cert", o.NatsTLSCert, "The path to the NATS TLS certificate file.")
	fs.StringVar(&o.NatsTLSKey, "nats-tls-key", o.NatsTLSKey, "The path to the NATS TLS key file.")

	// Multi-tenancy
	fs.BoolVar(&o.EnableMultiTenancy, "enable-multi-tenancy", o.EnableMultiTenancy, "Enable multi-tenant mode to index resources from all project control planes.")
	fs.StringVar(&o.ProjectLabelSelector, "project-label-selector", o.ProjectLabelSelector, "Label selector to filter which projects are indexed (empty = all projects).")
}

// Validate validates the options
func (o *ControllerManagerOptions) Validate() error {
	if o.MaxCELDepth < 1 {
		return fmt.Errorf("max-cel-depth must be greater than 0")
	}

	if o.MeilisearchChunkSize < 10 {
		return fmt.Errorf("meilisearch-chunk-size must be greater than 10")
	}

	if o.MeilisearchDomain == "" {
		return fmt.Errorf("meilisearch-domain must be set")
	}

	if os.Getenv("MEILISEARCH_API_KEY") == "" {
		return fmt.Errorf("meilisearch-api-key must be set")
	}

	if o.NatsURL == "" {
		return fmt.Errorf("nats-url must be set")
	}

	return nil
}

// Complete completes the options
func (o *ControllerManagerOptions) Complete() error {
	return nil
}

// NewControllerManagerCommand creates the controller-manager subcommand.
func NewControllerManagerCommand() *cobra.Command {
	o := NewControllerManagerOptions()

	cmd := &cobra.Command{
		Use:   "controller-manager",
		Short: "Start the controller manager",
		Long:  `Start the controller manager to reconcile and validate resources.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := o.Complete(); err != nil {
				return err
			}
			if err := o.Validate(); err != nil {
				return err
			}
			return Run(o, cmd.Context())
		},
	}

	o.AddFlags(cmd.Flags())

	return cmd
}

// Run starts the controller manager
func Run(o *ControllerManagerOptions, ctx context.Context) error {
	ctrl.SetLogger(zap.New(zap.UseDevMode(true)))

	var tlsOpts []func(*tls.Config)
	if !o.EnableHTTP2 {
		tlsOpts = append(tlsOpts, func(c *tls.Config) {
			c.NextProtos = []string{"http/1.1"}
		})
	}

	cfg := ctrl.GetConfigOrDie()

	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:                  scheme,
		Metrics:                 metricsserver.Options{BindAddress: o.MetricsAddr, SecureServing: o.SecureMetrics, TLSOpts: tlsOpts},
		HealthProbeBindAddress:  o.ProbeAddr,
		LeaderElection:          o.EnableLeaderElection,
		LeaderElectionID:        "controller.search.miloapis.com",
		LeaderElectionNamespace: o.LeaderElectionNamespace,
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	celValidator, err := cel.NewValidator(o.MaxCELDepth)
	if err != nil {
		setupLog.Error(err, "unable to create CEL validator")
		os.Exit(1)
	}

	searchSDK, err := meilisearch.NewSDKClient(meilisearch.SDKConfig{
		Domain:      o.MeilisearchDomain,
		APIKey:      os.Getenv("MEILISEARCH_API_KEY"),
		WaitTimeout: o.MeilisearchTaskWaitTimeout,
		ChunkSize:   o.MeilisearchChunkSize,
		HTTPTimeout: o.MeilisearchHTTPTimeout,
	})
	if err != nil {
		setupLog.Error(err, "unable to create Meilisearch SDK")
		os.Exit(1)
	}

	// Dynamic client — used by the policy controller to list target resources.
	dynamicClient, err := dynamic.NewForConfig(cfg)
	if err != nil {
		setupLog.Error(err, "unable to create dynamic client")
		os.Exit(1)
	}

	// Connect to NATS and set up the re-index stream + publisher.
	setupLog.Info("Connecting to NATS for re-index publishing", "url", o.NatsURL)

	var natsOpts []nats.Option
	if o.NatsTLSCert != "" && o.NatsTLSKey != "" {
		if o.NatsTLSCA != "" {
			setupLog.Info("Using NATS TLS CA", "ca", o.NatsTLSCA)
			natsOpts = append(natsOpts, nats.RootCAs(o.NatsTLSCA))
		}
		setupLog.Info("Using NATS TLS cert", "cert", o.NatsTLSCert)
		setupLog.Info("Using NATS TLS key", "key", o.NatsTLSKey)
		natsOpts = append(natsOpts, nats.ClientCert(o.NatsTLSCert, o.NatsTLSKey))
	}

	nc, err := nats.Connect(o.NatsURL, natsOpts...)
	if err != nil {
		setupLog.Error(err, "unable to connect to NATS")
		os.Exit(1)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		setupLog.Error(err, "unable to create JetStream context")
		os.Exit(1)
	}

	reindexPub := indexer.NewReindexPublisher(js, o.NatsReindexSubject)

	// Build TenantRegistry based on deployment mode.
	var registry tenant.TenantRegistry
	if o.EnableMultiTenancy {
		// Create a PolicyCache backed by the manager's shared informer cache.
		// requireReadyCondition=true ensures only fully-initialized policies
		// (index created, attributes synced) are included in the cache.
		policyCache, err := indexer.NewPolicyCache(mgr.GetCache(), true)
		if err != nil {
			setupLog.Error(err, "unable to create policy cache")
			os.Exit(1)
		}
		if err := policyCache.RegisterHandlers(ctx); err != nil {
			setupLog.Error(err, "unable to register policy cache handlers")
			os.Exit(1)
		}

		// ProjectWatcher handles tenant lifecycle: on disengagement it purges all
		// tenant documents from each index.
		projectWatcher := tenant.NewProjectWatcher(policyCache, searchSDK)

		multiRegistry := tenant.NewMultiTenantRegistry(
			cfg,
			dynamicClient,
			o.ProjectLabelSelector,
			projectWatcher.OnTenantEngaged,
			projectWatcher.OnTenantDisengaged,
		)
		go func() {
			if err := multiRegistry.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
				setupLog.Error(err, "MultiTenantRegistry stopped unexpectedly")
			}
		}()
		registry = multiRegistry
	} else {
		registry = tenant.NewSingleTenantRegistry(dynamicClient)
	}

	if err = (&policycontroller.ResourceIndexPolicyReconciler{
		Client:           mgr.GetClient(),
		Scheme:           mgr.GetScheme(),
		CelValidator:     celValidator,
		SearchSDK:        searchSDK,
		DynamicClient:    dynamicClient,
		RESTMapper:       mgr.GetRESTMapper(),
		ReindexPublisher: reindexPub,
		TenantRegistry:   registry,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "ResourceIndexPolicy")
		os.Exit(1)
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	setupLog.Info("starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
	return nil
}
