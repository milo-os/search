package manager

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"go.miloapis.net/search/pkg/apis/search/install"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
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
	ProbeAddr                  string
	SecureMetrics              bool
	EnableHTTP2                bool
	MaxCELDepth                int
	MeilisearchDomain          string
	MeilisearchChunkSize       int
	MeilisearchTaskWaitTimeout time.Duration
	MeilisearchHTTPTimeout     time.Duration
}

// NewControllerManagerOptions creates a new ControllerManagerOptions with default values
func NewControllerManagerOptions() *ControllerManagerOptions {
	return &ControllerManagerOptions{
		MetricsAddr:                ":8080",
		ProbeAddr:                  ":8081",
		EnableLeaderElection:       true,
		SecureMetrics:              false,
		EnableHTTP2:                false,
		MaxCELDepth:                50,
		MeilisearchChunkSize:       1000,
		MeilisearchTaskWaitTimeout: 30 * time.Second,
		MeilisearchHTTPTimeout:     60 * time.Second,
		MeilisearchDomain:          "http://meilisearch.meilisearch-system.svc.cluster.local:7700",
	}
}

// AddFlags adds flags to the specified FlagSet
func (o *ControllerManagerOptions) AddFlags(fs *pflag.FlagSet) {
	fs.StringVar(&o.MetricsAddr, "metrics-bind-address", o.MetricsAddr, "The address the metric endpoint binds to.")
	fs.StringVar(&o.ProbeAddr, "health-probe-bind-address", o.ProbeAddr, "The address the probe endpoint binds to.")
	fs.BoolVar(&o.EnableLeaderElection, "leader-elect", o.EnableLeaderElection,
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")
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

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: o.MetricsAddr, SecureServing: o.SecureMetrics, TLSOpts: tlsOpts},
		HealthProbeBindAddress: o.ProbeAddr,
		LeaderElection:         o.EnableLeaderElection,
		LeaderElectionID:       "controller.search.miloapis.com",
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	// Register Webhook
	celValidator, err := cel.NewValidator(o.MaxCELDepth)
	if err != nil {
		setupLog.Error(err, "unable to create CEL validator")
		os.Exit(1)
	}

	// Initialize Meilisearch SDK
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

	if err = (&policycontroller.ResourceIndexPolicyReconciler{
		Client:       mgr.GetClient(),
		Scheme:       mgr.GetScheme(),
		CelValidator: celValidator,
		SearchSDK:    searchSDK,
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
