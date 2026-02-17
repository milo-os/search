package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	searchapiserver "go.miloapis.net/search/internal/apiserver"
	"go.miloapis.net/search/internal/version"
	searchv1alpha1 "go.miloapis.net/search/pkg/apis/search/v1alpha1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	apiopenapi "k8s.io/apiserver/pkg/endpoints/openapi"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/apiserver/pkg/server/options"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/component-base/cli"
	basecompatibility "k8s.io/component-base/compatibility"
	"k8s.io/component-base/logs"
	logsapi "k8s.io/component-base/logs/api/v1"
	"k8s.io/klog/v2"
	"k8s.io/kube-openapi/pkg/common"

	"go.miloapis.net/search/cmd/search/manager"

	// Register JSON logging format
	_ "k8s.io/component-base/logs/json/register"
)

func init() {
	utilruntime.Must(logsapi.AddFeatureGates(utilfeature.DefaultMutableFeatureGate))
	utilfeature.DefaultMutableFeatureGate.Set("LoggingBetaOptions=true")
	utilfeature.DefaultMutableFeatureGate.Set("RemoteRequestHeaderUID=true")
}

func GetOpenAPIDefinitions(cb common.ReferenceCallback) map[string]common.OpenAPIDefinition {
	defs := make(map[string]common.OpenAPIDefinition)

	merge := func(pkgDefs map[string]common.OpenAPIDefinition) {
		for k, v := range pkgDefs {
			// For k8s.io types, store both the original key and the transformed key
			// because the namer behavior is inconsistent across different types
			if strings.HasPrefix(k, "k8s.io/") {
				// Store original key (with slashes)
				defs[k] = v
				// Also store transformed key (io.k8s with dots)
				newK := "io.k8s." + k[7:]
				newK = strings.ReplaceAll(newK, "/", ".")
				defs[newK] = v
			} else {
				// For non-k8s.io types, keep as-is
				defs[k] = v
			}
		}
	}

	merge(searchv1alpha1.GetOpenAPIDefinitions(cb))
	return defs
}

func main() {
	cmd := NewSearchServerCommand()
	code := cli.Run(cmd)
	os.Exit(code)
}

// NewSearchServerCommand creates the root command with subcommands for the search server.
func NewSearchServerCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "search",
		Short: "Search - generic aggregated API server",
		Long: `Search is a generic Kubernetes aggregated API server that can be extended
with custom search implementations.

Exposes SearchQuery resources accessible through kubectl or any Kubernetes client.`,
	}

	cmd.AddCommand(NewServeCommand())
	cmd.AddCommand(NewVersionCommand())
	cmd.AddCommand(manager.NewControllerManagerCommand())

	return cmd
}

// NewServeCommand creates the serve subcommand that starts the API server.
func NewServeCommand() *cobra.Command {
	options := NewSearchServerOptions()

	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Start the API server",
		Long: `Start the API server and begin serving requests.

Exposes SearchQuery resources through kubectl.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := options.Complete(); err != nil {
				return err
			}
			if err := options.Validate(); err != nil {
				return err
			}
			return Run(options, cmd.Context())
		},
	}

	flags := cmd.Flags()
	options.AddFlags(flags)

	// Add logging flags - this includes the -v flag for verbosity
	logsapi.AddFlags(options.Logs, flags)

	return cmd
}

// NewVersionCommand creates the version subcommand to display build information.
func NewVersionCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "version",
		Short: "Show version information",
		Long:  `Show the version, git commit, and build details.`,
		Run: func(cmd *cobra.Command, args []string) {
			info := version.Get()
			fmt.Printf("Search Server\n")
			fmt.Printf("  Version:       %s\n", info.Version)
			fmt.Printf("  Git Commit:    %s\n", info.GitCommit)
			fmt.Printf("  Git Tree:      %s\n", info.GitTreeState)
			fmt.Printf("  Build Date:    %s\n", info.BuildDate)
			fmt.Printf("  Go Version:    %s\n", info.GoVersion)
			fmt.Printf("  Go Compiler:   %s\n", info.Compiler)
			fmt.Printf("  Platform:      %s\n", info.Platform)
		},
	}

	return cmd
}

// SearchServerOptions contains configuration for the search server.
type SearchServerOptions struct {
	RecommendedOptions *options.RecommendedOptions
	Logs               *logsapi.LoggingConfiguration
}

// NewSearchServerOptions creates options with default values.
func NewSearchServerOptions() *SearchServerOptions {
	o := &SearchServerOptions{
		RecommendedOptions: options.NewRecommendedOptions(
			"/registry/search.miloapis.com",
			searchapiserver.Codecs.LegacyCodec(searchapiserver.Scheme.PrioritizedVersionsAllGroups()...),
		),
		Logs: logsapi.NewLoggingConfiguration(),
	}

	return o
}

func (o *SearchServerOptions) AddFlags(fs *pflag.FlagSet) {
	o.RecommendedOptions.AddFlags(fs)
}

func (o *SearchServerOptions) Complete() error {
	return nil
}

// Validate ensures required configuration is provided.
func (o *SearchServerOptions) Validate() error {
	// Add validation as needed
	return nil
}

// Config builds the complete server configuration from options.
func (o *SearchServerOptions) Config() (*searchapiserver.Config, error) {
	if err := o.RecommendedOptions.SecureServing.MaybeDefaultWithSelfSignedCerts(
		"localhost", nil, nil); err != nil {
		return nil, fmt.Errorf("error creating self-signed certificates: %v", err)
	}

	genericConfig := genericapiserver.NewRecommendedConfig(searchapiserver.Codecs)

	// Set effective version to match the Kubernetes version we're built against.
	genericConfig.EffectiveVersion = basecompatibility.NewEffectiveVersionFromString("1.34", "", "")

	namer := apiopenapi.NewDefinitionNamer(searchapiserver.Scheme)
	genericConfig.OpenAPIV3Config = genericapiserver.DefaultOpenAPIV3Config(GetOpenAPIDefinitions, namer)
	genericConfig.OpenAPIV3Config.Info.Title = "Search"
	genericConfig.OpenAPIV3Config.Info.Version = version.Version

	// Configure OpenAPI v2
	genericConfig.OpenAPIConfig = genericapiserver.DefaultOpenAPIConfig(GetOpenAPIDefinitions, namer)
	genericConfig.OpenAPIConfig.Info.Title = "Search"
	genericConfig.OpenAPIConfig.Info.Version = version.Version

	if err := o.RecommendedOptions.ApplyTo(genericConfig); err != nil {
		return nil, fmt.Errorf("failed to apply recommended options: %w", err)
	}

	serverConfig := &searchapiserver.Config{
		GenericConfig: genericConfig,
		ExtraConfig:   searchapiserver.ExtraConfig{},
	}

	return serverConfig, nil
}

// Run initializes and starts the server.
func Run(options *SearchServerOptions, ctx context.Context) error {
	if err := logsapi.ValidateAndApply(options.Logs, utilfeature.DefaultMutableFeatureGate); err != nil {
		return fmt.Errorf("failed to apply logging configuration: %w", err)
	}

	config, err := options.Config()
	if err != nil {
		return err
	}

	server, err := config.Complete().New()
	if err != nil {
		return err
	}

	defer logs.FlushLogs()

	klog.Info("Starting Search server...")
	klog.Info("Metrics available at https://<server-address>/metrics")
	return server.Run(ctx)
}
