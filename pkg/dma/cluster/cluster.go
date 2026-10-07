// Package cluster bundles the API clients, the persisted state and the resolved configuration
// which every migration step of the DANM Migration Assistant operates on.
package cluster

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/danm-cni/danm-utils/pkg/dma/state"
	apiextclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// Mode is the DANM network management API set installed in the cluster. The two sets are
// mutually exclusive: netwatcher discovers its APIs once during start-up, so a cluster serving
// a mixture of them is not a migratable cluster.
type Mode string

const (
	ModeUnknown     Mode = ""
	ModeLightweight Mode = "lightweight"
	ModeProduction  Mode = "production"
)

// ParseMode validates a textual deployment mode.
func ParseMode(value string) (Mode, error) {
	switch Mode(value) {
	case ModeUnknown:
		return ModeUnknown, nil
	case ModeLightweight:
		return ModeLightweight, nil
	case ModeProduction:
		return ModeProduction, nil
	}
	return ModeUnknown, fmt.Errorf("deployment mode %q is neither lightweight nor production", value)
}

// Options carries the command line switches individual steps are allowed to honour.
type Options struct {
	// DryRun describes what a step would do without changing anything.
	DryRun bool
	// Force relaxes the ordering and the last-executed checks the runner enforces. It never
	// relaxes a confirmation.
	Force bool
	// AcceptDefaultMtu lets preflight pass even though networks which would silently lose
	// their MTU carry no migration tag.
	AcceptDefaultMtu bool
	// NoCanary skips the single node trial run before a fleet wide CNI binary rollout.
	NoCanary bool
	// BinaryImage replaces the assistant's own image in the CNI binary copier.
	BinaryImage string
	// StepTimeout bounds how long a single step may run.
	StepTimeout time.Duration
	// RolloutTimeout bounds how long a step waits for a patched workload to become ready
	// before it reverts that workload to its snapshot.
	RolloutTimeout time.Duration
}

// Handle bundles everything a migration step needs in order to inspect and modify the cluster.
type Handle struct {
	Kube       kubernetes.Interface
	ApiExt     apiextclient.Interface
	Dynamic    dynamic.Interface
	Discovery  discovery.DiscoveryInterface
	RestConfig *rest.Config
	State      *state.Store
	// Images is the resolution rule set, not the resolved references. Steps resolve the
	// components they need, so that a plan declaring more components than a step rolls does
	// not force every step to care.
	Images ImageConfig
	// Mode is authoritative for the whole plan once preflight has recorded it.
	Mode Mode
	// PayloadRoot is the directory inside the assistant's own image holding the CNI binaries
	// of each supported target release.
	PayloadRoot string
	Namespace   string
	// Operator identifies who is running the migration, and ends up in the recorded state.
	Operator string
	Options  Options
	Log      *log.Logger
	Out      io.Writer
}

// Config describes how to build a Handle.
type Config struct {
	Kubeconfig  string
	Namespace   string
	StateName   string
	ConfigName  string
	PayloadRoot string
	Overrides   map[string]ComponentImage
	Options     Options
	Out         io.Writer
}

// New builds the clients the assistant needs and loads its configuration. An empty kubeconfig
// path means the assistant runs inside the cluster it migrates, which is the normal case.
func New(ctx context.Context, config Config) (*Handle, error) {
	if config.Namespace == "" {
		config.Namespace = state.DefaultNamespace
	}
	if config.ConfigName == "" {
		config.ConfigName = DefaultConfigName
	}
	if config.Out == nil {
		config.Out = os.Stdout
	}
	restConfig, err := restConfig(config.Kubeconfig)
	if err != nil {
		return nil, err
	}
	kubeClient, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("cannot build the Kubernetes client: %w", err)
	}
	apiExtClient, err := apiextclient.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("cannot build the CustomResourceDefinition client: %w", err)
	}
	dynamicClient, err := dynamic.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("cannot build the dynamic client: %w", err)
	}
	images, err := LoadImageConfig(ctx, kubeClient, config.Namespace, config.ConfigName)
	if err != nil {
		return nil, err
	}
	return &Handle{
		Kube:        kubeClient,
		ApiExt:      apiExtClient,
		Dynamic:     dynamicClient,
		Discovery:   kubeClient.Discovery(),
		RestConfig:  restConfig,
		State:       state.NewStore(kubeClient, config.Namespace, config.StateName),
		Images:      images.WithOverrides(config.Overrides),
		PayloadRoot: config.PayloadRoot,
		Namespace:   config.Namespace,
		Operator:    operator(),
		Options:     config.Options,
		Log:         log.New(config.Out, "", log.LstdFlags),
		Out:         config.Out,
	}, nil
}

// Printf writes a line of human readable progress to the assistant's output.
func (handle *Handle) Printf(format string, args ...any) {
	fmt.Fprintf(handle.Out, format+"\n", args...)
}

// PayloadDir returns the directory holding the CNI binaries of one target release.
func (handle *Handle) PayloadDir(payload string) string {
	if payload == "" {
		return ""
	}
	return handle.PayloadRoot + "/" + payload
}

func restConfig(kubeconfig string) (*rest.Config, error) {
	if kubeconfig != "" {
		config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
		if err != nil {
			return nil, fmt.Errorf("cannot build the cluster configuration from %s: %w", kubeconfig, err)
		}
		return config, nil
	}
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("cannot build the in-cluster configuration, pass --kubeconf when running outside of a cluster: %w", err)
	}
	return config, nil
}

func operator() string {
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		return "unknown"
	}
	return hostname
}
