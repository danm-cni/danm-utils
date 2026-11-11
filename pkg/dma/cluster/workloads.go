package cluster

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/danm-cni/danm-utils/pkg/dma/state"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const WorkloadKeyPrefix = "workload_"

type WorkloadKind string

const (
	WorkloadDaemonSet  WorkloadKind = "DaemonSet"
	WorkloadDeployment WorkloadKind = "Deployment"
)

type WorkloadRef struct {
	Kind      WorkloadKind
	Namespace string
	Name      string
}

func (ref WorkloadRef) String() string {
	return string(ref.Kind) + "/" + ref.Namespace + "/" + ref.Name
}

func ParseWorkloadRef(value string) (WorkloadRef, error) {
	parts := strings.Split(strings.TrimSpace(value), "/")
	if len(parts) != 3 {
		return WorkloadRef{}, fmt.Errorf("workload reference %q is not of the form <Kind>/<namespace>/<name>", value)
	}
	ref := WorkloadRef{Kind: WorkloadKind(parts[0]), Namespace: parts[1], Name: parts[2]}
	if ref.Kind != WorkloadDaemonSet && ref.Kind != WorkloadDeployment {
		return WorkloadRef{}, fmt.Errorf("workload reference %q names the kind %s, only %s and %s are supported", value, parts[0], WorkloadDaemonSet, WorkloadDeployment)
	}
	if ref.Namespace == "" || ref.Name == "" {
		return WorkloadRef{}, fmt.Errorf("workload reference %q has an empty namespace or name", value)
	}
	return ref, nil
}

func ParseWorkloadOverride(value string) (string, WorkloadRef, error) {
	component, raw, found := strings.Cut(value, "=")
	component = strings.TrimSpace(component)
	if !found || component == "" {
		return "", WorkloadRef{}, fmt.Errorf("workload override %q is not of the form component=<Kind>/<namespace>/<name>", value)
	}
	ref, err := ParseWorkloadRef(raw)
	if err != nil {
		return "", WorkloadRef{}, err
	}
	return component, ref, nil
}

type ComponentWorkload struct {
	Component string
	Default   WorkloadRef
	Required  bool
}

type WorkloadConfig struct {
	Overrides map[string]WorkloadRef
}

func (config WorkloadConfig) WithOverrides(overrides map[string]WorkloadRef) WorkloadConfig {
	merged := make(map[string]WorkloadRef, len(config.Overrides)+len(overrides))
	for component, ref := range config.Overrides {
		merged[component] = ref
	}
	for component, ref := range overrides {
		merged[component] = ref
	}
	config.Overrides = merged
	return config
}

func (config WorkloadConfig) ResolveAll(declared []ComponentWorkload) (map[string]WorkloadRef, error) {
	known := make(map[string]bool, len(declared))
	resolved := make(map[string]WorkloadRef, len(declared))
	for _, workload := range declared {
		known[workload.Component] = true
		resolved[workload.Component] = workload.Default
		if override, found := config.Overrides[workload.Component]; found {
			resolved[workload.Component] = override
		}
	}
	unknown := []string{}
	for component := range config.Overrides {
		if !known[component] {
			unknown = append(unknown, component)
		}
	}
	if len(unknown) > 0 {
		sort.Strings(unknown)
		return nil, fmt.Errorf("workloads are configured for %s, which the plan does not declare", strings.Join(unknown, ", "))
	}
	return resolved, nil
}

func LoadWorkloadConfig(ctx context.Context, client kubernetes.Interface, namespace, name string) (WorkloadConfig, error) {
	config := WorkloadConfig{Overrides: map[string]WorkloadRef{}}
	configMap, err := client.CoreV1().ConfigMaps(namespace).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return config, nil
	}
	if err != nil {
		return config, fmt.Errorf("cannot read configuration ConfigMap %s/%s: %w", namespace, name, err)
	}
	for key, value := range configMap.Data {
		if !strings.HasPrefix(key, WorkloadKeyPrefix) || strings.TrimSpace(value) == "" {
			continue
		}
		component := strings.TrimPrefix(key, WorkloadKeyPrefix)
		if component == "" {
			return config, fmt.Errorf("configuration ConfigMap %s/%s contains a workload without a component name", namespace, name)
		}
		ref, err := ParseWorkloadRef(value)
		if err != nil {
			return config, fmt.Errorf("configuration ConfigMap %s/%s is invalid: %w", namespace, name, err)
		}
		config.Overrides[component] = ref
	}
	return config, nil
}

func RecordWorkload(ref WorkloadRef, present bool) state.WorkloadRecord {
	return state.WorkloadRecord{Kind: string(ref.Kind), Namespace: ref.Namespace, Name: ref.Name, Present: present}
}

func RecordedWorkload(record state.WorkloadRecord) WorkloadRef {
	return WorkloadRef{Kind: WorkloadKind(record.Kind), Namespace: record.Namespace, Name: record.Name}
}
