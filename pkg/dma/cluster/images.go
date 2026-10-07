package cluster

import (
	"context"
	"fmt"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const (
	// DefaultConfigName is the ConfigMap the assistant reads its own configuration from.
	DefaultConfigName = "dma-config"
	// DefaultTag is used when no image tag is configured.
	DefaultTag = "latest"
	// OverrideKeyPrefix marks a ConfigMap key as a full image URL override for one component.
	OverrideKeyPrefix = "image_override_"
)

// ComponentImage is a fully resolved reference to the image of one DANM component.
type ComponentImage struct {
	Image      string
	PullPolicy corev1.PullPolicy
}

// ImageConfig describes how the image reference of every component the plan rolls is built.
// A full URL override always wins over the prefix and tag, which allows digest pinned references.
type ImageConfig struct {
	RegistryPrefix string
	Tag            string
	PullSecret     string
	PullPolicy     corev1.PullPolicy
	Overrides      map[string]ComponentImage
}

// Resolve returns the image reference to use for one component.
func (config ImageConfig) Resolve(component string) (ComponentImage, error) {
	if component == "" {
		return ComponentImage{}, fmt.Errorf("cannot resolve the image of an unnamed component")
	}
	policy := config.PullPolicy
	if policy == "" {
		policy = corev1.PullIfNotPresent
	}
	if override, found := config.Overrides[component]; found && override.Image != "" {
		if override.PullPolicy != "" {
			policy = override.PullPolicy
		}
		return ComponentImage{Image: override.Image, PullPolicy: policy}, nil
	}
	tag := config.Tag
	if tag == "" {
		tag = DefaultTag
	}
	return ComponentImage{Image: config.RegistryPrefix + component + ":" + tag, PullPolicy: policy}, nil
}

// ResolveAll resolves every component a plan declares, so that an unresolvable reference is
// discovered during preflight rather than in the middle of a rollout.
func (config ImageConfig) ResolveAll(components []string) (map[string]ComponentImage, error) {
	resolved := make(map[string]ComponentImage, len(components))
	for _, component := range components {
		image, err := config.Resolve(component)
		if err != nil {
			return nil, err
		}
		resolved[component] = image
	}
	return resolved, nil
}

// WithOverrides returns a copy of the configuration with the given component overrides applied
// on top of the ones already present. It is how command line overrides beat the ConfigMap.
func (config ImageConfig) WithOverrides(overrides map[string]ComponentImage) ImageConfig {
	merged := make(map[string]ComponentImage, len(config.Overrides)+len(overrides))
	for component, image := range config.Overrides {
		merged[component] = image
	}
	for component, image := range overrides {
		merged[component] = image
	}
	config.Overrides = merged
	return config
}

// Components returns the names of every component carrying an explicit override, ordered by name.
func (config ImageConfig) Components() []string {
	names := make([]string, 0, len(config.Overrides))
	for component := range config.Overrides {
		names = append(names, component)
	}
	sort.Strings(names)
	return names
}

// LoadImageConfig reads the image configuration of the assistant from its own ConfigMap. A
// missing ConfigMap is not an error: it simply means every setting keeps its default.
func LoadImageConfig(ctx context.Context, client kubernetes.Interface, namespace, name string) (ImageConfig, error) {
	config := ImageConfig{Tag: DefaultTag, PullPolicy: corev1.PullIfNotPresent, Overrides: map[string]ComponentImage{}}
	configMap, err := client.CoreV1().ConfigMaps(namespace).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return config, nil
	}
	if err != nil {
		return config, fmt.Errorf("cannot read configuration ConfigMap %s/%s: %w", namespace, name, err)
	}
	config.RegistryPrefix = configMap.Data["image_registry_prefix"]
	config.PullSecret = configMap.Data["image_pull_secret"]
	if tag := configMap.Data["image_tag"]; tag != "" {
		config.Tag = tag
	}
	if policy := configMap.Data["image_pull_policy"]; policy != "" {
		parsed, err := ParsePullPolicy(policy)
		if err != nil {
			return config, fmt.Errorf("configuration ConfigMap %s/%s is invalid: %w", namespace, name, err)
		}
		config.PullPolicy = parsed
	}
	for key, value := range configMap.Data {
		if !strings.HasPrefix(key, OverrideKeyPrefix) || value == "" {
			continue
		}
		component := strings.TrimPrefix(key, OverrideKeyPrefix)
		if component == "" {
			return config, fmt.Errorf("configuration ConfigMap %s/%s contains an override without a component name", namespace, name)
		}
		config.Overrides[component] = ComponentImage{Image: value}
	}
	return config, nil
}

// ParsePullPolicy validates a textual image pull policy.
func ParsePullPolicy(value string) (corev1.PullPolicy, error) {
	switch corev1.PullPolicy(value) {
	case corev1.PullAlways:
		return corev1.PullAlways, nil
	case corev1.PullNever:
		return corev1.PullNever, nil
	case corev1.PullIfNotPresent:
		return corev1.PullIfNotPresent, nil
	}
	return "", fmt.Errorf("image pull policy %q is not one of Always, Never, IfNotPresent", value)
}

// ParseOverride turns a command line override of the form component=image into its two parts.
func ParseOverride(value string) (string, ComponentImage, error) {
	component, image, found := strings.Cut(value, "=")
	if !found || strings.TrimSpace(component) == "" || strings.TrimSpace(image) == "" {
		return "", ComponentImage{}, fmt.Errorf("image override %q is not of the form component=image", value)
	}
	return strings.TrimSpace(component), ComponentImage{Image: strings.TrimSpace(image)}, nil
}
