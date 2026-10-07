package cluster

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubefake "k8s.io/client-go/kubernetes/fake"
)

func TestResolveBuildsTheReferenceFromThePrefixAndTheTag(t *testing.T) {
	config := ImageConfig{RegistryPrefix: "registry.example.com/danm/", Tag: "4.4.0"}
	resolved, err := config.Resolve("netwatcher")
	if err != nil {
		t.Fatalf("a component image cannot be resolved: %v", err)
	}
	if resolved.Image != "registry.example.com/danm/netwatcher:4.4.0" {
		t.Fatalf("the image was resolved to %s", resolved.Image)
	}
	if resolved.PullPolicy != corev1.PullIfNotPresent {
		t.Fatalf("the default pull policy is %s", resolved.PullPolicy)
	}
}

func TestResolvePrefersAFullOverride(t *testing.T) {
	config := ImageConfig{
		RegistryPrefix: "registry.example.com/danm/",
		Tag:            "4.4.0",
		Overrides:      map[string]ComponentImage{"webhook": {Image: "other.example.com/webhook@sha256:abc", PullPolicy: corev1.PullAlways}},
	}
	resolved, err := config.Resolve("webhook")
	if err != nil {
		t.Fatalf("an overridden image cannot be resolved: %v", err)
	}
	if resolved.Image != "other.example.com/webhook@sha256:abc" {
		t.Fatalf("the override was ignored, the image is %s", resolved.Image)
	}
	if resolved.PullPolicy != corev1.PullAlways {
		t.Fatalf("the pull policy of the override was ignored, it is %s", resolved.PullPolicy)
	}
}

func TestWithOverridesBeatsTheLoadedConfiguration(t *testing.T) {
	config := ImageConfig{Overrides: map[string]ComponentImage{"netwatcher": {Image: "from-configmap"}}}
	merged := config.WithOverrides(map[string]ComponentImage{"netwatcher": {Image: "from-command-line"}})
	resolved, err := merged.Resolve("netwatcher")
	if err != nil {
		t.Fatalf("a merged image cannot be resolved: %v", err)
	}
	if resolved.Image != "from-command-line" {
		t.Fatalf("the command line override lost, the image is %s", resolved.Image)
	}
	if config.Overrides["netwatcher"].Image != "from-configmap" {
		t.Fatalf("merging mutated the original configuration")
	}
}

func TestResolveAllFailsOnAnUnnamedComponent(t *testing.T) {
	if _, err := (ImageConfig{}).ResolveAll([]string{"netwatcher", ""}); err == nil {
		t.Fatalf("an unnamed component was resolved")
	}
}

func TestLoadImageConfigReadsEverySetting(t *testing.T) {
	client := kubefake.NewSimpleClientset(&corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "dma-config", Namespace: "kube-system"},
		Data: map[string]string{
			"image_registry_prefix":     "registry.example.com/danm/",
			"image_tag":                 "4.4.0",
			"image_pull_secret":         "registry-credentials",
			"image_pull_policy":         "Always",
			"image_override_svcwatcher": "other.example.com/svcwatcher:custom",
		},
	})
	config, err := LoadImageConfig(context.Background(), client, "kube-system", "dma-config")
	if err != nil {
		t.Fatalf("the configuration cannot be loaded: %v", err)
	}
	if config.RegistryPrefix != "registry.example.com/danm/" || config.Tag != "4.4.0" {
		t.Fatalf("the prefix or the tag was not loaded: %+v", config)
	}
	if config.PullSecret != "registry-credentials" || config.PullPolicy != corev1.PullAlways {
		t.Fatalf("the pull settings were not loaded: %+v", config)
	}
	if config.Overrides["svcwatcher"].Image != "other.example.com/svcwatcher:custom" {
		t.Fatalf("the override was not loaded: %+v", config.Overrides)
	}
}

func TestLoadImageConfigAcceptsAMissingConfigMap(t *testing.T) {
	config, err := LoadImageConfig(context.Background(), kubefake.NewSimpleClientset(), "kube-system", "dma-config")
	if err != nil {
		t.Fatalf("a missing ConfigMap was treated as an error: %v", err)
	}
	if config.Tag != DefaultTag {
		t.Fatalf("the default tag was not applied: %+v", config)
	}
}

func TestLoadImageConfigRejectsABadPullPolicy(t *testing.T) {
	client := kubefake.NewSimpleClientset(&corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "dma-config", Namespace: "kube-system"},
		Data:       map[string]string{"image_pull_policy": "Sometimes"},
	})
	if _, err := LoadImageConfig(context.Background(), client, "kube-system", "dma-config"); err == nil {
		t.Fatalf("an invalid pull policy was accepted")
	}
}

func TestParseOverrideRejectsMalformedInput(t *testing.T) {
	for _, malformed := range []string{"", "netwatcher", "=image", "netwatcher="} {
		if _, _, err := ParseOverride(malformed); err == nil {
			t.Fatalf("the malformed override %q was accepted", malformed)
		}
	}
	component, image, err := ParseOverride(" netwatcher = registry.example.com/netwatcher:1.0 ")
	if err != nil {
		t.Fatalf("a well formed override was rejected: %v", err)
	}
	if component != "netwatcher" || image.Image != "registry.example.com/netwatcher:1.0" {
		t.Fatalf("the override was parsed as %q and %q", component, image.Image)
	}
}

func TestParseModeRejectsAnythingElse(t *testing.T) {
	if _, err := ParseMode("semi-production"); err == nil {
		t.Fatalf("an unknown deployment mode was accepted")
	}
	for _, valid := range []string{"", "lightweight", "production"} {
		if _, err := ParseMode(valid); err != nil {
			t.Fatalf("the valid mode %q was rejected: %v", valid, err)
		}
	}
}
