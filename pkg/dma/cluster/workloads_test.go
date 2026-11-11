package cluster

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubefake "k8s.io/client-go/kubernetes/fake"
)

var declaredWorkloads = []ComponentWorkload{
	{Component: "netwatcher", Required: true, Default: WorkloadRef{Kind: WorkloadDaemonSet, Namespace: "kube-system", Name: "netwatcher"}},
	{Component: "svcwatcher", Default: WorkloadRef{Kind: WorkloadDaemonSet, Namespace: "kube-system", Name: "svcwatcher"}},
}

func TestParseWorkloadRef(t *testing.T) {
	testCases := []struct {
		value   string
		ref     WorkloadRef
		invalid string
	}{
		{value: "DaemonSet/danm/netwatcher", ref: WorkloadRef{Kind: WorkloadDaemonSet, Namespace: "danm", Name: "netwatcher"}},
		{value: " Deployment/kube-system/webhook ", ref: WorkloadRef{Kind: WorkloadDeployment, Namespace: "kube-system", Name: "webhook"}},
		{value: "StatefulSet/danm/netwatcher", invalid: "only DaemonSet and Deployment"},
		{value: "daemonset/danm/netwatcher", invalid: "only DaemonSet and Deployment"},
		{value: "DaemonSet/netwatcher", invalid: "not of the form"},
		{value: "DaemonSet/danm/net/watcher", invalid: "not of the form"},
		{value: "DaemonSet//netwatcher", invalid: "empty namespace or name"},
		{value: "none", invalid: "not of the form"},
	}
	for _, testCase := range testCases {
		ref, err := ParseWorkloadRef(testCase.value)
		if testCase.invalid != "" {
			if err == nil || !strings.Contains(err.Error(), testCase.invalid) {
				t.Errorf("parsing %q returned %v, expected an error mentioning %q", testCase.value, err, testCase.invalid)
			}
			continue
		}
		if err != nil || ref != testCase.ref {
			t.Errorf("parsing %q returned %v and %v", testCase.value, ref, err)
		}
	}
}

func TestParseWorkloadOverride(t *testing.T) {
	component, ref, err := ParseWorkloadOverride("cleaner=Deployment/danm/danm-cleaner")
	if err != nil || component != "cleaner" || ref.String() != "Deployment/danm/danm-cleaner" {
		t.Fatalf("the override was parsed to %s, %s, %v", component, ref, err)
	}
	if _, _, err := ParseWorkloadOverride("Deployment/danm/danm-cleaner"); err == nil {
		t.Fatal("an override without a component was accepted")
	}
}

func TestResolveAllPrefersOverrides(t *testing.T) {
	config := WorkloadConfig{Overrides: map[string]WorkloadRef{"netwatcher": {Kind: WorkloadDeployment, Namespace: "danm", Name: "nw"}}}
	resolved, err := config.ResolveAll(declaredWorkloads)
	if err != nil {
		t.Fatal(err)
	}
	if resolved["netwatcher"].String() != "Deployment/danm/nw" {
		t.Fatalf("the override was ignored, netwatcher resolved to %s", resolved["netwatcher"])
	}
	if resolved["svcwatcher"].String() != "DaemonSet/kube-system/svcwatcher" {
		t.Fatalf("the default was not used, svcwatcher resolved to %s", resolved["svcwatcher"])
	}
}

func TestResolveAllRejectsUndeclaredComponents(t *testing.T) {
	config := WorkloadConfig{Overrides: map[string]WorkloadRef{"netwatchr": {Kind: WorkloadDaemonSet, Namespace: "danm", Name: "nw"}}}
	if _, err := config.ResolveAll(declaredWorkloads); err == nil || !strings.Contains(err.Error(), "netwatchr") {
		t.Fatalf("an override of an undeclared component returned %v", err)
	}
}

func TestFlagOverridesBeatTheConfigMap(t *testing.T) {
	client := kubefake.NewSimpleClientset(&corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "dma-config", Namespace: "kube-system"},
		Data: map[string]string{
			"workload_netwatcher": "DaemonSet/danm/netwatcher",
			"workload_svcwatcher": "Deployment/danm/svcwatcher",
			"image_tag":           "4.4.0",
		},
	})
	config, err := LoadWorkloadConfig(context.Background(), client, "kube-system", "dma-config")
	if err != nil {
		t.Fatal(err)
	}
	config = config.WithOverrides(map[string]WorkloadRef{"svcwatcher": {Kind: WorkloadDaemonSet, Namespace: "other", Name: "svc"}})
	resolved, err := config.ResolveAll(declaredWorkloads)
	if err != nil {
		t.Fatal(err)
	}
	if resolved["netwatcher"].String() != "DaemonSet/danm/netwatcher" || resolved["svcwatcher"].String() != "DaemonSet/other/svc" {
		t.Fatalf("resolved %v", resolved)
	}
}

func TestLoadWorkloadConfigRejectsABadReference(t *testing.T) {
	client := kubefake.NewSimpleClientset(&corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "dma-config", Namespace: "kube-system"},
		Data:       map[string]string{"workload_netwatcher": "StatefulSet/danm/netwatcher"},
	})
	if _, err := LoadWorkloadConfig(context.Background(), client, "kube-system", "dma-config"); err == nil {
		t.Fatal("a StatefulSet reference was accepted")
	}
}

func TestLoadWorkloadConfigAcceptsAMissingConfigMap(t *testing.T) {
	config, err := LoadWorkloadConfig(context.Background(), kubefake.NewSimpleClientset(), "kube-system", "dma-config")
	if err != nil || len(config.Overrides) != 0 {
		t.Fatalf("a missing ConfigMap returned %v and %v", config, err)
	}
}
