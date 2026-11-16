package v43to44

import (
	"bytes"
	"context"
	"io"
	"log"
	"testing"

	"github.com/danm-cni/danm-utils/pkg/dma/cluster"
	"github.com/danm-cni/danm-utils/pkg/dma/engine"
	"github.com/danm-cni/danm-utils/pkg/dma/state"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	kubefake "k8s.io/client-go/kubernetes/fake"
)

func annotatedPod(namespace, name string, annotations map[string]string) *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name, Annotations: annotations}}
}

func terminatingPod(namespace, name string, annotations map[string]string) *corev1.Pod {
	pod := annotatedPod(namespace, name, annotations)
	now := metav1.Now()
	pod.DeletionTimestamp = &now
	return pod
}

func podHandle(t *testing.T, mode cluster.Mode, pods ...k8sruntime.Object) *cluster.Handle {
	t.Helper()
	kube := kubefake.NewSimpleClientset(pods...)
	listKinds := map[schema.GroupVersionResource]string{kindReservedIP.NewResource(): "ReservedIPList"}
	return &cluster.Handle{
		Kube:      kube,
		Dynamic:   dynamicfake.NewSimpleDynamicClientWithCustomListKinds(k8sruntime.NewScheme(), listKinds),
		State:     state.NewStore(kube, state.DefaultNamespace, state.DefaultName),
		Namespace: state.DefaultNamespace,
		Mode:      mode,
		Operator:  "tester",
		Log:       log.New(io.Discard, "", 0),
		Out:       &bytes.Buffer{},
	}
}

func createReservedIp(t *testing.T, handle *cluster.Handle, namespace, name, networkType, network string, addresses ...string) {
	t.Helper()
	ips := []any{}
	for _, address := range addresses {
		ips = append(ips, map[string]any{"address": address})
	}
	rip := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": NewGroup + "/" + Version,
		"kind":       kindReservedIP.Kind,
		"metadata": map[string]any{
			"namespace": namespace,
			"name":      name,
		},
		"spec": map[string]any{
			"network": map[string]any{"name": network, "type": networkType},
			"ips":     ips,
		},
	}}
	if _, err := handle.Dynamic.Resource(kindReservedIP.NewResource()).Namespace(namespace).Create(context.Background(), rip, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
}

func detectPods(t *testing.T, handle *cluster.Handle) engine.StepState {
	t.Helper()
	detected, err := annotateExistingPods{}.Detect(context.Background(), handle)
	if err != nil {
		t.Fatalf("detecting step 5 failed: %v", err)
	}
	return detected
}

func podAnnotations(t *testing.T, handle *cluster.Handle, namespace, name string) map[string]string {
	t.Helper()
	pod, err := handle.Kube.CoreV1().Pods(namespace).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return pod.Annotations
}

func TestAnnotateExistingPods(t *testing.T) {
	static := map[string]string{OldInterfacesKey: `[{"tenantNetwork":"internal","ip":"10.0.0.5/24"}]`, corev1.MirrorPodAnnotationKey: "node1"}
	handle := podHandle(t, cluster.ModeProduction,
		annotatedPod("app", "web-1", map[string]string{OldInterfacesKey: `[{"tenantNetwork":"internal","ip":"10.0.0.5/24"},{"network":"flannel"}]`}),
		annotatedPod("app", "db-0", map[string]string{OldInterfacesKey: `[{"tenantNetwork":"internal","ip":"dynamic"}]`}),
		annotatedPod("app", "fresh", map[string]string{OldInterfacesKey: `[{"tenantNetwork":"internal"}]`, NewInterfacesKey: `[{"tenantNetwork":"internal"}]`, CopiedMarkerKey: CopiedByShim}),
		annotatedPod("app", "manual", map[string]string{OldInterfacesKey: `[{"tenantNetwork":"internal"}]`, NewInterfacesKey: `[{"tenantNetwork":"other"}]`}),
		annotatedPod("app", "plain", map[string]string{"example.com/key": "value"}),
		terminatingPod("app", "leaving", map[string]string{OldInterfacesKey: `[{"tenantNetwork":"internal"}]`}),
		annotatedPod("kube-system", "static-node1", static),
	)
	createReservedIp(t, handle, "app", "db", "TenantNetwork", "internal", "10.0.0.5")
	ctx := context.Background()
	if detectPods(t, handle) != engine.StateNotStarted {
		t.Fatal("step 5 does not read as not started before it ran")
	}
	if err := (annotateExistingPods{}).Execute(ctx, handle); err != nil {
		t.Fatal(err)
	}
	if detectPods(t, handle) != engine.StateDone {
		t.Fatal("step 5 does not read as done after it ran")
	}
	web := podAnnotations(t, handle, "app", "web-1")
	if web[NewInterfacesKey] != `[{"tenantNetwork":"internal","ip":"db"},{"network":"flannel"}]` || web[CopiedMarkerKey] != CopiedByStep {
		t.Fatalf("Pod app/web-1 carries %v", web)
	}
	db := podAnnotations(t, handle, "app", "db-0")
	if db[NewInterfacesKey] != `[{"tenantNetwork":"internal","ip":"dynamic"}]` {
		t.Fatalf("the dynamic request of Pod app/db-0 was rewritten: %v", db)
	}
	if annotations := podAnnotations(t, handle, "app", "fresh"); annotations[CopiedMarkerKey] != CopiedByShim {
		t.Fatalf("a Pod the shim admitted was patched again: %v", annotations)
	}
	if annotations := podAnnotations(t, handle, "app", "manual"); annotations[NewInterfacesKey] != `[{"tenantNetwork":"other"}]` || annotations[CopiedMarkerKey] != "" {
		t.Fatalf("a hand-set %s was overwritten: %v", NewInterfacesKey, annotations)
	}
	for _, name := range []string{"plain", "leaving"} {
		if annotations := podAnnotations(t, handle, "app", name); annotations[NewInterfacesKey] != "" {
			t.Fatalf("Pod app/%s was patched: %v", name, annotations)
		}
	}
	if annotations := podAnnotations(t, handle, "kube-system", "static-node1"); annotations[NewInterfacesKey] != "" {
		t.Fatalf("a mirror Pod was patched: %v", annotations)
	}
	if err := (annotateExistingPods{}).Execute(ctx, handle); err != nil {
		t.Fatal(err)
	}
	if err := (annotateExistingPods{}).Rollback(ctx, handle); err != nil {
		t.Fatal(err)
	}
	if detectPods(t, handle) != engine.StateNotStarted {
		t.Fatal("step 5 does not read as not started after its rollback")
	}
	for _, name := range []string{"web-1", "db-0"} {
		annotations := podAnnotations(t, handle, "app", name)
		if annotations[NewInterfacesKey] != "" || annotations[CopiedMarkerKey] != "" {
			t.Fatalf("Pod app/%s was not reverted: %v", name, annotations)
		}
		if annotations[OldInterfacesKey] == "" {
			t.Fatalf("the rollback removed %s of Pod app/%s", OldInterfacesKey, name)
		}
	}
	if annotations := podAnnotations(t, handle, "app", "fresh"); annotations[NewInterfacesKey] == "" {
		t.Fatalf("the rollback reverted a Pod the shim admitted: %v", annotations)
	}
	if annotations := podAnnotations(t, handle, "app", "manual"); annotations[NewInterfacesKey] == "" {
		t.Fatalf("the rollback reverted a hand-set %s: %v", NewInterfacesKey, annotations)
	}
}

func TestAnnotateExistingPodsIsPartialAfterAnInterruption(t *testing.T) {
	handle := podHandle(t, cluster.ModeLightweight,
		annotatedPod("app", "done", map[string]string{OldInterfacesKey: `[{"network":"internal"}]`, NewInterfacesKey: `[{"network":"internal"}]`, CopiedMarkerKey: CopiedByStep}),
		annotatedPod("app", "left", map[string]string{OldInterfacesKey: `[{"network":"internal"}]`}),
	)
	if detectPods(t, handle) != engine.StatePartial {
		t.Fatal("step 5 does not read as partial while patched and unpatched Pods coexist")
	}
	if err := (annotateExistingPods{}).Execute(context.Background(), handle); err != nil {
		t.Fatal(err)
	}
	if detectPods(t, handle) != engine.StateDone {
		t.Fatal("step 5 does not read as done after the remaining Pod was patched")
	}
	if annotations := podAnnotations(t, handle, "app", "left"); annotations[NewInterfacesKey] != `[{"network":"internal"}]` {
		t.Fatalf("Pod app/left carries %v", annotations)
	}
}

func TestAnnotateExistingPodsRefusesAnUnknownMode(t *testing.T) {
	handle := podHandle(t, cluster.ModeUnknown, annotatedPod("app", "web-1", map[string]string{OldInterfacesKey: `[{"network":"internal"}]`}))
	if err := (annotateExistingPods{}).Execute(context.Background(), handle); err == nil {
		t.Fatal("step 5 ran without a known deployment mode")
	}
}
