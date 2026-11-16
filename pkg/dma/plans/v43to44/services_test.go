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
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	kubefake "k8s.io/client-go/kubernetes/fake"
)

func annotatedService(namespace, name string, annotations map[string]string) *corev1.Service {
	return &corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name, Annotations: annotations}}
}

func serviceHandle(t *testing.T, services ...k8sruntime.Object) *cluster.Handle {
	t.Helper()
	kube := kubefake.NewSimpleClientset(services...)
	return &cluster.Handle{
		Kube:      kube,
		State:     state.NewStore(kube, state.DefaultNamespace, state.DefaultName),
		Namespace: state.DefaultNamespace,
		Mode:      cluster.ModeProduction,
		Operator:  "tester",
		Log:       log.New(io.Discard, "", 0),
		Out:       &bytes.Buffer{},
	}
}

func detectServiceStep(t *testing.T, handle *cluster.Handle) engine.StepState {
	t.Helper()
	detected, err := dualAnnotateServices{}.Detect(context.Background(), handle)
	if err != nil {
		t.Fatalf("detecting step 8 failed: %v", err)
	}
	return detected
}

func serviceAnnotationsOf(t *testing.T, handle *cluster.Handle, namespace, name string) map[string]string {
	t.Helper()
	service, err := handle.Kube.CoreV1().Services(namespace).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return service.Annotations
}

func TestDualAnnotateServices(t *testing.T) {
	terminating := annotatedService("app", "leaving", map[string]string{OldGroup + "/network": "internal"})
	now := metav1.Now()
	terminating.DeletionTimestamp = &now
	handle := serviceHandle(t,
		annotatedService("app", "web", map[string]string{
			OldGroup + "/selector":       `{"app":"web"}`,
			OldGroup + "/tenantNetwork":  "internal",
			OldGroup + "/clusterNetwork": "external",
			"example.com/key":            "value",
		}),
		annotatedService("app", "half", map[string]string{
			OldGroup + "/selector": `{"app":"half"}`,
			OldGroup + "/network":  "internal",
			NewGroup + "/selector": `{"app":"by-hand"}`,
		}),
		annotatedService("app", "plain", map[string]string{"example.com/key": "value"}),
		terminating,
	)
	ctx := context.Background()
	if detectServiceStep(t, handle) != engine.StateNotStarted {
		t.Fatal("step 8 does not read as not started before it ran")
	}
	if err := (dualAnnotateServices{}).Execute(ctx, handle); err != nil {
		t.Fatal(err)
	}
	if detectServiceStep(t, handle) != engine.StateDone {
		t.Fatal("step 8 does not read as done after it ran")
	}
	web := serviceAnnotationsOf(t, handle, "app", "web")
	if web[NewGroup+"/selector"] != `{"app":"web"}` || web[NewGroup+"/tenantNetwork"] != "internal" || web[NewGroup+"/clusterNetwork"] != "external" {
		t.Fatalf("the copied annotations are %v", web)
	}
	if web[OldGroup+"/selector"] != `{"app":"web"}` || web["example.com/key"] != "value" {
		t.Fatalf("the original annotations are %v", web)
	}
	if web[ServiceCopiedMarkerKey] != "selector,tenantNetwork,clusterNetwork" {
		t.Fatalf("the marker of Service app/web is %q", web[ServiceCopiedMarkerKey])
	}
	half := serviceAnnotationsOf(t, handle, "app", "half")
	if half[NewGroup+"/selector"] != `{"app":"by-hand"}` {
		t.Fatalf("a hand-set annotation was overwritten: %v", half)
	}
	if half[NewGroup+"/network"] != "internal" || half[ServiceCopiedMarkerKey] != "network" {
		t.Fatalf("the annotations of Service app/half are %v", half)
	}
	if plain := serviceAnnotationsOf(t, handle, "app", "plain"); len(plain) != 1 {
		t.Fatalf("a Service without DANM annotations was patched: %v", plain)
	}
	if leaving := serviceAnnotationsOf(t, handle, "app", "leaving"); leaving[NewGroup+"/network"] != "" {
		t.Fatalf("a terminating Service was patched: %v", leaving)
	}
	if err := (dualAnnotateServices{}).Rollback(ctx, handle); err != nil {
		t.Fatal(err)
	}
	if detectServiceStep(t, handle) != engine.StateNotStarted {
		t.Fatal("step 8 does not read as not started after its rollback")
	}
	web = serviceAnnotationsOf(t, handle, "app", "web")
	for _, key := range serviceAnnotations {
		if _, found := web[NewGroup+"/"+key]; found {
			t.Fatalf("the rollback left %s behind: %v", NewGroup+"/"+key, web)
		}
	}
	if web[OldGroup+"/selector"] != `{"app":"web"}` || web["example.com/key"] != "value" || web[ServiceCopiedMarkerKey] != "" {
		t.Fatalf("the rollback did not restore Service app/web: %v", web)
	}
	half = serviceAnnotationsOf(t, handle, "app", "half")
	if half[NewGroup+"/selector"] != `{"app":"by-hand"}` {
		t.Fatalf("the rollback removed a hand-set annotation: %v", half)
	}
	if _, found := half[NewGroup+"/network"]; found {
		t.Fatalf("the rollback kept a copied annotation: %v", half)
	}
}

func TestDualAnnotateServicesIsPartialAfterAnInterruption(t *testing.T) {
	handle := serviceHandle(t,
		annotatedService("app", "done", map[string]string{
			OldGroup + "/network":  "internal",
			NewGroup + "/network":  "internal",
			ServiceCopiedMarkerKey: "network",
		}),
		annotatedService("app", "left", map[string]string{OldGroup + "/network": "internal"}),
	)
	if detectServiceStep(t, handle) != engine.StatePartial {
		t.Fatal("step 8 does not read as partial while annotated and unannotated Services coexist")
	}
	if err := (dualAnnotateServices{}).Execute(context.Background(), handle); err != nil {
		t.Fatal(err)
	}
	if detectServiceStep(t, handle) != engine.StateDone {
		t.Fatal("step 8 does not read as done after the remaining Service was annotated")
	}
}

func TestDualAnnotateServicesKeepsEarlierCopiedKeysInTheMarker(t *testing.T) {
	handle := serviceHandle(t, annotatedService("app", "web", map[string]string{
		OldGroup + "/selector": `{"app":"web"}`,
		OldGroup + "/network":  "internal",
		NewGroup + "/selector": `{"app":"web"}`,
		ServiceCopiedMarkerKey: "selector",
	}))
	if err := (dualAnnotateServices{}).Execute(context.Background(), handle); err != nil {
		t.Fatal(err)
	}
	if marker := serviceAnnotationsOf(t, handle, "app", "web")[ServiceCopiedMarkerKey]; marker != "selector,network" {
		t.Fatalf("the marker of a resumed Service is %q", marker)
	}
}
