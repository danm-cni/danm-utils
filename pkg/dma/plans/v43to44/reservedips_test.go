package v43to44

import (
	"bytes"
	"context"
	"io"
	"log"
	"strings"
	"testing"

	"github.com/danm-cni/danm-utils/pkg/dma/cluster"
	"github.com/danm-cni/danm-utils/pkg/dma/engine"
	"github.com/danm-cni/danm-utils/pkg/dma/state"
	"github.com/danm-cni/danm/pkg/datastructs"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	kubefake "k8s.io/client-go/kubernetes/fake"
)

func ripHandle(t *testing.T, mode cluster.Mode, objects ...k8sruntime.Object) *cluster.Handle {
	t.Helper()
	listKinds := map[schema.GroupVersionResource]string{}
	for _, kind := range []apiKind{kindDanmNet, kindTenantNetwork, kindClusterNetwork, kindTenantConfig, kindDanmEp, kindReservedIP} {
		listKinds[kind.OldResource()] = kind.Kind + "List"
		listKinds[kind.NewResource()] = kind.Kind + "List"
	}
	kube := kubefake.NewSimpleClientset(objects...)
	handle := &cluster.Handle{
		Kube:      kube,
		Dynamic:   dynamicfake.NewSimpleDynamicClientWithCustomListKinds(k8sruntime.NewScheme(), listKinds),
		State:     state.NewStore(kube, state.DefaultNamespace, state.DefaultName),
		Namespace: state.DefaultNamespace,
		Mode:      mode,
		Operator:  "tester",
		Log:       log.New(io.Discard, "", 0),
		Out:       &bytes.Buffer{},
	}
	if _, err := handle.State.Mutate(context.Background(), PlanID, func(record *state.PlanRecord) error {
		record.UpsertStep(state.StepRecord{ID: 9, Name: "replicate-reserved-ips", Status: state.StatusRunning})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	handle.BeginStep(PlanID, 9)
	return handle
}

func createTaggedEp(t *testing.T, handle *cluster.Handle, namespace, name, pod, address string, tags map[string]any) {
	t.Helper()
	createSource(t, handle, kindDanmEp, namespace, name, map[string]any{"annotations": tags},
		map[string]any{"spec": map[string]any{
			"NetworkName": "internal",
			"EndpointID":  name,
			"Pod":         pod,
			"apiType":     kindTenantNetwork.Kind,
			"Interface":   map[string]any{"Name": "eth0", "Address": address},
		}})
}

func detectRipStep(t *testing.T, handle *cluster.Handle) engine.StepState {
	t.Helper()
	detected, err := replicateReservedIps{}.Detect(context.Background(), handle)
	if err != nil {
		t.Fatalf("detecting step 9 failed: %v", err)
	}
	return detected
}

func readRip(t *testing.T, handle *cluster.Handle, namespace, name string) *unstructured.Unstructured {
	t.Helper()
	item, err := objectClient(handle, kindReservedIP, kindReservedIP.NewResource(), namespace).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return item
}

func ripFixture(t *testing.T) *cluster.Handle {
	t.Helper()
	handle := ripHandle(t, cluster.ModeProduction,
		annotatedPod("app", "db-0", map[string]string{OldInterfacesKey: `[{"tenantNetwork":"internal","ip":"10.0.0.5/24"}]`}),
		annotatedPod("app", "db-1", map[string]string{OldInterfacesKey: `[{"tenantNetwork":"internal","ip":"10.0.0.5/24"}]`}),
		annotatedPod("app", "web-1", map[string]string{OldInterfacesKey: `[{"tenantNetwork":"internal","ip":"dynamic"}]`}),
	)
	ctx := context.Background()
	createSource(t, handle, kindTenantNetwork, "app", "internal", nil,
		map[string]any{"spec": map[string]any{"NetworkID": "internal", "Options": map[string]any{"cidr": "10.0.0.0/24", "alloc": "gAAA"}}})
	createTaggedEp(t, handle, "app", "ep1", "db-0", "10.0.0.5/24", map[string]any{SharedRipTagV4: "db"})
	createTaggedEp(t, handle, "app", "ep2", "db-1", "10.0.0.5/24", map[string]any{SharedRipTagV4: "db"})
	createTaggedEp(t, handle, "app", "ep3", "web-1", "10.0.0.9/24", map[string]any{RipTagV4: "lb"})
	for _, step := range []engine.Step{copyNetworkObjects{}, copyDanmEps{}, annotateExistingPods{}} {
		if err := step.Execute(ctx, handle); err != nil {
			t.Fatalf("preparing with step %d failed: %v", step.Meta().ID, err)
		}
	}
	return handle
}

func TestReplicateReservedIps(t *testing.T) {
	handle := ripFixture(t)
	ctx := context.Background()
	if detectRipStep(t, handle) != engine.StateNotStarted {
		t.Fatal("step 9 does not read as not started before it ran")
	}
	if err := (replicateReservedIps{}).Execute(ctx, handle); err != nil {
		t.Fatal(err)
	}
	if detectRipStep(t, handle) != engine.StateDone {
		t.Fatal("step 9 does not read as done after it ran")
	}
	shared := readRip(t, handle, "app", "db")
	if shared.GetLabels()[datastructs.NetworkNameLabel] != "internal" || shared.GetLabels()[datastructs.NetworkTypeLabel] != kindTenantNetwork.Kind {
		t.Fatalf("the labels of ReservedIP app/db are %v", shared.GetLabels())
	}
	ips, _, _ := unstructured.NestedSlice(shared.Object, "spec", "ips")
	if len(ips) != 1 || ips[0].(map[string]any)["address"] != "10.0.0.5" || ips[0].(map[string]any)["shareable"] != true {
		t.Fatalf("the addresses of ReservedIP app/db are %v", ips)
	}
	if description, _, _ := unstructured.NestedString(shared.Object, "spec", "description"); !strings.Contains(description, "app/ep1") {
		t.Fatalf("the description of ReservedIP app/db is %q", description)
	}
	plain := readRip(t, handle, "app", "lb")
	plainIps, _, _ := unstructured.NestedSlice(plain.Object, "spec", "ips")
	if shareable, found := plainIps[0].(map[string]any)["shareable"]; found && shareable == true {
		t.Fatalf("ReservedIP app/lb is shareable: %v", plainIps)
	}
	network := readCopy(t, handle, kindTenantNetwork, "app", "internal")
	entries, _, _ := unstructured.NestedSlice(network.Object, "status", "reservedIPs")
	if len(entries) != 1 {
		t.Fatalf("the status of the network copy is %v", entries)
	}
	entry := entries[0].(map[string]any)
	objects, _ := entry["objects"].([]any)
	if entry["namespace"] != "app" || len(objects) != 2 {
		t.Fatalf("the status entry of the network copy is %v", entry)
	}
	for _, name := range []string{"ep1", "ep2"} {
		copied := readCopy(t, handle, kindDanmEp, "app", name)
		if reference, _, _ := unstructured.NestedString(copied.Object, "spec", ripRefsField, "v4"); reference != "db" {
			t.Fatalf("the copy of DanmEp app/%s references %q", name, reference)
		}
		if copied.GetLabels()[datastructs.V4RipLabel] != "db" {
			t.Fatalf("the copy of DanmEp app/%s carries labels %v", name, copied.GetLabels())
		}
	}
	notShared := readCopy(t, handle, kindDanmEp, "app", "ep3")
	if _, found, _ := unstructured.NestedMap(notShared.Object, "spec", ripRefsField); found {
		t.Fatal("a DanmEp copy under a non-shared tag references a ReservedIP")
	}
	if value := podAnnotations(t, handle, "app", "db-0")[NewInterfacesKey]; value != `[{"tenantNetwork":"internal","ip":"db"}]` {
		t.Fatalf("Pod app/db-0 asks for %s", value)
	}
	if value := podAnnotations(t, handle, "app", "web-1")[NewInterfacesKey]; value != `[{"tenantNetwork":"internal","ip":"dynamic"}]` {
		t.Fatalf("the dynamic request of the source Pod of a ReservedIP is %s", value)
	}
	warnings := strings.Join(handle.TakeWarnings(), "\n")
	if !strings.Contains(warnings, "app/ep3") || !strings.Contains(warnings, "dynamically") {
		t.Fatalf("the step did not warn about the dynamically allocated tag: %q", warnings)
	}
	if strings.Contains(warnings, "app/ep1") {
		t.Fatalf("the step warned about a tag whose Pod asks for its address literally: %q", warnings)
	}
	if err := (replicateReservedIps{}).Execute(ctx, handle); err != nil {
		t.Fatalf("a second run failed: %v", err)
	}
	if err := (replicateReservedIps{}).Rollback(ctx, handle); err != nil {
		t.Fatal(err)
	}
	if detectRipStep(t, handle) != engine.StateNotStarted {
		t.Fatal("step 9 does not read as not started after its rollback")
	}
	if _, err := objectClient(handle, kindReservedIP, kindReservedIP.NewResource(), "app").Get(ctx, "db", metav1.GetOptions{}); err == nil {
		t.Fatal("the rollback kept a ReservedIP")
	}
	network = readCopy(t, handle, kindTenantNetwork, "app", "internal")
	if entries, found, _ := unstructured.NestedSlice(network.Object, "status", "reservedIPs"); found && len(entries) != 0 {
		t.Fatalf("the rollback left the network status at %v", entries)
	}
	reverted := readCopy(t, handle, kindDanmEp, "app", "ep1")
	if _, found, _ := unstructured.NestedMap(reverted.Object, "spec", ripRefsField); found {
		t.Fatal("the rollback kept a ReservedIP reference")
	}
	if _, found := reverted.GetLabels()[datastructs.V4RipLabel]; found {
		t.Fatalf("the rollback kept the ReservedIP label: %v", reverted.GetLabels())
	}
	if value := podAnnotations(t, handle, "app", "db-0")[NewInterfacesKey]; value != `[{"tenantNetwork":"internal","ip":"10.0.0.5/24"}]` {
		t.Fatalf("the rollback left Pod app/db-0 asking for %s", value)
	}
}

func TestReplicateReservedIpsFailsOnABadTag(t *testing.T) {
	handle := ripHandle(t, cluster.ModeProduction)
	ctx := context.Background()
	createSource(t, handle, kindTenantNetwork, "app", "internal", nil,
		map[string]any{"spec": map[string]any{"NetworkID": "internal", "Options": map[string]any{"cidr": "10.0.0.0/24", "alloc": "gAAA"}}})
	createTaggedEp(t, handle, "app", "ep1", "db-0", "192.168.1.5/24", map[string]any{RipTagV4: "db"})
	for _, step := range []engine.Step{copyNetworkObjects{}, copyDanmEps{}} {
		if err := step.Execute(ctx, handle); err != nil {
			t.Fatal(err)
		}
	}
	err := (replicateReservedIps{}).Execute(ctx, handle)
	if err == nil || !strings.Contains(err.Error(), "outside of the CIDR") {
		t.Fatalf("a tag naming an address outside of its network was accepted: %v", err)
	}
	if _, err := objectClient(handle, kindReservedIP, kindReservedIP.NewResource(), "app").Get(ctx, "db", metav1.GetOptions{}); err == nil {
		t.Fatal("a ReservedIP was created although the tags are invalid")
	}
}

func TestReplicateReservedIpsSkipsLightweightMode(t *testing.T) {
	handle := ripHandle(t, cluster.ModeLightweight)
	if detectRipStep(t, handle) != engine.StateDone {
		t.Fatal("step 9 does not read as done in lightweight mode")
	}
	if err := (replicateReservedIps{}).Execute(context.Background(), handle); err != nil {
		t.Fatal(err)
	}
}
