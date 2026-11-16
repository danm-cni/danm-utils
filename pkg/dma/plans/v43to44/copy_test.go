package v43to44

import (
	"bytes"
	"context"
	"encoding/base64"
	"io"
	"log"
	"testing"

	"github.com/danm-cni/danm-utils/pkg/dma/cluster"
	"github.com/danm-cni/danm-utils/pkg/dma/engine"
	"github.com/danm-cni/danm-utils/pkg/dma/state"
	"github.com/danm-cni/danm/pkg/bitarray"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	kubefake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func copyHandle(t *testing.T, mode cluster.Mode) *cluster.Handle {
	t.Helper()
	listKinds := map[schema.GroupVersionResource]string{}
	for _, kind := range []apiKind{kindDanmNet, kindTenantNetwork, kindClusterNetwork, kindTenantConfig, kindDanmEp, kindReservedIP} {
		listKinds[kind.OldResource()] = kind.Kind + "List"
		listKinds[kind.NewResource()] = kind.Kind + "List"
	}
	kube := kubefake.NewSimpleClientset()
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

func createSource(t *testing.T, handle *cluster.Handle, kind apiKind, namespace, name string, metadata, content map[string]any) {
	t.Helper()
	object := map[string]any{"apiVersion": OldGroup + "/" + Version, "kind": kind.Kind}
	meta := map[string]any{"name": name, "uid": "11111111-2222-3333-4444-555555555555", "resourceVersion": "17"}
	if namespace != "" {
		meta["namespace"] = namespace
	}
	for key, value := range metadata {
		meta[key] = value
	}
	object["metadata"] = meta
	for key, value := range content {
		object[key] = value
	}
	item := &unstructured.Unstructured{Object: object}
	if _, err := objectClient(handle, kind, kind.OldResource(), namespace).Create(context.Background(), item, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
}

func readCopy(t *testing.T, handle *cluster.Handle, kind apiKind, namespace, name string) *unstructured.Unstructured {
	t.Helper()
	item, err := objectClient(handle, kind, kind.NewResource(), namespace).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return item
}

func detectCopyStep(t *testing.T, handle *cluster.Handle) engine.StepState {
	t.Helper()
	detected, err := copyNetworkObjects{}.Detect(context.Background(), handle)
	if err != nil {
		t.Fatalf("detecting step 6 failed: %v", err)
	}
	return detected
}

func vniAlloc(t *testing.T, length uint32, set ...uint32) string {
	t.Helper()
	bits, err := bitarray.NewBitArray(length)
	if err != nil {
		t.Fatal(err)
	}
	for _, bit := range set {
		bits.Set(bit)
	}
	return bits.Encode()
}

func setVnis(t *testing.T, alloc string) []uint32 {
	t.Helper()
	bits := bitarray.NewBitArrayFromBase64(alloc)
	set := []uint32{}
	for bit := uint32(0); bit < bits.Len(); bit++ {
		if bits.Get(bit) {
			set = append(set, bit)
		}
	}
	return set
}

func TestCopyNetworkObjectsInProductionMode(t *testing.T) {
	handle := copyHandle(t, cluster.ModeProduction)
	ctx := context.Background()
	createSource(t, handle, kindTenantNetwork, "app", "internal",
		map[string]any{
			"labels":      map[string]any{"owner": "app"},
			"annotations": map[string]any{MtuTagKey: "1500", "example.com/key": "value"},
			"ownerReferences": []any{
				map[string]any{"apiVersion": OldGroup + "/" + Version, "kind": "TenantConfig", "name": "tconf", "uid": "a"},
				map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "name": "keep", "uid": "b"},
			},
		},
		map[string]any{"spec": map[string]any{
			"NetworkID": "internal",
			"Options": map[string]any{
				"cidr":            "10.0.0.0/24",
				"alloc":           "gAAA",
				"allocation_pool": map[string]any{"start": "10.0.0.2", "end": "10.0.0.20", "lastIp": "10.0.0.5"},
			},
		}})
	createSource(t, handle, kindClusterNetwork, "", "external", nil,
		map[string]any{"spec": map[string]any{"NetworkID": "external", "NetworkType": "macvlan"}})
	createSource(t, handle, kindTenantConfig, "", "tconf", nil, map[string]any{
		"hostDevices": []any{
			map[string]any{"name": "ens3", "vniType": "vxlan", "vniRange": "1-4000", "alloc": vniAlloc(t, 5001, 1, 42, 5000)},
			map[string]any{"name": "ens4", "vniType": "vlan", "vniRange": "1-100", "alloc": vniAlloc(t, 5001, 7)},
			map[string]any{"name": "ens5"},
		},
		"networkIds": map[string]any{"internal": "int"},
	})
	if detectCopyStep(t, handle) != engine.StateNotStarted {
		t.Fatal("step 6 does not read as not started before it ran")
	}
	if err := (copyNetworkObjects{}).Execute(ctx, handle); err != nil {
		t.Fatal(err)
	}
	if detectCopyStep(t, handle) != engine.StateDone {
		t.Fatal("step 6 does not read as done after it ran")
	}
	network := readCopy(t, handle, kindTenantNetwork, "app", "internal")
	if network.GetAPIVersion() != NewGroup+"/"+Version {
		t.Fatalf("the copy is of apiVersion %s", network.GetAPIVersion())
	}
	if network.GetUID() != "" || network.GetLabels()["owner"] != "app" {
		t.Fatalf("the copy carries %+v", network.Object["metadata"])
	}
	if _, tagged := network.GetAnnotations()[MtuTagKey]; tagged || network.GetAnnotations()["example.com/key"] != "value" {
		t.Fatalf("the annotations of the copy are %v", network.GetAnnotations())
	}
	owners := network.GetOwnerReferences()
	if len(owners) != 1 || owners[0].Kind != "ConfigMap" {
		t.Fatalf("the owner references of the copy are %+v", owners)
	}
	options, _, _ := unstructured.NestedMap(network.Object, "spec", "Options")
	if options["mtu"] != int64(1500) || options["alloc"] != "gAAA" {
		t.Fatalf("the Options of the copy are %v", options)
	}
	pool, found, _ := unstructured.NestedMap(network.Object, "spec", "Options", "allocation_pool")
	if !found || pool["lastIp"] != "10.0.0.5" {
		t.Fatalf("the allocation pool of the copy is %v", pool)
	}
	if _, found, _ := unstructured.NestedMap(readCopy(t, handle, kindClusterNetwork, "", "external").Object, "spec", "Options", "allocation_pool"); !found {
		t.Fatal("the copy of a network without Options carries no explicit allocation pool")
	}
	tenantConfig := readCopy(t, handle, kindTenantConfig, "", "tconf")
	devices, _, _ := unstructured.NestedSlice(tenantConfig.Object, "hostDevices")
	if len(devices) != 3 {
		t.Fatalf("the copy carries %d host devices", len(devices))
	}
	vxlan := devices[0].(map[string]any)["alloc"].(string)
	decoded, err := base64.StdEncoding.DecodeString(vxlan)
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded) != (MaxAllowedVniVxlan+1+7)/8 {
		t.Fatalf("the VxLAN bitmask of the copy is %d bytes", len(decoded))
	}
	if set := setVnis(t, vxlan); len(set) != 4 || set[0] != 0 || set[1] != 1 || set[2] != 42 || set[3] != 5000 {
		t.Fatalf("the resized VxLAN bitmask holds %v", set)
	}
	if devices[1].(map[string]any)["alloc"] != vniAlloc(t, 5001, 7) {
		t.Fatal("the VLAN bitmask of the copy was resized")
	}
	if _, set := devices[2].(map[string]any)["alloc"]; set {
		t.Fatal("a host device without an allocation got one")
	}
	writes := 0
	handle.Dynamic.(*dynamicfake.FakeDynamicClient).PrependReactor("*", "*", func(action k8stesting.Action) (bool, k8sruntime.Object, error) {
		if action.GetVerb() == "update" || action.GetVerb() == "create" {
			writes++
		}
		return false, nil, nil
	})
	if err := (copyNetworkObjects{}).Execute(ctx, handle); err != nil {
		t.Fatal(err)
	}
	if writes != 0 {
		t.Fatalf("a second run rewrote %d copies instead of skipping them", writes)
	}
	if err := (copyNetworkObjects{}).Rollback(ctx, handle); err != nil {
		t.Fatal(err)
	}
	if detectCopyStep(t, handle) != engine.StateNotStarted {
		t.Fatal("step 6 does not read as not started after its rollback")
	}
}

func TestCopyNetworkObjectsCorrectsADriftedCopy(t *testing.T) {
	handle := copyHandle(t, cluster.ModeLightweight)
	ctx := context.Background()
	createSource(t, handle, kindDanmNet, "app", "internal", nil,
		map[string]any{"spec": map[string]any{"NetworkID": "internal", "Options": map[string]any{"alloc": "gAAA"}}})
	if err := (copyNetworkObjects{}).Execute(ctx, handle); err != nil {
		t.Fatal(err)
	}
	drifted := readCopy(t, handle, kindDanmNet, "app", "internal")
	unstructured.SetNestedField(drifted.Object, "elsewhere", "spec", "NetworkID")
	if _, err := objectClient(handle, kindDanmNet, kindDanmNet.NewResource(), "app").Update(ctx, drifted, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := (copyNetworkObjects{}).Execute(ctx, handle); err != nil {
		t.Fatal(err)
	}
	corrected := readCopy(t, handle, kindDanmNet, "app", "internal")
	if id, _, _ := unstructured.NestedString(corrected.Object, "spec", "NetworkID"); id != "internal" {
		t.Fatalf("the drifted copy was not corrected, its NetworkID is %s", id)
	}
}

func TestCopyNetworkObjectsPrunesOnlyWithAnAcceptance(t *testing.T) {
	handle := copyHandle(t, cluster.ModeLightweight)
	ctx := context.Background()
	createSource(t, handle, kindDanmNet, "app", "internal", nil,
		map[string]any{"spec": map[string]any{"NetworkID": "internal", "LegacyField": "gone in 4.4"}})
	err := (copyNetworkObjects{}).Execute(ctx, handle)
	if err == nil {
		t.Fatal("an object losing fields was copied without an acceptance")
	}
	if _, getErr := objectClient(handle, kindDanmNet, kindDanmNet.NewResource(), "app").Get(ctx, "internal", metav1.GetOptions{}); getErr == nil {
		t.Fatal("the copy of a rejected object was written")
	}
	if _, mutateErr := handle.State.Mutate(ctx, PlanID, func(record *state.PlanRecord) error {
		record.PrunedFieldsAccepted = true
		return nil
	}); mutateErr != nil {
		t.Fatal(mutateErr)
	}
	if err := (copyNetworkObjects{}).Execute(ctx, handle); err != nil {
		t.Fatal(err)
	}
	copied := readCopy(t, handle, kindDanmNet, "app", "internal")
	if _, found, _ := unstructured.NestedString(copied.Object, "spec", "LegacyField"); found {
		t.Fatal("the pruned field survived in the copy")
	}
	if detectCopyStep(t, handle) != engine.StateDone {
		t.Fatal("step 6 does not read as done after the accepted copy was written")
	}
}

func TestCopyNetworkObjectsRefusesAnUnknownMode(t *testing.T) {
	handle := copyHandle(t, cluster.ModeUnknown)
	if err := (copyNetworkObjects{}).Execute(context.Background(), handle); err == nil {
		t.Fatal("step 6 ran without a known deployment mode")
	}
}

func TestPlanAdvisesWhileTheOldGroupIsFrozen(t *testing.T) {
	rows := []engine.StepRow{
		{Meta: engine.StepMeta{ID: copyNetworksStep}, Detected: engine.StateDone},
		{Meta: engine.StepMeta{ID: removeOldApiStep}, Detected: engine.StateNotStarted},
	}
	advisor, isAdvisor := New().(engine.Advisor)
	if !isAdvisor {
		t.Fatal("the plan raises no advisories")
	}
	if len(advisor.Advisories(rows)) != 1 {
		t.Fatal("the plan does not warn while the old group is no longer carried over")
	}
	rows[1].Detected = engine.StateDone
	if len(advisor.Advisories(rows)) != 0 {
		t.Fatal("the plan still warns after the old group is removed")
	}
}
