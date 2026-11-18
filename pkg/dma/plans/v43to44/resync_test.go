package v43to44

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/danm-cni/danm-utils/pkg/dma/cluster"
	"github.com/danm-cni/danm-utils/pkg/dma/engine"
	"github.com/danm-cni/danm-utils/pkg/dma/state"
	"github.com/danm-cni/danm/pkg/datastructs"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apiextfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	kubefake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

var crdInstalled = metav1.NewTime(time.Date(2026, 11, 1, 10, 0, 0, 0, time.UTC))

func resyncHandle(t *testing.T, mode cluster.Mode, objects ...k8sruntime.Object) (*cluster.Handle, *bytes.Buffer) {
	t.Helper()
	listKinds := map[schema.GroupVersionResource]string{}
	for _, kind := range []apiKind{kindDanmNet, kindTenantNetwork, kindClusterNetwork, kindTenantConfig, kindDanmEp, kindReservedIP} {
		listKinds[kind.OldResource()] = kind.Kind + "List"
		listKinds[kind.NewResource()] = kind.Kind + "List"
	}
	objects = append(objects, resyncShim(), resyncNetwatcher(), resyncNetwatcherPod(crdInstalled.Add(time.Minute)))
	kube := kubefake.NewSimpleClientset(objects...)
	crds := []k8sruntime.Object{}
	for _, kind := range targetKinds(mode) {
		crd := newCrd(kind)
		crd.CreationTimestamp = crdInstalled
		crds = append(crds, crd)
	}
	out := &bytes.Buffer{}
	handle := &cluster.Handle{
		Kube:      kube,
		ApiExt:    apiextfake.NewSimpleClientset(crds...),
		Dynamic:   dynamicfake.NewSimpleDynamicClientWithCustomListKinds(k8sruntime.NewScheme(), listKinds),
		State:     state.NewStore(kube, state.DefaultNamespace, state.DefaultName),
		Namespace: state.DefaultNamespace,
		Mode:      mode,
		Operator:  "tester",
		Log:       log.New(io.Discard, "", 0),
		Out:       out,
	}
	ref := cluster.WorkloadRef{Kind: cluster.WorkloadDaemonSet, Namespace: DanmNamespace, Name: "netwatcher"}
	if _, err := handle.State.Mutate(context.Background(), PlanID, func(record *state.PlanRecord) error {
		record.Mode = string(mode)
		record.Workloads = map[string]state.WorkloadRecord{ComponentNetwatcher: cluster.RecordWorkload(ref, true, false)}
		record.UpsertStep(state.StepRecord{ID: 9, Name: "replicate-reserved-ips", Status: state.StatusDone})
		record.UpsertStep(state.StepRecord{ID: 13, Name: "resync-and-verify", Status: state.StatusRunning})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	handle.BeginStep(PlanID, 13)
	return handle, out
}

func resyncShim() *appsv1.Deployment {
	replicas := int32(1)
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: state.DefaultNamespace, Name: ShimName},
		Spec:       appsv1.DeploymentSpec{Replicas: &replicas},
		Status:     appsv1.DeploymentStatus{UpdatedReplicas: 1, AvailableReplicas: 1},
	}
}

func resyncNetwatcher() *appsv1.DaemonSet {
	daemonSet := componentDaemonSet("netwatcher", "netwatcher", "registry.example.com/netwatcher:4.4", nil)
	daemonSet.Spec.Selector = &metav1.LabelSelector{MatchLabels: map[string]string{"danm.k8s.io": "netwatcher"}}
	return daemonSet
}

func resyncNetwatcherPod(start time.Time) *corev1.Pod {
	started := metav1.NewTime(start)
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: DanmNamespace, Name: "netwatcher-abcde", Labels: map[string]string{"danm.k8s.io": "netwatcher"}},
		Spec:       corev1.PodSpec{NodeName: "node1"},
		Status:     corev1.PodStatus{StartTime: &started},
	}
}

func recordStep9Rips(t *testing.T, handle *cluster.Handle, rips ...recordedRip) {
	t.Helper()
	encoded, err := json.Marshal(rips)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := handle.State.Mutate(context.Background(), PlanID, func(record *state.PlanRecord) error {
		step := record.Step(9)
		step.Snapshots = map[string]string{reservedIpsRecordKey: string(encoded)}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func createCopy(t *testing.T, handle *cluster.Handle, kind apiKind, namespace, name string, metadata, content map[string]any) {
	t.Helper()
	object := map[string]any{"apiVersion": NewGroup + "/" + Version, "kind": kind.Kind}
	meta := map[string]any{"name": name, "resourceVersion": "42"}
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
	if _, err := objectClient(handle, kind, kind.NewResource(), namespace).Create(context.Background(), item, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
}

func networkContent(alloc, lastIp string, mtu int) map[string]any {
	options := map[string]any{"cidr": "10.0.0.0/24", "allocation_pool": map[string]any{"start": "10.0.0.1", "end": "10.0.0.63", "lastIp": lastIp}}
	if alloc != "" {
		options["alloc"] = alloc
	}
	if mtu > 0 {
		options["mtu"] = int64(mtu)
	}
	return map[string]any{"spec": map[string]any{"NetworkID": "internal", "Options": options}}
}

func endpointContent(address, pod string) map[string]any {
	return map[string]any{"spec": map[string]any{
		"NetworkName": "internal",
		"NetworkType": "ipvlan",
		"EndpointID":  "ep",
		"Pod":         pod,
		"apiType":     kindTenantNetwork.Kind,
		"Interface":   map[string]any{"Name": "eth0", "Address": address},
	}}
}

func detectResyncStep(t *testing.T, handle *cluster.Handle) engine.StepState {
	t.Helper()
	detected, err := resyncAndVerify{}.Detect(context.Background(), handle)
	if err != nil {
		t.Fatalf("detecting step 13 failed: %v", err)
	}
	return detected
}

func TestResyncCarriesAllocationStateOver(t *testing.T) {
	handle, out := resyncHandle(t, cluster.ModeProduction)
	ctx := context.Background()
	createSource(t, handle, kindTenantNetwork, "app", "internal", nil, networkContent("AQAB", "10.0.0.9", 1500))
	createCopy(t, handle, kindTenantNetwork, "app", "internal", nil, networkContent("AQAA", "10.0.0.5", 1500))
	if detectResyncStep(t, handle) != engine.StateNotStarted {
		t.Fatal("step 13 does not read as not started before it ran")
	}
	if err := (resyncAndVerify{}).Execute(ctx, handle); err != nil {
		t.Fatal(err)
	}
	copied := readCopy(t, handle, kindTenantNetwork, "app", "internal")
	options, _, err := unstructured.NestedMap(copied.Object, "spec", "Options")
	if err != nil {
		t.Fatal(err)
	}
	if options["alloc"] != "AQAB" {
		t.Fatalf("the copy holds the allocation %v", options["alloc"])
	}
	pool, _, _ := unstructured.NestedMap(copied.Object, "spec", "Options", "allocation_pool")
	if pool["lastIp"] != "10.0.0.9" {
		t.Fatalf("the copy holds the last IP %v", pool["lastIp"])
	}
	if pool["start"] != "10.0.0.1" || pool["end"] != "10.0.0.63" {
		t.Fatalf("the pool of the copy was rewritten: %v", pool)
	}
	if detectResyncStep(t, handle) != engine.StateDone {
		t.Fatal("step 13 does not read as done after it ran")
	}
	if strings.Contains(out.String(), "WARNING") {
		t.Fatalf("a clean resync warned: %s", out.String())
	}
}

func TestResyncReportsNonIpamDrift(t *testing.T) {
	handle, out := resyncHandle(t, cluster.ModeProduction)
	ctx := context.Background()
	createSource(t, handle, kindTenantNetwork, "app", "internal", nil, networkContent("AQAB", "10.0.0.9", 9000))
	createCopy(t, handle, kindTenantNetwork, "app", "internal", nil, networkContent("AQAA", "10.0.0.5", 1500))
	createSource(t, handle, kindTenantNetwork, "app", "fresh", nil, networkContent("AQAA", "10.0.0.1", 1500))
	createCopy(t, handle, kindTenantNetwork, "app", "removed", nil, networkContent("AQAA", "10.0.0.1", 1500))
	if err := (resyncAndVerify{}).Execute(ctx, handle); err != nil {
		t.Fatal(err)
	}
	report := out.String()
	for _, wanted := range []string{"drifted outside its IPAM state", "app/fresh was created", "app/removed was deleted"} {
		if !strings.Contains(report, wanted) {
			t.Fatalf("the report misses %q: %s", wanted, report)
		}
	}
	copied := readCopy(t, handle, kindTenantNetwork, "app", "internal")
	mtu, _, _ := unstructured.NestedInt64(copied.Object, "spec", "Options", "mtu")
	if mtu != 1500 {
		t.Fatalf("the MTU of the copy was changed to %d", mtu)
	}
	if _, err := objectClient(handle, kindTenantNetwork, kindTenantNetwork.NewResource(), "app").Get(ctx, "removed", metav1.GetOptions{}); err != nil {
		t.Fatalf("the copy of a deleted network was removed: %v", err)
	}
}

func TestResyncCarriesVniAllocationOver(t *testing.T) {
	handle, _ := resyncHandle(t, cluster.ModeProduction)
	ctx := context.Background()
	devices := func(alloc string) map[string]any {
		return map[string]any{"hostDevices": []any{
			map[string]any{"name": "ens3", "vniType": "vxlan", "vniRange": "1-4000", "alloc": alloc},
			map[string]any{"name": "ens4", "vniType": "vlan", "vniRange": "1-100"},
		}}
	}
	createSource(t, handle, kindTenantConfig, "", "tconf", nil, devices(vniAlloc(t, 4096, 10, 20)))
	createCopy(t, handle, kindTenantConfig, "", "tconf", nil, devices(vniAlloc(t, MaxAllowedVniVxlan+1, 10)))
	if err := (resyncAndVerify{}).Execute(ctx, handle); err != nil {
		t.Fatal(err)
	}
	copied := readCopy(t, handle, kindTenantConfig, "", "tconf")
	list, _, err := unstructured.NestedSlice(copied.Object, "hostDevices")
	if err != nil {
		t.Fatal(err)
	}
	alloc, _ := list[0].(map[string]any)["alloc"].(string)
	if vnis := setVnis(t, alloc); len(vnis) != 3 || vnis[1] != 10 || vnis[2] != 20 {
		t.Fatalf("the carried over VNI allocation holds %v", vnis)
	}
	if decoded, err := base64.StdEncoding.DecodeString(alloc); err != nil || len(decoded) != (MaxAllowedVniVxlan+1+7)/8 {
		t.Fatalf("the carried over VxLAN bitmask is %d bytes: %v", len(decoded), err)
	}
}

func TestResyncSynchronisesDanmEps(t *testing.T) {
	handle, _ := resyncHandle(t, cluster.ModeProduction)
	ctx := context.Background()
	createSource(t, handle, kindDanmEp, "app", "fresh-ep", nil, endpointContent("10.0.0.9/24", "fresh-pod"))
	createSource(t, handle, kindDanmEp, "app", "known-ep", nil, endpointContent("10.0.0.10/24", "known-pod"))
	createCopy(t, handle, kindDanmEp, "app", "known-ep", nil, endpointContent("10.0.0.3/24", "known-pod"))
	createCopy(t, handle, kindDanmEp, "app", "gone-ep",
		map[string]any{"annotations": map[string]any{EpCopiedMarkerKey: PlanID}}, endpointContent("10.0.0.4/24", "gone-pod"))
	createCopy(t, handle, kindDanmEp, "app", "native-ep", nil, endpointContent("10.0.0.5/24", "native-pod"))
	if err := (resyncAndVerify{}).Execute(ctx, handle); err != nil {
		t.Fatal(err)
	}
	fresh := readCopy(t, handle, kindDanmEp, "app", "fresh-ep")
	if address, _, _ := unstructured.NestedString(fresh.Object, "spec", "Interface", "Address"); address != "10.0.0.9/24" {
		t.Fatalf("the created copy holds the address %q", address)
	}
	known := readCopy(t, handle, kindDanmEp, "app", "known-ep")
	if address, _, _ := unstructured.NestedString(known.Object, "spec", "Interface", "Address"); address != "10.0.0.10/24" {
		t.Fatalf("the updated copy holds the address %q", address)
	}
	if _, err := objectClient(handle, kindDanmEp, kindDanmEp.NewResource(), "app").Get(ctx, "gone-ep", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("the copy of a deleted DanmEp is still there: %v", err)
	}
	if _, err := objectClient(handle, kindDanmEp, kindDanmEp.NewResource(), "app").Get(ctx, "native-ep", metav1.GetOptions{}); err != nil {
		t.Fatalf("a DanmEp the 4.4 CNI wrote was deleted: %v", err)
	}
	if marker, _, _ := unstructured.NestedString(fresh.Object, "metadata", "annotations", EpCopiedMarkerKey); marker != PlanID {
		t.Fatalf("a created copy carries the marker %q", marker)
	}
}

func TestResyncKeepsAndReplicatesRipRefs(t *testing.T) {
	handle, out := resyncHandle(t, cluster.ModeProduction)
	ctx := context.Background()
	recordStep9Rips(t, handle, recordedRip{Namespace: "app", Name: "shared-rip"})
	createCopy(t, handle, kindReservedIP, "app", "shared-rip", nil, map[string]any{"spec": map[string]any{
		"network": map[string]any{"name": "internal", "type": kindTenantNetwork.Kind},
		"ips":     []any{map[string]any{"address": "10.0.0.20", "shareable": true}},
	}})
	createSource(t, handle, kindDanmEp, "app", "new-ep", map[string]any{"annotations": map[string]any{SharedRipTagV4: "shared-rip"}}, endpointContent("10.0.0.20/24", "new-pod"))
	createSource(t, handle, kindDanmEp, "app", "lost-ep", map[string]any{"annotations": map[string]any{RipTagV4: "missing-rip"}}, endpointContent("10.0.0.21/24", "lost-pod"))
	createSource(t, handle, kindDanmEp, "app", "old-ep", map[string]any{"annotations": map[string]any{SharedRipTagV4: "shared-rip"}}, endpointContent("10.0.0.20/24", "old-pod"))
	createCopy(t, handle, kindDanmEp, "app", "old-ep",
		map[string]any{"labels": map[string]any{datastructs.V4RipLabel: "shared-rip"}},
		map[string]any{"spec": map[string]any{
			"NetworkName": "internal",
			"NetworkType": "ipvlan",
			"EndpointID":  "ep",
			"Pod":         "old-pod",
			"apiType":     kindTenantNetwork.Kind,
			"Interface":   map[string]any{"Name": "eth0", "Address": "10.0.0.20/24"},
			ripRefsField:  map[string]any{"v4": "shared-rip"},
		}})
	if err := (resyncAndVerify{}).Execute(ctx, handle); err != nil {
		t.Fatal(err)
	}
	created := readCopy(t, handle, kindDanmEp, "app", "new-ep")
	if ref, _, _ := unstructured.NestedString(created.Object, "spec", ripRefsField, "v4"); ref != "shared-rip" {
		t.Fatalf("the new copy references %q", ref)
	}
	if label, _, _ := unstructured.NestedString(created.Object, "metadata", "labels", datastructs.V4RipLabel); label != "shared-rip" {
		t.Fatalf("the new copy carries the ReservedIP label %q", label)
	}
	kept := readCopy(t, handle, kindDanmEp, "app", "old-ep")
	if ref, _, _ := unstructured.NestedString(kept.Object, "spec", ripRefsField, "v4"); ref != "shared-rip" {
		t.Fatalf("the reference of an existing copy became %q", ref)
	}
	lost := readCopy(t, handle, kindDanmEp, "app", "lost-ep")
	if _, found, _ := unstructured.NestedMap(lost.Object, "spec", ripRefsField); found {
		t.Fatal("an unreplicated tag became a reference")
	}
	if !strings.Contains(out.String(), "which step 9 did not create") {
		t.Fatalf("the unreplicated tag is not reported: %s", out.String())
	}
}

func TestResyncFailsOnStaleNetwatcher(t *testing.T) {
	handle, _ := resyncHandle(t, cluster.ModeProduction)
	ctx := context.Background()
	stale := resyncNetwatcherPod(crdInstalled.Add(-time.Hour))
	if err := handle.Kube.CoreV1().Pods(DanmNamespace).Delete(ctx, stale.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := handle.Kube.CoreV1().Pods(DanmNamespace).Create(ctx, stale, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	err := (resyncAndVerify{}).Execute(ctx, handle)
	if err == nil || !strings.Contains(err.Error(), "discovered only the "+OldGroup+" group") {
		t.Fatalf("a netwatcher older than the CRDs passed verification: %v", err)
	}
	if detectResyncStep(t, handle) != engine.StateNotStarted {
		t.Fatal("a failed step 13 reads as done")
	}
}

func TestResyncFailsOnMissingShim(t *testing.T) {
	handle, _ := resyncHandle(t, cluster.ModeProduction)
	ctx := context.Background()
	if err := handle.Kube.AppsV1().Deployments(state.DefaultNamespace).Delete(ctx, ShimName, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	err := (resyncAndVerify{}).Execute(ctx, handle)
	if err == nil || !strings.Contains(err.Error(), "annotation shim no longer exists") {
		t.Fatalf("a missing annotation shim passed verification: %v", err)
	}
}

func TestResyncRollbackClearsTheRecord(t *testing.T) {
	handle, _ := resyncHandle(t, cluster.ModeLightweight)
	ctx := context.Background()
	if err := (resyncAndVerify{}).Execute(ctx, handle); err != nil {
		t.Fatal(err)
	}
	if detectResyncStep(t, handle) != engine.StateDone {
		t.Fatal("step 13 does not read as done after it ran")
	}
	if err := (resyncAndVerify{}).Rollback(ctx, handle); err != nil {
		t.Fatal(err)
	}
	if detectResyncStep(t, handle) != engine.StateNotStarted {
		t.Fatal("step 13 still reads as done after its rollback")
	}
}

func countWrites(handle *cluster.Handle) *[]string {
	writes := &[]string{}
	record := func(action k8stesting.Action) (bool, k8sruntime.Object, error) {
		if action.GetResource().Resource != "configmaps" {
			*writes = append(*writes, action.GetVerb()+" "+action.GetResource().Resource)
		}
		return false, nil, nil
	}
	for _, verb := range []string{"create", "update", "patch", "delete"} {
		handle.Dynamic.(*dynamicfake.FakeDynamicClient).PrependReactor(verb, "*", record)
		handle.Kube.(*kubefake.Clientset).PrependReactor(verb, "*", record)
	}
	return writes
}

func TestResyncWritesNothingWhenNothingChanged(t *testing.T) {
	handle, _ := resyncHandle(t, cluster.ModeProduction)
	ctx := context.Background()
	recordStep9Rips(t, handle, recordedRip{Namespace: "app", Name: "shared-rip"})
	createCopy(t, handle, kindReservedIP, "app", "shared-rip", nil, map[string]any{"spec": map[string]any{
		"network": map[string]any{"name": "internal", "type": kindTenantNetwork.Kind},
		"ips":     []any{map[string]any{"address": "10.0.0.20", "shareable": true}},
	}})
	createSource(t, handle, kindTenantNetwork, "app", "internal", nil, networkContent("AQAB", "10.0.0.9", 1500))
	createCopy(t, handle, kindTenantNetwork, "app", "internal", nil, networkContent("AQAA", "10.0.0.5", 1500))
	createSource(t, handle, kindTenantConfig, "", "tconf", nil, map[string]any{"hostDevices": []any{
		map[string]any{"name": "ens3", "vniType": "vlan", "vniRange": "1-100", "alloc": vniAlloc(t, 4095, 7)},
	}})
	createCopy(t, handle, kindTenantConfig, "", "tconf", nil, map[string]any{"hostDevices": []any{
		map[string]any{"name": "ens3", "vniType": "vlan", "vniRange": "1-100", "alloc": vniAlloc(t, 4095, 3)},
	}})
	createSource(t, handle, kindDanmEp, "app", "known-ep", map[string]any{"annotations": map[string]any{SharedRipTagV4: "shared-rip"}}, endpointContent("10.0.0.20/24", "known-pod"))
	if _, err := handle.Kube.CoreV1().Pods("app").Create(ctx, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "known-pod", Annotations: map[string]string{OldInterfacesKey: `[{"tenantNetwork":"internal","ip":"10.0.0.20/24"}]`}},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := (resyncAndVerify{}).Execute(ctx, handle); err != nil {
		t.Fatal(err)
	}
	writes := countWrites(handle)
	if err := (resyncAndVerify{}).Execute(ctx, handle); err != nil {
		t.Fatal(err)
	}
	if len(*writes) > 0 {
		t.Fatalf("a second run rewrote objects which did not change: %v", *writes)
	}
}

func TestDetectReadsTheRecordBeforeTheStepBegins(t *testing.T) {
	handle, _ := resyncHandle(t, cluster.ModeProduction)
	ctx := context.Background()
	handle.PlanID, handle.StepID = "", 0
	for name, detect := range map[string]func() (engine.StepState, error){
		"step 12": func() (engine.StepState, error) { return installCniBinaries{}.Detect(ctx, handle) },
		"step 13": func() (engine.StepState, error) { return resyncAndVerify{}.Detect(ctx, handle) },
	} {
		detected, err := detect()
		if err != nil {
			t.Fatalf("detecting %s outside of a running step failed: %v", name, err)
		}
		if detected != engine.StateNotStarted {
			t.Fatalf("%s reads as %s before it ran", name, detected)
		}
	}
	if _, err := handle.State.Mutate(ctx, PlanID, func(record *state.PlanRecord) error {
		record.UpsertStep(state.StepRecord{
			ID:        resyncAndVerify{}.Meta().ID,
			Name:      resyncAndVerify{}.Meta().Name,
			Status:    state.StatusDone,
			Snapshots: map[string]string{resyncRecordKey: "2026-11-18T10:00:00Z"},
		})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	detected, err := resyncAndVerify{}.Detect(ctx, handle)
	if err != nil {
		t.Fatal(err)
	}
	if detected != engine.StateDone {
		t.Fatalf("step 13 reads as %s although its run is recorded", detected)
	}
}
