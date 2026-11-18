package v43to44

import (
	"context"
	"testing"

	"github.com/danm-cni/danm-utils/pkg/dma/cluster"
	"github.com/danm-cni/danm-utils/pkg/dma/engine"
	danmtypes "github.com/danm-cni/danm/crd/apis/danm/v1"
	"github.com/danm-cni/danm/pkg/datastructs"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"
)

func detectDanmEpStep(t *testing.T, handle *cluster.Handle) engine.StepState {
	t.Helper()
	detected, err := copyDanmEps{}.Detect(context.Background(), handle)
	if err != nil {
		t.Fatalf("detecting step 7 failed: %v", err)
	}
	return detected
}

func TestCopyDanmEps(t *testing.T) {
	handle := copyHandle(t, cluster.ModeProduction)
	ctx := context.Background()
	createSource(t, handle, kindDanmEp, "app", "ep1",
		map[string]any{
			"labels": map[string]any{"app": "web", datastructs.V4RipLabel: "forged"},
			"annotations": map[string]any{
				RipTagV4:          "db",
				SharedRipTagV6:    "db6",
				"example.com/key": "value",
			},
			"ownerReferences": []any{
				map[string]any{"apiVersion": "v1", "kind": "Pod", "name": "web-1", "uid": "p"},
				map[string]any{"apiVersion": OldGroup + "/" + Version, "kind": "DanmNet", "name": "internal", "uid": "n"},
			},
		},
		map[string]any{"spec": map[string]any{
			"NetworkName": "internal",
			"NetworkType": "ipvlan",
			"EndpointID":  "e1",
			"Pod":         "web-1",
			"apiType":     kindTenantNetwork.Kind,
			"Interface":   map[string]any{"Name": "eth0", "Address": "10.0.0.5/24"},
		}})
	createSource(t, handle, kindDanmEp, "other", "ep2", nil,
		map[string]any{"spec": map[string]any{"NetworkName": "internal", "EndpointID": "e2", "Pod": "db-0", "apiType": kindTenantNetwork.Kind,
			"Interface": map[string]any{"Name": "eth0"}}})
	if detectDanmEpStep(t, handle) != engine.StateNotStarted {
		t.Fatal("step 7 does not read as not started before it ran")
	}
	if err := (copyDanmEps{}).Execute(ctx, handle); err != nil {
		t.Fatal(err)
	}
	if detectDanmEpStep(t, handle) != engine.StateDone {
		t.Fatal("step 7 does not read as done after it ran")
	}
	copied := readCopy(t, handle, kindDanmEp, "app", "ep1")
	if copied.GetAPIVersion() != NewGroup+"/"+Version || copied.GetUID() != "" {
		t.Fatalf("the copy is %s with uid %q", copied.GetAPIVersion(), copied.GetUID())
	}
	labels := copied.GetLabels()
	if labels["app"] != "web" || len(labels) != 1 {
		t.Fatalf("the labels of the copy are %v", labels)
	}
	annotations := copied.GetAnnotations()
	if annotations["example.com/key"] != "value" || annotations[EpCopiedMarkerKey] != PlanID || len(annotations) != 2 {
		t.Fatalf("the annotations of the copy are %v", annotations)
	}
	owners := copied.GetOwnerReferences()
	if len(owners) != 1 || owners[0].Kind != "Pod" {
		t.Fatalf("the owner references of the copy are %+v", owners)
	}
	if _, found, _ := unstructured.NestedMap(copied.Object, "spec", "reservedIpRefs"); found {
		t.Fatal("the copy carries ReservedIP references")
	}
	if address, _, _ := unstructured.NestedString(copied.Object, "spec", "Interface", "Address"); address != "10.0.0.5/24" {
		t.Fatalf("the address of the copy is %q", address)
	}
	if plain := readCopy(t, handle, kindDanmEp, "other", "ep2"); plain.GetLabels() != nil {
		t.Fatalf("an unlabelled DanmEp got labels: %v", plain.GetLabels())
	}
	writes := 0
	handle.Dynamic.(*dynamicfake.FakeDynamicClient).PrependReactor("*", "*", func(action k8stesting.Action) (bool, k8sruntime.Object, error) {
		if action.GetVerb() == "update" || action.GetVerb() == "create" {
			writes++
		}
		return false, nil, nil
	})
	if err := (copyDanmEps{}).Execute(ctx, handle); err != nil {
		t.Fatal(err)
	}
	if writes != 0 {
		t.Fatalf("a second run rewrote %d copies instead of skipping them", writes)
	}
	if err := (copyDanmEps{}).Rollback(ctx, handle); err != nil {
		t.Fatal(err)
	}
	if detectDanmEpStep(t, handle) != engine.StateNotStarted {
		t.Fatal("step 7 does not read as not started after its rollback")
	}
	if _, err := objectClient(handle, kindDanmEp, kindDanmEp.OldResource(), "app").Get(ctx, "ep1", metav1.GetOptions{}); err != nil {
		t.Fatalf("the rollback touched the source: %v", err)
	}
}

func TestCopyDanmEpsCorrectsADriftedCopy(t *testing.T) {
	handle := copyHandle(t, cluster.ModeLightweight)
	ctx := context.Background()
	createSource(t, handle, kindDanmEp, "app", "ep1", nil,
		map[string]any{"spec": map[string]any{"NetworkName": "internal", "EndpointID": "e1", "Pod": "web-1", "apiType": kindDanmNet.Kind,
			"Interface": map[string]any{"Name": "eth0", "Address": "10.0.0.5/24"}}})
	if err := (copyDanmEps{}).Execute(ctx, handle); err != nil {
		t.Fatal(err)
	}
	drifted := readCopy(t, handle, kindDanmEp, "app", "ep1")
	unstructured.SetNestedField(drifted.Object, "10.9.9.9/24", "spec", "Interface", "Address")
	if _, err := objectClient(handle, kindDanmEp, kindDanmEp.NewResource(), "app").Update(ctx, drifted, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := (copyDanmEps{}).Execute(ctx, handle); err != nil {
		t.Fatal(err)
	}
	corrected := readCopy(t, handle, kindDanmEp, "app", "ep1")
	if address, _, _ := unstructured.NestedString(corrected.Object, "spec", "Interface", "Address"); address != "10.0.0.5/24" {
		t.Fatalf("the drifted copy was not corrected, its address is %s", address)
	}
}

func TestCopyDanmEpsRefusesAnUnknownMode(t *testing.T) {
	handle := copyHandle(t, cluster.ModeUnknown)
	if err := (copyDanmEps{}).Execute(context.Background(), handle); err == nil {
		t.Fatal("step 7 ran without a known deployment mode")
	}
}

func TestCopyDanmEpsKeepsTheReferencesOfStep9(t *testing.T) {
	handle := copyHandle(t, cluster.ModeProduction)
	ctx := context.Background()
	createSource(t, handle, kindDanmEp, "app", "shared-ep", map[string]any{"annotations": map[string]any{SharedRipTagV4: "db"}},
		map[string]any{"spec": map[string]any{
			"NetworkName": "internal",
			"EndpointID":  "ep",
			"Pod":         "web-1",
			"apiType":     kindTenantNetwork.Kind,
			"Interface":   map[string]any{"Name": "eth0", "Address": "10.0.0.20/24"},
		}})
	if err := (copyDanmEps{}).Execute(ctx, handle); err != nil {
		t.Fatal(err)
	}
	copied := readCopy(t, handle, kindDanmEp, "app", "shared-ep")
	if err := unstructured.SetNestedField(copied.Object, "db", "spec", ripRefsField, "v4"); err != nil {
		t.Fatal(err)
	}
	copied.SetLabels(datastructs.EpLabels(copied.GetLabels(), danmtypes.RipReferences{V4: "db"}))
	if _, err := objectClient(handle, kindDanmEp, kindDanmEp.NewResource(), "app").Update(ctx, copied, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	createSource(t, handle, kindDanmEp, "app", "late-ep", nil,
		map[string]any{"spec": map[string]any{
			"NetworkName": "internal",
			"EndpointID":  "late",
			"Pod":         "web-2",
			"apiType":     kindTenantNetwork.Kind,
			"Interface":   map[string]any{"Name": "eth0", "Address": "10.0.0.21/24"},
		}})
	if detectDanmEpStep(t, handle) != engine.StatePartial {
		t.Fatal("step 7 does not read as partial while an endpoint is uncopied")
	}
	if err := (copyDanmEps{}).Execute(ctx, handle); err != nil {
		t.Fatal(err)
	}
	again := readCopy(t, handle, kindDanmEp, "app", "shared-ep")
	if ref, _, _ := unstructured.NestedString(again.Object, "spec", ripRefsField, "v4"); ref != "db" {
		t.Fatalf("re-running step 7 left the reference %q", ref)
	}
	if label := again.GetLabels()[datastructs.V4RipLabel]; label != "db" {
		t.Fatalf("re-running step 7 left the ReservedIP label %q", label)
	}
	if _, err := objectClient(handle, kindDanmEp, kindDanmEp.NewResource(), "app").Get(ctx, "late-ep", metav1.GetOptions{}); err != nil {
		t.Fatalf("the endpoint created after step 7 was not copied: %v", err)
	}
}

func TestCopyDanmEpsRollbackKeepsNativeEndpoints(t *testing.T) {
	handle := copyHandle(t, cluster.ModeProduction)
	ctx := context.Background()
	createSource(t, handle, kindDanmEp, "app", "copied-ep", nil, endpointContent("10.0.0.30/24", "web-1"))
	if err := (copyDanmEps{}).Execute(ctx, handle); err != nil {
		t.Fatal(err)
	}
	createCopy(t, handle, kindDanmEp, "app", "native-ep", nil, endpointContent("10.0.0.31/24", "web-2"))
	if err := (copyDanmEps{}).Rollback(ctx, handle); err != nil {
		t.Fatal(err)
	}
	client := objectClient(handle, kindDanmEp, kindDanmEp.NewResource(), "app")
	if _, err := client.Get(ctx, "copied-ep", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("the rollback left the copy behind: %v", err)
	}
	if _, err := client.Get(ctx, "native-ep", metav1.GetOptions{}); err != nil {
		t.Fatalf("the rollback deleted a DanmEp the 4.4 CNI wrote: %v", err)
	}
}
