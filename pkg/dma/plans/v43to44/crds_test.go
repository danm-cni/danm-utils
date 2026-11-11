package v43to44

import (
	"bytes"
	"context"
	"io"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/danm-cni/danm-utils/pkg/dma/cluster"
	"github.com/danm-cni/danm-utils/pkg/dma/engine"
	"github.com/danm-cni/danm-utils/pkg/dma/state"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	kubefake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

type crdFixture struct {
	mode           cluster.Mode
	existing       []*apiextensionsv1.CustomResourceDefinition
	objects        []k8sruntime.Object
	neverEstablish bool
}

func (fixture *crdFixture) handle(t *testing.T) (*cluster.Handle, *bytes.Buffer) {
	crds := []k8sruntime.Object{}
	for _, crd := range fixture.existing {
		crds = append(crds, crd)
	}
	apiExt := apiextfake.NewSimpleClientset(crds...)
	apiExt.PrependReactor("create", "customresourcedefinitions", func(action k8stesting.Action) (bool, k8sruntime.Object, error) {
		if fixture.neverEstablish {
			return false, nil, nil
		}
		return establishOnCreate(action)
	})
	listKinds := map[schema.GroupVersionResource]string{}
	for _, kind := range newKinds {
		listKinds[kind.NewResource()] = kind.Kind + "List"
	}
	kube := kubefake.NewSimpleClientset()
	out := &bytes.Buffer{}
	return &cluster.Handle{
		Kube:      kube,
		ApiExt:    apiExt,
		Dynamic:   dynamicfake.NewSimpleDynamicClientWithCustomListKinds(k8sruntime.NewScheme(), listKinds, fixture.objects...),
		State:     state.NewStore(kube, state.DefaultNamespace, state.DefaultName),
		Namespace: state.DefaultNamespace,
		Mode:      fixture.mode,
		Operator:  "tester",
		Log:       log.New(io.Discard, "", 0),
		Out:       out,
	}, out
}

func newCrd(kind apiKind, labelled, established bool) *apiextensionsv1.CustomResourceDefinition {
	crd := &apiextensionsv1.CustomResourceDefinition{ObjectMeta: metav1.ObjectMeta{Name: kind.NewCrdName()}}
	if labelled {
		crd.Labels = map[string]string{PlanLabel: PlanID}
	}
	if established {
		crd.Status.Conditions = []apiextensionsv1.CustomResourceDefinitionCondition{{Type: apiextensionsv1.Established, Status: apiextensionsv1.ConditionTrue}}
	}
	return crd
}

func detectCrds(t *testing.T, handle *cluster.Handle) engine.StepState {
	t.Helper()
	detected, err := installNewCrds{}.Detect(context.Background(), handle)
	if err != nil {
		t.Fatalf("detecting step 2 failed: %v", err)
	}
	return detected
}

func newGroupCrds(t *testing.T, handle *cluster.Handle) []string {
	t.Helper()
	list, err := handle.ApiExt.ApiextensionsV1().CustomResourceDefinitions().List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	names := []string{}
	for _, crd := range list.Items {
		if crd.Spec.Group == NewGroup || strings.HasSuffix(crd.Name, "."+NewGroup) {
			names = append(names, crd.Name)
		}
	}
	return names
}

func shortCrdWait(t *testing.T) {
	interval, timeout := crdPollInterval, crdWaitTimeout
	crdPollInterval, crdWaitTimeout = 10*time.Millisecond, 100*time.Millisecond
	t.Cleanup(func() { crdPollInterval, crdWaitTimeout = interval, timeout })
}

func TestInstallNewCrdsInstallsTheProductionSet(t *testing.T) {
	handle, out := (&crdFixture{mode: cluster.ModeProduction}).handle(t)
	if detectCrds(t, handle) != engine.StateNotStarted {
		t.Fatal("step 2 does not read as not started on a clean cluster")
	}
	if err := (installNewCrds{}).Execute(context.Background(), handle); err != nil {
		t.Fatalf("installing the CRDs failed: %v\n%s", err, out.String())
	}
	if detectCrds(t, handle) != engine.StateDone {
		t.Fatal("step 2 does not read as done after installing the CRDs")
	}
	installed := newGroupCrds(t, handle)
	if len(installed) != len(targetKinds(cluster.ModeProduction)) {
		t.Fatalf("installed %v", installed)
	}
	crd, err := handle.ApiExt.ApiextensionsV1().CustomResourceDefinitions().Get(context.Background(), kindReservedIP.NewCrdName(), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if crd.Labels[PlanLabel] != PlanID || crd.Spec.Group != NewGroup || crd.Spec.Names.Kind != kindReservedIP.Kind {
		t.Fatalf("the ReservedIP CRD is not the embedded one, or is not labelled: %v %v", crd.Labels, crd.Spec.Names)
	}
	if err := (installNewCrds{}).Rollback(context.Background(), handle); err != nil {
		t.Fatal(err)
	}
	if detectCrds(t, handle) != engine.StateNotStarted || len(newGroupCrds(t, handle)) != 0 {
		t.Fatalf("CRDs %v remain after the rollback", newGroupCrds(t, handle))
	}
}

func TestInstallNewCrdsInstallsTheLightweightSet(t *testing.T) {
	handle, _ := (&crdFixture{mode: cluster.ModeLightweight}).handle(t)
	if err := (installNewCrds{}).Execute(context.Background(), handle); err != nil {
		t.Fatal(err)
	}
	installed := strings.Join(newGroupCrds(t, handle), ",")
	if installed != kindDanmEp.NewCrdName()+","+kindDanmNet.NewCrdName() && installed != kindDanmNet.NewCrdName()+","+kindDanmEp.NewCrdName() {
		t.Fatalf("installed %s", installed)
	}
}

func TestInstallNewCrdsNeedsTheMode(t *testing.T) {
	handle, _ := (&crdFixture{}).handle(t)
	err := (installNewCrds{}).Execute(context.Background(), handle)
	if err == nil || !strings.Contains(err.Error(), "mode is unknown") {
		t.Fatalf("installing without a mode returned %v", err)
	}
}

func TestInstallNewCrdsRefusesTheOtherModesSet(t *testing.T) {
	handle, _ := (&crdFixture{mode: cluster.ModeProduction, existing: []*apiextensionsv1.CustomResourceDefinition{newCrd(kindDanmNet, true, true)}}).handle(t)
	err := (installNewCrds{}).Execute(context.Background(), handle)
	if err == nil || !strings.Contains(err.Error(), kindDanmNet.NewCrdName()+" belongs to the other deployment mode") {
		t.Fatalf("installing next to the other mode's CRD returned %v", err)
	}
	if len(newGroupCrds(t, handle)) != 1 {
		t.Fatalf("CRDs were created although the step failed: %v", newGroupCrds(t, handle))
	}
}

func TestInstallNewCrdsRefusesForeignCrds(t *testing.T) {
	handle, _ := (&crdFixture{mode: cluster.ModeLightweight, existing: []*apiextensionsv1.CustomResourceDefinition{newCrd(kindDanmEp, false, true)}}).handle(t)
	err := (installNewCrds{}).Execute(context.Background(), handle)
	if err == nil || !strings.Contains(err.Error(), "was not installed by plan") {
		t.Fatalf("installing over a foreign CRD returned %v", err)
	}
	if detectCrds(t, handle) != engine.StateNotStarted {
		t.Fatal("a foreign CRD reads as installed by step 2")
	}
}

func TestInstallNewCrdsCompletesAPartialSet(t *testing.T) {
	handle, _ := (&crdFixture{mode: cluster.ModeLightweight, existing: []*apiextensionsv1.CustomResourceDefinition{newCrd(kindDanmEp, true, true)}}).handle(t)
	if detectCrds(t, handle) != engine.StatePartial {
		t.Fatal("a partial set does not read as partial")
	}
	if err := (installNewCrds{}).Execute(context.Background(), handle); err != nil {
		t.Fatal(err)
	}
	if detectCrds(t, handle) != engine.StateDone {
		t.Fatal("the completed set does not read as done")
	}
}

func TestInstallNewCrdsRevertsASetWhichIsNotEstablished(t *testing.T) {
	shortCrdWait(t)
	handle, _ := (&crdFixture{mode: cluster.ModeProduction, neverEstablish: true}).handle(t)
	err := (installNewCrds{}).Execute(context.Background(), handle)
	if err == nil || !strings.Contains(err.Error(), "not established") {
		t.Fatalf("installing a set which never becomes established returned %v", err)
	}
	if remaining := newGroupCrds(t, handle); len(remaining) != 0 {
		t.Fatalf("CRDs %v remain after the failed installation", remaining)
	}
}

func TestRollbackRefusesCrdsHoldingObjects(t *testing.T) {
	ep := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": NewGroup + "/" + Version, "kind": kindDanmEp.Kind,
		"metadata": map[string]any{"name": "pod1-eth1", "namespace": "app"},
	}}
	fixture := &crdFixture{
		mode:     cluster.ModeLightweight,
		existing: []*apiextensionsv1.CustomResourceDefinition{newCrd(kindDanmEp, true, true), newCrd(kindDanmNet, true, true)},
		objects:  []k8sruntime.Object{ep},
	}
	handle, _ := fixture.handle(t)
	err := (installNewCrds{}).Rollback(context.Background(), handle)
	if err == nil || !strings.Contains(err.Error(), kindDanmEp.NewCrdName()+" hold objects") {
		t.Fatalf("rolling back populated CRDs returned %v", err)
	}
	if len(newGroupCrds(t, handle)) != 2 {
		t.Fatalf("CRDs were deleted although the rollback was refused: %v", newGroupCrds(t, handle))
	}
}

func establishOnCreate(action k8stesting.Action) (bool, k8sruntime.Object, error) {
	crd := action.(k8stesting.CreateAction).GetObject().(*apiextensionsv1.CustomResourceDefinition)
	crd.Status.Conditions = []apiextensionsv1.CustomResourceDefinitionCondition{{Type: apiextensionsv1.Established, Status: apiextensionsv1.ConditionTrue}}
	return false, nil, nil
}

func TestInstallNewCrdsUsesTheRecordedMode(t *testing.T) {
	fixture := productionFixture(t)
	fixture.options.AcceptDefaultMtu = true
	handle, out := fixture.handle(t)
	handle.ApiExt.(*apiextfake.Clientset).PrependReactor("create", "customresourcedefinitions", establishOnCreate)
	if err := execute(t, handle); err != nil {
		t.Fatalf("%v\n%s", err, out.String())
	}
	handle.Mode = cluster.ModeUnknown
	runner := engine.NewRunner(New(), handle, &engine.TokenConfirmer{Supplied: "install-new-crds", Out: io.Discard})
	if err := runner.Execute(context.Background(), 2); err != nil {
		t.Fatalf("%v\n%s", err, out.String())
	}
	if installed := newGroupCrds(t, handle); len(installed) != len(targetKinds(cluster.ModeProduction)) {
		t.Fatalf("installed %v after a production preflight", installed)
	}
	if err := runner.Rollback(context.Background(), 2); err != nil {
		t.Fatal(err)
	}
	if detectCrds(t, handle) != engine.StateNotStarted {
		t.Fatal("step 2 does not read as not started after its rollback")
	}
}
