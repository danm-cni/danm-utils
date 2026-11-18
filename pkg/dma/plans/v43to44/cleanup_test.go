package v43to44

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/danm-cni/danm-utils/pkg/dma/cluster"
	"github.com/danm-cni/danm-utils/pkg/dma/engine"
	"github.com/danm-cni/danm-utils/pkg/dma/state"
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	kubefake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func cleanupHandle(t *testing.T, mode cluster.Mode, objects ...k8sruntime.Object) (*cluster.Handle, *bytes.Buffer) {
	t.Helper()
	cniInterval, workloadInterval, crdInterval := cniPollInterval, workloadPollInterval, crdPollInterval
	cniPollInterval, workloadPollInterval, crdPollInterval = 5*time.Millisecond, 5*time.Millisecond, 5*time.Millisecond
	deleteTimeout := crdDeleteTimeout
	crdDeleteTimeout = 200 * time.Millisecond
	t.Cleanup(func() {
		cniPollInterval, workloadPollInterval, crdPollInterval = cniInterval, workloadInterval, crdInterval
		crdDeleteTimeout = deleteTimeout
	})
	netwatcher := componentDaemonSet("netwatcher", "netwatcher", "registry.example.com/netwatcher:4.4",
		map[string]string{OldInterfacesKey: `[{"network":"internal"}]`, NewInterfacesKey: `[{"network":"internal"}]`})
	objects = append(objects, netwatcher)
	kube := kubefake.NewSimpleClientset(objects...)
	ready := func(action k8stesting.Action) (bool, k8sruntime.Object, error) {
		daemonSet, isDaemonSet := action.(k8stesting.CreateAction).GetObject().(*appsv1.DaemonSet)
		if isDaemonSet {
			daemonSet.Status = appsv1.DaemonSetStatus{DesiredNumberScheduled: 2, UpdatedNumberScheduled: 2, NumberReady: 2}
		}
		return false, nil, nil
	}
	kube.PrependReactor("create", "daemonsets", ready)
	kube.PrependReactor("update", "daemonsets", ready)
	crds := []k8sruntime.Object{}
	for _, kind := range sourceKinds(mode) {
		crds = append(crds, &apiextensionsv1.CustomResourceDefinition{ObjectMeta: metav1.ObjectMeta{Name: kind.OldCrdName()}})
	}
	listKinds := map[schema.GroupVersionResource]string{}
	for _, kind := range []apiKind{kindDanmNet, kindTenantNetwork, kindClusterNetwork, kindTenantConfig, kindDanmEp, kindReservedIP} {
		listKinds[kind.OldResource()] = kind.Kind + "List"
		listKinds[kind.NewResource()] = kind.Kind + "List"
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
		Options:   cluster.Options{BinaryImage: "registry.example.com/dma:1.0", RolloutTimeout: 500 * time.Millisecond},
		Log:       log.New(io.Discard, "", 0),
		Out:       out,
	}
	ref := cluster.WorkloadRef{Kind: cluster.WorkloadDaemonSet, Namespace: DanmNamespace, Name: "netwatcher"}
	if _, err := handle.State.Mutate(context.Background(), PlanID, func(record *state.PlanRecord) error {
		record.Mode = string(mode)
		record.Workloads = map[string]state.WorkloadRecord{ComponentNetwatcher: cluster.RecordWorkload(ref, true, false)}
		record.UpsertStep(state.StepRecord{ID: 12, Name: "install-cni-binaries", Status: state.StatusDone, Snapshots: map[string]string{cniInstalledKey: cniBackupSuffix()}})
		record.UpsertStep(state.StepRecord{ID: removeOldApiStep, Name: "remove-old-api", Status: state.StatusRunning})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	handle.BeginStep(PlanID, removeOldApiStep)
	return handle, out
}

func recordRolledWebhook(t *testing.T, handle *cluster.Handle, name string) {
	t.Helper()
	encoded, err := json.Marshal(&admissionregistrationv1.MutatingWebhookConfiguration{ObjectMeta: metav1.ObjectMeta{Name: name}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := handle.State.Mutate(context.Background(), PlanID, func(record *state.PlanRecord) error {
		record.UpsertStep(state.StepRecord{ID: 10, Name: "deploy-new-webhook", Status: state.StatusDone,
			Snapshots: map[string]string{webhookConfigurationKey: string(encoded)}})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func webhookConfigurationOf(name string, groups ...string) *admissionregistrationv1.MutatingWebhookConfiguration {
	return &admissionregistrationv1.MutatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Webhooks: []admissionregistrationv1.MutatingWebhook{{
			Name:  "hook.example.com",
			Rules: []admissionregistrationv1.RuleWithOperations{{Rule: admissionregistrationv1.Rule{APIGroups: groups}}},
		}},
	}
}

func clusterRoleOf(name string, groups ...string) *rbacv1.ClusterRole {
	return &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Rules:      []rbacv1.PolicyRule{{APIGroups: groups, Resources: []string{"danmnets"}, Verbs: []string{"get"}}},
	}
}

func clusterRoleBindingOf(name, role string) *rbacv1.ClusterRoleBinding {
	return &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		RoleRef:    rbacv1.RoleRef{Kind: "ClusterRole", Name: role, APIGroup: rbacv1.GroupName},
	}
}

func detectCleanupStep(t *testing.T, handle *cluster.Handle) engine.StepState {
	t.Helper()
	detected, err := removeOldApi{}.Detect(context.Background(), handle)
	if err != nil {
		t.Fatalf("detecting step 14 failed: %v", err)
	}
	return detected
}

func TestRemoveOldApi(t *testing.T) {
	handle, out := cleanupHandle(t, cluster.ModeProduction,
		webhookConfigurationOf("danm-webhook-config", NewGroup),
		webhookConfigurationOf("danm-legacy-webhook", OldGroup),
		webhookConfigurationOf("danm-mixed-webhook", OldGroup, "apps"),
		clusterRoleOf("caas:danm", OldGroup),
		clusterRoleOf("danm-mixed", OldGroup, "apps"),
		clusterRoleOf(RbacName(ComponentCni), NewGroup),
		clusterRoleBindingOf("caas:danm", "caas:danm"),
		clusterRoleBindingOf("danm-mixed", "danm-mixed"),
	)
	ctx := context.Background()
	recordRolledWebhook(t, handle, "danm-webhook-config")
	if _, err := handle.ApiExt.ApiextensionsV1().CustomResourceDefinitions().Create(ctx,
		&apiextensionsv1.CustomResourceDefinition{ObjectMeta: metav1.ObjectMeta{Name: kindDanmNet.OldCrdName()}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if detectCleanupStep(t, handle) != engine.StateNotStarted {
		t.Fatal("step 14 does not read as not started before it ran")
	}
	if err := (removeOldApi{}).Execute(ctx, handle); err != nil {
		t.Fatal(err)
	}
	for _, kind := range sourceKinds(cluster.ModeProduction) {
		if _, err := handle.ApiExt.ApiextensionsV1().CustomResourceDefinitions().Get(ctx, kind.OldCrdName(), metav1.GetOptions{}); !apierrors.IsNotFound(err) {
			t.Fatalf("CRD %s is still installed: %v", kind.OldCrdName(), err)
		}
	}
	if _, err := handle.ApiExt.ApiextensionsV1().CustomResourceDefinitions().Get(ctx, kindDanmNet.OldCrdName(), metav1.GetOptions{}); err != nil {
		t.Fatalf("the CRD of the other mode was deleted: %v", err)
	}
	configurations := handle.Kube.AdmissionregistrationV1().MutatingWebhookConfigurations()
	if _, err := configurations.Get(ctx, "danm-legacy-webhook", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("the old webhook configuration is still there: %v", err)
	}
	for _, name := range []string{"danm-webhook-config", "danm-mixed-webhook"} {
		if _, err := configurations.Get(ctx, name, metav1.GetOptions{}); err != nil {
			t.Fatalf("webhook configuration %s was deleted: %v", name, err)
		}
	}
	if _, err := handle.Kube.RbacV1().ClusterRoles().Get(ctx, "caas:danm", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("the old ClusterRole is still there: %v", err)
	}
	if _, err := handle.Kube.RbacV1().ClusterRoleBindings().Get(ctx, "caas:danm", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("the old ClusterRoleBinding is still there: %v", err)
	}
	for _, name := range []string{"danm-mixed", RbacName(ComponentCni)} {
		if _, err := handle.Kube.RbacV1().ClusterRoles().Get(ctx, name, metav1.GetOptions{}); err != nil {
			t.Fatalf("ClusterRole %s was deleted: %v", name, err)
		}
	}
	netwatcher := readDaemonSet(t, handle, "netwatcher")
	if _, hasOld := netwatcher.Spec.Template.Annotations[OldInterfacesKey]; hasOld {
		t.Fatal("the netwatcher template still carries the old annotation")
	}
	if _, hasNew := netwatcher.Spec.Template.Annotations[NewInterfacesKey]; !hasNew {
		t.Fatal("the netwatcher template lost the new annotation")
	}
	if _, err := handle.Kube.AppsV1().DaemonSets(state.DefaultNamespace).Get(ctx, CniCopierName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("the CNI copier was left behind: %v", err)
	}
	report := out.String()
	for _, wanted := range []string{"does not belong to the detected production mode", "carries rules on the " + OldGroup + " group as well as on others", "grants the " + OldGroup + " group alongside others"} {
		if !strings.Contains(report, wanted) {
			t.Fatalf("the report misses %q: %s", wanted, report)
		}
	}
	if detectCleanupStep(t, handle) != engine.StateDone {
		t.Fatal("step 14 does not read as done after it ran")
	}
}

func TestRemoveOldApiKeepsTheRolledWebhookConfiguration(t *testing.T) {
	handle, out := cleanupHandle(t, cluster.ModeLightweight, webhookConfigurationOf("danm-webhook-config", OldGroup))
	ctx := context.Background()
	recordRolledWebhook(t, handle, "danm-webhook-config")
	if err := (removeOldApi{}).Execute(ctx, handle); err != nil {
		t.Fatal(err)
	}
	if _, err := handle.Kube.AdmissionregistrationV1().MutatingWebhookConfigurations().Get(ctx, "danm-webhook-config", metav1.GetOptions{}); err != nil {
		t.Fatalf("the configuration step 10 rolled was deleted: %v", err)
	}
	if !strings.Contains(out.String(), "it is the configuration step 10 pointed at") {
		t.Fatalf("keeping the rolled configuration is not reported: %s", out.String())
	}
}

func TestRemoveOldApiRefusesWhileTheClusterIsNotReady(t *testing.T) {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "web",
		Annotations: map[string]string{OldInterfacesKey: `[{"network":"internal"}]`}}}
	handle, _ := cleanupHandle(t, cluster.ModeLightweight, pod)
	ctx := context.Background()
	err := (removeOldApi{}).Execute(ctx, handle)
	if err == nil || !strings.Contains(err.Error(), "app/web") {
		t.Fatalf("a Pod carrying only the old key did not stop the step: %v", err)
	}
	if _, err := handle.ApiExt.ApiextensionsV1().CustomResourceDefinitions().Get(ctx, kindDanmEp.OldCrdName(), metav1.GetOptions{}); err != nil {
		t.Fatalf("a refused step deleted a CRD: %v", err)
	}
}

func TestRemoveOldApiRefusesWithoutInstalledBinaries(t *testing.T) {
	handle, _ := cleanupHandle(t, cluster.ModeLightweight)
	ctx := context.Background()
	if _, err := handle.State.Mutate(ctx, PlanID, func(record *state.PlanRecord) error {
		record.Step(12).Snapshots = nil
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	err := (removeOldApi{}).Execute(ctx, handle)
	if err == nil || !strings.Contains(err.Error(), "step 12 did not record") {
		t.Fatalf("an uninstalled CNI binary did not stop the step: %v", err)
	}
}

func TestRemoveOldApiRefusesOnAnUnconvertedTemplate(t *testing.T) {
	handle, _ := cleanupHandle(t, cluster.ModeLightweight)
	ctx := context.Background()
	netwatcher := readDaemonSet(t, handle, "netwatcher")
	netwatcher.Spec.Template.Annotations = map[string]string{OldInterfacesKey: `[{"network":"internal"}]`}
	if _, err := handle.Kube.AppsV1().DaemonSets(DanmNamespace).Update(ctx, netwatcher, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	err := (removeOldApi{}).Execute(ctx, handle)
	if err == nil || !strings.Contains(err.Error(), "run step 11 again") {
		t.Fatalf("a template carrying only the old key did not stop the step: %v", err)
	}
}

func TestRemoveOldApiWarnsAboutWhatIsLost(t *testing.T) {
	handle, _ := cleanupHandle(t, cluster.ModeLightweight)
	ctx := context.Background()
	createSource(t, handle, kindDanmNet, "app", "internal", nil, map[string]any{"spec": map[string]any{"NetworkID": "internal"}})
	if _, err := handle.State.Mutate(ctx, PlanID, func(record *state.PlanRecord) error {
		record.PrunedFieldsAccepted = true
		record.PrunedObjects = 3
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	warnings, err := (removeOldApi{}).Warnings(ctx, handle)
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(warnings, "\n")
	if !strings.Contains(joined, "CRD danmnets.danm.k8s.io is deleted with the 1 DanmNets it holds") {
		t.Fatalf("the warnings miss the objects about to be deleted: %s", joined)
	}
	if !strings.Contains(joined, "3 objects hold fields") {
		t.Fatalf("the warnings miss the pruned fields: %s", joined)
	}
}

func TestRemoveOldApiCannotBeRolledBack(t *testing.T) {
	handle, _ := cleanupHandle(t, cluster.ModeLightweight)
	if err := (removeOldApi{}).Rollback(context.Background(), handle); err == nil || !strings.Contains(err.Error(), "irreversible") {
		t.Fatalf("rolling back step 14 returned %v", err)
	}
}

func TestCopyCniBinariesDeletesTheBackups(t *testing.T) {
	target := t.TempDir()
	marker := filepath.Join(t.TempDir(), "done")
	for _, binary := range PayloadBinaries {
		if err := os.WriteFile(filepath.Join(target, binary), []byte("4.4 "+binary), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(target, binary+cniBackupSuffix()), []byte("4.3 "+binary), 0755); err != nil {
			t.Fatal(err)
		}
	}
	config := CniCopyConfig{Target: target, Suffix: cniBackupSuffix(), Marker: marker, Cleanup: true}
	if err := CopyCniBinaries(context.Background(), config, io.Discard); err != nil {
		t.Fatal(err)
	}
	for _, binary := range PayloadBinaries {
		if _, err := os.Stat(filepath.Join(target, binary+cniBackupSuffix())); !os.IsNotExist(err) {
			t.Fatalf("the backup of %s is still there: %v", binary, err)
		}
		live, err := os.ReadFile(filepath.Join(target, binary))
		if err != nil || string(live) != "4.4 "+binary {
			t.Fatalf("%s holds %q: %v", binary, live, err)
		}
	}
	if err := CopyCniBinaries(context.Background(), config, io.Discard); err != nil {
		t.Fatalf("a second cleanup failed: %v", err)
	}
}
