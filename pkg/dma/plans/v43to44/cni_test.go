package v43to44

import (
	"bytes"
	"context"
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
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	kubefake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func TestCopyCniBinariesInstallsAndRestores(t *testing.T) {
	payload, target := t.TempDir(), t.TempDir()
	marker := filepath.Join(t.TempDir(), "done")
	for _, binary := range PayloadBinaries {
		if err := os.WriteFile(filepath.Join(payload, binary), []byte("4.4 "+binary), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(target, binary), []byte("4.3 "+binary), 0755); err != nil {
			t.Fatal(err)
		}
	}
	config := CniCopyConfig{Payload: payload, Target: target, Suffix: cniBackupSuffix(), Marker: marker}
	if err := CopyCniBinaries(context.Background(), config, io.Discard); err != nil {
		t.Fatal(err)
	}
	for _, binary := range PayloadBinaries {
		live, err := os.ReadFile(filepath.Join(target, binary))
		if err != nil {
			t.Fatal(err)
		}
		if string(live) != "4.4 "+binary {
			t.Fatalf("%s holds %q", binary, live)
		}
		backup, err := os.ReadFile(filepath.Join(target, binary+cniBackupSuffix()))
		if err != nil {
			t.Fatal(err)
		}
		if string(backup) != "4.3 "+binary {
			t.Fatalf("the backup of %s holds %q", binary, backup)
		}
		info, err := os.Stat(filepath.Join(target, binary))
		if err != nil || info.Mode().Perm()&0111 == 0 {
			t.Fatalf("%s is not executable: %v", binary, err)
		}
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("no marker file was written: %v", err)
	}
	if err := CopyCniBinaries(context.Background(), config, io.Discard); err != nil {
		t.Fatal(err)
	}
	backup, err := os.ReadFile(filepath.Join(target, PayloadBinaries[0]+cniBackupSuffix()))
	if err != nil {
		t.Fatal(err)
	}
	if string(backup) != "4.3 "+PayloadBinaries[0] {
		t.Fatalf("a second run overwrote the backup with %q", backup)
	}
	config.Restore = true
	if err := CopyCniBinaries(context.Background(), config, io.Discard); err != nil {
		t.Fatal(err)
	}
	for _, binary := range PayloadBinaries {
		live, err := os.ReadFile(filepath.Join(target, binary))
		if err != nil {
			t.Fatal(err)
		}
		if string(live) != "4.3 "+binary {
			t.Fatalf("the restore left %s at %q", binary, live)
		}
		if _, err := os.Stat(filepath.Join(target, binary+cniBackupSuffix())); !os.IsNotExist(err) {
			t.Fatalf("the restore kept the backup of %s", binary)
		}
	}
}

func cniHandle(t *testing.T, objects ...k8sruntime.Object) *cluster.Handle {
	t.Helper()
	interval, workloadInterval := cniPollInterval, workloadPollInterval
	cniPollInterval, workloadPollInterval = 5*time.Millisecond, 5*time.Millisecond
	t.Cleanup(func() { cniPollInterval, workloadPollInterval = interval, workloadInterval })
	kube := kubefake.NewSimpleClientset(objects...)
	kube.PrependReactor("create", "daemonsets", func(action k8stesting.Action) (bool, k8sruntime.Object, error) {
		daemonSet := action.(k8stesting.CreateAction).GetObject().(*appsv1.DaemonSet)
		daemonSet.Status = appsv1.DaemonSetStatus{DesiredNumberScheduled: 3, UpdatedNumberScheduled: 3, NumberReady: 3}
		return false, nil, nil
	})
	kube.PrependReactor("create", "jobs", func(action k8stesting.Action) (bool, k8sruntime.Object, error) {
		job := action.(k8stesting.CreateAction).GetObject().(*batchv1.Job)
		job.Status = batchv1.JobStatus{Succeeded: 1}
		return false, nil, nil
	})
	kube.PrependReactor("create", "pods", func(action k8stesting.Action) (bool, k8sruntime.Object, error) {
		pod := action.(k8stesting.CreateAction).GetObject().(*corev1.Pod)
		pod.Status = corev1.PodStatus{Phase: corev1.PodRunning}
		return false, nil, nil
	})
	listKinds := map[schema.GroupVersionResource]string{}
	for _, kind := range []apiKind{kindDanmNet, kindTenantNetwork, kindClusterNetwork, kindTenantConfig, kindDanmEp, kindReservedIP} {
		listKinds[kind.OldResource()] = kind.Kind + "List"
		listKinds[kind.NewResource()] = kind.Kind + "List"
	}
	handle := &cluster.Handle{
		Kube:        kube,
		Dynamic:     dynamicfake.NewSimpleDynamicClientWithCustomListKinds(k8sruntime.NewScheme(), listKinds),
		State:       state.NewStore(kube, state.DefaultNamespace, state.DefaultName),
		Namespace:   state.DefaultNamespace,
		Mode:        cluster.ModeProduction,
		PayloadRoot: "/payload",
		Operator:    "tester",
		Options:     cluster.Options{BinaryImage: "registry.example.com/dma:1.0", RolloutTimeout: 200 * time.Millisecond},
		Log:         log.New(io.Discard, "", 0),
		Out:         &bytes.Buffer{},
	}
	if _, err := handle.State.Mutate(context.Background(), PlanID, func(record *state.PlanRecord) error {
		record.UpsertStep(state.StepRecord{ID: 12, Name: "install-cni-binaries", Status: state.StatusRunning})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	handle.BeginStep(PlanID, 12)
	return handle
}

func detectCniStep(t *testing.T, handle *cluster.Handle) engine.StepState {
	t.Helper()
	detected, err := installCniBinaries{}.Detect(context.Background(), handle)
	if err != nil {
		t.Fatalf("detecting step 12 failed: %v", err)
	}
	return detected
}

func TestInstallCniBinariesWithoutCanary(t *testing.T) {
	handle := cniHandle(t)
	handle.Options.NoCanary = true
	ctx := context.Background()
	modes := []string{}
	handle.Kube.(*kubefake.Clientset).PrependReactor("create", "daemonsets", func(action k8stesting.Action) (bool, k8sruntime.Object, error) {
		daemonSet := action.(k8stesting.CreateAction).GetObject().(*appsv1.DaemonSet)
		modes = append(modes, strings.Join(daemonSet.Spec.Template.Spec.Containers[0].Command, " "))
		return false, nil, nil
	})
	if detectCniStep(t, handle) != engine.StateNotStarted {
		t.Fatal("step 12 does not read as not started before it ran")
	}
	if err := (installCniBinaries{}).Execute(ctx, handle); err != nil {
		t.Fatal(err)
	}
	if detectCniStep(t, handle) != engine.StateDone {
		t.Fatal("step 12 does not read as done after it ran")
	}
	if len(modes) != 1 || !strings.Contains(modes[0], "cni install") || !strings.Contains(modes[0], "/payload/"+PayloadName) {
		t.Fatalf("the copier ran %v", modes)
	}
	if _, err := handle.Kube.AppsV1().DaemonSets(handle.Namespace).Get(ctx, CniCopierName, metav1.GetOptions{}); err == nil {
		t.Fatal("the copier DaemonSet was left behind")
	}
	if _, err := handle.Kube.BatchV1().Jobs(handle.Namespace).Get(ctx, CniCanaryName, metav1.GetOptions{}); err == nil {
		t.Fatal("a canary Job ran although --no-canary was given")
	}
	if err := (installCniBinaries{}).Rollback(ctx, handle); err != nil {
		t.Fatal(err)
	}
	if len(modes) != 2 || !strings.Contains(modes[1], "cni restore") {
		t.Fatalf("the rollback ran %v", modes)
	}
}

func TestInstallCniBinariesRunsTheCanary(t *testing.T) {
	handle := cniHandle(t, &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "worker-0"},
		Status:     corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}},
	})
	ctx := context.Background()
	createSource(t, handle, kindTenantNetwork, "app", "internal", nil,
		map[string]any{"spec": map[string]any{"NetworkID": "internal", "Options": map[string]any{"cidr": "10.0.0.0/24", "alloc": "gAAA"}}})
	if err := (copyNetworkObjects{}).Execute(ctx, handle); err != nil {
		t.Fatal(err)
	}
	createSource(t, handle, kindDanmEp, "app", "canary-ep", nil,
		map[string]any{"spec": map[string]any{"NetworkName": "internal", "Pod": CniCanaryName, "apiType": kindTenantNetwork.Kind,
			"Interface": map[string]any{"Name": "eth0", "Address": "10.0.0.5/24"}}})
	if err := (copyDanmEps{}).Execute(ctx, handle); err != nil {
		t.Fatal(err)
	}
	if err := (installCniBinaries{}).Execute(ctx, handle); err != nil {
		t.Fatal(err)
	}
	if detectCniStep(t, handle) != engine.StateDone {
		t.Fatal("step 12 does not read as done after the canary and the rollout")
	}
	if _, err := handle.Kube.CoreV1().Pods("app").Get(ctx, CniCanaryName, metav1.GetOptions{}); err == nil {
		t.Fatal("the canary Pod was left behind")
	}
}

func TestInstallCniBinariesFailsWithoutACanaryAddress(t *testing.T) {
	handle := cniHandle(t, &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "worker-0"},
		Status:     corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}},
	})
	ctx := context.Background()
	createSource(t, handle, kindTenantNetwork, "app", "internal", nil,
		map[string]any{"spec": map[string]any{"NetworkID": "internal", "Options": map[string]any{"cidr": "10.0.0.0/24", "alloc": "gAAA"}}})
	if err := (copyNetworkObjects{}).Execute(ctx, handle); err != nil {
		t.Fatal(err)
	}
	err := (installCniBinaries{}).Execute(ctx, handle)
	if err == nil || !strings.Contains(err.Error(), "no DanmEp") {
		t.Fatalf("a canary Pod without a DanmEp was accepted: %v", err)
	}
	if detectCniStep(t, handle) != engine.StatePartial {
		t.Fatal("step 12 does not read as partial while the canary of a failed run is left for inspection")
	}
}
