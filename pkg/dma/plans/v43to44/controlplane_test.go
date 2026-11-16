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
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	kubefake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func componentDaemonSet(name, container, image string, annotations map[string]string, arguments ...string) *appsv1.DaemonSet {
	return &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{Namespace: DanmNamespace, Name: name},
		Spec: appsv1.DaemonSetSpec{
			UpdateStrategy: appsv1.DaemonSetUpdateStrategy{Type: appsv1.OnDeleteDaemonSetStrategyType},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Annotations: annotations},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: container, Image: image, Args: arguments}}},
			},
		},
		Status: appsv1.DaemonSetStatus{DesiredNumberScheduled: 2, UpdatedNumberScheduled: 2, NumberReady: 2},
	}
}

func controlPlaneHandle(t *testing.T, ready bool, present, configured map[string]bool, objects ...k8sruntime.Object) *cluster.Handle {
	t.Helper()
	interval := workloadPollInterval
	workloadPollInterval = 5 * time.Millisecond
	t.Cleanup(func() { workloadPollInterval = interval })
	kube := kubefake.NewSimpleClientset(objects...)
	kube.PrependReactor("update", "daemonsets", func(action k8stesting.Action) (bool, k8sruntime.Object, error) {
		daemonSet := action.(k8stesting.UpdateAction).GetObject().(*appsv1.DaemonSet)
		daemonSet.Status = appsv1.DaemonSetStatus{DesiredNumberScheduled: 2, UpdatedNumberScheduled: 2, NumberReady: 2}
		if !ready {
			daemonSet.Status = appsv1.DaemonSetStatus{DesiredNumberScheduled: 2, UpdatedNumberScheduled: 1, NumberReady: 1, NumberUnavailable: 1}
		}
		return false, nil, nil
	})
	handle := &cluster.Handle{
		Kube:      kube,
		State:     state.NewStore(kube, state.DefaultNamespace, state.DefaultName),
		Namespace: state.DefaultNamespace,
		Mode:      cluster.ModeProduction,
		Images:    cluster.ImageConfig{RegistryPrefix: "registry.example.com/", Tag: "4.4"},
		Operator:  "tester",
		Options:   cluster.Options{RolloutTimeout: 50 * time.Millisecond},
		Log:       log.New(io.Discard, "", 0),
		Out:       &bytes.Buffer{},
	}
	names := map[string]string{ComponentNetwatcher: "netwatcher", ComponentSvcwatcher: "svcwatcher", ComponentCleaner: "danm-cleaner"}
	if _, err := handle.State.Mutate(context.Background(), PlanID, func(record *state.PlanRecord) error {
		record.Workloads = map[string]state.WorkloadRecord{}
		for component, name := range names {
			ref := cluster.WorkloadRef{Kind: cluster.WorkloadDaemonSet, Namespace: DanmNamespace, Name: name}
			record.Workloads[component] = cluster.RecordWorkload(ref, present[component], configured[component])
		}
		record.UpsertStep(state.StepRecord{ID: 11, Name: "roll-control-plane", Status: state.StatusRunning})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	handle.BeginStep(PlanID, 11)
	return handle
}

func readDaemonSet(t *testing.T, handle *cluster.Handle, name string) *appsv1.DaemonSet {
	t.Helper()
	daemonSet, err := handle.Kube.AppsV1().DaemonSets(DanmNamespace).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return daemonSet
}

func detectControlPlaneStep(t *testing.T, handle *cluster.Handle) engine.StepState {
	t.Helper()
	detected, err := rollControlPlane{}.Detect(context.Background(), handle)
	if err != nil {
		t.Fatalf("detecting step 11 failed: %v", err)
	}
	return detected
}

func TestRollControlPlane(t *testing.T) {
	interfaces := `[{"tenantNetwork":"internal","ip":"dynamic"}]`
	handle := controlPlaneHandle(t, true,
		map[string]bool{ComponentNetwatcher: true, ComponentSvcwatcher: true}, nil,
		componentDaemonSet("netwatcher", "netwatcher", "registry.example.com/netwatcher:4.3", map[string]string{OldInterfacesKey: interfaces}),
		componentDaemonSet("svcwatcher", "svcwatcher", "registry.example.com/svcwatcher:4.3", nil, "--logtostderr", "--kubeconfig=/etc/kubeconfig"),
	)
	ctx := context.Background()
	if detectControlPlaneStep(t, handle) != engine.StateNotStarted {
		t.Fatal("step 11 does not read as not started before it ran")
	}
	if err := (rollControlPlane{}).Execute(ctx, handle); err != nil {
		t.Fatal(err)
	}
	if detectControlPlaneStep(t, handle) != engine.StateDone {
		t.Fatal("step 11 does not read as done after it ran")
	}
	netwatcher := readDaemonSet(t, handle, "netwatcher")
	if netwatcher.Spec.Template.Spec.Containers[0].Image != "registry.example.com/netwatcher:4.4" {
		t.Fatalf("netwatcher runs %s", netwatcher.Spec.Template.Spec.Containers[0].Image)
	}
	if netwatcher.Spec.Template.Spec.PriorityClassName != "system-node-critical" {
		t.Fatalf("netwatcher has the priority class %q", netwatcher.Spec.Template.Spec.PriorityClassName)
	}
	if netwatcher.Spec.Template.Spec.TerminationGracePeriodSeconds == nil || *netwatcher.Spec.Template.Spec.TerminationGracePeriodSeconds != 15 {
		t.Fatalf("netwatcher has the grace period %v", netwatcher.Spec.Template.Spec.TerminationGracePeriodSeconds)
	}
	annotations := netwatcher.Spec.Template.Annotations
	if annotations[NewInterfacesKey] != interfaces || annotations[OldInterfacesKey] != interfaces {
		t.Fatalf("the template of netwatcher carries %v", annotations)
	}
	if len(netwatcher.Spec.Template.Spec.Tolerations) != 2 || netwatcher.Spec.Template.Spec.NodeSelector != nil {
		t.Fatalf("netwatcher is scheduled with %+v and %v", netwatcher.Spec.Template.Spec.Tolerations, netwatcher.Spec.Template.Spec.NodeSelector)
	}
	svcwatcher := readDaemonSet(t, handle, "svcwatcher")
	if svcwatcher.Spec.Template.Spec.PriorityClassName != "system-cluster-critical" {
		t.Fatalf("svcwatcher has the priority class %q", svcwatcher.Spec.Template.Spec.PriorityClassName)
	}
	if _, found := svcwatcher.Spec.Template.Spec.NodeSelector[ControlPlaneLabel]; !found {
		t.Fatalf("svcwatcher is scheduled with %v", svcwatcher.Spec.Template.Spec.NodeSelector)
	}
	if len(svcwatcher.Spec.Template.Spec.Tolerations) != 2 {
		t.Fatalf("svcwatcher tolerates %+v", svcwatcher.Spec.Template.Spec.Tolerations)
	}
	if strings.Join(svcwatcher.Spec.Template.Spec.Containers[0].Args, " ") != "--kubeconfig=/etc/kubeconfig" {
		t.Fatalf("svcwatcher runs with %v", svcwatcher.Spec.Template.Spec.Containers[0].Args)
	}
	if svcwatcher.Spec.UpdateStrategy.Type != appsv1.OnDeleteDaemonSetStrategyType {
		t.Fatalf("the rollout strategy of svcwatcher was not restored: %+v", svcwatcher.Spec.UpdateStrategy)
	}
	if _, err := handle.Kube.AppsV1().DaemonSets(DanmNamespace).Get(ctx, "danm-cleaner", metav1.GetOptions{}); err == nil {
		t.Fatal("the undeployed cleaner was created")
	}
	if err := (rollControlPlane{}).Rollback(ctx, handle); err != nil {
		t.Fatal(err)
	}
	if detectControlPlaneStep(t, handle) != engine.StateNotStarted {
		t.Fatal("step 11 does not read as not started after its rollback")
	}
	netwatcher = readDaemonSet(t, handle, "netwatcher")
	if netwatcher.Spec.Template.Spec.Containers[0].Image != "registry.example.com/netwatcher:4.3" ||
		netwatcher.Spec.Template.Spec.PriorityClassName != "" ||
		netwatcher.Spec.Template.Annotations[NewInterfacesKey] != "" {
		t.Fatalf("the rollback left netwatcher at %+v", netwatcher.Spec.Template)
	}
	svcwatcher = readDaemonSet(t, handle, "svcwatcher")
	if strings.Join(svcwatcher.Spec.Template.Spec.Containers[0].Args, " ") != "--logtostderr --kubeconfig=/etc/kubeconfig" {
		t.Fatalf("the rollback left svcwatcher with %v", svcwatcher.Spec.Template.Spec.Containers[0].Args)
	}
}

func TestRollControlPlaneRollsSvcwatcherAllAtOnce(t *testing.T) {
	handle := controlPlaneHandle(t, true,
		map[string]bool{ComponentNetwatcher: true, ComponentSvcwatcher: true}, nil,
		componentDaemonSet("netwatcher", "netwatcher", "registry.example.com/netwatcher:4.3", nil),
		componentDaemonSet("svcwatcher", "svcwatcher", "registry.example.com/svcwatcher:4.3", nil),
	)
	strategies := []appsv1.DaemonSetUpdateStrategy{}
	handle.Kube.(*kubefake.Clientset).PrependReactor("update", "daemonsets", func(action k8stesting.Action) (bool, k8sruntime.Object, error) {
		daemonSet := action.(k8stesting.UpdateAction).GetObject().(*appsv1.DaemonSet)
		if daemonSet.Name == "svcwatcher" {
			strategies = append(strategies, daemonSet.Spec.UpdateStrategy)
		}
		return false, nil, nil
	})
	if err := (rollControlPlane{}).Execute(context.Background(), handle); err != nil {
		t.Fatal(err)
	}
	if len(strategies) != 2 {
		t.Fatalf("svcwatcher was written %d times", len(strategies))
	}
	rolling := strategies[0].RollingUpdate
	if strategies[0].Type != appsv1.RollingUpdateDaemonSetStrategyType || rolling == nil ||
		rolling.MaxUnavailable.StrVal != "100%" || rolling.MaxSurge.IntValue() != 0 {
		t.Fatalf("svcwatcher was rolled with %+v", strategies[0])
	}
	if strategies[1].Type != appsv1.OnDeleteDaemonSetStrategyType {
		t.Fatalf("the strategy restored is %+v", strategies[1])
	}
}

func TestRollControlPlaneRevertsOnATimeout(t *testing.T) {
	handle := controlPlaneHandle(t, false,
		map[string]bool{ComponentNetwatcher: true}, nil,
		componentDaemonSet("netwatcher", "netwatcher", "registry.example.com/netwatcher:4.3", nil),
	)
	ctx := context.Background()
	err := (rollControlPlane{}).Execute(ctx, handle)
	if err == nil || !strings.Contains(err.Error(), "not ready") {
		t.Fatalf("a component which never became ready was accepted: %v", err)
	}
	netwatcher := readDaemonSet(t, handle, "netwatcher")
	if netwatcher.Spec.Template.Spec.Containers[0].Image != "registry.example.com/netwatcher:4.3" {
		t.Fatalf("the failed roll was not reverted, netwatcher runs %s", netwatcher.Spec.Template.Spec.Containers[0].Image)
	}
}

func TestRollControlPlaneNeedsNetwatcher(t *testing.T) {
	handle := controlPlaneHandle(t, true, map[string]bool{ComponentSvcwatcher: true}, nil)
	err := (rollControlPlane{}).Execute(context.Background(), handle)
	if err == nil || !strings.Contains(err.Error(), "netwatcher") {
		t.Fatalf("the step ran without a recorded netwatcher: %v", err)
	}
}

func TestRollControlPlaneDeploysAConfiguredComponent(t *testing.T) {
	handle := controlPlaneHandle(t, true,
		map[string]bool{ComponentNetwatcher: true},
		map[string]bool{ComponentCleaner: true},
		componentDaemonSet("netwatcher", "netwatcher", "registry.example.com/netwatcher:4.3", nil),
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "control-0", Labels: map[string]string{ControlPlaneLabel: ""}}},
	)
	ctx := context.Background()
	if detectControlPlaneStep(t, handle) != engine.StateNotStarted {
		t.Fatal("step 11 does not read as not started while a configured component is missing")
	}
	if err := (rollControlPlane{}).Execute(ctx, handle); err != nil {
		t.Fatal(err)
	}
	if detectControlPlaneStep(t, handle) != engine.StateDone {
		t.Fatal("step 11 does not read as done after it deployed the configured component")
	}
	cleaner := readDaemonSet(t, handle, "danm-cleaner")
	if cleaner.Spec.Template.Spec.Containers[0].Image != "registry.example.com/cleaner:4.4" {
		t.Fatalf("the deployed cleaner runs %s", cleaner.Spec.Template.Spec.Containers[0].Image)
	}
	if cleaner.Spec.Template.Spec.PriorityClassName != "system-cluster-critical" ||
		cleaner.Spec.Template.Spec.NodeSelector[ControlPlaneLabel] != "" ||
		len(cleaner.Spec.Template.Spec.Tolerations) != 2 {
		t.Fatalf("the deployed cleaner is scheduled as %+v", cleaner.Spec.Template.Spec)
	}
	if _, err := handle.Kube.CoreV1().ServiceAccounts(DanmNamespace).Get(ctx, "danm-cleaner", metav1.GetOptions{}); err != nil {
		t.Fatalf("the ServiceAccount of the deployed cleaner is missing: %v", err)
	}
	if err := (rollControlPlane{}).Rollback(ctx, handle); err != nil {
		t.Fatal(err)
	}
	if _, err := handle.Kube.AppsV1().DaemonSets(DanmNamespace).Get(ctx, "danm-cleaner", metav1.GetOptions{}); err == nil {
		t.Fatal("the rollback kept the deployed cleaner")
	}
	if _, err := handle.Kube.CoreV1().ServiceAccounts(DanmNamespace).Get(ctx, "danm-cleaner", metav1.GetOptions{}); err == nil {
		t.Fatal("the rollback kept the ServiceAccount of the deployed cleaner")
	}
}

func TestRollControlPlaneWarnsWithoutControlPlaneNodes(t *testing.T) {
	handle := controlPlaneHandle(t, true,
		map[string]bool{ComponentNetwatcher: true},
		map[string]bool{ComponentSvcwatcher: true},
		componentDaemonSet("netwatcher", "netwatcher", "registry.example.com/netwatcher:4.3", nil),
	)
	if err := (rollControlPlane{}).Execute(context.Background(), handle); err != nil {
		t.Fatal(err)
	}
	warnings := strings.Join(handle.TakeWarnings(), "\n")
	if !strings.Contains(warnings, ControlPlaneLabel) {
		t.Fatalf("the step did not warn about the missing control plane label: %q", warnings)
	}
}
