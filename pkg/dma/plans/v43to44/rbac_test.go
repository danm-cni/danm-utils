package v43to44

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/danm-cni/danm-utils/pkg/dma/cluster"
	"github.com/danm-cni/danm-utils/pkg/dma/engine"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
)

var cniUser = rbacv1.Subject{Kind: rbacv1.UserKind, APIGroup: rbacv1.GroupName, Name: "danm-cni"}

func preflightedHandle(t *testing.T, fixture *fixture, overrides ...map[string]cluster.WorkloadRef) *cluster.Handle {
	t.Helper()
	fixture.options.AcceptDefaultMtu = true
	handle, out := fixture.handle(t)
	for _, override := range overrides {
		handle.Workloads = handle.Workloads.WithOverrides(override)
	}
	if err := execute(t, handle); err != nil {
		t.Fatalf("%v\n%s", err, out.String())
	}
	handle.Options.Force = true
	subject := cniUser
	handle.CniSubject = &subject
	return handle
}

func rbacRunner(handle *cluster.Handle) *engine.Runner {
	return engine.NewRunner(New(), handle, &engine.TokenConfirmer{Supplied: "install-new-rbac", Out: io.Discard})
}

func detectRbac(t *testing.T, handle *cluster.Handle) engine.StepState {
	t.Helper()
	detected, err := installNewRbac{}.Detect(context.Background(), handle)
	if err != nil {
		t.Fatalf("detecting step 3 failed: %v", err)
	}
	return detected
}

func binding(t *testing.T, handle *cluster.Handle, component string) *rbacv1.ClusterRoleBinding {
	t.Helper()
	found, err := handle.Kube.RbacV1().ClusterRoleBindings().Get(context.Background(), RbacName(component), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return found
}

func TestInstallNewRbacBindsTheRecordedIdentities(t *testing.T) {
	netwatcher := healthyDaemonSet("netwatcher")
	netwatcher.Spec.Template.Spec.ServiceAccountName = "netwatcher"
	fixture := productionFixture(t)
	fixture.workloads = []k8sruntime.Object{netwatcher, healthyDeployment("danm-webhook-deployment")}
	handle := preflightedHandle(t, fixture)
	if detectRbac(t, handle) != engine.StateNotStarted {
		t.Fatal("step 3 does not read as not started before it ran")
	}
	if err := rbacRunner(handle).Execute(context.Background(), 3); err != nil {
		t.Fatal(err)
	}
	if detectRbac(t, handle) != engine.StateDone {
		t.Fatal("step 3 does not read as done after it ran")
	}
	if subjects := binding(t, handle, ComponentNetwatcher).Subjects; len(subjects) != 1 || cluster.SubjectString(subjects[0]) != "ServiceAccount/kube-system/netwatcher" {
		t.Fatalf("netwatcher is bound to %v", subjects)
	}
	if subjects := binding(t, handle, ComponentWebhook).Subjects; len(subjects) != 1 || cluster.SubjectString(subjects[0]) != "ServiceAccount/kube-system/default" {
		t.Fatalf("the webhook is bound to %v", subjects)
	}
	if subjects := binding(t, handle, ComponentCni).Subjects; len(subjects) != 1 || subjects[0] != cniUser {
		t.Fatalf("the CNI is bound to %v", subjects)
	}
	role, err := handle.Kube.RbacV1().ClusterRoles().Get(context.Background(), "danm:svcwatcher-4.4", metav1.GetOptions{})
	if err == nil {
		t.Fatalf("a ClusterRole was created for the undeployed svcwatcher: %v", role.Name)
	}
	role, err = handle.Kube.RbacV1().ClusterRoles().Get(context.Background(), "danm:cni-4.4", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.Join(role.Rules[0].Resources, ","), "reservedips/status") || role.Rules[0].APIGroups[0] != NewGroup {
		t.Fatalf("the CNI ClusterRole does not carry the 4.4 rules: %v", role.Rules)
	}
	if err := rbacRunner(handle).Rollback(context.Background(), 3); err != nil {
		t.Fatal(err)
	}
	if detectRbac(t, handle) != engine.StateNotStarted {
		t.Fatal("step 3 does not read as not started after its rollback")
	}
}

func TestInstallNewRbacUsesTailoredWorkloads(t *testing.T) {
	cleaner := healthyDeployment("cleaner")
	cleaner.Namespace = "danm"
	cleaner.Spec.Template.Spec.ServiceAccountName = "danm-cleaner"
	fixture := productionFixture(t)
	fixture.workloads = append(fixture.workloads, cleaner)
	handle := preflightedHandle(t, fixture, map[string]cluster.WorkloadRef{ComponentCleaner: {Kind: cluster.WorkloadDeployment, Namespace: "danm", Name: "cleaner"}})
	if err := rbacRunner(handle).Execute(context.Background(), 3); err != nil {
		t.Fatal(err)
	}
	if subjects := binding(t, handle, ComponentCleaner).Subjects; cluster.SubjectString(subjects[0]) != "ServiceAccount/danm/danm-cleaner" {
		t.Fatalf("the cleaner is bound to %v", subjects)
	}
}

func TestInstallNewRbacCorrectsExistingObjects(t *testing.T) {
	handle := preflightedHandle(t, productionFixture(t))
	name := RbacName(ComponentNetwatcher)
	ctx := context.Background()
	if _, err := handle.Kube.RbacV1().ClusterRoles().Create(ctx, &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: name}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	stale := &rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: name}, RoleRef: roleRef("other"), Subjects: []rbacv1.Subject{cniUser}}
	if _, err := handle.Kube.RbacV1().ClusterRoleBindings().Create(ctx, stale, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if detectRbac(t, handle) != engine.StatePartial {
		t.Fatal("foreign objects of the step's names do not read as partial")
	}
	if err := rbacRunner(handle).Execute(ctx, 3); err != nil {
		t.Fatal(err)
	}
	if detectRbac(t, handle) != engine.StateDone {
		t.Fatal("the corrected objects do not read as done")
	}
	if found := binding(t, handle, ComponentNetwatcher); found.RoleRef.Name != name {
		t.Fatalf("the binding still refers to %s", found.RoleRef.Name)
	}
	role, _ := handle.Kube.RbacV1().ClusterRoles().Get(ctx, name, metav1.GetOptions{})
	role.Rules = nil
	if _, err := handle.Kube.RbacV1().ClusterRoles().Update(ctx, role, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if detectRbac(t, handle) != engine.StatePartial {
		t.Fatal("a ClusterRole which lost its rules does not read as partial")
	}
}

func TestInstallNewRbacNeedsTheRecordedWorkloads(t *testing.T) {
	handle, _ := healthyFixture(cluster.ModeLightweight).handle(t)
	handle.Mode = cluster.ModeLightweight
	handle.Options.Force = true
	err := rbacRunner(handle).Execute(context.Background(), 3)
	if err == nil || !strings.Contains(err.Error(), "run step 1 first") {
		t.Fatalf("step 3 without a preflight returned %v", err)
	}
}

func TestInstallNewRbacFailsWithoutACniIdentity(t *testing.T) {
	handle := preflightedHandle(t, productionFixture(t))
	handle.CniSubject = nil
	err := rbacRunner(handle).Execute(context.Background(), 3)
	if err == nil || !strings.Contains(err.Error(), "--cni-subject") {
		t.Fatalf("step 3 without a CNI identity returned %v", err)
	}
	list, _ := handle.Kube.RbacV1().ClusterRoles().List(context.Background(), metav1.ListOptions{})
	if len(list.Items) != 0 {
		t.Fatalf("ClusterRoles were created although the CNI identity is unknown: %d", len(list.Items))
	}
}

func TestEmbeddedRolesCoverEveryComponent(t *testing.T) {
	for _, component := range rbacComponents {
		rules, err := targetRules(component)
		if err != nil || len(rules) == 0 {
			t.Errorf("component %s has no embedded rules: %v", component, err)
		}
		for _, rule := range rules {
			for _, group := range rule.APIGroups {
				if group == OldGroup {
					t.Errorf("the embedded ClusterRole of %s grants the old group", component)
				}
			}
		}
	}
}
