package v43to44

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/danm-cni/danm-utils/pkg/dma/cluster"
	"github.com/danm-cni/danm-utils/pkg/dma/engine"
	"github.com/danm-cni/danm-utils/pkg/dma/state"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"
)

const (
	ComponentCni          = "cni"
	rbacSubjectsKey       = "subjects"
	defaultServiceAccount = "default"
)

//go:embed manifests/rbac
var rbacManifests embed.FS

var rbacComponents = []string{ComponentCni, ComponentWebhook, ComponentNetwatcher, ComponentSvcwatcher, ComponentCleaner}

func RbacName(component string) string {
	return "danm:" + component + "-" + TargetRelease
}

func targetRules(component string) ([]rbacv1.PolicyRule, error) {
	raw, err := rbacManifests.ReadFile("manifests/rbac/" + component + ".yaml")
	if err != nil {
		return nil, fmt.Errorf("the assistant carries no ClusterRole for %s: %w", component, err)
	}
	role := &rbacv1.ClusterRole{}
	if err := yaml.UnmarshalStrict(raw, role); err != nil {
		return nil, fmt.Errorf("the embedded ClusterRole of %s is malformed: %w", component, err)
	}
	if role.Name != RbacName(component) {
		return nil, fmt.Errorf("the embedded ClusterRole of %s is named %s instead of %s", component, role.Name, RbacName(component))
	}
	return role.Rules, nil
}

type installNewRbac struct{}

func (installNewRbac) Meta() engine.StepMeta {
	return engine.StepMeta{
		ID:          3,
		Name:        "install-new-rbac",
		Description: "Authorise the DANM components for the danm.io group alongside the danm.k8s.io one.",
		Phase:       engine.PhaseDeploy,
		Reversible:  true,
	}
}

func (installNewRbac) Detect(ctx context.Context, handle *cluster.Handle) (engine.StepState, error) {
	existing := 0
	for _, component := range rbacComponents {
		for _, found := range []func() (bool, error){
			func() (bool, error) { return clusterRoleExists(ctx, handle, RbacName(component)) },
			func() (bool, error) { return clusterRoleBindingExists(ctx, handle, RbacName(component)) },
		} {
			exists, err := found()
			if err != nil {
				return engine.StateNotStarted, err
			}
			if exists {
				existing++
			}
		}
	}
	if existing == 0 {
		return engine.StateNotStarted, nil
	}
	subjects, err := recordedSubjects(ctx, handle)
	if err != nil {
		return engine.StateNotStarted, err
	}
	if subjects == nil {
		return engine.StatePartial, nil
	}
	for _, component := range rbacComponents {
		subject, expected := subjects[component]
		if !expected {
			continue
		}
		current, err := rbacCurrent(ctx, handle, component, subject)
		if err != nil {
			return engine.StateNotStarted, err
		}
		if !current {
			return engine.StatePartial, nil
		}
	}
	return engine.StateDone, nil
}

func (installNewRbac) Execute(ctx context.Context, handle *cluster.Handle) error {
	subjects, err := rbacSubjects(ctx, handle)
	if err != nil {
		return err
	}
	encoded, err := json.Marshal(subjects)
	if err != nil {
		return fmt.Errorf("cannot serialise the RBAC subjects: %w", err)
	}
	if err := handle.SaveSnapshot(ctx, rbacSubjectsKey, string(encoded)); err != nil {
		return err
	}
	for _, component := range rbacComponents {
		subject, expected := subjects[component]
		if !expected {
			handle.Printf("%s is not deployed, it gets no ClusterRole.", component)
			continue
		}
		rules, err := targetRules(component)
		if err != nil {
			return err
		}
		if err := ensureClusterRole(ctx, handle, RbacName(component), rules); err != nil {
			return err
		}
		if err := ensureClusterRoleBinding(ctx, handle, RbacName(component), subject); err != nil {
			return err
		}
		handle.Printf("ClusterRole and ClusterRoleBinding %s bind %s.", RbacName(component), cluster.SubjectString(subject))
	}
	return nil
}

func (installNewRbac) Rollback(ctx context.Context, handle *cluster.Handle) error {
	for _, component := range rbacComponents {
		name := RbacName(component)
		err := handle.Kube.RbacV1().ClusterRoleBindings().Delete(ctx, name, metav1.DeleteOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("cannot delete ClusterRoleBinding %s: %w", name, err)
		}
		err = handle.Kube.RbacV1().ClusterRoles().Delete(ctx, name, metav1.DeleteOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("cannot delete ClusterRole %s: %w", name, err)
		}
		handle.Printf("Deleted ClusterRole and ClusterRoleBinding %s.", name)
	}
	return nil
}

func rbacSubjects(ctx context.Context, handle *cluster.Handle) (map[string]rbacv1.Subject, error) {
	record, err := handle.State.Load(ctx, PlanID)
	if err != nil {
		return nil, err
	}
	if record == nil || record.Workloads == nil {
		return nil, errors.New("the component workloads are not recorded, run step 1 first")
	}
	subjects := map[string]rbacv1.Subject{}
	for _, component := range rbacComponents {
		if component == ComponentCni {
			continue
		}
		workload, found := record.Workloads[component]
		if !found || !workload.Present {
			continue
		}
		serviceAccount, err := workloadServiceAccount(ctx, handle, cluster.RecordedWorkload(workload))
		if err != nil {
			return nil, err
		}
		subjects[component] = rbacv1.Subject{Kind: rbacv1.ServiceAccountKind, Namespace: workload.Namespace, Name: serviceAccount}
	}
	if handle.CniSubject == nil {
		return nil, fmt.Errorf("the identity the CNI authenticates as is not configured, name it with --cni-subject or as %s in the configuration ConfigMap", cluster.CniSubjectKey)
	}
	subjects[ComponentCni] = *handle.CniSubject
	return subjects, nil
}

func workloadServiceAccount(ctx context.Context, handle *cluster.Handle, ref cluster.WorkloadRef) (string, error) {
	var serviceAccount string
	switch ref.Kind {
	case cluster.WorkloadDaemonSet:
		daemonSet, err := handle.Kube.AppsV1().DaemonSets(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
		if err != nil {
			return "", fmt.Errorf("cannot read %s: %w", ref, err)
		}
		serviceAccount = daemonSet.Spec.Template.Spec.ServiceAccountName
		if serviceAccount == "" {
			serviceAccount = daemonSet.Spec.Template.Spec.DeprecatedServiceAccount
		}
	case cluster.WorkloadDeployment:
		deployment, err := handle.Kube.AppsV1().Deployments(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
		if err != nil {
			return "", fmt.Errorf("cannot read %s: %w", ref, err)
		}
		serviceAccount = deployment.Spec.Template.Spec.ServiceAccountName
		if serviceAccount == "" {
			serviceAccount = deployment.Spec.Template.Spec.DeprecatedServiceAccount
		}
	default:
		return "", fmt.Errorf("workload %s is of an unsupported kind", ref)
	}
	if serviceAccount == "" {
		return defaultServiceAccount, nil
	}
	return serviceAccount, nil
}

func recordedSubjects(ctx context.Context, handle *cluster.Handle) (map[string]rbacv1.Subject, error) {
	record, err := handle.State.Load(ctx, PlanID)
	if err != nil || record == nil {
		return nil, err
	}
	return subjectsOf(record)
}

func subjectsOf(record *state.PlanRecord) (map[string]rbacv1.Subject, error) {
	stepRecord := record.Step(3)
	if stepRecord == nil {
		return nil, nil
	}
	encoded, found := stepRecord.Snapshots[rbacSubjectsKey]
	if !found {
		return nil, nil
	}
	subjects := map[string]rbacv1.Subject{}
	if err := json.Unmarshal([]byte(encoded), &subjects); err != nil {
		return nil, fmt.Errorf("the recorded RBAC subjects are malformed: %w", err)
	}
	return subjects, nil
}

func rbacCurrent(ctx context.Context, handle *cluster.Handle, component string, subject rbacv1.Subject) (bool, error) {
	name := RbacName(component)
	rules, err := targetRules(component)
	if err != nil {
		return false, err
	}
	role, err := handle.Kube.RbacV1().ClusterRoles().Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("cannot read ClusterRole %s: %w", name, err)
	}
	if role.AggregationRule != nil || !equality.Semantic.DeepEqual(role.Rules, rules) {
		return false, nil
	}
	binding, err := handle.Kube.RbacV1().ClusterRoleBindings().Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("cannot read ClusterRoleBinding %s: %w", name, err)
	}
	return binding.RoleRef == roleRef(name) && equality.Semantic.DeepEqual(binding.Subjects, []rbacv1.Subject{subject}), nil
}

func clusterRoleExists(ctx context.Context, handle *cluster.Handle, name string) (bool, error) {
	_, err := handle.Kube.RbacV1().ClusterRoles().Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("cannot read ClusterRole %s: %w", name, err)
	}
	return true, nil
}

func clusterRoleBindingExists(ctx context.Context, handle *cluster.Handle, name string) (bool, error) {
	_, err := handle.Kube.RbacV1().ClusterRoleBindings().Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("cannot read ClusterRoleBinding %s: %w", name, err)
	}
	return true, nil
}

func roleRef(name string) rbacv1.RoleRef {
	return rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: name}
}

func ensureClusterRole(ctx context.Context, handle *cluster.Handle, name string, rules []rbacv1.PolicyRule) error {
	roles := handle.Kube.RbacV1().ClusterRoles()
	existing, err := roles.Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = roles.Create(ctx, &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: name}, Rules: rules}, metav1.CreateOptions{})
		if err != nil {
			return fmt.Errorf("cannot create ClusterRole %s: %w", name, err)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("cannot read ClusterRole %s: %w", name, err)
	}
	if existing.AggregationRule == nil && equality.Semantic.DeepEqual(existing.Rules, rules) {
		return nil
	}
	existing.AggregationRule = nil
	existing.Rules = rules
	if _, err := roles.Update(ctx, existing, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("cannot update ClusterRole %s: %w", name, err)
	}
	return nil
}

func ensureClusterRoleBinding(ctx context.Context, handle *cluster.Handle, name string, subject rbacv1.Subject) error {
	bindings := handle.Kube.RbacV1().ClusterRoleBindings()
	desired := &rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: name}, RoleRef: roleRef(name), Subjects: []rbacv1.Subject{subject}}
	existing, err := bindings.Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		if _, err := bindings.Create(ctx, desired, metav1.CreateOptions{}); err != nil {
			return fmt.Errorf("cannot create ClusterRoleBinding %s: %w", name, err)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("cannot read ClusterRoleBinding %s: %w", name, err)
	}
	if existing.RoleRef != desired.RoleRef {
		if err := bindings.Delete(ctx, name, metav1.DeleteOptions{}); err != nil {
			return fmt.Errorf("cannot replace ClusterRoleBinding %s: %w", name, err)
		}
		if _, err := bindings.Create(ctx, desired, metav1.CreateOptions{}); err != nil {
			return fmt.Errorf("cannot create ClusterRoleBinding %s: %w", name, err)
		}
		return nil
	}
	if equality.Semantic.DeepEqual(existing.Subjects, desired.Subjects) {
		return nil
	}
	existing.Subjects = desired.Subjects
	if _, err := bindings.Update(ctx, existing, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("cannot update ClusterRoleBinding %s: %w", name, err)
	}
	return nil
}
