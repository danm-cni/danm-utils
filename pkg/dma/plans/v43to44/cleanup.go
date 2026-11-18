package v43to44

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/danm-cni/danm-utils/pkg/dma/cluster"
	"github.com/danm-cni/danm-utils/pkg/dma/engine"
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/wait"
)

var crdDeleteTimeout = 5 * time.Minute

type removeOldApi struct{}

func (removeOldApi) Meta() engine.StepMeta {
	return engine.StepMeta{
		ID:          removeOldApiStep,
		Name:        "remove-old-api",
		Description: "Remove the danm.k8s.io webhook configuration, CRDs and RBAC, and the backed up CNI binaries.",
		Phase:       engine.PhaseCleanup,
		Reversible:  false,
	}
}

func (removeOldApi) Warnings(ctx context.Context, handle *cluster.Handle) ([]string, error) {
	if handle.Mode == cluster.ModeUnknown {
		return nil, nil
	}
	warnings := []string{}
	for _, kind := range sourceKinds(handle.Mode) {
		exists, err := oldCrdExists(ctx, handle, kind)
		if err != nil {
			return nil, err
		}
		if !exists {
			continue
		}
		count := 0
		if err := eachObject(ctx, handle, kind.OldResource(), func(_ *unstructured.Unstructured) error {
			count++
			return nil
		}); err != nil {
			return nil, err
		}
		warnings = append(warnings, fmt.Sprintf("CRD %s is deleted with the %d %ss it holds", kind.OldCrdName(), count, kind.Kind))
	}
	record, err := handle.State.Load(ctx, PlanID)
	if err != nil {
		return nil, err
	}
	if record != nil && record.PrunedFieldsAccepted && record.PrunedObjects > 0 {
		warnings = append(warnings, fmt.Sprintf("%d objects hold fields the DANM %s schema does not know, and the %s copies deleted here are the last ones carrying them",
			record.PrunedObjects, TargetRelease, OldGroup))
	}
	return warnings, nil
}

func (removeOldApi) Detect(ctx context.Context, handle *cluster.Handle) (engine.StepState, error) {
	if handle.Mode == cluster.ModeUnknown {
		return engine.StateNotStarted, nil
	}
	kinds := sourceKinds(handle.Mode)
	present := 0
	for _, kind := range kinds {
		exists, err := oldCrdExists(ctx, handle, kind)
		if err != nil {
			return engine.StateNotStarted, err
		}
		if exists {
			present++
		}
	}
	if present == len(kinds) {
		return engine.StateNotStarted, nil
	}
	if present > 0 {
		return engine.StatePartial, nil
	}
	templates, err := templatesWithOldKey(ctx, handle)
	if err != nil {
		return engine.StateNotStarted, err
	}
	if len(templates) > 0 {
		return engine.StatePartial, nil
	}
	return engine.StateDone, nil
}

func (removeOldApi) Execute(ctx context.Context, handle *cluster.Handle) error {
	if handle.Mode == cluster.ModeUnknown {
		return errors.New("the deployment mode is unknown, run step 1 first or name the mode with --mode")
	}
	if err := verifyCleanupIsSafe(ctx, handle); err != nil {
		return err
	}
	if err := removeOldWebhookConfigurations(ctx, handle); err != nil {
		return err
	}
	if err := deleteOldCrds(ctx, handle); err != nil {
		return err
	}
	if err := deleteOldRbac(ctx, handle); err != nil {
		return err
	}
	if err := dropOldInterfacesKey(ctx, handle); err != nil {
		return err
	}
	return removeCniBackups(ctx, handle)
}

func (removeOldApi) Rollback(_ context.Context, _ *cluster.Handle) error {
	return fmt.Errorf("step %d (remove-old-api) is irreversible, the %s group it deleted cannot be put back", removeOldApiStep, OldGroup)
}

func verifyCleanupIsSafe(ctx context.Context, handle *cluster.Handle) error {
	problems := []string{}
	installed, err := cniInstallRecorded(ctx, handle)
	if err != nil {
		return err
	}
	if !installed {
		problems = append(problems, fmt.Sprintf("step 12 did not record the DANM %s CNI binaries as installed, and a node still running the 4.3 binary loses its Pod networking when the %s group goes",
			TargetRelease, OldGroup))
	}
	pods, err := podsWithOldKey(ctx, handle)
	if err != nil {
		return err
	}
	if len(pods) > 0 {
		problems = append(problems, fmt.Sprintf("%d Pods carry only %s and lose their networks on their next restart: %s; run step 5 again", len(pods), OldInterfacesKey, listed(pods)))
	}
	templates, err := templatesWithOldKey(ctx, handle)
	if err != nil {
		return err
	}
	if len(templates) > 0 {
		problems = append(problems, fmt.Sprintf("%d component templates carry only %s: %s; run step 11 again", len(templates), OldInterfacesKey, listed(templates)))
	}
	if len(problems) > 0 {
		return fmt.Errorf("the cluster is not ready to lose the %s group, %d problems:\n  - %s", OldGroup, len(problems), strings.Join(problems, "\n  - "))
	}
	return nil
}

func cniInstallRecorded(ctx context.Context, handle *cluster.Handle) (bool, error) {
	record, err := handle.State.Load(ctx, PlanID)
	if err != nil || record == nil {
		return false, err
	}
	stepRecord := record.Step(installCniBinaries{}.Meta().ID)
	if stepRecord == nil {
		return false, nil
	}
	_, found := stepRecord.Snapshots[cniInstalledKey]
	return found, nil
}

func podsWithOldKey(ctx context.Context, handle *cluster.Handle) ([]string, error) {
	names := []string{}
	err := eachPod(ctx, handle, func(pod *corev1.Pod) error {
		if podNeedsNewInterfaces(pod) {
			names = append(names, objectName(pod.Namespace, pod.Name))
		}
		return nil
	})
	sort.Strings(names)
	return names, err
}

func templatesWithOldKey(ctx context.Context, handle *cluster.Handle) ([]string, error) {
	names := []string{}
	for _, component := range controlPlaneComponents {
		object, err := rolledWorkload(ctx, handle, component)
		if err != nil {
			return nil, err
		}
		if object == nil {
			continue
		}
		annotations := object.template().Annotations
		if _, hasOld := annotations[OldInterfacesKey]; !hasOld {
			continue
		}
		if _, hasNew := annotations[NewInterfacesKey]; !hasNew {
			names = append(names, object.ref.String())
		}
	}
	return names, nil
}

func rolledWorkload(ctx context.Context, handle *cluster.Handle, component string) (*workload, error) {
	workloadState, err := componentState(ctx, handle, component)
	if err != nil {
		return nil, err
	}
	if !workloadState.present && !workloadState.configured {
		return nil, nil
	}
	object, err := loadWorkload(ctx, handle, workloadState.ref)
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return object, nil
}

func removeOldWebhookConfigurations(ctx context.Context, handle *cluster.Handle) error {
	kept, err := rolledWebhookConfiguration(ctx, handle)
	if err != nil {
		return err
	}
	mutating, err := handle.Kube.AdmissionregistrationV1().MutatingWebhookConfigurations().List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("cannot list the MutatingWebhookConfigurations of the cluster: %w", err)
	}
	deleted := 0
	for index := range mutating.Items {
		configuration := &mutating.Items[index]
		rules := []admissionregistrationv1.RuleWithOperations{}
		for _, webhook := range configuration.Webhooks {
			rules = append(rules, webhook.Rules...)
		}
		if !removableWebhook(handle, configuration.Name, rules, kept) {
			continue
		}
		if err := handle.Kube.AdmissionregistrationV1().MutatingWebhookConfigurations().Delete(ctx, configuration.Name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("cannot delete MutatingWebhookConfiguration %s: %w", configuration.Name, err)
		}
		handle.Printf("Deleted MutatingWebhookConfiguration %s of the %s group.", configuration.Name, OldGroup)
		deleted++
	}
	validating, err := handle.Kube.AdmissionregistrationV1().ValidatingWebhookConfigurations().List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("cannot list the ValidatingWebhookConfigurations of the cluster: %w", err)
	}
	for index := range validating.Items {
		configuration := &validating.Items[index]
		rules := []admissionregistrationv1.RuleWithOperations{}
		for _, webhook := range configuration.Webhooks {
			rules = append(rules, webhook.Rules...)
		}
		if !removableWebhook(handle, configuration.Name, rules, kept) {
			continue
		}
		if err := handle.Kube.AdmissionregistrationV1().ValidatingWebhookConfigurations().Delete(ctx, configuration.Name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("cannot delete ValidatingWebhookConfiguration %s: %w", configuration.Name, err)
		}
		handle.Printf("Deleted ValidatingWebhookConfiguration %s of the %s group.", configuration.Name, OldGroup)
		deleted++
	}
	if deleted == 0 {
		handle.Printf("No webhook configuration of the %s group is left to remove.", OldGroup)
	}
	return nil
}

func removableWebhook(handle *cluster.Handle, name string, rules []admissionregistrationv1.RuleWithOperations, kept string) bool {
	old, other := false, false
	for _, rule := range rules {
		for _, group := range rule.APIGroups {
			if group == OldGroup {
				old = true
				continue
			}
			other = true
		}
	}
	switch {
	case !old:
		return false
	case name == kept:
		handle.Warnf("webhook configuration %s still carries rules on the %s group, and it is the configuration step 10 pointed at the %s group, so it is left in place",
			name, OldGroup, NewGroup)
		return false
	case other:
		handle.Warnf("webhook configuration %s carries rules on the %s group as well as on others, so it is left in place; remove its %s rules by hand",
			name, OldGroup, OldGroup)
		return false
	}
	return true
}

func rolledWebhookConfiguration(ctx context.Context, handle *cluster.Handle) (string, error) {
	record, err := handle.State.Load(ctx, PlanID)
	if err != nil || record == nil {
		return "", err
	}
	stepRecord := record.Step(deployNewWebhook{}.Meta().ID)
	if stepRecord == nil {
		return "", nil
	}
	snapshot, found := stepRecord.Snapshots[webhookConfigurationKey]
	if !found {
		return "", nil
	}
	configuration := &admissionregistrationv1.MutatingWebhookConfiguration{}
	if err := json.Unmarshal([]byte(snapshot), configuration); err != nil {
		return "", fmt.Errorf("the webhook configuration recorded by step 10 is corrupted: %w", err)
	}
	return configuration.Name, nil
}

func deleteOldCrds(ctx context.Context, handle *cluster.Handle) error {
	kinds := sourceKinds(handle.Mode)
	wanted := map[apiKind]bool{}
	for _, kind := range kinds {
		wanted[kind] = true
	}
	for _, kind := range oldKinds {
		if wanted[kind] {
			continue
		}
		exists, err := oldCrdExists(ctx, handle, kind)
		if err != nil {
			return err
		}
		if exists {
			handle.Warnf("CRD %s does not belong to the detected %s mode, so it is left in place; delete it by hand once nothing needs it", kind.OldCrdName(), handle.Mode)
		}
	}
	for _, kind := range kinds {
		err := handle.ApiExt.ApiextensionsV1().CustomResourceDefinitions().Delete(ctx, kind.OldCrdName(), metav1.DeleteOptions{})
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return fmt.Errorf("cannot delete CRD %s: %w", kind.OldCrdName(), err)
		}
		handle.Printf("Deleted CRD %s with every object it held.", kind.OldCrdName())
	}
	return waitForOldCrdsGone(ctx, handle, kinds)
}

func waitForOldCrdsGone(ctx context.Context, handle *cluster.Handle, kinds []apiKind) error {
	remaining := []string{}
	err := wait.PollUntilContextTimeout(ctx, crdPollInterval, crdDeleteTimeout, true, func(ctx context.Context) (bool, error) {
		remaining = nil
		for _, kind := range kinds {
			exists, err := oldCrdExists(ctx, handle, kind)
			if err != nil {
				return false, err
			}
			if exists {
				remaining = append(remaining, kind.OldCrdName())
			}
		}
		return len(remaining) == 0, nil
	})
	if wait.Interrupted(err) {
		return fmt.Errorf("the CRDs %s are still being deleted after %s, run this step again once they are gone", strings.Join(remaining, ", "), crdDeleteTimeout)
	}
	return err
}

func oldCrdExists(ctx context.Context, handle *cluster.Handle, kind apiKind) (bool, error) {
	_, err := handle.ApiExt.ApiextensionsV1().CustomResourceDefinitions().Get(ctx, kind.OldCrdName(), metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("cannot read CRD %s: %w", kind.OldCrdName(), err)
	}
	return true, nil
}

func deleteOldRbac(ctx context.Context, handle *cluster.Handle) error {
	roles, err := handle.Kube.RbacV1().ClusterRoles().List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("cannot list the ClusterRoles of the cluster: %w", err)
	}
	obsolete := map[string]bool{}
	for index := range roles.Items {
		role := &roles.Items[index]
		if newRbacName(role.Name) || role.AggregationRule != nil || len(role.Rules) == 0 {
			continue
		}
		old, other := rbacRuleGroups(role.Rules)
		switch {
		case !old:
		case other:
			handle.Warnf("ClusterRole %s grants the %s group alongside others, so it is left in place; remove its %s rules by hand", role.Name, OldGroup, OldGroup)
		default:
			obsolete[role.Name] = true
		}
	}
	if len(obsolete) == 0 {
		handle.Printf("No ClusterRole of the %s group is left to remove.", OldGroup)
		return nil
	}
	bindings, err := handle.Kube.RbacV1().ClusterRoleBindings().List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("cannot list the ClusterRoleBindings of the cluster: %w", err)
	}
	for index := range bindings.Items {
		binding := &bindings.Items[index]
		if binding.RoleRef.Kind != "ClusterRole" || !obsolete[binding.RoleRef.Name] {
			continue
		}
		if err := handle.Kube.RbacV1().ClusterRoleBindings().Delete(ctx, binding.Name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("cannot delete ClusterRoleBinding %s: %w", binding.Name, err)
		}
		handle.Printf("Deleted ClusterRoleBinding %s of the %s group.", binding.Name, OldGroup)
	}
	for _, name := range sortedKeys(obsolete) {
		if err := handle.Kube.RbacV1().ClusterRoles().Delete(ctx, name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("cannot delete ClusterRole %s: %w", name, err)
		}
		handle.Printf("Deleted ClusterRole %s of the %s group.", name, OldGroup)
	}
	return nil
}

func rbacRuleGroups(rules []rbacv1.PolicyRule) (bool, bool) {
	old, other := false, false
	for _, rule := range rules {
		if len(rule.APIGroups) == 0 {
			other = true
			continue
		}
		for _, group := range rule.APIGroups {
			if group == OldGroup {
				old = true
				continue
			}
			other = true
		}
	}
	return old, other
}

func newRbacName(name string) bool {
	for _, component := range rbacComponents {
		if name == RbacName(component) {
			return true
		}
	}
	return false
}

func dropOldInterfacesKey(ctx context.Context, handle *cluster.Handle) error {
	for _, component := range controlPlaneComponents {
		object, err := rolledWorkload(ctx, handle, component)
		if err != nil {
			return err
		}
		if object == nil {
			continue
		}
		template := object.template()
		if _, hasOld := template.Annotations[OldInterfacesKey]; !hasOld {
			continue
		}
		delete(template.Annotations, OldInterfacesKey)
		if err := object.update(ctx, handle); err != nil {
			return err
		}
		handle.Printf("%s no longer carries %s, waiting for its Pods to restart.", object.ref, OldInterfacesKey)
		if err := waitForWorkload(ctx, handle, object.ref); err != nil {
			return err
		}
	}
	return nil
}

func removeCniBackups(ctx context.Context, handle *cluster.Handle) error {
	image, err := cniImage(ctx, handle)
	if err != nil {
		return err
	}
	if err := deleteCniObjects(ctx, handle); err != nil {
		return err
	}
	if err := runCopier(ctx, handle, image, cniModeCleanup); err != nil {
		return err
	}
	handle.Printf("The CNI binaries backed up as %s are deleted from every node.", cniBackupSuffix())
	return nil
}
