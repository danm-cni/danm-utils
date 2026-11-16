package v43to44

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/danm-cni/danm-utils/pkg/dma/cluster"
	"github.com/danm-cni/danm-utils/pkg/dma/engine"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

const logToStderrArgument = "logtostderr"

type componentWorkloadState struct {
	ref        cluster.WorkloadRef
	present    bool
	configured bool
}

var controlPlaneComponents = []string{ComponentNetwatcher, ComponentSvcwatcher, ComponentCleaner}

type rollControlPlane struct{}

func (rollControlPlane) Meta() engine.StepMeta {
	return engine.StepMeta{
		ID:          11,
		Name:        "roll-control-plane",
		Description: "Roll netwatcher, svcwatcher and cleaner to 4.4.",
		Phase:       engine.PhaseCutover,
		Reversible:  true,
	}
}

func (rollControlPlane) Detect(ctx context.Context, handle *cluster.Handle) (engine.StepState, error) {
	rolled, pending := 0, 0
	for _, component := range controlPlaneComponents {
		workloadState, err := componentState(ctx, handle, component)
		if err != nil {
			return engine.StateNotStarted, err
		}
		if !workloadState.present && !workloadState.configured {
			continue
		}
		image, err := handle.Images.Resolve(component)
		if err != nil {
			return engine.StateNotStarted, err
		}
		object, err := loadWorkload(ctx, handle, workloadState.ref)
		if apierrors.IsNotFound(err) {
			pending++
			continue
		}
		if err != nil {
			return engine.StateNotStarted, err
		}
		current, err := templateIsCurrent(component, object.template())
		if err != nil {
			return engine.StateNotStarted, err
		}
		running, imageErr := containerImage(object.template(), component)
		if imageErr != nil || running != image.Image || !current || object.problem() != "" {
			pending++
			continue
		}
		rolled++
	}
	switch {
	case pending == 0 && rolled == 0:
		return engine.StateNotStarted, nil
	case pending == 0:
		return engine.StateDone, nil
	case rolled == 0:
		return engine.StateNotStarted, nil
	}
	return engine.StatePartial, nil
}

func (rollControlPlane) Execute(ctx context.Context, handle *cluster.Handle) error {
	for _, component := range controlPlaneComponents {
		workloadState, err := componentState(ctx, handle, component)
		if err != nil {
			return err
		}
		switch {
		case workloadState.present:
			err = rollComponent(ctx, handle, component, workloadState.ref)
		case component == ComponentNetwatcher:
			err = errors.New("step 1 recorded no netwatcher workload, run it before this step")
		case workloadState.configured:
			err = createComponent(ctx, handle, component, workloadState.ref)
		default:
			handle.Printf("%s is not deployed, nothing to roll.", component)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func componentState(ctx context.Context, handle *cluster.Handle, component string) (componentWorkloadState, error) {
	record, err := handle.State.Load(ctx, PlanID)
	if err != nil || record == nil {
		return componentWorkloadState{}, err
	}
	stored, found := record.Workloads[component]
	if !found {
		return componentWorkloadState{}, nil
	}
	return componentWorkloadState{ref: cluster.RecordedWorkload(stored), present: stored.Present, configured: stored.Configured}, nil
}

func createComponent(ctx context.Context, handle *cluster.Handle, component string, ref cluster.WorkloadRef) error {
	image, err := handle.Images.Resolve(component)
	if err != nil {
		return err
	}
	if err := warnMissingControlPlaneNodes(ctx, handle, component); err != nil {
		return err
	}
	if err := deployComponent(ctx, handle, component, ref, image); err != nil {
		return err
	}
	if err := waitForWorkload(ctx, handle, ref); err != nil {
		if deleteErr := deleteComponent(ctx, handle, component, ref); deleteErr != nil {
			return fmt.Errorf("%w, and removing it again failed as well: %s", err, deleteErr.Error())
		}
		handle.Printf("%s is removed again, as it never became ready.", ref)
		return err
	}
	return nil
}

func deleteComponent(ctx context.Context, handle *cluster.Handle, component string, ref cluster.WorkloadRef) error {
	var err error
	if ref.Kind == cluster.WorkloadDaemonSet {
		err = handle.Kube.AppsV1().DaemonSets(ref.Namespace).Delete(ctx, ref.Name, metav1.DeleteOptions{})
	} else {
		err = handle.Kube.AppsV1().Deployments(ref.Namespace).Delete(ctx, ref.Name, metav1.DeleteOptions{})
	}
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("cannot delete %s: %w", ref, err)
	}
	account, err := componentAccountName(component)
	if err != nil {
		return err
	}
	if err := handle.Kube.CoreV1().ServiceAccounts(ref.Namespace).Delete(ctx, account, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("cannot delete ServiceAccount %s: %w", objectName(ref.Namespace, account), err)
	}
	return nil
}

func (rollControlPlane) Rollback(ctx context.Context, handle *cluster.Handle) error {
	for _, component := range controlPlaneComponents {
		workloadState, err := componentState(ctx, handle, component)
		if err != nil {
			return err
		}
		ref := workloadState.ref
		if !workloadState.present {
			if !workloadState.configured {
				continue
			}
			if err := deleteComponent(ctx, handle, component, ref); err != nil {
				return err
			}
			handle.Printf("%s is removed again.", ref)
			continue
		}
		if err := revertWorkloadSpec(ctx, handle, ref, workloadSpecKey(component)); err != nil {
			return err
		}
		if err := waitForWorkload(ctx, handle, ref); err != nil {
			return err
		}
		handle.Printf("%s is restored to the spec recorded before this step.", ref)
	}
	return nil
}

func workloadSpecKey(component string) string {
	return component + "-workload"
}

func rollComponent(ctx context.Context, handle *cluster.Handle, component string, ref cluster.WorkloadRef) error {
	image, err := handle.Images.Resolve(component)
	if err != nil {
		return err
	}
	object, err := loadWorkload(ctx, handle, ref)
	if err != nil {
		return err
	}
	if err := snapshotWorkloadSpec(ctx, handle, object, workloadSpecKey(component)); err != nil {
		return err
	}
	if err := setContainerImage(object.template(), component, image); err != nil {
		return fmt.Errorf("%s cannot be rolled: %w", ref, err)
	}
	if err := applyTemplateChanges(component, object.template()); err != nil {
		return err
	}
	if err := warnMissingControlPlaneNodes(ctx, handle, component); err != nil {
		return err
	}
	if component == ComponentSvcwatcher {
		setAllAtOnceRollout(object)
	}
	if err := object.update(ctx, handle); err != nil {
		return err
	}
	handle.Printf("%s now runs %s, waiting for it to become ready.", ref, image.Image)
	if err := waitForWorkload(ctx, handle, ref); err != nil {
		if revertErr := revertWorkloadSpec(ctx, handle, ref, workloadSpecKey(component)); revertErr != nil {
			return fmt.Errorf("%w, and the revert failed as well: %s", err, revertErr.Error())
		}
		handle.Printf("%s is reverted to the spec recorded before this step.", ref)
		return err
	}
	if component == ComponentSvcwatcher {
		if err := restoreRolloutStrategy(ctx, handle, ref, workloadSpecKey(component)); err != nil {
			return err
		}
	}
	return nil
}

func applyTemplateChanges(component string, template *corev1.PodTemplateSpec) error {
	if component == ComponentSvcwatcher {
		for index := range template.Spec.Containers {
			template.Spec.Containers[index].Command = withoutLogToStderr(template.Spec.Containers[index].Command)
			template.Spec.Containers[index].Args = withoutLogToStderr(template.Spec.Containers[index].Args)
		}
	}
	addNewInterfacesKey(template)
	return applyComponentTemplate(component, template)
}

func templateIsCurrent(component string, template *corev1.PodTemplateSpec) (bool, error) {
	if component == ComponentSvcwatcher {
		for _, container := range template.Spec.Containers {
			if len(withoutLogToStderr(container.Command)) != len(container.Command) || len(withoutLogToStderr(container.Args)) != len(container.Args) {
				return false, nil
			}
		}
	}
	if _, hasOld := template.Annotations[OldInterfacesKey]; hasOld {
		if _, hasNew := template.Annotations[NewInterfacesKey]; !hasNew {
			return false, nil
		}
	}
	return componentTemplateIsCurrent(component, template)
}

func addNewInterfacesKey(template *corev1.PodTemplateSpec) {
	old, hasOld := template.Annotations[OldInterfacesKey]
	if !hasOld {
		return
	}
	if _, hasNew := template.Annotations[NewInterfacesKey]; hasNew {
		return
	}
	if template.Annotations == nil {
		template.Annotations = map[string]string{}
	}
	template.Annotations[NewInterfacesKey] = old
}

func withoutLogToStderr(arguments []string) []string {
	kept := make([]string, 0, len(arguments))
	for _, argument := range arguments {
		name, _, _ := strings.Cut(strings.TrimLeft(argument, "-"), "=")
		if strings.HasPrefix(argument, "-") && name == logToStderrArgument {
			continue
		}
		kept = append(kept, argument)
	}
	if len(kept) == len(arguments) {
		return arguments
	}
	return kept
}

func setAllAtOnceRollout(object *workload) {
	if object.daemonSet != nil {
		everything := intstr.FromString("100%")
		none := intstr.FromInt32(0)
		object.daemonSet.Spec.UpdateStrategy = appsv1.DaemonSetUpdateStrategy{
			Type:          appsv1.RollingUpdateDaemonSetStrategyType,
			RollingUpdate: &appsv1.RollingUpdateDaemonSet{MaxUnavailable: &everything, MaxSurge: &none},
		}
		return
	}
	object.deployment.Spec.Strategy = appsv1.DeploymentStrategy{Type: appsv1.RecreateDeploymentStrategyType}
}

func restoreRolloutStrategy(ctx context.Context, handle *cluster.Handle, ref cluster.WorkloadRef, key string) error {
	snapshot, found, err := handle.LoadSnapshot(ctx, key)
	if err != nil || !found {
		return err
	}
	recorded, err := loadWorkload(ctx, handle, ref)
	if err != nil {
		return err
	}
	if err := recorded.setSpec(snapshot); err != nil {
		return err
	}
	object, err := loadWorkload(ctx, handle, ref)
	if err != nil {
		return err
	}
	if object.daemonSet != nil {
		object.daemonSet.Spec.UpdateStrategy = recorded.daemonSet.Spec.UpdateStrategy
	} else {
		object.deployment.Spec.Strategy = recorded.deployment.Spec.Strategy
	}
	if err := object.update(ctx, handle); err != nil {
		return err
	}
	handle.Printf("The rollout strategy of %s is restored.", ref)
	return nil
}
