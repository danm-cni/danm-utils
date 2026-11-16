package v43to44

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/danm-cni/danm-utils/pkg/dma/cluster"
	"github.com/danm-cni/danm-utils/pkg/dma/engine"
	danmtypes "github.com/danm-cni/danm/crd/apis/danm/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/pager"
)

const CopiedByStep = "patched"

type annotateExistingPods struct{}

func (annotateExistingPods) Meta() engine.StepMeta {
	return engine.StepMeta{
		ID:          5,
		Name:        "annotate-existing-pods",
		Description: "Add danm.io/interfaces to every existing Pod carrying only danm.k8s.io/interfaces.",
		Phase:       engine.PhaseConvert,
		Reversible:  true,
	}
}

func (annotateExistingPods) Detect(ctx context.Context, handle *cluster.Handle) (engine.StepState, error) {
	unconverted, patched := 0, 0
	err := eachPod(ctx, handle, func(pod *corev1.Pod) error {
		if podNeedsNewInterfaces(pod) {
			unconverted++
		}
		if pod.Annotations[CopiedMarkerKey] == CopiedByStep {
			patched++
		}
		return nil
	})
	if err != nil {
		return engine.StateNotStarted, err
	}
	if unconverted == 0 {
		return engine.StateDone, nil
	}
	if patched == 0 {
		return engine.StateNotStarted, nil
	}
	return engine.StatePartial, nil
}

func (annotateExistingPods) Execute(ctx context.Context, handle *cluster.Handle) error {
	if handle.Mode == cluster.ModeUnknown {
		return errors.New("the deployment mode is unknown, run step 1 first or name the mode with --mode")
	}
	rips, err := modeReservedIps(ctx, handle)
	if err != nil {
		return err
	}
	patched, gone := 0, 0
	mirrors := []string{}
	err = eachPod(ctx, handle, func(pod *corev1.Pod) error {
		if !podCarriesOnlyOldInterfaces(pod) {
			return nil
		}
		if isMirrorPod(pod) {
			mirrors = append(mirrors, objectName(pod.Namespace, pod.Name))
			return nil
		}
		value, notes := NewInterfaces(pod.Annotations[OldInterfacesKey], pod.Namespace, pod.Name, rips)
		for _, note := range notes {
			handle.Warnf("%s", note)
		}
		changed, err := patchPodAnnotations(ctx, handle, pod, map[string]any{NewInterfacesKey: value, CopiedMarkerKey: CopiedByStep})
		if err != nil {
			return err
		}
		if !changed {
			gone++
			return nil
		}
		patched++
		return nil
	})
	if err != nil {
		return err
	}
	if len(mirrors) > 0 {
		handle.Warnf("%d mirror Pods carry only %s and are not patched, as the API server is not their source of truth: %s; add %s to their static manifests on the nodes",
			len(mirrors), OldInterfacesKey, listed(mirrors), NewInterfacesKey)
	}
	if gone > 0 {
		handle.Printf("%d Pods were deleted while the step ran.", gone)
	}
	handle.Printf("Patched %d Pods with %s.", patched, NewInterfacesKey)
	return nil
}

func (annotateExistingPods) Rollback(ctx context.Context, handle *cluster.Handle) error {
	reverted := 0
	err := eachPod(ctx, handle, func(pod *corev1.Pod) error {
		if pod.Annotations[CopiedMarkerKey] != CopiedByStep {
			return nil
		}
		changed, err := patchPodAnnotations(ctx, handle, pod, map[string]any{NewInterfacesKey: nil, CopiedMarkerKey: nil})
		if err != nil {
			return err
		}
		if changed {
			reverted++
		}
		return nil
	})
	if err != nil {
		return err
	}
	handle.Printf("Removed %s from %d Pods.", NewInterfacesKey, reverted)
	return nil
}

func podCarriesOnlyOldInterfaces(pod *corev1.Pod) bool {
	if pod.DeletionTimestamp != nil {
		return false
	}
	if _, hasOld := pod.Annotations[OldInterfacesKey]; !hasOld {
		return false
	}
	_, hasNew := pod.Annotations[NewInterfacesKey]
	return !hasNew
}

func podNeedsNewInterfaces(pod *corev1.Pod) bool {
	return podCarriesOnlyOldInterfaces(pod) && !isMirrorPod(pod)
}

func isMirrorPod(pod *corev1.Pod) bool {
	_, mirror := pod.Annotations[corev1.MirrorPodAnnotationKey]
	return mirror
}

func eachPod(ctx context.Context, handle *cluster.Handle, visit func(*corev1.Pod) error) error {
	lister := pager.New(func(ctx context.Context, options metav1.ListOptions) (runtime.Object, error) {
		return handle.Kube.CoreV1().Pods(metav1.NamespaceAll).List(ctx, options)
	})
	lister.PageSize = listPageSize
	err := lister.EachListItem(ctx, metav1.ListOptions{}, func(object runtime.Object) error {
		pod, isPod := object.(*corev1.Pod)
		if !isPod {
			return fmt.Errorf("listing Pods returned an unexpected %T", object)
		}
		return visit(pod)
	})
	if err != nil {
		return fmt.Errorf("cannot walk the Pods of the cluster: %w", err)
	}
	return nil
}

func patchPodAnnotations(ctx context.Context, handle *cluster.Handle, pod *corev1.Pod, annotations map[string]any) (bool, error) {
	patch, err := json.Marshal(map[string]any{"metadata": map[string]any{"annotations": annotations}})
	if err != nil {
		return false, fmt.Errorf("cannot build the annotation patch of Pod %s: %w", objectName(pod.Namespace, pod.Name), err)
	}
	_, err = handle.Kube.CoreV1().Pods(pod.Namespace).Patch(ctx, pod.Name, types.MergePatchType, patch, metav1.PatchOptions{})
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("cannot patch the annotations of Pod %s: %w", objectName(pod.Namespace, pod.Name), err)
	}
	return true, nil
}

func modeReservedIps(ctx context.Context, handle *cluster.Handle) ([]*danmtypes.ReservedIP, error) {
	if handle.Mode != cluster.ModeProduction {
		return nil, nil
	}
	return allReservedIps(ctx, handle)
}
