package v43to44

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/danm-cni/danm-utils/pkg/dma/cluster"
	"github.com/danm-cni/danm-utils/pkg/dma/engine"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/pager"
)

const ServiceCopiedMarkerKey = "dma.danm.io/service-annotations-copied"

var serviceAnnotations = []string{"selector", "network", "tenantNetwork", "clusterNetwork"}

type dualAnnotateServices struct{}

func (dualAnnotateServices) Meta() engine.StepMeta {
	return engine.StepMeta{
		ID:          8,
		Name:        "dual-annotate-services",
		Description: "Add the danm.io Service annotations next to the danm.k8s.io ones.",
		Phase:       engine.PhaseConvert,
		Reversible:  true,
	}
}

func (dualAnnotateServices) Detect(ctx context.Context, handle *cluster.Handle) (engine.StepState, error) {
	unconverted, patched := 0, 0
	err := eachService(ctx, handle, func(service *corev1.Service) error {
		if len(serviceKeysToCopy(service)) > 0 {
			unconverted++
		}
		if service.Annotations[ServiceCopiedMarkerKey] != "" {
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

func (dualAnnotateServices) Execute(ctx context.Context, handle *cluster.Handle) error {
	patched, gone := 0, 0
	err := eachService(ctx, handle, func(service *corev1.Service) error {
		keys := serviceKeysToCopy(service)
		if len(keys) == 0 {
			return nil
		}
		annotations := map[string]any{ServiceCopiedMarkerKey: copiedKeys(service.Annotations[ServiceCopiedMarkerKey], keys)}
		for _, key := range keys {
			annotations[NewGroup+"/"+key] = service.Annotations[OldGroup+"/"+key]
		}
		changed, err := patchServiceAnnotations(ctx, handle, service, annotations)
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
	if gone > 0 {
		handle.Printf("%d Services were deleted while the step ran.", gone)
	}
	handle.Printf("Annotated %d Services with their %s keys.", patched, NewGroup)
	return nil
}

func (dualAnnotateServices) Rollback(ctx context.Context, handle *cluster.Handle) error {
	reverted := 0
	err := eachService(ctx, handle, func(service *corev1.Service) error {
		marker := service.Annotations[ServiceCopiedMarkerKey]
		if marker == "" {
			return nil
		}
		annotations := map[string]any{ServiceCopiedMarkerKey: nil}
		for _, key := range strings.Split(marker, ",") {
			annotations[NewGroup+"/"+key] = nil
		}
		changed, err := patchServiceAnnotations(ctx, handle, service, annotations)
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
	handle.Printf("Removed the %s annotations from %d Services.", NewGroup, reverted)
	return nil
}

func serviceKeysToCopy(service *corev1.Service) []string {
	if service.DeletionTimestamp != nil {
		return nil
	}
	keys := []string{}
	for _, key := range serviceAnnotations {
		if _, hasOld := service.Annotations[OldGroup+"/"+key]; !hasOld {
			continue
		}
		if _, hasNew := service.Annotations[NewGroup+"/"+key]; hasNew {
			continue
		}
		keys = append(keys, key)
	}
	return keys
}

func copiedKeys(marker string, added []string) string {
	copied := map[string]bool{}
	for _, key := range strings.Split(marker, ",") {
		copied[key] = true
	}
	for _, key := range added {
		copied[key] = true
	}
	ordered := []string{}
	for _, key := range serviceAnnotations {
		if copied[key] {
			ordered = append(ordered, key)
		}
	}
	return strings.Join(ordered, ",")
}

func eachService(ctx context.Context, handle *cluster.Handle, visit func(*corev1.Service) error) error {
	lister := pager.New(func(ctx context.Context, options metav1.ListOptions) (runtime.Object, error) {
		return handle.Kube.CoreV1().Services(metav1.NamespaceAll).List(ctx, options)
	})
	lister.PageSize = listPageSize
	err := lister.EachListItem(ctx, metav1.ListOptions{}, func(object runtime.Object) error {
		service, isService := object.(*corev1.Service)
		if !isService {
			return fmt.Errorf("listing Services returned an unexpected %T", object)
		}
		return visit(service)
	})
	if err != nil {
		return fmt.Errorf("cannot walk the Services of the cluster: %w", err)
	}
	return nil
}

func patchServiceAnnotations(ctx context.Context, handle *cluster.Handle, service *corev1.Service, annotations map[string]any) (bool, error) {
	patch, err := json.Marshal(map[string]any{"metadata": map[string]any{"annotations": annotations}})
	if err != nil {
		return false, fmt.Errorf("cannot build the annotation patch of Service %s: %w", objectName(service.Namespace, service.Name), err)
	}
	_, err = handle.Kube.CoreV1().Services(service.Namespace).Patch(ctx, service.Name, types.MergePatchType, patch, metav1.PatchOptions{})
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("cannot patch the annotations of Service %s: %w", objectName(service.Namespace, service.Name), err)
	}
	return true, nil
}
