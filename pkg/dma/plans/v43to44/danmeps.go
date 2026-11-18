package v43to44

import (
	"context"
	"errors"

	"github.com/danm-cni/danm-utils/pkg/dma/cluster"
	"github.com/danm-cni/danm-utils/pkg/dma/engine"
	danmtypes "github.com/danm-cni/danm/crd/apis/danm/v1"
	"github.com/danm-cni/danm/pkg/datastructs"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

const (
	ripRefsField      = "reservedIpRefs"
	EpCopiedMarkerKey = "dma.danm.io/copied"
)

type copyDanmEps struct{}

func (copyDanmEps) Meta() engine.StepMeta {
	return engine.StepMeta{
		ID:          7,
		Name:        "copy-danmeps",
		Description: "Copy the DanmEps of the danm.k8s.io group into the danm.io group.",
		Phase:       engine.PhaseConvert,
		Reversible:  true,
	}
}

func (copyDanmEps) Detect(ctx context.Context, handle *cluster.Handle) (engine.StepState, error) {
	if handle.Mode == cluster.ModeUnknown {
		return engine.StateNotStarted, nil
	}
	return detectCopies(ctx, handle, endpointKinds())
}

func (copyDanmEps) Execute(ctx context.Context, handle *cluster.Handle) error {
	if handle.Mode == cluster.ModeUnknown {
		return errors.New("the deployment mode is unknown, run step 1 first or name the mode with --mode")
	}
	return copyKinds(ctx, handle, endpointKinds())
}

func (copyDanmEps) Rollback(ctx context.Context, handle *cluster.Handle) error {
	if handle.Mode == cluster.ModeUnknown {
		return errors.New("the deployment mode is unknown, run step 1 first or name the mode with --mode")
	}
	return deleteCopies(ctx, handle, endpointKinds())
}

func endpointKinds() []apiKind {
	return []apiKind{kindDanmEp}
}

func transformedDanmEp(source *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	endpoint := &danmtypes.DanmEp{}
	if err := decodeObjectInto(kindDanmEp, source, endpoint); err != nil {
		return nil, err
	}
	copied := &danmtypes.DanmEp{ObjectMeta: copiedMeta(endpoint.ObjectMeta, RipTagKeys...), Spec: endpoint.Spec}
	copied.Labels = orNilMap(datastructs.EpLabels(copied.Labels, copied.Spec.RipRefs))
	copied.Annotations = markedAsCopy(copied.Annotations)
	encoded, err := encodeCopy(kindDanmEp, copied)
	if err != nil {
		return nil, err
	}
	unstructured.RemoveNestedField(encoded.Object, "spec", ripRefsField)
	return encoded, nil
}

func danmEpsMatch(desired, existing *danmtypes.DanmEp) bool {
	return metadataMatches(desired.ObjectMeta, existing.ObjectMeta) && equality.Semantic.DeepEqual(desired.Spec, existing.Spec)
}

func markedAsCopy(annotations map[string]string) map[string]string {
	marked := map[string]string{EpCopiedMarkerKey: PlanID}
	for key, value := range annotations {
		marked[key] = value
	}
	return marked
}

func reconcileEndpointCopy(desired, existing *unstructured.Unstructured, fallback func() danmtypes.RipReferences) (bool, error) {
	wanted, copied := &danmtypes.DanmEp{}, &danmtypes.DanmEp{}
	if err := decodeObjectInto(kindDanmEp, desired, wanted); err != nil {
		return false, err
	}
	if err := decodeObjectInto(kindDanmEp, existing, copied); err != nil {
		return false, err
	}
	refs := copied.Spec.RipRefs
	if refs == (danmtypes.RipReferences{}) && fallback != nil {
		refs = fallback()
	}
	encoded, err := endpointWithRipRefs(wanted, refs)
	if err != nil {
		return false, err
	}
	*desired = *encoded
	return danmEpsMatch(wanted, copied), nil
}
