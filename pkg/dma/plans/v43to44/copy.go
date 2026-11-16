package v43to44

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"

	"github.com/danm-cni/danm-utils/pkg/dma/cluster"
	"github.com/danm-cni/danm-utils/pkg/dma/engine"
	danmtypes "github.com/danm-cni/danm/crd/apis/danm/v1"
	"github.com/danm-cni/danm/pkg/bitarray"
	structuralschema "k8s.io/apiextensions-apiserver/pkg/apiserver/schema"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/schema/pruning"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/pager"
)

const (
	MaxAllowedVniVxlan = 100000
	vniTypeVxlan       = "vxlan"
)

const frozenGroupWarning = "from now on, changes to " + OldGroup + " network objects and TenantConfigs are not carried over to " + NewGroup +
	": step 13 synchronises IP and VNI allocation state only, and every other change made after this step is lost at cutover"

type copyOutcome int

const (
	copyCreated copyOutcome = iota
	copyUpdated
	copySkipped
)

type copyNetworkObjects struct{}

func (copyNetworkObjects) Meta() engine.StepMeta {
	return engine.StepMeta{
		ID:          6,
		Name:        "copy-network-objects",
		Description: "Copy the network objects and TenantConfigs of the danm.k8s.io group into the danm.io group.",
		Phase:       engine.PhaseConvert,
		Reversible:  true,
	}
}

func (copyNetworkObjects) Warnings(_ context.Context, _ *cluster.Handle) ([]string, error) {
	return []string{frozenGroupWarning}, nil
}

func (copyNetworkObjects) Detect(ctx context.Context, handle *cluster.Handle) (engine.StepState, error) {
	if handle.Mode == cluster.ModeUnknown {
		return engine.StateNotStarted, nil
	}
	return detectCopies(ctx, handle, networkKinds(handle.Mode))
}

func (copyNetworkObjects) Execute(ctx context.Context, handle *cluster.Handle) error {
	if handle.Mode == cluster.ModeUnknown {
		return errors.New("the deployment mode is unknown, run step 1 first or name the mode with --mode")
	}
	handle.Warnf("%s", frozenGroupWarning)
	return copyKinds(ctx, handle, networkKinds(handle.Mode))
}

func (copyNetworkObjects) Rollback(ctx context.Context, handle *cluster.Handle) error {
	if handle.Mode == cluster.ModeUnknown {
		return errors.New("the deployment mode is unknown, run step 1 first or name the mode with --mode")
	}
	return deleteCopies(ctx, handle, networkKinds(handle.Mode))
}

func networkKinds(mode cluster.Mode) []apiKind {
	if mode == cluster.ModeProduction {
		return []apiKind{kindTenantNetwork, kindClusterNetwork, kindTenantConfig}
	}
	return []apiKind{kindDanmNet}
}

func detectCopies(ctx context.Context, handle *cluster.Handle, kinds []apiKind) (engine.StepState, error) {
	copied, missing := 0, 0
	for _, kind := range kinds {
		present, err := copiedNames(ctx, handle, kind)
		if err != nil {
			return engine.StateNotStarted, err
		}
		err = eachObject(ctx, handle, kind.OldResource(), func(item *unstructured.Unstructured) error {
			if present[objectName(item.GetNamespace(), item.GetName())] {
				copied++
			} else {
				missing++
			}
			return nil
		})
		if err != nil {
			return engine.StateNotStarted, err
		}
	}
	if missing == 0 {
		return engine.StateDone, nil
	}
	if copied == 0 {
		return engine.StateNotStarted, nil
	}
	return engine.StatePartial, nil
}

func copyKinds(ctx context.Context, handle *cluster.Handle, kinds []apiKind) error {
	accepted, err := prunedFieldsAccepted(ctx, handle)
	if err != nil {
		return err
	}
	problems := []string{}
	for _, kind := range kinds {
		target, err := targetSchema(handle.Mode, kind)
		if err != nil {
			return err
		}
		created, updated, skipped := 0, 0, 0
		err = eachObject(ctx, handle, kind.OldResource(), func(item *unstructured.Unstructured) error {
			name := objectName(item.GetNamespace(), item.GetName())
			pruned, fields := pruneCopy(item, target)
			if len(fields) > 0 && !accepted {
				problems = append(problems, fmt.Sprintf("%s %s holds fields the DANM %s schema does not know, which its copy would lose: %s",
					kind.Kind, name, TargetRelease, strings.Join(fields, ", ")))
				return nil
			}
			if len(fields) > 0 {
				handle.Warnf("the copy of %s %s loses fields the DANM %s schema does not know, accepted with --accept-pruned-fields: %s",
					kind.Kind, name, TargetRelease, strings.Join(fields, ", "))
			}
			desired, err := transformed(kind, pruned)
			if err != nil {
				problems = append(problems, err.Error())
				return nil
			}
			outcome, err := applyCopy(ctx, handle, kind, desired)
			switch {
			case err != nil:
				return err
			case outcome == copyCreated:
				created++
			case outcome == copyUpdated:
				updated++
			default:
				skipped++
			}
			return nil
		})
		if err != nil {
			return err
		}
		handle.Printf("%s: %d copied, %d corrected, %d already up to date.", kind.Kind, created, updated, skipped)
	}
	if len(problems) > 0 {
		return copyFailure(problems)
	}
	return nil
}

func deleteCopies(ctx context.Context, handle *cluster.Handle, kinds []apiKind) error {
	for _, kind := range kinds {
		deleted := 0
		err := eachObject(ctx, handle, kind.NewResource(), func(item *unstructured.Unstructured) error {
			err := objectClient(handle, kind, kind.NewResource(), item.GetNamespace()).Delete(ctx, item.GetName(), metav1.DeleteOptions{})
			if apierrors.IsNotFound(err) {
				return nil
			}
			if err != nil {
				return fmt.Errorf("cannot delete %s %s: %w", kind.Kind, objectName(item.GetNamespace(), item.GetName()), err)
			}
			deleted++
			return nil
		})
		if err != nil {
			return err
		}
		handle.Printf("%s: %d copies deleted from the %s group.", kind.Kind, deleted, NewGroup)
	}
	return nil
}

func applyCopy(ctx context.Context, handle *cluster.Handle, kind apiKind, desired *unstructured.Unstructured) (copyOutcome, error) {
	client := objectClient(handle, kind, kind.NewResource(), desired.GetNamespace())
	name := objectName(desired.GetNamespace(), desired.GetName())
	existing, err := client.Get(ctx, desired.GetName(), metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		if _, err := client.Create(ctx, desired, metav1.CreateOptions{}); err != nil {
			return copySkipped, fmt.Errorf("cannot create %s %s in the %s group: %w", kind.Kind, name, NewGroup, err)
		}
		return copyCreated, nil
	}
	if err != nil {
		return copySkipped, fmt.Errorf("cannot read %s %s of the %s group: %w", kind.Kind, name, NewGroup, err)
	}
	same, err := sameCopy(kind, desired, existing)
	if err != nil {
		return copySkipped, err
	}
	if same {
		return copySkipped, nil
	}
	update := desired.DeepCopy()
	update.SetResourceVersion(existing.GetResourceVersion())
	if _, err := client.Update(ctx, update, metav1.UpdateOptions{}); err != nil {
		return copySkipped, fmt.Errorf("cannot update %s %s of the %s group: %w", kind.Kind, name, NewGroup, err)
	}
	return copyUpdated, nil
}

func transformed(kind apiKind, source *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	switch kind {
	case kindTenantConfig:
		return transformedTenantConfig(source)
	case kindDanmEp:
		return transformedDanmEp(source)
	}
	return transformedNetwork(kind, source)
}

func transformedNetwork(kind apiKind, source *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	network := &danmtypes.DanmNet{}
	if err := decodeObjectInto(kind, source, network); err != nil {
		return nil, err
	}
	network.Kind = kind.Kind
	mtu, tagged, err := ParseMtuTag(network)
	if err != nil {
		return nil, err
	}
	copied := &danmtypes.DanmNet{ObjectMeta: copiedMeta(network.ObjectMeta, MtuTagKey), Spec: network.Spec}
	if tagged {
		copied.Spec.Options.Mtu = mtu
	}
	return encodeCopy(kind, copied)
}

func transformedTenantConfig(source *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	tenantConfig := &danmtypes.TenantConfig{}
	if err := decodeObjectInto(kindTenantConfig, source, tenantConfig); err != nil {
		return nil, err
	}
	copied := &danmtypes.TenantConfig{
		ObjectMeta:   copiedMeta(tenantConfig.ObjectMeta),
		NetworkIds:   tenantConfig.NetworkIds,
		AllowedCNets: tenantConfig.AllowedCNets,
	}
	for _, device := range tenantConfig.HostDevices {
		alloc, err := copiedVniAlloc(tenantConfig.Name, device)
		if err != nil {
			return nil, err
		}
		device.Alloc = alloc
		copied.HostDevices = append(copied.HostDevices, device)
	}
	return encodeCopy(kindTenantConfig, copied)
}

func copiedVniAlloc(name string, device danmtypes.IfaceProfile) (string, error) {
	if device.Alloc == "" || !strings.EqualFold(device.VniType, vniTypeVxlan) {
		return device.Alloc, nil
	}
	decoded, err := base64.StdEncoding.DecodeString(device.Alloc)
	if err != nil {
		return "", fmt.Errorf("the VNI allocation of device %s of TenantConfig %s is not a valid bitmask: %w", device.Name, name, err)
	}
	source := bitarray.NewBitArrayFromBase64(device.Alloc)
	target, err := bitarray.NewBitArray(MaxAllowedVniVxlan + 1)
	if err != nil {
		return "", fmt.Errorf("the VxLAN VNI bitmask of device %s of TenantConfig %s cannot be created: %w", device.Name, name, err)
	}
	if len(decoded)*8 > int(target.Len()) {
		return "", fmt.Errorf("the VNI allocation of device %s of TenantConfig %s holds %d bits, more than the %d bits DANM %s allows",
			device.Name, name, len(decoded)*8, target.Len(), TargetRelease)
	}
	for vni := uint32(0); vni < source.Len(); vni++ {
		if source.Get(vni) {
			target.Set(vni)
		}
	}
	return target.Encode(), nil
}

func copiedMeta(source metav1.ObjectMeta, dropped ...string) metav1.ObjectMeta {
	annotations := map[string]string{}
	for key, value := range source.Annotations {
		annotations[key] = value
	}
	for _, key := range dropped {
		delete(annotations, key)
	}
	labels := map[string]string{}
	for key, value := range source.Labels {
		labels[key] = value
	}
	owners := []metav1.OwnerReference{}
	for _, owner := range source.OwnerReferences {
		if !strings.HasPrefix(owner.APIVersion, OldGroup+"/") {
			owners = append(owners, owner)
		}
	}
	return metav1.ObjectMeta{
		Name:            source.Name,
		Namespace:       source.Namespace,
		Labels:          orNilMap(labels),
		Annotations:     orNilMap(annotations),
		OwnerReferences: orNilOwners(owners),
	}
}

func sameCopy(kind apiKind, desired, existing *unstructured.Unstructured) (bool, error) {
	switch kind {
	case kindTenantConfig:
		left, right := &danmtypes.TenantConfig{}, &danmtypes.TenantConfig{}
		if err := decodeObjectInto(kind, desired, left); err != nil {
			return false, err
		}
		if err := decodeObjectInto(kind, existing, right); err != nil {
			return false, err
		}
		return tenantConfigsMatch(left, right), nil
	case kindDanmEp:
		left, right := &danmtypes.DanmEp{}, &danmtypes.DanmEp{}
		if err := decodeObjectInto(kind, desired, left); err != nil {
			return false, err
		}
		if err := decodeObjectInto(kind, existing, right); err != nil {
			return false, err
		}
		return danmEpsMatch(left, right), nil
	}
	left, right := &danmtypes.DanmNet{}, &danmtypes.DanmNet{}
	if err := decodeObjectInto(kind, desired, left); err != nil {
		return false, err
	}
	if err := decodeObjectInto(kind, existing, right); err != nil {
		return false, err
	}
	return networksMatch(left, right), nil
}

func networksMatch(desired, existing *danmtypes.DanmNet) bool {
	if !metadataMatches(desired.ObjectMeta, existing.ObjectMeta) {
		return false
	}
	left, right := desired.Spec, existing.Spec
	if !sameAlloc(left.Options.Alloc, right.Options.Alloc) || !sameAlloc(left.Options.Alloc6, right.Options.Alloc6) {
		return false
	}
	left.Options.Alloc, right.Options.Alloc = "", ""
	left.Options.Alloc6, right.Options.Alloc6 = "", ""
	return equality.Semantic.DeepEqual(left, right)
}

func tenantConfigsMatch(desired, existing *danmtypes.TenantConfig) bool {
	if !metadataMatches(desired.ObjectMeta, existing.ObjectMeta) {
		return false
	}
	if !equality.Semantic.DeepEqual(desired.NetworkIds, existing.NetworkIds) || !equality.Semantic.DeepEqual(desired.AllowedCNets, existing.AllowedCNets) {
		return false
	}
	if len(desired.HostDevices) != len(existing.HostDevices) {
		return false
	}
	for index, device := range desired.HostDevices {
		other := existing.HostDevices[index]
		if !sameAlloc(device.Alloc, other.Alloc) {
			return false
		}
		device.Alloc, other.Alloc = "", ""
		if device != other {
			return false
		}
	}
	return true
}

func metadataMatches(desired, existing metav1.ObjectMeta) bool {
	return equality.Semantic.DeepEqual(desired.Labels, existing.Labels) &&
		equality.Semantic.DeepEqual(desired.Annotations, existing.Annotations) &&
		equality.Semantic.DeepEqual(desired.OwnerReferences, existing.OwnerReferences)
}

func sameAlloc(desired, existing string) bool {
	if desired == existing {
		return true
	}
	left, leftErr := base64.StdEncoding.DecodeString(desired)
	right, rightErr := base64.StdEncoding.DecodeString(existing)
	if leftErr != nil || rightErr != nil {
		return false
	}
	if len(left) < len(right) {
		left, right = right, left
	}
	for index, octet := range right {
		if left[index] != octet {
			return false
		}
	}
	for _, octet := range left[len(right):] {
		if octet != 0 {
			return false
		}
	}
	return true
}

func pruneCopy(item *unstructured.Unstructured, target *structuralschema.Structural) (*unstructured.Unstructured, []string) {
	copied := item.DeepCopy()
	copied.SetAPIVersion(NewGroup + "/" + Version)
	fields := pruning.PruneWithOptions(copied.Object, target, true, structuralschema.UnknownFieldPathOptions{TrackUnknownFieldPaths: true})
	return copied, fields
}

func decodeObjectInto(kind apiKind, item *unstructured.Unstructured, target any) error {
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(item.Object, target); err != nil {
		return fmt.Errorf("%s %s cannot be read as a DANM %s: %w", kind.Kind, objectName(item.GetNamespace(), item.GetName()), kind.Kind, err)
	}
	return nil
}

func encodeCopy(kind apiKind, object any) (*unstructured.Unstructured, error) {
	content, err := runtime.DefaultUnstructuredConverter.ToUnstructured(object)
	if err != nil {
		return nil, fmt.Errorf("the copy of a %s cannot be serialised: %w", kind.Kind, err)
	}
	copied := &unstructured.Unstructured{Object: content}
	copied.SetAPIVersion(NewGroup + "/" + Version)
	copied.SetKind(kind.Kind)
	return copied, nil
}

func copiedNames(ctx context.Context, handle *cluster.Handle, kind apiKind) (map[string]bool, error) {
	names := map[string]bool{}
	err := eachObject(ctx, handle, kind.NewResource(), func(item *unstructured.Unstructured) error {
		names[objectName(item.GetNamespace(), item.GetName())] = true
		return nil
	})
	return names, err
}

func eachObject(ctx context.Context, handle *cluster.Handle, resource schema.GroupVersionResource, visit func(*unstructured.Unstructured) error) error {
	lister := pager.New(func(ctx context.Context, options metav1.ListOptions) (runtime.Object, error) {
		return handle.Dynamic.Resource(resource).List(ctx, options)
	})
	lister.PageSize = listPageSize
	err := lister.EachListItem(ctx, metav1.ListOptions{}, func(object runtime.Object) error {
		item, isUnstructured := object.(*unstructured.Unstructured)
		if !isUnstructured {
			return fmt.Errorf("listing %s returned an unexpected %T", resource, object)
		}
		return visit(item)
	})
	if apierrors.IsNotFound(err) || meta.IsNoMatchError(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("cannot list %s: %w", resource, err)
	}
	return nil
}

func objectClient(handle *cluster.Handle, kind apiKind, resource schema.GroupVersionResource, namespace string) dynamic.ResourceInterface {
	if kind.Namespaced {
		return handle.Dynamic.Resource(resource).Namespace(namespace)
	}
	return handle.Dynamic.Resource(resource)
}

func prunedFieldsAccepted(ctx context.Context, handle *cluster.Handle) (bool, error) {
	record, err := handle.State.Load(ctx, PlanID)
	if err != nil || record == nil {
		return false, err
	}
	return record.PrunedFieldsAccepted, nil
}

func copyFailure(problems []string) error {
	reported := problems
	if len(reported) > maxReportedProblems {
		reported = append(append([]string{}, reported[:maxReportedProblems]...),
			fmt.Sprintf("%d more objects are affected", len(problems)-maxReportedProblems))
	}
	return fmt.Errorf("%d objects were not copied:\n  - %s", len(problems), strings.Join(reported, "\n  - "))
}

func orNilMap(values map[string]string) map[string]string {
	if len(values) == 0 {
		return nil
	}
	return values
}

func orNilOwners(owners []metav1.OwnerReference) []metav1.OwnerReference {
	if len(owners) == 0 {
		return nil
	}
	return owners
}
