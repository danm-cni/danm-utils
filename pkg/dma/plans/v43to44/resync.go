package v43to44

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/danm-cni/danm-utils/pkg/dma/cluster"
	"github.com/danm-cni/danm-utils/pkg/dma/engine"
	"github.com/danm-cni/danm-utils/pkg/dma/state"
	danmtypes "github.com/danm-cni/danm/crd/apis/danm/v1"
	"github.com/danm-cni/danm/pkg/datastructs"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

const resyncRecordKey = "resync-verified"

type resyncAndVerify struct{}

func (resyncAndVerify) Meta() engine.StepMeta {
	return engine.StepMeta{
		ID:          13,
		Name:        "resync-and-verify",
		Description: "Carry over the allocations made in the danm.k8s.io group since the copy, and verify the cluster.",
		Phase:       engine.PhaseCutover,
		Reversible:  true,
	}
}

func (resyncAndVerify) Detect(ctx context.Context, handle *cluster.Handle) (engine.StepState, error) {
	_, verified, err := handle.LoadSnapshot(ctx, resyncRecordKey)
	if err != nil {
		return engine.StateNotStarted, err
	}
	if verified {
		return engine.StateDone, nil
	}
	return engine.StateNotStarted, nil
}

func (resyncAndVerify) Execute(ctx context.Context, handle *cluster.Handle) error {
	if handle.Mode == cluster.ModeUnknown {
		return errors.New("the deployment mode is unknown, run step 1 first or name the mode with --mode")
	}
	if err := resyncNetworkObjects(ctx, handle); err != nil {
		return err
	}
	if err := resyncEndpoints(ctx, handle); err != nil {
		return err
	}
	if err := resyncPods(ctx, handle); err != nil {
		return err
	}
	if err := verifyCluster(ctx, handle); err != nil {
		return err
	}
	return handle.SaveSnapshot(ctx, resyncRecordKey, time.Now().UTC().Format(time.RFC3339))
}

func (resyncAndVerify) Rollback(ctx context.Context, handle *cluster.Handle) error {
	stepID := handle.StepID
	_, err := handle.State.Mutate(ctx, PlanID, func(record *state.PlanRecord) error {
		stepRecord := record.Step(stepID)
		if stepRecord == nil {
			return nil
		}
		delete(stepRecord.Snapshots, resyncRecordKey)
		return nil
	})
	return err
}

func resyncNetworkObjects(ctx context.Context, handle *cluster.Handle) error {
	for _, kind := range networkKinds(handle.Mode) {
		target, err := targetSchema(handle.Mode, kind)
		if err != nil {
			return err
		}
		sources := map[string]bool{}
		carried, drifted := 0, 0
		err = eachObject(ctx, handle, kind.OldResource(), func(item *unstructured.Unstructured) error {
			name := objectName(item.GetNamespace(), item.GetName())
			sources[name] = true
			pruned, _ := pruneCopy(item, target)
			desired, err := transformed(kind, pruned)
			if err != nil {
				handle.Warnf("%s", err.Error())
				return nil
			}
			client := objectClient(handle, kind, kind.NewResource(), item.GetNamespace())
			existing, err := client.Get(ctx, item.GetName(), metav1.GetOptions{})
			if apierrors.IsNotFound(err) {
				handle.Warnf("%s %s was created in the %s group after step 6, and this step does not copy it; create it in the %s group by hand if it is still wanted",
					kind.Kind, name, OldGroup, NewGroup)
				return nil
			}
			if err != nil {
				return fmt.Errorf("cannot read %s %s of the %s group: %w", kind.Kind, name, NewGroup, err)
			}
			written, drift, err := resyncObject(ctx, handle, kind, desired, existing)
			if err != nil {
				return err
			}
			if written {
				carried++
			}
			if drift {
				drifted++
				handle.Warnf("%s %s drifted outside its IPAM state since step 6, and only its allocation state is carried over; apply the rest to the %s group by hand",
					kind.Kind, name, NewGroup)
			}
			return nil
		})
		if err != nil {
			return err
		}
		err = eachObject(ctx, handle, kind.NewResource(), func(item *unstructured.Unstructured) error {
			name := objectName(item.GetNamespace(), item.GetName())
			if !sources[name] {
				handle.Warnf("%s %s was deleted from the %s group after step 6, and its copy is left in place; delete it from the %s group by hand if that is still wanted",
					kind.Kind, name, OldGroup, NewGroup)
			}
			return nil
		})
		if err != nil {
			return err
		}
		handle.Printf("%s: the allocation state of %d objects is carried over, %d drifted outside IPAM.", kind.Kind, carried, drifted)
	}
	return nil
}

func resyncObject(ctx context.Context, handle *cluster.Handle, kind apiKind, desired, existing *unstructured.Unstructured) (bool, bool, error) {
	if kind == kindTenantConfig {
		return resyncTenantConfig(ctx, handle, desired, existing)
	}
	return resyncNetwork(ctx, handle, kind, desired, existing)
}

func resyncNetwork(ctx context.Context, handle *cluster.Handle, kind apiKind, desired, existing *unstructured.Unstructured) (bool, bool, error) {
	source, copied := &danmtypes.DanmNet{}, &danmtypes.DanmNet{}
	if err := decodeObjectInto(kind, desired, source); err != nil {
		return false, false, err
	}
	if err := decodeObjectInto(kind, existing, copied); err != nil {
		return false, false, err
	}
	drift := !networksMatchOutsideIpam(source, copied)
	if networkIpamMatches(source, copied) {
		return false, drift, nil
	}
	update := existing.DeepCopy()
	if err := setNetworkIpam(update, source); err != nil {
		return false, drift, err
	}
	client := objectClient(handle, kind, kind.NewResource(), existing.GetNamespace())
	if _, err := client.Update(ctx, update, metav1.UpdateOptions{}); err != nil {
		return false, drift, fmt.Errorf("cannot carry the allocation state of %s %s over to the %s group: %w",
			kind.Kind, objectName(existing.GetNamespace(), existing.GetName()), NewGroup, err)
	}
	return true, drift, nil
}

func resyncTenantConfig(ctx context.Context, handle *cluster.Handle, desired, existing *unstructured.Unstructured) (bool, bool, error) {
	source, copied := &danmtypes.TenantConfig{}, &danmtypes.TenantConfig{}
	if err := decodeObjectInto(kindTenantConfig, desired, source); err != nil {
		return false, false, err
	}
	if err := decodeObjectInto(kindTenantConfig, existing, copied); err != nil {
		return false, false, err
	}
	drift := !tenantConfigsMatchOutsideIpam(source, copied)
	allocs := map[string]string{}
	for _, device := range source.HostDevices {
		allocs[device.Name] = device.Alloc
	}
	changed := false
	for _, device := range copied.HostDevices {
		if alloc, known := allocs[device.Name]; known && !sameAlloc(alloc, device.Alloc) {
			changed = true
		}
	}
	if !changed {
		return false, drift, nil
	}
	update := existing.DeepCopy()
	if err := setTenantConfigAllocs(update, allocs); err != nil {
		return false, drift, err
	}
	client := objectClient(handle, kindTenantConfig, kindTenantConfig.NewResource(), existing.GetNamespace())
	if _, err := client.Update(ctx, update, metav1.UpdateOptions{}); err != nil {
		return false, drift, fmt.Errorf("cannot carry the VNI allocation of TenantConfig %s over to the %s group: %w", existing.GetName(), NewGroup, err)
	}
	return true, drift, nil
}

func networksMatchOutsideIpam(desired, existing *danmtypes.DanmNet) bool {
	left, right := desired.DeepCopy(), existing.DeepCopy()
	blankNetworkIpam(left)
	blankNetworkIpam(right)
	return networksMatch(left, right)
}

func blankNetworkIpam(network *danmtypes.DanmNet) {
	network.Spec.Options.Alloc = ""
	network.Spec.Options.Alloc6 = ""
	network.Spec.Options.Pool.LastIp = ""
	network.Spec.Options.Pool6.LastIp = ""
}

func networkIpamMatches(desired, existing *danmtypes.DanmNet) bool {
	return sameAlloc(desired.Spec.Options.Alloc, existing.Spec.Options.Alloc) &&
		sameAlloc(desired.Spec.Options.Alloc6, existing.Spec.Options.Alloc6) &&
		desired.Spec.Options.Pool.LastIp == existing.Spec.Options.Pool.LastIp &&
		desired.Spec.Options.Pool6.LastIp == existing.Spec.Options.Pool6.LastIp
}

func setNetworkIpam(item *unstructured.Unstructured, source *danmtypes.DanmNet) error {
	fields := []struct {
		path  []string
		value string
	}{
		{[]string{"spec", "Options", "alloc"}, source.Spec.Options.Alloc},
		{[]string{"spec", "Options", "alloc6"}, source.Spec.Options.Alloc6},
		{[]string{"spec", "Options", "allocation_pool", "lastIp"}, source.Spec.Options.Pool.LastIp},
		{[]string{"spec", "Options", "allocation_pool_v6", "lastIp"}, source.Spec.Options.Pool6.LastIp},
	}
	for _, field := range fields {
		if field.value == "" {
			unstructured.RemoveNestedField(item.Object, field.path...)
			continue
		}
		if err := unstructured.SetNestedField(item.Object, field.value, field.path...); err != nil {
			return fmt.Errorf("the allocation state of %s %s cannot be set: %w", item.GetKind(), objectName(item.GetNamespace(), item.GetName()), err)
		}
	}
	return nil
}

func tenantConfigsMatchOutsideIpam(desired, existing *danmtypes.TenantConfig) bool {
	left, right := desired.DeepCopy(), existing.DeepCopy()
	blankTenantConfigIpam(left)
	blankTenantConfigIpam(right)
	return tenantConfigsMatch(left, right)
}

func blankTenantConfigIpam(tenantConfig *danmtypes.TenantConfig) {
	for index := range tenantConfig.HostDevices {
		tenantConfig.HostDevices[index].Alloc = ""
	}
}

func setTenantConfigAllocs(item *unstructured.Unstructured, allocs map[string]string) error {
	devices, found, err := unstructured.NestedSlice(item.Object, "hostDevices")
	if err != nil {
		return fmt.Errorf("the host devices of TenantConfig %s cannot be read: %w", item.GetName(), err)
	}
	if !found {
		return nil
	}
	for index, entry := range devices {
		device, isObject := entry.(map[string]any)
		if !isObject {
			continue
		}
		name, _ := device["name"].(string)
		alloc, known := allocs[name]
		if !known {
			continue
		}
		if alloc == "" {
			delete(device, "alloc")
		} else {
			device["alloc"] = alloc
		}
		devices[index] = device
	}
	if err := unstructured.SetNestedSlice(item.Object, devices, "hostDevices"); err != nil {
		return fmt.Errorf("the VNI allocation of TenantConfig %s cannot be set: %w", item.GetName(), err)
	}
	return nil
}

func resyncEndpoints(ctx context.Context, handle *cluster.Handle) error {
	target, err := targetSchema(handle.Mode, kindDanmEp)
	if err != nil {
		return err
	}
	recorded, err := recordedRips(ctx, handle)
	if err != nil {
		return err
	}
	rips, err := modeReservedIps(ctx, handle)
	if err != nil {
		return err
	}
	sources := map[string]bool{}
	created, updated, deleted := 0, 0, 0
	err = eachObject(ctx, handle, kindDanmEp.OldResource(), func(item *unstructured.Unstructured) error {
		name := objectName(item.GetNamespace(), item.GetName())
		sources[name] = true
		endpoint := &danmtypes.DanmEp{}
		if err := decodeObjectInto(kindDanmEp, item, endpoint); err != nil {
			return err
		}
		pruned, fields := pruneCopy(item, target)
		if len(fields) > 0 {
			handle.Warnf("the copy of DanmEp %s loses fields the DANM %s schema does not know: %s", name, TargetRelease, strings.Join(fields, ", "))
		}
		desired, err := transformed(kindDanmEp, pruned)
		if err != nil {
			return err
		}
		client := objectClient(handle, kindDanmEp, kindDanmEp.NewResource(), item.GetNamespace())
		existing, err := client.Get(ctx, item.GetName(), metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			if err := setEndpointRipRefs(desired, replicatedRipRefs(handle, endpoint, recorded, rips)); err != nil {
				return err
			}
			if _, err := client.Create(ctx, desired, metav1.CreateOptions{}); err != nil {
				return fmt.Errorf("cannot create DanmEp %s in the %s group: %w", name, NewGroup, err)
			}
			created++
			return nil
		}
		if err != nil {
			return fmt.Errorf("cannot read DanmEp %s of the %s group: %w", name, NewGroup, err)
		}
		copied := &danmtypes.DanmEp{}
		if err := decodeObjectInto(kindDanmEp, existing, copied); err != nil {
			return err
		}
		refs := copied.Spec.RipRefs
		if refs == (danmtypes.RipReferences{}) {
			refs = replicatedRipRefs(handle, endpoint, recorded, rips)
		}
		if err := setEndpointRipRefs(desired, refs); err != nil {
			return err
		}
		same, err := sameCopy(kindDanmEp, desired, existing)
		if err != nil {
			return err
		}
		if same {
			return nil
		}
		update := desired.DeepCopy()
		update.SetResourceVersion(existing.GetResourceVersion())
		if _, err := client.Update(ctx, update, metav1.UpdateOptions{}); err != nil {
			return fmt.Errorf("cannot update DanmEp %s of the %s group: %w", name, NewGroup, err)
		}
		updated++
		return nil
	})
	if err != nil {
		return err
	}
	err = eachObject(ctx, handle, kindDanmEp.NewResource(), func(item *unstructured.Unstructured) error {
		name := objectName(item.GetNamespace(), item.GetName())
		if sources[name] {
			return nil
		}
		client := objectClient(handle, kindDanmEp, kindDanmEp.NewResource(), item.GetNamespace())
		if err := client.Delete(ctx, item.GetName(), metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("cannot delete DanmEp %s of the %s group: %w", name, NewGroup, err)
		}
		deleted++
		return nil
	})
	if err != nil {
		return err
	}
	handle.Printf("DanmEps: %d created, %d updated, %d deleted.", created, updated, deleted)
	return nil
}

func setEndpointRipRefs(item *unstructured.Unstructured, refs danmtypes.RipReferences) error {
	endpoint := &danmtypes.DanmEp{}
	if err := decodeObjectInto(kindDanmEp, item, endpoint); err != nil {
		return err
	}
	endpoint.Spec.RipRefs = refs
	endpoint.Labels = orNilMap(datastructs.EpLabels(endpoint.Labels, refs))
	encoded, err := encodeCopy(kindDanmEp, endpoint)
	if err != nil {
		return err
	}
	if refs == (danmtypes.RipReferences{}) {
		unstructured.RemoveNestedField(encoded.Object, "spec", ripRefsField)
	}
	*item = *encoded
	return nil
}

func replicatedRipRefs(handle *cluster.Handle, endpoint *danmtypes.DanmEp, recorded []recordedRip, rips []*danmtypes.ReservedIP) danmtypes.RipReferences {
	refs := danmtypes.RipReferences{}
	if !isTagged(endpoint) {
		return refs
	}
	name := objectName(endpoint.Namespace, endpoint.Name)
	for _, family := range []ipFamily{familyV4, familyV6} {
		plainKey, sharedKey := family.tagKeys()
		plainName, plainTagged := endpoint.Annotations[plainKey]
		sharedName, sharedTagged := endpoint.Annotations[sharedKey]
		switch {
		case plainTagged && !ripRecorded(recorded, endpoint.Namespace, plainName):
			handle.Warnf("DanmEp %s carries %s naming ReservedIP %s, which step 9 did not create; the DANM %s webhook refuses a ReservedIP for an address already in use, so it is not replicated",
				name, plainKey, plainName, TargetRelease)
		case !sharedTagged:
		case !ripRecorded(recorded, endpoint.Namespace, sharedName):
			handle.Warnf("DanmEp %s carries %s naming ReservedIP %s, which step 9 did not create; the DANM %s webhook refuses a ReservedIP for an address already in use, so it is not replicated",
				name, sharedKey, sharedName, TargetRelease)
		case !ripHoldsEndpoint(rips, endpoint, sharedName, family):
			handle.Warnf("DanmEp %s carries %s naming ReservedIP %s, which holds neither its network nor its %s address, so the endpoint is copied without a reference to it",
				name, sharedKey, sharedName, family)
		case family == familyV6:
			refs.V6 = sharedName
		default:
			refs.V4 = sharedName
		}
	}
	return refs
}

func ripRecorded(recorded []recordedRip, namespace, name string) bool {
	for _, rip := range recorded {
		if rip.Namespace == namespace && rip.Name == name {
			return true
		}
	}
	return false
}

func ripHoldsEndpoint(rips []*danmtypes.ReservedIP, endpoint *danmtypes.DanmEp, name string, family ipFamily) bool {
	raw := endpoint.Spec.Iface.Address
	if family == familyV6 {
		raw = endpoint.Spec.Iface.AddressIPv6
	}
	address := parseAddress(raw)
	if address == nil {
		return false
	}
	for _, rip := range rips {
		if rip.Namespace != endpoint.Namespace || rip.Name != name {
			continue
		}
		if rip.Spec.Network.Name != endpoint.Spec.NetworkName || rip.Spec.Network.Type != endpoint.Spec.ApiType {
			return false
		}
		return ripAddress(rip, family == familyV6) == address.String()
	}
	return false
}

func resyncPods(ctx context.Context, handle *cluster.Handle) error {
	if err := (annotateExistingPods{}).Execute(ctx, handle); err != nil {
		return err
	}
	if handle.Mode != cluster.ModeProduction {
		return nil
	}
	patched, err := rewriteMarkedPods(ctx, handle)
	if err != nil {
		return err
	}
	handle.Printf("%d Pods now reference their ReservedIP by name.", patched)
	return nil
}

func verifyCluster(ctx context.Context, handle *cluster.Handle) error {
	problems, err := unhealthyWorkloads(ctx, handle)
	if err != nil {
		return err
	}
	installed, newest, crdProblems, err := verifiedNewCrds(ctx, handle)
	if err != nil {
		return err
	}
	problems = append(problems, crdProblems...)
	if installed {
		stale, err := staleNetwatcherPods(ctx, handle, newest)
		if err != nil {
			return err
		}
		problems = append(problems, stale...)
	}
	if len(problems) > 0 {
		return fmt.Errorf("the cluster does not serve DANM %s yet, %d problems:\n  - %s", TargetRelease, len(problems), strings.Join(problems, "\n  - "))
	}
	handle.Printf("Every DANM %s component is healthy, and netwatcher serves the %s group.", TargetRelease, NewGroup)
	return nil
}

func unhealthyWorkloads(ctx context.Context, handle *cluster.Handle) ([]string, error) {
	problems := []string{}
	for _, component := range New().Meta().Components {
		ref, present, err := recordedWorkload(ctx, handle, component)
		if err != nil {
			return nil, err
		}
		if !present {
			continue
		}
		problem, found, err := workloadHealth(ctx, handle, ref)
		if err != nil {
			return nil, err
		}
		switch {
		case !found:
			problems = append(problems, fmt.Sprintf("%s of %s no longer exists", ref, component))
		case problem != "":
			problems = append(problems, fmt.Sprintf("%s of %s is not healthy: %s", ref, component, problem))
		}
	}
	shim, err := handle.Kube.AppsV1().Deployments(handle.Namespace).Get(ctx, ShimName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return append(problems, fmt.Sprintf("Deployment %s of the annotation shim no longer exists, run step 4 again", objectName(handle.Namespace, ShimName))), nil
	}
	if err != nil {
		return nil, fmt.Errorf("cannot read Deployment %s: %w", objectName(handle.Namespace, ShimName), err)
	}
	if problem := deploymentProblem(shim); problem != "" {
		problems = append(problems, fmt.Sprintf("Deployment %s of the annotation shim is not healthy: %s", objectName(handle.Namespace, ShimName), problem))
	}
	return problems, nil
}

func verifiedNewCrds(ctx context.Context, handle *cluster.Handle) (bool, metav1.Time, []string, error) {
	problems := []string{}
	newest := metav1.Time{}
	for _, kind := range targetKinds(handle.Mode) {
		crd, err := handle.ApiExt.ApiextensionsV1().CustomResourceDefinitions().Get(ctx, kind.NewCrdName(), metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			problems = append(problems, fmt.Sprintf("CRD %s is missing, run step 2 again", kind.NewCrdName()))
			continue
		}
		if err != nil {
			return false, newest, nil, fmt.Errorf("cannot read CRD %s: %w", kind.NewCrdName(), err)
		}
		if !crdCondition(crd, apiextensionsv1.Established) {
			problems = append(problems, fmt.Sprintf("CRD %s is not established", kind.NewCrdName()))
			continue
		}
		if crd.CreationTimestamp.After(newest.Time) {
			newest = crd.CreationTimestamp
		}
	}
	return len(problems) == 0, newest, problems, nil
}

func staleNetwatcherPods(ctx context.Context, handle *cluster.Handle, newest metav1.Time) ([]string, error) {
	ref, present, err := recordedWorkload(ctx, handle, ComponentNetwatcher)
	if err != nil || !present {
		return nil, err
	}
	object, err := loadWorkload(ctx, handle, ref)
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	selector, err := metav1.LabelSelectorAsSelector(object.selector())
	if err != nil {
		return nil, fmt.Errorf("the Pod selector of %s cannot be read: %w", ref, err)
	}
	pods, err := handle.Kube.CoreV1().Pods(ref.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector.String()})
	if err != nil {
		return nil, fmt.Errorf("cannot list the Pods of %s: %w", ref, err)
	}
	problems := []string{}
	for index := range pods.Items {
		pod := &pods.Items[index]
		if pod.DeletionTimestamp != nil {
			continue
		}
		if started(pod).After(newest.Time) {
			continue
		}
		problems = append(problems, fmt.Sprintf("netwatcher Pod %s on node %s started before the %s CRDs were installed, so it discovered only the %s group; delete the Pod to restart it",
			objectName(pod.Namespace, pod.Name), orDash(pod.Spec.NodeName), NewGroup, OldGroup))
	}
	return problems, nil
}

func started(pod *corev1.Pod) metav1.Time {
	if pod.Status.StartTime != nil {
		return *pod.Status.StartTime
	}
	return pod.CreationTimestamp
}
