package v43to44

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/danm-cni/danm-utils/pkg/dma/cluster"
	"github.com/danm-cni/danm-utils/pkg/dma/engine"
	danmtypes "github.com/danm-cni/danm/crd/apis/danm/v1"
	"github.com/danm-cni/danm/pkg/datastructs"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
)

const reservedIpsRecordKey = "reserved-ips"

type recordedRip struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
}

type ripSources struct {
	tagged   []*danmtypes.DanmEp
	byName   map[string]*danmtypes.DanmEp
	networks map[networkKey]*danmtypes.DanmNet
	configs  []*danmtypes.TenantConfig
}

type replicateReservedIps struct{}

func (replicateReservedIps) Meta() engine.StepMeta {
	return engine.StepMeta{
		ID:          9,
		Name:        "replicate-reserved-ips",
		Description: "Create the ReservedIPs tagged on old-group DanmEps.",
		Phase:       engine.PhaseConvert,
		Reversible:  true,
	}
}

func (replicateReservedIps) Detect(ctx context.Context, handle *cluster.Handle) (engine.StepState, error) {
	if handle.Mode == cluster.ModeUnknown {
		return engine.StateNotStarted, nil
	}
	if handle.Mode != cluster.ModeProduction {
		return engine.StateDone, nil
	}
	sources, err := gatherRipSources(ctx, handle)
	if err != nil {
		return engine.StateNotStarted, err
	}
	planned, _ := planReservedIps(sources.tagged, sources.networks, sources.configs)
	if len(planned) == 0 {
		return engine.StateDone, nil
	}
	present, missing := 0, 0
	for _, plan := range planned {
		_, err := objectClient(handle, kindReservedIP, kindReservedIP.NewResource(), plan.Namespace).Get(ctx, plan.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			missing++
			continue
		}
		if err != nil {
			return engine.StateNotStarted, fmt.Errorf("cannot read ReservedIP %s: %w", objectName(plan.Namespace, plan.Name), err)
		}
		present++
	}
	if present == 0 {
		return engine.StateNotStarted, nil
	}
	if missing > 0 {
		return engine.StatePartial, nil
	}
	pending, err := pendingRipWork(ctx, handle, planned)
	if err != nil {
		return engine.StateNotStarted, err
	}
	if pending {
		return engine.StatePartial, nil
	}
	return engine.StateDone, nil
}

func (replicateReservedIps) Execute(ctx context.Context, handle *cluster.Handle) error {
	if handle.Mode == cluster.ModeUnknown {
		return errors.New("the deployment mode is unknown, run step 1 first or name the mode with --mode")
	}
	if handle.Mode != cluster.ModeProduction {
		handle.Printf("Lightweight mode has no ReservedIPs, there is nothing to replicate.")
		return nil
	}
	sources, err := gatherRipSources(ctx, handle)
	if err != nil {
		return err
	}
	planned, problems := planReservedIps(sources.tagged, sources.networks, sources.configs)
	if len(problems) > 0 {
		return ripFailure(problems)
	}
	if len(planned) == 0 {
		handle.Printf("No DanmEp carries a ReservedIP replication tag, there is nothing to replicate.")
		return nil
	}
	if err := recordPlannedRips(ctx, handle, planned); err != nil {
		return err
	}
	created, kept := 0, 0
	for _, plan := range planned {
		desired := desiredReservedIp(plan)
		existed, err := applyReservedIp(ctx, handle, desired)
		if err != nil {
			return err
		}
		if existed {
			kept++
		} else {
			created++
		}
		if err := updateNetworkStatus(ctx, handle, plan.Network, desired.Namespace, desired.Name, true); err != nil {
			return err
		}
	}
	handle.Printf("ReservedIPs: %d created, %d already present.", created, kept)
	if err := applyRipRefs(ctx, handle, planned); err != nil {
		return err
	}
	patched, err := rewriteMarkedPods(ctx, handle)
	if err != nil {
		return err
	}
	handle.Printf("%d Pods now reference their ReservedIP by name.", patched)
	return warnDynamicTags(ctx, handle, sources.tagged)
}

func (replicateReservedIps) Rollback(ctx context.Context, handle *cluster.Handle) error {
	recorded, err := recordedRips(ctx, handle)
	if err != nil {
		return err
	}
	if len(recorded) == 0 {
		return nil
	}
	names := map[string]bool{}
	for _, rip := range recorded {
		names[objectName(rip.Namespace, rip.Name)] = true
	}
	if err := clearRipRefs(ctx, handle, names); err != nil {
		return err
	}
	deleted := 0
	for _, rip := range recorded {
		client := objectClient(handle, kindReservedIP, kindReservedIP.NewResource(), rip.Namespace)
		item, err := client.Get(ctx, rip.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return fmt.Errorf("cannot read ReservedIP %s: %w", objectName(rip.Namespace, rip.Name), err)
		}
		object := &danmtypes.ReservedIP{}
		if err := decodeObjectInto(kindReservedIP, item, object); err != nil {
			return err
		}
		network := networkKey{Kind: object.Spec.Network.Type, Namespace: rip.Namespace, Name: object.Spec.Network.Name}
		if network.Kind == kindClusterNetwork.Kind {
			network.Namespace = ""
		}
		if err := updateNetworkStatus(ctx, handle, network, rip.Namespace, rip.Name, false); err != nil {
			return err
		}
		if err := client.Delete(ctx, rip.Name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("cannot delete ReservedIP %s: %w", objectName(rip.Namespace, rip.Name), err)
		}
		deleted++
	}
	handle.Printf("Deleted %d ReservedIPs.", deleted)
	patched, err := rewriteMarkedPods(ctx, handle)
	if err != nil {
		return err
	}
	handle.Printf("%d Pods no longer reference a ReservedIP.", patched)
	return nil
}

func gatherRipSources(ctx context.Context, handle *cluster.Handle) (*ripSources, error) {
	sources := &ripSources{byName: map[string]*danmtypes.DanmEp{}, networks: map[networkKey]*danmtypes.DanmNet{}}
	err := eachObject(ctx, handle, kindDanmEp.OldResource(), func(item *unstructured.Unstructured) error {
		endpoint := &danmtypes.DanmEp{}
		if err := decodeObjectInto(kindDanmEp, item, endpoint); err != nil {
			return err
		}
		if !isTagged(endpoint) {
			return nil
		}
		sources.tagged = append(sources.tagged, endpoint)
		sources.byName[objectName(endpoint.Namespace, endpoint.Name)] = endpoint
		return nil
	})
	if err != nil {
		return nil, err
	}
	for _, kind := range []apiKind{kindTenantNetwork, kindClusterNetwork} {
		err := eachObject(ctx, handle, kind.OldResource(), func(item *unstructured.Unstructured) error {
			network := &danmtypes.DanmNet{}
			if err := decodeObjectInto(kind, item, network); err != nil {
				return err
			}
			network.Kind = kind.Kind
			sources.networks[keyOfNetwork(network)] = network
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	err = eachObject(ctx, handle, kindTenantConfig.OldResource(), func(item *unstructured.Unstructured) error {
		config := &danmtypes.TenantConfig{}
		if err := decodeObjectInto(kindTenantConfig, item, config); err != nil {
			return err
		}
		sources.configs = append(sources.configs, config)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return sources, nil
}

func desiredReservedIp(plan *plannedRip) *danmtypes.ReservedIP {
	object := &danmtypes.ReservedIP{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: plan.Namespace,
			Name:      plan.Name,
			Labels:    map[string]string{datastructs.NetworkNameLabel: plan.Network.Name, datastructs.NetworkTypeLabel: plan.Network.Kind},
		},
		Spec: danmtypes.ReservedIPSpec{
			Network:     danmtypes.RipNetworkSelector{Name: plan.Network.Name, Type: plan.Network.Kind},
			Description: "replicated by dma from DanmEp " + strings.Join(plan.Sources, ", "),
		},
	}
	for _, address := range plan.Addresses {
		if address == nil {
			continue
		}
		object.Spec.Ips = append(object.Spec.Ips, danmtypes.RipIP{Address: address.IP.String(), Shareable: address.Shared})
	}
	return object
}

func applyReservedIp(ctx context.Context, handle *cluster.Handle, desired *danmtypes.ReservedIP) (bool, error) {
	name := objectName(desired.Namespace, desired.Name)
	client := objectClient(handle, kindReservedIP, kindReservedIP.NewResource(), desired.Namespace)
	existing, err := client.Get(ctx, desired.Name, metav1.GetOptions{})
	if err == nil {
		object := &danmtypes.ReservedIP{}
		if err := decodeObjectInto(kindReservedIP, existing, object); err != nil {
			return false, err
		}
		if !metadataMatches(desired.ObjectMeta, object.ObjectMeta) || !equality.Semantic.DeepEqual(desired.Spec, object.Spec) {
			return false, fmt.Errorf("ReservedIP %s already exists, and it is not the one the tags describe", name)
		}
		return true, nil
	}
	if !apierrors.IsNotFound(err) {
		return false, fmt.Errorf("cannot read ReservedIP %s: %w", name, err)
	}
	item, err := encodeCopy(kindReservedIP, desired)
	if err != nil {
		return false, err
	}
	unstructured.RemoveNestedField(item.Object, "status")
	if _, err := client.Create(ctx, item, metav1.CreateOptions{}); err != nil {
		return false, fmt.Errorf("cannot create ReservedIP %s: %w", name, err)
	}
	return false, nil
}

func updateNetworkStatus(ctx context.Context, handle *cluster.Handle, network networkKey, namespace, name string, add bool) error {
	kind := kindTenantNetwork
	if network.Kind == kindClusterNetwork.Kind {
		kind = kindClusterNetwork
	}
	client := objectClient(handle, kind, kind.NewResource(), network.Namespace)
	item, err := client.Get(ctx, network.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return fmt.Errorf("the copy of %s is missing, run step 6 again", network)
	}
	if err != nil {
		return fmt.Errorf("cannot read the copy of %s: %w", network, err)
	}
	object := &danmtypes.DanmNet{}
	if err := decodeObjectInto(kind, item, object); err != nil {
		return err
	}
	status := object.Status
	if add {
		status = statusWithRip(status, namespace, name)
	} else {
		status = statusWithoutRip(status, namespace, name)
	}
	if equality.Semantic.DeepEqual(status, object.Status) {
		return nil
	}
	content, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&status)
	if err != nil {
		return fmt.Errorf("the status of %s cannot be serialised: %w", network, err)
	}
	if err := unstructured.SetNestedMap(item.Object, content, "status"); err != nil {
		return fmt.Errorf("the status of %s cannot be set: %w", network, err)
	}
	if _, err := client.UpdateStatus(ctx, item, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("cannot update the status of %s: %w", network, err)
	}
	return nil
}

func statusWithRip(status danmtypes.DanmNetStatus, namespace, name string) danmtypes.DanmNetStatus {
	updated := danmtypes.DanmNetStatus{}
	found := false
	for _, entry := range status.ReservedIPs {
		if entry.Namespace == namespace {
			found = true
			if !containsName(entry.Objects, name) {
				entry.Objects = append(append([]string{}, entry.Objects...), name)
			}
		}
		updated.ReservedIPs = append(updated.ReservedIPs, entry)
	}
	if !found {
		updated.ReservedIPs = append(updated.ReservedIPs, danmtypes.NetRipStatus{Namespace: namespace, Objects: []string{name}})
	}
	return updated
}

func statusWithoutRip(status danmtypes.DanmNetStatus, namespace, name string) danmtypes.DanmNetStatus {
	updated := danmtypes.DanmNetStatus{}
	for _, entry := range status.ReservedIPs {
		if entry.Namespace != namespace {
			updated.ReservedIPs = append(updated.ReservedIPs, entry)
			continue
		}
		objects := []string{}
		for _, object := range entry.Objects {
			if object != name {
				objects = append(objects, object)
			}
		}
		if len(objects) == 0 {
			continue
		}
		entry.Objects = objects
		updated.ReservedIPs = append(updated.ReservedIPs, entry)
	}
	return updated
}

func containsName(names []string, name string) bool {
	for _, candidate := range names {
		if candidate == name {
			return true
		}
	}
	return false
}

func applyRipRefs(ctx context.Context, handle *cluster.Handle, planned []*plannedRip) error {
	refs := plannedRipRefs(planned)
	patched := 0
	for epName, wanted := range refs {
		namespace, name, _ := strings.Cut(epName, "/")
		client := objectClient(handle, kindDanmEp, kindDanmEp.NewResource(), namespace)
		item, err := client.Get(ctx, name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return fmt.Errorf("the copy of DanmEp %s is missing, run step 7 again", epName)
		}
		if err != nil {
			return fmt.Errorf("cannot read the copy of DanmEp %s: %w", epName, err)
		}
		endpoint := &danmtypes.DanmEp{}
		if err := decodeObjectInto(kindDanmEp, item, endpoint); err != nil {
			return err
		}
		if endpoint.Spec.RipRefs == wanted {
			continue
		}
		endpoint.Spec.RipRefs = wanted
		endpoint.Labels = orNilMap(datastructs.EpLabels(endpoint.Labels, wanted))
		updated, err := encodeCopy(kindDanmEp, endpoint)
		if err != nil {
			return err
		}
		updated.SetResourceVersion(item.GetResourceVersion())
		if _, err := client.Update(ctx, updated, metav1.UpdateOptions{}); err != nil {
			return fmt.Errorf("cannot update the copy of DanmEp %s: %w", epName, err)
		}
		patched++
	}
	handle.Printf("%d DanmEp copies now reference a shared ReservedIP.", patched)
	return nil
}

func plannedRipRefs(planned []*plannedRip) map[string]danmtypes.RipReferences {
	refs := map[string]danmtypes.RipReferences{}
	for _, plan := range planned {
		for family, address := range plan.Addresses {
			if address == nil || !address.Shared {
				continue
			}
			for _, epName := range address.Users {
				reference := refs[epName]
				if ipFamily(family) == familyV6 {
					reference.V6 = plan.Name
				} else {
					reference.V4 = plan.Name
				}
				refs[epName] = reference
			}
		}
	}
	return refs
}

func clearRipRefs(ctx context.Context, handle *cluster.Handle, names map[string]bool) error {
	cleared := 0
	err := eachObject(ctx, handle, kindDanmEp.NewResource(), func(item *unstructured.Unstructured) error {
		endpoint := &danmtypes.DanmEp{}
		if err := decodeObjectInto(kindDanmEp, item, endpoint); err != nil {
			return err
		}
		refs := endpoint.Spec.RipRefs
		if refs.V4 != "" && names[objectName(endpoint.Namespace, refs.V4)] {
			refs.V4 = ""
		}
		if refs.V6 != "" && names[objectName(endpoint.Namespace, refs.V6)] {
			refs.V6 = ""
		}
		if refs == endpoint.Spec.RipRefs {
			return nil
		}
		endpoint.Spec.RipRefs = refs
		endpoint.Labels = orNilMap(datastructs.EpLabels(endpoint.Labels, refs))
		updated, err := encodeCopy(kindDanmEp, endpoint)
		if err != nil {
			return err
		}
		if refs == (danmtypes.RipReferences{}) {
			unstructured.RemoveNestedField(updated.Object, "spec", ripRefsField)
		}
		updated.SetResourceVersion(item.GetResourceVersion())
		client := objectClient(handle, kindDanmEp, kindDanmEp.NewResource(), endpoint.Namespace)
		if _, err := client.Update(ctx, updated, metav1.UpdateOptions{}); err != nil {
			return fmt.Errorf("cannot update the copy of DanmEp %s: %w", objectName(endpoint.Namespace, endpoint.Name), err)
		}
		cleared++
		return nil
	})
	if err != nil {
		return err
	}
	handle.Printf("%d DanmEp copies no longer reference a ReservedIP.", cleared)
	return nil
}

func rewriteMarkedPods(ctx context.Context, handle *cluster.Handle) (int, error) {
	rips, err := allReservedIps(ctx, handle)
	if err != nil {
		return 0, err
	}
	patched := 0
	err = eachPod(ctx, handle, func(pod *corev1.Pod) error {
		if pod.Annotations[CopiedMarkerKey] == "" || pod.DeletionTimestamp != nil {
			return nil
		}
		old, hasOld := pod.Annotations[OldInterfacesKey]
		if !hasOld {
			return nil
		}
		value, notes := NewInterfaces(old, pod.Namespace, rips)
		for _, note := range notes {
			handle.Warnf("%s", note)
		}
		if value == pod.Annotations[NewInterfacesKey] {
			return nil
		}
		changed, err := patchPodAnnotations(ctx, handle, pod, map[string]any{NewInterfacesKey: value})
		if err != nil {
			return err
		}
		if changed {
			patched++
		}
		return nil
	})
	return patched, err
}

func pendingRipWork(ctx context.Context, handle *cluster.Handle, planned []*plannedRip) (bool, error) {
	for epName, wanted := range plannedRipRefs(planned) {
		namespace, name, _ := strings.Cut(epName, "/")
		item, err := objectClient(handle, kindDanmEp, kindDanmEp.NewResource(), namespace).Get(ctx, name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		if err != nil {
			return false, fmt.Errorf("cannot read the copy of DanmEp %s: %w", epName, err)
		}
		endpoint := &danmtypes.DanmEp{}
		if err := decodeObjectInto(kindDanmEp, item, endpoint); err != nil {
			return false, err
		}
		if endpoint.Spec.RipRefs != wanted {
			return true, nil
		}
	}
	rips, err := allReservedIps(ctx, handle)
	if err != nil {
		return false, err
	}
	pending := false
	err = eachPod(ctx, handle, func(pod *corev1.Pod) error {
		if pending || pod.Annotations[CopiedMarkerKey] == "" || pod.DeletionTimestamp != nil {
			return nil
		}
		old, hasOld := pod.Annotations[OldInterfacesKey]
		if !hasOld {
			return nil
		}
		value, _ := NewInterfaces(old, pod.Namespace, rips)
		if value != pod.Annotations[NewInterfacesKey] {
			pending = true
		}
		return nil
	})
	return pending, err
}

func recordPlannedRips(ctx context.Context, handle *cluster.Handle, planned []*plannedRip) error {
	records := make([]recordedRip, 0, len(planned))
	for _, plan := range planned {
		records = append(records, recordedRip{Namespace: plan.Namespace, Name: plan.Name})
	}
	encoded, err := json.Marshal(records)
	if err != nil {
		return fmt.Errorf("the planned ReservedIPs cannot be serialised: %w", err)
	}
	return handle.SaveSnapshot(ctx, reservedIpsRecordKey, string(encoded))
}

func recordedRips(ctx context.Context, handle *cluster.Handle) ([]recordedRip, error) {
	snapshot, found, err := handle.LoadStepSnapshot(ctx, PlanID, replicateReservedIps{}.Meta().ID, reservedIpsRecordKey)
	if err != nil || !found {
		return nil, err
	}
	records := []recordedRip{}
	if err := json.Unmarshal([]byte(snapshot), &records); err != nil {
		return nil, fmt.Errorf("the recorded ReservedIPs of step 9 are corrupted: %w", err)
	}
	return records, nil
}

func ripFailure(problems []string) error {
	reported := problems
	if len(reported) > maxReportedProblems {
		reported = append(append([]string{}, reported[:maxReportedProblems]...),
			fmt.Sprintf("%d more problems", len(problems)-maxReportedProblems))
	}
	return fmt.Errorf("the ReservedIP replication tags describe %d problems, nothing was created:\n  - %s", len(problems), strings.Join(reported, "\n  - "))
}

func warnDynamicTags(ctx context.Context, handle *cluster.Handle, tagged []*danmtypes.DanmEp) error {
	for _, endpoint := range tagged {
		for _, family := range []ipFamily{familyV4, familyV6} {
			plainKey, sharedKey := family.tagKeys()
			name, tagged := endpoint.Annotations[plainKey]
			if !tagged {
				name, tagged = endpoint.Annotations[sharedKey]
			}
			if !tagged {
				continue
			}
			dynamic, err := podAsksDynamically(ctx, handle, endpoint, family)
			if err != nil {
				return err
			}
			if !dynamic {
				continue
			}
			handle.Warnf("DanmEp %s is tagged for ReservedIP %s, but Pod %s asks for its %s address dynamically; DANM %s hands such a Pod a different address, so the reservation keeps its address for no one until the request names the ReservedIP",
				objectName(endpoint.Namespace, endpoint.Name), name, objectName(endpoint.Namespace, endpoint.Spec.Pod), family, TargetRelease)
		}
	}
	return nil
}

func podAsksDynamically(ctx context.Context, handle *cluster.Handle, endpoint *danmtypes.DanmEp, family ipFamily) (bool, error) {
	if endpoint.Spec.Pod == "" {
		return false, nil
	}
	pod, err := handle.Kube.CoreV1().Pods(endpoint.Namespace).Get(ctx, endpoint.Spec.Pod, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("cannot read Pod %s: %w", objectName(endpoint.Namespace, endpoint.Spec.Pod), err)
	}
	entries := []json.RawMessage{}
	if err := json.Unmarshal([]byte(pod.Annotations[OldInterfacesKey]), &entries); err != nil {
		return false, nil
	}
	networkField := tenantNetworkKey
	if endpoint.Spec.ApiType == kindClusterNetwork.Kind {
		networkField = clusterNetKey
	}
	addressField := ipKey
	if family == familyV6 {
		addressField = ip6Key
	}
	for _, entry := range entries {
		fields, err := decodeObject(entry)
		if err != nil {
			return false, nil
		}
		if fieldString(fields, networkField) != endpoint.Spec.NetworkName {
			continue
		}
		if fieldString(fields, addressField) == dynamicIp {
			return true, nil
		}
	}
	return false, nil
}
