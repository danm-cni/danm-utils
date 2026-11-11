package v43to44

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/danm-cni/danm-utils/pkg/dma/cluster"
	"github.com/danm-cni/danm-utils/pkg/dma/engine"
	"github.com/danm-cni/danm-utils/pkg/dma/state"
	danmtypes "github.com/danm-cni/danm/crd/apis/danm/v1"
	"github.com/danm-cni/danm/pkg/bitarray"
	appsv1 "k8s.io/api/apps/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	structuralschema "k8s.io/apiextensions-apiserver/pkg/apiserver/schema"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/schema/pruning"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/pager"
	"k8s.io/utils/cpuset"
)

const (
	preflightReportKey  = "preflight-report"
	listPageSize        = 500
	maxListedObjects    = 10
	maxReportedProblems = 20
	MaxAllowedVniVlan   = 4094
)

type verifySourceCluster struct{}

type preflightReport struct {
	Mode          string         `json:"mode"`
	Inventory     map[string]int `json:"inventory"`
	MtuAtRisk     int            `json:"mtuAtRisk,omitempty"`
	PrunedObjects int            `json:"prunedObjects,omitempty"`
	ReservedIPs   int            `json:"reservedIPs,omitempty"`
	Architecture  string         `json:"architecture,omitempty"`
}

func (verifySourceCluster) Meta() engine.StepMeta {
	return engine.StepMeta{
		ID:   1,
		Name: "verify-source-cluster",
		Description: "Verify that the cluster runs a healthy DANM 4.3 installation which migrates without data loss, " +
			"and record its deployment mode. Changes nothing in the cluster.",
		Phase:      engine.PhasePreflight,
		Reversible: true,
	}
}

func (verifySourceCluster) Detect(ctx context.Context, handle *cluster.Handle) (engine.StepState, error) {
	record, err := handle.State.Load(ctx, PlanID)
	if err != nil {
		return engine.StateNotStarted, err
	}
	if record == nil || record.Mode == "" {
		return engine.StateNotStarted, nil
	}
	stepRecord := record.Step(1)
	if stepRecord == nil {
		return engine.StateNotStarted, nil
	}
	if _, verified := stepRecord.Snapshots[preflightReportKey]; !verified {
		return engine.StateNotStarted, nil
	}
	return engine.StateDone, nil
}

func (verifySourceCluster) Execute(ctx context.Context, handle *cluster.Handle) error {
	check := &preflight{handle: handle, report: preflightReport{Inventory: map[string]int{}}}
	if err := check.run(ctx); err != nil {
		return err
	}
	if len(check.problems) > 0 {
		return check.failure()
	}
	check.report.Mode = string(check.mode)
	encoded, err := json.Marshal(check.report)
	if err != nil {
		return fmt.Errorf("cannot serialise the preflight report: %w", err)
	}
	if err := handle.SaveSnapshot(ctx, preflightReportKey, string(encoded)); err != nil {
		return err
	}
	if _, err := handle.State.Mutate(ctx, PlanID, func(record *state.PlanRecord) error {
		record.Mode = string(check.mode)
		record.Workloads = check.workloads
		record.PrunedFieldsAccepted = handle.Options.AcceptPrunedFields
		record.PrunedObjects = check.report.PrunedObjects
		return nil
	}); err != nil {
		return err
	}
	handle.Mode = check.mode
	handle.Printf("The source cluster is a migratable DANM 4.3 installation in %s mode.", check.mode)
	return nil
}

func (verifySourceCluster) Rollback(ctx context.Context, handle *cluster.Handle) error {
	if _, err := handle.State.Mutate(ctx, PlanID, func(record *state.PlanRecord) error {
		record.Mode = ""
		record.Workloads = nil
		record.PrunedFieldsAccepted = false
		record.PrunedObjects = 0
		return nil
	}); err != nil {
		return err
	}
	handle.Mode = cluster.ModeUnknown
	return nil
}

type preflight struct {
	handle    *cluster.Handle
	mode      cluster.Mode
	report    preflightReport
	problems  []string
	sources   sourceObjects
	workloads map[string]state.WorkloadRecord
}

type sourceObjects struct {
	networks      []*danmtypes.DanmNet
	tenantConfigs []*danmtypes.TenantConfig
	taggedEps     []*danmtypes.DanmEp
}

func (check *preflight) fail(format string, args ...any) {
	problem := fmt.Sprintf(format, args...)
	check.problems = append(check.problems, problem)
	check.handle.Printf("PROBLEM: %s", problem)
}

func (check *preflight) failure() error {
	reported := check.problems
	if len(reported) > maxReportedProblems {
		reported = append(append([]string{}, reported[:maxReportedProblems]...),
			fmt.Sprintf("%d more problems, printed above", len(check.problems)-maxReportedProblems))
	}
	return fmt.Errorf("the source cluster cannot be migrated, preflight found %d problems:\n  - %s", len(check.problems), strings.Join(reported, "\n  - "))
}

func (check *preflight) run(ctx context.Context) error {
	crds, err := check.oldCrds(ctx)
	if err != nil {
		return err
	}
	check.mode = check.resolveMode(crds)
	if check.mode == cluster.ModeUnknown {
		return nil
	}
	present := check.verifyOldCrds(crds)
	if err := check.verifyWorkloads(ctx); err != nil {
		return err
	}
	if err := check.scanSources(ctx, present); err != nil {
		return err
	}
	check.verifyMtu()
	if check.mode == cluster.ModeProduction {
		check.verifyVlanRanges()
	}
	if err := check.verifyRipTags(ctx); err != nil {
		return err
	}
	if err := check.verifyImages(ctx); err != nil {
		return err
	}
	return check.verifyPayload(ctx)
}

func (check *preflight) oldCrds(ctx context.Context) (map[apiKind]*apiextensionsv1.CustomResourceDefinition, error) {
	crds := map[apiKind]*apiextensionsv1.CustomResourceDefinition{}
	for _, kind := range oldKinds {
		crd, err := check.handle.ApiExt.ApiextensionsV1().CustomResourceDefinitions().Get(ctx, kind.OldCrdName(), metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("cannot read CRD %s: %w", kind.OldCrdName(), err)
		}
		crds[kind] = crd
	}
	return crds, nil
}

func (check *preflight) resolveMode(crds map[apiKind]*apiextensionsv1.CustomResourceDefinition) cluster.Mode {
	lightweight := crds[kindDanmNet] != nil
	production := false
	for _, kind := range productionOnlyKinds {
		production = production || crds[kind] != nil
	}
	override := check.handle.Mode
	force := check.handle.Options.Force
	if !lightweight && !production {
		check.fail("no DANM 4.3 network API is installed: neither %s nor any of the production CRDs exist", kindDanmNet.OldCrdName())
		return cluster.ModeUnknown
	}
	if lightweight && production {
		if override == cluster.ModeUnknown || !force {
			check.fail("both the lightweight and the production DANM 4.3 network APIs are installed, which DANM does not support; " +
				"pass --mode together with --force to migrate one of them and leave the other in place")
			return cluster.ModeUnknown
		}
		check.handle.Warnf("both the lightweight and the production network APIs are installed, migrating only the %s ones because --mode and --force were given", override)
		return override
	}
	detected := cluster.ModeLightweight
	if production {
		detected = cluster.ModeProduction
	}
	if override == cluster.ModeUnknown || override == detected {
		check.handle.Printf("Detected deployment mode: %s", detected)
		return detected
	}
	if !force {
		check.fail("--mode %s contradicts the %s mode detected from the installed CRDs, add --force to override the detection", override, detected)
		return cluster.ModeUnknown
	}
	check.handle.Warnf("migrating in %s mode, overriding the detected %s mode because --force was given", override, detected)
	return override
}

func (check *preflight) verifyOldCrds(crds map[apiKind]*apiextensionsv1.CustomResourceDefinition) map[apiKind]bool {
	present := map[apiKind]bool{}
	for _, kind := range sourceKinds(check.mode) {
		crd, found := crds[kind]
		if !found {
			check.fail("CRD %s is missing, a DANM 4.3 installation in %s mode needs it", kind.OldCrdName(), check.mode)
			continue
		}
		if problem := legacyShapeProblem(crd, kind); problem != "" {
			check.fail("CRD %s is not in the DANM 4.3 shape: %s", kind.OldCrdName(), problem)
			continue
		}
		present[kind] = true
	}
	return present
}

func legacyShapeProblem(crd *apiextensionsv1.CustomResourceDefinition, kind apiKind) string {
	if crd.Spec.Group != OldGroup {
		return fmt.Sprintf("its group is %s instead of %s", crd.Spec.Group, OldGroup)
	}
	if crd.Spec.Names.Kind != kind.Kind {
		return fmt.Sprintf("its kind is %s instead of %s", crd.Spec.Names.Kind, kind.Kind)
	}
	if crd.Spec.Scope != kind.Scope() {
		return fmt.Sprintf("its scope is %s instead of %s", crd.Spec.Scope, kind.Scope())
	}
	for _, version := range crd.Spec.Versions {
		if version.Name != Version {
			continue
		}
		if !version.Served || !version.Storage {
			return fmt.Sprintf("version %s is not both served and stored", Version)
		}
		return ""
	}
	return fmt.Sprintf("it has no version %s", Version)
}

func (check *preflight) verifyWorkloads(ctx context.Context) error {
	resolved, err := check.handle.Workloads.ResolveAll(workloads)
	if err != nil {
		check.fail("%s", err.Error())
		return nil
	}
	check.workloads = map[string]state.WorkloadRecord{}
	for _, workload := range workloads {
		ref := resolved[workload.Component]
		problem, found, err := workloadHealth(ctx, check.handle, ref)
		if err != nil {
			return err
		}
		check.workloads[workload.Component] = cluster.RecordWorkload(ref, found)
		switch {
		case !found && !workload.Required:
			check.handle.Printf("%s is not deployed as %s, later steps skip it.", workload.Component, ref)
		case !found:
			check.fail("%s of %s is not deployed", ref, workload.Component)
		case problem != "":
			check.fail("%s of %s is not healthy: %s", ref, workload.Component, problem)
		default:
			check.handle.Printf("%s is healthy, deployed as %s.", workload.Component, ref)
		}
	}
	return nil
}

func workloadHealth(ctx context.Context, handle *cluster.Handle, ref cluster.WorkloadRef) (string, bool, error) {
	var (
		problem string
		err     error
	)
	switch ref.Kind {
	case cluster.WorkloadDaemonSet:
		var daemonSet *appsv1.DaemonSet
		daemonSet, err = handle.Kube.AppsV1().DaemonSets(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
		if err == nil {
			problem = daemonSetProblem(daemonSet)
		}
	case cluster.WorkloadDeployment:
		var deployment *appsv1.Deployment
		deployment, err = handle.Kube.AppsV1().Deployments(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
		if err == nil {
			problem = deploymentProblem(deployment)
		}
	default:
		return "", false, fmt.Errorf("workload %s has an unsupported kind", ref)
	}
	if apierrors.IsNotFound(err) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("cannot read %s: %w", ref, err)
	}
	return problem, true, nil
}

func daemonSetProblem(daemonSet *appsv1.DaemonSet) string {
	status := daemonSet.Status
	switch {
	case status.ObservedGeneration < daemonSet.Generation:
		return "its latest change is not rolled out yet"
	case status.UpdatedNumberScheduled != status.DesiredNumberScheduled:
		return fmt.Sprintf("%d of %d Pods run its current template", status.UpdatedNumberScheduled, status.DesiredNumberScheduled)
	case status.NumberReady != status.DesiredNumberScheduled || status.NumberUnavailable > 0:
		return fmt.Sprintf("%d of %d Pods are ready", status.NumberReady, status.DesiredNumberScheduled)
	}
	return ""
}

func deploymentProblem(deployment *appsv1.Deployment) string {
	replicas := int32(1)
	if deployment.Spec.Replicas != nil {
		replicas = *deployment.Spec.Replicas
	}
	status := deployment.Status
	switch {
	case status.ObservedGeneration < deployment.Generation:
		return "its latest change is not rolled out yet"
	case status.UpdatedReplicas != replicas:
		return fmt.Sprintf("%d of %d Pods run its current template", status.UpdatedReplicas, replicas)
	case status.AvailableReplicas != replicas || status.UnavailableReplicas > 0:
		return fmt.Sprintf("%d of %d Pods are available", status.AvailableReplicas, replicas)
	}
	return ""
}

func (check *preflight) scanSources(ctx context.Context, present map[apiKind]bool) error {
	check.handle.Printf("Objects in the %s group:", OldGroup)
	for _, kind := range sourceKinds(check.mode) {
		if !present[kind] {
			continue
		}
		target, err := targetSchema(check.mode, kind)
		if err != nil {
			return err
		}
		pruned := []string{}
		count := 0
		lister := pager.New(func(ctx context.Context, options metav1.ListOptions) (runtime.Object, error) {
			return check.handle.Dynamic.Resource(kind.OldResource()).List(ctx, options)
		})
		lister.PageSize = listPageSize
		err = lister.EachListItem(ctx, metav1.ListOptions{}, func(object runtime.Object) error {
			item, isUnstructured := object.(*unstructured.Unstructured)
			if !isUnstructured {
				return fmt.Errorf("listing %s returned an unexpected %T", kind.OldResource(), object)
			}
			count++
			name := objectName(item.GetNamespace(), item.GetName())
			if fields := prunedFields(item, target); len(fields) > 0 {
				pruned = append(pruned, name+" ("+strings.Join(fields, ", ")+")")
			}
			check.keep(kind, name, item)
			return nil
		})
		if err != nil {
			return fmt.Errorf("cannot list %s: %w", kind.OldResource(), err)
		}
		check.report.Inventory[kind.Kind] = count
		check.handle.Printf("  %s: %d", kind.Kind, count)
		check.report.PrunedObjects += len(pruned)
		if len(pruned) > 0 && check.handle.Options.AcceptPrunedFields {
			for _, finding := range pruned {
				check.handle.Warnf("%s %s loses fields the DANM 4.4 schema does not know, accepted with --accept-pruned-fields", kind.Kind, finding)
			}
		} else if len(pruned) > 0 {
			check.fail("%d %ss hold fields the DANM 4.4 schema does not know, which would be lost when they are copied: %s",
				len(pruned), kind.Kind, listed(pruned))
		}
	}
	return nil
}

func prunedFields(item *unstructured.Unstructured, target *structuralschema.Structural) []string {
	copied := item.DeepCopy()
	copied.SetAPIVersion(NewGroup + "/" + Version)
	return pruning.PruneWithOptions(copied.Object, target, true, structuralschema.UnknownFieldPathOptions{TrackUnknownFieldPaths: true})
}

func (check *preflight) keep(kind apiKind, name string, item *unstructured.Unstructured) {
	switch kind {
	case kindDanmEp:
		ep := &danmtypes.DanmEp{}
		if !check.convert(kind, name, item, ep) || !isTagged(ep) {
			return
		}
		check.sources.taggedEps = append(check.sources.taggedEps, ep)
	case kindTenantConfig:
		tenantConfig := &danmtypes.TenantConfig{}
		if !check.convert(kind, name, item, tenantConfig) {
			return
		}
		check.sources.tenantConfigs = append(check.sources.tenantConfigs, tenantConfig)
	default:
		network := &danmtypes.DanmNet{}
		if !check.convert(kind, name, item, network) {
			return
		}
		network.Kind = kind.Kind
		check.sources.networks = append(check.sources.networks, network)
	}
}

func (check *preflight) convert(kind apiKind, name string, item *unstructured.Unstructured, target any) bool {
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(item.Object, target); err != nil {
		check.fail("%s %s cannot be read as a DANM %s: %s", kind.Kind, name, kind.Kind, err.Error())
		return false
	}
	return true
}

func (check *preflight) verifyMtu() {
	risks := classifyMtu(check.sources.networks)
	if len(risks) == 0 {
		return
	}
	check.handle.Printf("MTU of the networks after migration:")
	writeMtuTable(check.handle.Out, risks)
	atRisk := []string{}
	for _, risk := range risks {
		if risk.Err != nil {
			check.fail("%s", risk.Err.Error())
			continue
		}
		if risk.AtRisk() {
			atRisk = append(atRisk, risk.Network.Kind+" "+objectName(risk.Network.Namespace, risk.Network.Name))
		}
	}
	check.report.MtuAtRisk = len(atRisk)
	if len(atRisk) == 0 {
		return
	}
	if check.handle.Options.AcceptDefaultMtu {
		check.handle.Warnf("%d IPVLAN networks carry no %s tag and get an MTU of 1500 instead of the MTU of their parent device, accepted with --accept-default-mtu: %s",
			len(atRisk), MtuTagKey, listed(atRisk))
		return
	}
	check.fail("%d IPVLAN networks carry no %s tag: DANM 4.3 gave them the MTU of their parent device, DANM 4.4 gives them 1500. "+
		"Annotate each with the MTU of its fabric, or pass --accept-default-mtu to accept 1500: %s", len(atRisk), MtuTagKey, listed(atRisk))
}

func (check *preflight) verifyVlanRanges() {
	for _, tenantConfig := range check.sources.tenantConfigs {
		for _, device := range tenantConfig.HostDevices {
			if device.VniType != "vlan" || device.VniRange == "" {
				continue
			}
			vnis, err := cpuset.Parse(device.VniRange)
			if err != nil {
				check.fail("TenantConfig %s has the unparseable vniRange %q for the vlan entry of host device %s: %s", tenantConfig.Name, device.VniRange, device.Name, err.Error())
				continue
			}
			above := []int{}
			for _, vni := range vnis.List() {
				if vni > MaxAllowedVniVlan {
					above = append(above, vni)
				}
			}
			if len(above) == 0 {
				continue
			}
			allocated := allocatedAbove(device.Alloc, MaxAllowedVniVlan)
			check.fail("TenantConfig %s has the vniRange %s for the vlan entry of host device %s, which reaches above the DANM 4.4 limit of %d with VLANs %s. "+
				"VLANs allocated above the limit: %s, held by TenantNetworks: %s. Move those TenantNetworks to a lower VLAN, then trim the vniRange, both in the %s group",
				tenantConfig.Name, device.VniRange, device.Name, MaxAllowedVniVlan, cpuset.New(above...).String(),
				orNone(cpuset.New(allocated...).String()), orNone(strings.Join(check.vlanHolders(device.Name, allocated), ", ")), OldGroup)
		}
	}
}

func allocatedAbove(alloc string, limit int) []int {
	if alloc == "" {
		return nil
	}
	bits := bitarray.NewBitArrayFromBase64(alloc)
	allocated := []int{}
	for vni := uint32(limit) + 1; vni < bits.Len(); vni++ {
		if bits.Get(vni) {
			allocated = append(allocated, int(vni))
		}
	}
	return allocated
}

func (check *preflight) vlanHolders(device string, vlans []int) []string {
	wanted := map[int]bool{}
	for _, vlan := range vlans {
		wanted[vlan] = true
	}
	holders := []string{}
	for _, network := range check.sources.networks {
		if network.Kind == kindTenantNetwork.Kind && wanted[network.Spec.Options.Vlan] && networkDevice(network) == device {
			holders = append(holders, objectName(network.Namespace, network.Name)+" (VLAN "+strconv.Itoa(network.Spec.Options.Vlan)+")")
		}
	}
	sort.Strings(holders)
	return holders
}

func (check *preflight) verifyRipTags(ctx context.Context) error {
	tagged := check.sources.taggedEps
	if len(tagged) == 0 {
		return nil
	}
	if check.mode != cluster.ModeProduction {
		names := make([]string, 0, len(tagged))
		for _, ep := range tagged {
			names = append(names, objectName(ep.Namespace, ep.Name))
		}
		check.fail("%d DanmEps carry ReservedIP replication tags, but ReservedIPs exist only in production mode: %s", len(names), listed(names))
		return nil
	}
	networks := map[networkKey]*danmtypes.DanmNet{}
	for _, network := range check.sources.networks {
		networks[keyOfNetwork(network)] = network
	}
	rips, problems := planReservedIps(tagged, networks, check.sources.tenantConfigs)
	for _, problem := range problems {
		check.fail("%s", problem)
	}
	for _, rip := range rips {
		existing, err := check.handle.Dynamic.Resource(kindReservedIP.NewResource()).Namespace(rip.Namespace).Get(ctx, rip.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return fmt.Errorf("cannot read ReservedIP %s/%s: %w", rip.Namespace, rip.Name, err)
		}
		if existing.GetLabels()[PlanLabel] != PlanID {
			check.fail("ReservedIP %s/%s already exists in the %s group, and was not created by this plan", rip.Namespace, rip.Name, NewGroup)
		}
	}
	check.report.ReservedIPs = len(rips)
	check.handle.Printf("%d DanmEps are tagged for ReservedIP replication, describing %d ReservedIPs.", len(tagged), len(rips))
	return nil
}

func (check *preflight) verifyImages(ctx context.Context) error {
	images, err := check.handle.Images.ResolveAll(New().Meta().Components)
	if err != nil {
		check.fail("the component images cannot be resolved: %s", err.Error())
		return nil
	}
	components := make([]string, 0, len(images))
	for component := range images {
		components = append(components, component)
	}
	sort.Strings(components)
	check.handle.Printf("DANM 4.4 images:")
	for _, component := range components {
		check.handle.Printf("  %s: %s (%s)", component, images[component].Image, images[component].PullPolicy)
	}
	secret := check.handle.Images.PullSecret
	if secret == "" {
		return nil
	}
	namespaces := map[string]bool{check.handle.Namespace: true}
	for _, workload := range check.workloads {
		if workload.Present {
			namespaces[workload.Namespace] = true
		}
	}
	for _, namespace := range sortedKeys(namespaces) {
		_, err := check.handle.Kube.CoreV1().Secrets(namespace).Get(ctx, secret, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			check.fail("image pull Secret %s/%s does not exist", namespace, secret)
			continue
		}
		if err != nil {
			return fmt.Errorf("cannot read image pull Secret %s/%s: %w", namespace, secret, err)
		}
	}
	return nil
}

func (check *preflight) verifyPayload(ctx context.Context) error {
	if check.handle.Options.BinaryImage != "" {
		check.handle.Printf("CNI binaries are copied from %s, its architecture is not checked.", check.handle.Options.BinaryImage)
		return nil
	}
	dir := check.handle.PayloadDir(PayloadName)
	architecture, err := cluster.InspectPayload(dir, PayloadBinaries)
	if err != nil {
		check.fail("%s", err.Error())
		return nil
	}
	check.report.Architecture = architecture
	nodes, err := check.handle.Kube.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("cannot list the nodes: %w", err)
	}
	mismatched := []string{}
	for _, node := range nodes.Items {
		if node.Status.NodeInfo.Architecture != architecture {
			mismatched = append(mismatched, node.Name+" ("+orDash(node.Status.NodeInfo.Architecture)+")")
		}
	}
	sort.Strings(mismatched)
	if len(mismatched) > 0 {
		check.fail("the CNI binaries in %s are built for %s, which %d nodes cannot run; use a multi-architecture image or --binary-image: %s",
			dir, architecture, len(mismatched), listed(mismatched))
		return nil
	}
	check.handle.Printf("CNI binaries in %s are built for %s, like all %d nodes.", dir, architecture, len(nodes.Items))
	return nil
}

func listed(names []string) string {
	if len(names) <= maxListedObjects {
		return strings.Join(names, ", ")
	}
	return strings.Join(names[:maxListedObjects], ", ") + fmt.Sprintf(" and %d more", len(names)-maxListedObjects)
}

func orNone(value string) string {
	if value == "" {
		return "none"
	}
	return value
}

func sortedKeys(set map[string]bool) []string {
	keys := make([]string, 0, len(set))
	for key := range set {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
