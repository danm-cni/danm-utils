package v43to44

import (
	"bytes"
	"context"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/danm-cni/danm-utils/pkg/dma/cluster"
	"github.com/danm-cni/danm-utils/pkg/dma/engine"
	"github.com/danm-cni/danm-utils/pkg/dma/state"
	danmtypes "github.com/danm-cni/danm/crd/apis/danm/v1"
	"github.com/danm-cni/danm/pkg/bitarray"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	kubefake "k8s.io/client-go/kubernetes/fake"
)

type fixture struct {
	crds      []apiKind
	workloads []k8sruntime.Object
	objects   map[apiKind][]map[string]any
	mode      cluster.Mode
	options   cluster.Options
	noPayload bool
}

func healthyFixture(mode cluster.Mode) *fixture {
	kinds := sourceKinds(mode)
	return &fixture{
		crds:      kinds,
		workloads: []k8sruntime.Object{healthyDaemonSet("netwatcher"), healthyDeployment("danm-webhook-deployment")},
		objects:   map[apiKind][]map[string]any{},
	}
}

func (fixture *fixture) add(kind apiKind, object map[string]any) *fixture {
	fixture.objects[kind] = append(fixture.objects[kind], object)
	return fixture
}

func healthyDaemonSet(name string) *appsv1.DaemonSet {
	return &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: DanmNamespace, Generation: 2},
		Status:     appsv1.DaemonSetStatus{ObservedGeneration: 2, DesiredNumberScheduled: 3, UpdatedNumberScheduled: 3, NumberReady: 3},
	}
}

func healthyDeployment(name string) *appsv1.Deployment {
	replicas := int32(2)
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: DanmNamespace, Generation: 1},
		Spec:       appsv1.DeploymentSpec{Replicas: &replicas},
		Status:     appsv1.DeploymentStatus{ObservedGeneration: 1, UpdatedReplicas: 2, AvailableReplicas: 2},
	}
}

func legacyCrd(kind apiKind) *apiextensionsv1.CustomResourceDefinition {
	return &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: kind.OldCrdName()},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group:    OldGroup,
			Names:    apiextensionsv1.CustomResourceDefinitionNames{Kind: kind.Kind, Plural: kind.Plural},
			Scope:    kind.Scope(),
			Versions: []apiextensionsv1.CustomResourceDefinitionVersion{{Name: Version, Served: true, Storage: true}},
		},
	}
}

func object(kind apiKind, namespace, name string, fields map[string]any) map[string]any {
	metadata := map[string]any{"name": name}
	if namespace != "" {
		metadata["namespace"] = namespace
	}
	result := map[string]any{"apiVersion": OldGroup + "/" + Version, "kind": kind.Kind, "metadata": metadata}
	for key, value := range fields {
		result[key] = value
	}
	return result
}

func network(kind apiKind, namespace, name string, annotations map[string]any, options map[string]any) map[string]any {
	result := object(kind, namespace, name, map[string]any{"spec": map[string]any{"NetworkID": name, "NetworkType": "ipvlan", "Options": options}})
	if annotations != nil {
		result["metadata"].(map[string]any)["annotations"] = annotations
	}
	return result
}

func danmEp(namespace, name, apiType, networkName, address string, annotations map[string]any) map[string]any {
	result := object(kindDanmEp, namespace, name, map[string]any{"spec": map[string]any{
		"NetworkName": networkName, "NetworkType": "ipvlan", "EndpointID": name, "Pod": name, "apiType": apiType,
		"Interface": map[string]any{"Name": "eth1", "Address": address},
	}})
	if annotations != nil {
		result["metadata"].(map[string]any)["annotations"] = annotations
	}
	return result
}

func v4Alloc(t *testing.T) string {
	bits, err := bitarray.NewBitArray(256)
	if err != nil {
		t.Fatal(err)
	}
	return bits.Encode()
}

func payloadRoot(t *testing.T) string {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	content, err := os.ReadFile(executable)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	dir := filepath.Join(root, PayloadName)
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	for _, binary := range PayloadBinaries {
		if err := os.WriteFile(filepath.Join(dir, binary), content, 0755); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

func (fixture *fixture) handle(t *testing.T) (*cluster.Handle, *bytes.Buffer) {
	kubeObjects := append([]k8sruntime.Object{
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node1"}, Status: corev1.NodeStatus{NodeInfo: corev1.NodeSystemInfo{Architecture: runtime.GOARCH}}},
	}, fixture.workloads...)
	kube := kubefake.NewSimpleClientset(kubeObjects...)
	crds := []k8sruntime.Object{}
	for _, kind := range fixture.crds {
		crds = append(crds, legacyCrd(kind))
	}
	listKinds := map[schema.GroupVersionResource]string{}
	for _, kind := range oldKinds {
		listKinds[kind.OldResource()] = kind.Kind + "List"
	}
	for _, kind := range newKinds {
		listKinds[kind.NewResource()] = kind.Kind + "List"
	}
	dynamicObjects := []k8sruntime.Object{}
	for _, objects := range fixture.objects {
		for _, raw := range objects {
			dynamicObjects = append(dynamicObjects, &unstructured.Unstructured{Object: raw})
		}
	}
	out := &bytes.Buffer{}
	root := payloadRoot(t)
	if fixture.noPayload {
		root = t.TempDir()
	}
	return &cluster.Handle{
		Kube:        kube,
		ApiExt:      apiextfake.NewSimpleClientset(crds...),
		Dynamic:     dynamicfake.NewSimpleDynamicClientWithCustomListKinds(k8sruntime.NewScheme(), listKinds, dynamicObjects...),
		Discovery:   kube.Discovery(),
		State:       state.NewStore(kube, state.DefaultNamespace, state.DefaultName),
		PayloadRoot: root,
		Namespace:   state.DefaultNamespace,
		Mode:        fixture.mode,
		Operator:    "tester",
		Options:     fixture.options,
		Log:         log.New(io.Discard, "", 0),
		Out:         out,
	}, out
}

func execute(t *testing.T, handle *cluster.Handle) error {
	runner := engine.NewRunner(New(), handle, &engine.TokenConfirmer{Supplied: "verify-source-cluster", Out: io.Discard})
	return runner.Execute(context.Background(), 1)
}

func detect(t *testing.T, handle *cluster.Handle) engine.StepState {
	detected, err := verifySourceCluster{}.Detect(context.Background(), handle)
	if err != nil {
		t.Fatalf("detecting step 1 failed: %v", err)
	}
	return detected
}

func expectFailure(t *testing.T, err error, fragments ...string) {
	t.Helper()
	if err == nil {
		t.Fatalf("preflight passed, expected it to fail with %q", fragments)
	}
	for _, fragment := range fragments {
		if !strings.Contains(err.Error(), fragment) {
			t.Fatalf("preflight failed with %q, which does not mention %q", err.Error(), fragment)
		}
	}
}

func productionFixture(t *testing.T) *fixture {
	return healthyFixture(cluster.ModeProduction).
		add(kindTenantNetwork, network(kindTenantNetwork, "app", "internal", map[string]any{MtuTagKey: "9000"},
			map[string]any{"host_device": "ens4", "vlan": int64(500), "cidr": "10.0.0.0/24", "alloc": v4Alloc(t)})).
		add(kindClusterNetwork, network(kindClusterNetwork, "", "external", nil,
			map[string]any{"host_device": "ens5", "cidr": "10.1.0.0/24", "alloc": v4Alloc(t)})).
		add(kindTenantConfig, object(kindTenantConfig, "", "danm-tenant-config", map[string]any{
			"hostDevices":            []any{map[string]any{"name": "ens4", "vniType": "vlan", "vniRange": "500-600"}},
			"allowedClusterNetworks": []any{map[string]any{"name": "external", "privileges": []any{"reservedIp"}}},
		})).
		add(kindDanmEp, danmEp("app", "pod1-eth1", "TenantNetwork", "internal", "10.0.0.5/24", nil))
}

func TestPreflightPassesAndRecordsTheMode(t *testing.T) {
	fixture := productionFixture(t)
	fixture.options.AcceptDefaultMtu = true
	handle, out := fixture.handle(t)
	if err := execute(t, handle); err != nil {
		t.Fatalf("preflight failed on a healthy cluster: %v\n%s", err, out.String())
	}
	if detect(t, handle) != engine.StateDone {
		t.Fatalf("step 1 does not read as done after passing")
	}
	record, _ := handle.State.Load(context.Background(), PlanID)
	if record.Mode != string(cluster.ModeProduction) {
		t.Fatalf("recorded mode is %q, expected production", record.Mode)
	}
	if !strings.Contains(out.String(), "TenantNetwork: 1") || !strings.Contains(out.String(), "DanmEp: 1") {
		t.Fatalf("the inventory is not printed:\n%s", out.String())
	}
	if !strings.Contains(out.String(), "svcwatcher is not deployed") {
		t.Fatalf("a missing optional component is not reported:\n%s", out.String())
	}
}

func TestRollbackForgetsTheMode(t *testing.T) {
	fixture := productionFixture(t)
	fixture.options.AcceptDefaultMtu = true
	handle, _ := fixture.handle(t)
	if err := execute(t, handle); err != nil {
		t.Fatal(err)
	}
	runner := engine.NewRunner(New(), handle, &engine.TokenConfirmer{Supplied: "verify-source-cluster", Out: io.Discard})
	if err := runner.Rollback(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	if detect(t, handle) != engine.StateNotStarted {
		t.Fatalf("step 1 still reads as done after its rollback")
	}
	record, _ := handle.State.Load(context.Background(), PlanID)
	if record.Mode != "" {
		t.Fatalf("the mode %q is still recorded after rollback", record.Mode)
	}
}

func TestPreflightFailsOnUntaggedIpvlanNetwork(t *testing.T) {
	handle, _ := productionFixture(t).handle(t)
	err := execute(t, handle)
	expectFailure(t, err, "IPVLAN networks carry no "+MtuTagKey, "ClusterNetwork external")
	if detect(t, handle) != engine.StateNotStarted {
		t.Fatalf("step 1 reads as done after failing")
	}
}

func TestPreflightFailsOnInvalidMtuTag(t *testing.T) {
	fixture := healthyFixture(cluster.ModeLightweight).
		add(kindDanmNet, network(kindDanmNet, "app", "jumbo", map[string]any{MtuTagKey: "jumbo"}, map[string]any{"host_device": "ens4"}))
	handle, _ := fixture.handle(t)
	expectFailure(t, execute(t, handle), "is not an integer")
}

func TestPreflightFailsOnBothModes(t *testing.T) {
	fixture := healthyFixture(cluster.ModeProduction)
	fixture.crds = oldKinds
	fixture.options.AcceptDefaultMtu = true
	handle, _ := fixture.handle(t)
	expectFailure(t, execute(t, handle), "both the lightweight and the production")

	fixture.mode = cluster.ModeProduction
	fixture.options.Force = true
	handle, out := fixture.handle(t)
	if err := execute(t, handle); err != nil {
		t.Fatalf("preflight failed although --mode and --force were given: %v\n%s", err, out.String())
	}
}

func TestPreflightRefusesContradictingModeWithoutForce(t *testing.T) {
	fixture := healthyFixture(cluster.ModeLightweight)
	fixture.mode = cluster.ModeProduction
	handle, _ := fixture.handle(t)
	expectFailure(t, execute(t, handle), "contradicts the lightweight mode")
}

func TestPreflightFailsOnIncompleteProductionApi(t *testing.T) {
	fixture := healthyFixture(cluster.ModeProduction)
	fixture.crds = []apiKind{kindDanmEp, kindTenantNetwork}
	handle, _ := fixture.handle(t)
	expectFailure(t, execute(t, handle), kindClusterNetwork.OldCrdName()+" is missing", kindTenantConfig.OldCrdName()+" is missing")
}

func TestPreflightFailsOnUnhealthyComponents(t *testing.T) {
	fixture := healthyFixture(cluster.ModeLightweight)
	unhealthy := healthyDaemonSet("svcwatcher")
	unhealthy.Status.NumberReady = 1
	fixture.workloads = []k8sruntime.Object{unhealthy}
	handle, _ := fixture.handle(t)
	expectFailure(t, execute(t, handle), "netwatcher is not deployed", "danm-webhook-deployment of webhook is not deployed", "svcwatcher of svcwatcher is not healthy: 1 of 3 Pods are ready")
}

func TestPreflightFailsOnPrunedFields(t *testing.T) {
	stray := network(kindDanmNet, "app", "stray", nil, map[string]any{"host_device": "ens4", "legacy_flag": true})
	stray["spec"].(map[string]any)["Unknown"] = "x"
	fixture := healthyFixture(cluster.ModeLightweight).add(kindDanmNet, stray)
	fixture.options.AcceptDefaultMtu = true
	handle, _ := fixture.handle(t)
	expectFailure(t, execute(t, handle), "app/stray", "spec.Options.legacy_flag", "spec.Unknown")
}

func TestPreflightFailsOnVlanRangeAboveLimit(t *testing.T) {
	alloc, err := bitarray.NewBitArray(5001)
	if err != nil {
		t.Fatal(err)
	}
	alloc.Set(4500)
	fixture := productionFixture(t)
	fixture.options.AcceptDefaultMtu = true
	fixture.objects[kindTenantConfig] = []map[string]any{object(kindTenantConfig, "", "danm-tenant-config", map[string]any{
		"hostDevices": []any{map[string]any{"name": "ens4", "vniType": "vlan", "vniRange": "4000-4600", "alloc": alloc.Encode()}},
	})}
	fixture.add(kindTenantNetwork, network(kindTenantNetwork, "app", "high", map[string]any{MtuTagKey: "1500"},
		map[string]any{"host_device": "ens4", "vlan": int64(4500)}))
	handle, _ := fixture.handle(t)
	expectFailure(t, execute(t, handle), "VLANs 4095-4600", "allocated above the limit: 4500", "app/high (VLAN 4500)")
}

func TestPreflightFailsOnTagsInLightweightMode(t *testing.T) {
	fixture := healthyFixture(cluster.ModeLightweight).
		add(kindDanmEp, danmEp("app", "pod1-eth1", "DanmNet", "internal", "10.0.0.5/24", map[string]any{RipTagV4: "pod1"}))
	handle, _ := fixture.handle(t)
	expectFailure(t, execute(t, handle), "ReservedIPs exist only in production mode", "app/pod1-eth1")
}

func TestPreflightValidatesReservedIpTags(t *testing.T) {
	fixture := productionFixture(t)
	fixture.options.AcceptDefaultMtu = true
	fixture.add(kindDanmEp, danmEp("app", "pod2-eth1", "TenantNetwork", "internal", "10.0.0.6/24", map[string]any{RipTagV4: "Bad_Name"}))
	handle, _ := fixture.handle(t)
	expectFailure(t, execute(t, handle), "not a valid ReservedIP name")
}

func TestPreflightFailsOnArchitectureMismatch(t *testing.T) {
	fixture := healthyFixture(cluster.ModeLightweight)
	fixture.workloads = append(fixture.workloads, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "odd"}, Status: corev1.NodeStatus{NodeInfo: corev1.NodeSystemInfo{Architecture: "mips"}}})
	handle, _ := fixture.handle(t)
	expectFailure(t, execute(t, handle), "1 nodes cannot run", "odd (mips)")
}

func TestPreflightFailsOnMissingPayload(t *testing.T) {
	fixture := healthyFixture(cluster.ModeLightweight)
	fixture.noPayload = true
	handle, _ := fixture.handle(t)
	expectFailure(t, execute(t, handle), "CNI binary payload", "is incomplete")
}

func TestPreflightFailsOnMissingPullSecret(t *testing.T) {
	handle, _ := healthyFixture(cluster.ModeLightweight).handle(t)
	handle.Images.PullSecret = "registry"
	expectFailure(t, execute(t, handle), "image pull Secret kube-system/registry does not exist")
}

func ripNetworks() map[networkKey]*danmtypes.DanmNet {
	tenant := &danmtypes.DanmNet{
		TypeMeta:   metav1.TypeMeta{Kind: "TenantNetwork"},
		ObjectMeta: metav1.ObjectMeta{Name: "internal", Namespace: "app"},
		Spec:       danmtypes.DanmNetSpec{Options: danmtypes.DanmNetOption{Cidr: "10.0.0.0/24", Alloc: "AAAA", Net6: "fd00::/64"}},
	}
	cnet := &danmtypes.DanmNet{
		TypeMeta:   metav1.TypeMeta{Kind: "ClusterNetwork"},
		ObjectMeta: metav1.ObjectMeta{Name: "external"},
		Spec:       danmtypes.DanmNetSpec{Options: danmtypes.DanmNetOption{Cidr: "10.1.0.0/24", Alloc: "AAAA"}},
	}
	dual := &danmtypes.DanmNet{
		TypeMeta:   metav1.TypeMeta{Kind: "TenantNetwork"},
		ObjectMeta: metav1.ObjectMeta{Name: "dual", Namespace: "app"},
		Spec:       danmtypes.DanmNetSpec{Options: danmtypes.DanmNetOption{Cidr: "10.2.0.0/24", Alloc: "AAAA", Net6: "fd00::/64", Alloc6: "AAAA"}},
	}
	return map[networkKey]*danmtypes.DanmNet{keyOfNetwork(tenant): tenant, keyOfNetwork(cnet): cnet, keyOfNetwork(dual): dual}
}

func taggedEp(name, apiType, networkName, v4, v6 string, tags map[string]string) *danmtypes.DanmEp {
	return &danmtypes.DanmEp{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "app", Annotations: tags},
		Spec: danmtypes.DanmEpSpec{ApiType: apiType, NetworkName: networkName,
			Iface: danmtypes.DanmEpIface{Address: v4, AddressIPv6: v6}},
	}
}

func TestPlanReservedIps(t *testing.T) {
	testCases := []struct {
		name     string
		eps      []*danmtypes.DanmEp
		rips     int
		problems []string
	}{
		{
			name: "shared address named by several DanmEps",
			eps: []*danmtypes.DanmEp{
				taggedEp("a", "TenantNetwork", "internal", "10.0.0.5/24", "", map[string]string{SharedRipTagV4: "vip"}),
				taggedEp("b", "TenantNetwork", "internal", "10.0.0.5/24", "", map[string]string{SharedRipTagV4: "vip"}),
			},
			rips: 1,
		},
		{
			name: "both families in one ReservedIP",
			eps: []*danmtypes.DanmEp{
				taggedEp("a", "TenantNetwork", "dual", "10.2.0.5/24", "fd00::5/64", map[string]string{RipTagV4: "pod-a", RipTagV6: "pod-a"}),
			},
			rips: 1,
		},
		{
			name: "non-shared name used twice",
			eps: []*danmtypes.DanmEp{
				taggedEp("a", "TenantNetwork", "internal", "10.0.0.5/24", "", map[string]string{RipTagV4: "vip"}),
				taggedEp("b", "TenantNetwork", "internal", "10.0.0.5/24", "", map[string]string{RipTagV4: "vip"}),
			},
			rips:     1,
			problems: []string{"is not shared, but 2 DanmEps name it"},
		},
		{
			name: "same name for different addresses",
			eps: []*danmtypes.DanmEp{
				taggedEp("a", "TenantNetwork", "internal", "10.0.0.5/24", "", map[string]string{SharedRipTagV4: "vip"}),
				taggedEp("b", "TenantNetwork", "internal", "10.0.0.6/24", "", map[string]string{SharedRipTagV4: "vip"}),
			},
			rips:     1,
			problems: []string{"named for different IPv4 addresses"},
		},
		{
			name: "two ReservedIPs claiming one address",
			eps: []*danmtypes.DanmEp{
				taggedEp("a", "TenantNetwork", "internal", "10.0.0.5/24", "", map[string]string{SharedRipTagV4: "vip1"}),
				taggedEp("b", "TenantNetwork", "internal", "10.0.0.5/24", "", map[string]string{SharedRipTagV4: "vip2"}),
			},
			rips:     2,
			problems: []string{"both claim the address 10.0.0.5"},
		},
		{
			name: "plain and shared tag on one family",
			eps: []*danmtypes.DanmEp{
				taggedEp("a", "TenantNetwork", "internal", "10.0.0.5/24", "", map[string]string{RipTagV4: "x", SharedRipTagV4: "y"}),
			},
			problems: []string{"carries both"},
		},
		{
			name:     "dynamic is reserved",
			eps:      []*danmtypes.DanmEp{taggedEp("a", "TenantNetwork", "internal", "10.0.0.5/24", "", map[string]string{RipTagV4: "dynamic"})},
			problems: []string{"reserved for dynamic allocation"},
		},
		{
			name:     "no address of the tagged family",
			eps:      []*danmtypes.DanmEp{taggedEp("a", "TenantNetwork", "internal", "10.0.0.5/24", "", map[string]string{RipTagV6: "a"})},
			problems: []string{"holds no IPv6 address"},
		},
		{
			name:     "address outside of the network",
			eps:      []*danmtypes.DanmEp{taggedEp("a", "TenantNetwork", "internal", "10.9.0.5/24", "", map[string]string{RipTagV4: "a"})},
			problems: []string{"is outside of the CIDR"},
		},
		{
			name:     "not an L3 family",
			eps:      []*danmtypes.DanmEp{taggedEp("a", "TenantNetwork", "internal", "10.0.0.5/24", "fd00::5/64", map[string]string{RipTagV6: "a"})},
			problems: []string{"has no IPv6 allocation"},
		},
		{
			name:     "DanmNet is not eligible",
			eps:      []*danmtypes.DanmEp{taggedEp("a", "DanmNet", "internal", "10.0.0.5/24", "", map[string]string{RipTagV4: "a"})},
			problems: []string{"ReservedIPs exist only on TenantNetworks and ClusterNetworks"},
		},
		{
			name:     "ClusterNetwork without the privilege",
			eps:      []*danmtypes.DanmEp{taggedEp("a", "ClusterNetwork", "external", "10.1.0.5/24", "", map[string]string{RipTagV4: "a"})},
			problems: []string{"is not granted the reservedIp privilege"},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			rips, problems := planReservedIps(testCase.eps, ripNetworks(), nil)
			if len(rips) != testCase.rips {
				t.Errorf("planned %d ReservedIPs, expected %d", len(rips), testCase.rips)
			}
			if len(problems) != len(testCase.problems) {
				t.Fatalf("found problems %q, expected %q", problems, testCase.problems)
			}
			for index, fragment := range testCase.problems {
				if !strings.Contains(problems[index], fragment) {
					t.Errorf("problem %q does not mention %q", problems[index], fragment)
				}
			}
		})
	}
}

func TestParseMtuTag(t *testing.T) {
	testCases := []struct {
		name    string
		value   string
		vxlan   int
		net6    string
		tagged  bool
		mtu     int
		invalid bool
	}{
		{name: "untagged"},
		{name: "jumbo", value: "9000", tagged: true, mtu: 9000},
		{name: "not a number", value: "big", tagged: true, invalid: true},
		{name: "too small for IPv6", value: "1000", net6: "fd00::/64", tagged: true, invalid: true},
		{name: "VxLAN overhead", value: "65500", vxlan: 100, tagged: true, invalid: true},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			network := &danmtypes.DanmNet{
				TypeMeta:   metav1.TypeMeta{Kind: "DanmNet"},
				ObjectMeta: metav1.ObjectMeta{Name: "net", Namespace: "app"},
				Spec:       danmtypes.DanmNetSpec{Options: danmtypes.DanmNetOption{Vxlan: testCase.vxlan, Net6: testCase.net6}},
			}
			if testCase.value != "" {
				network.Annotations = map[string]string{MtuTagKey: testCase.value}
			}
			mtu, tagged, err := ParseMtuTag(network)
			if tagged != testCase.tagged || (err != nil) != testCase.invalid || (err == nil && mtu != testCase.mtu) {
				t.Fatalf("got mtu %d, tagged %v, error %v", mtu, tagged, err)
			}
		})
	}
}

func TestPlanIsValid(t *testing.T) {
	if err := engine.Validate(New()); err != nil {
		t.Fatal(err)
	}
	for _, kind := range oldKinds {
		mode := cluster.ModeLightweight
		if kind != kindDanmEp && kind != kindDanmNet {
			mode = cluster.ModeProduction
		}
		if _, err := targetSchema(mode, kind); err != nil {
			t.Errorf("no target schema for %s: %v", kind.Kind, err)
		}
	}
}

func TestTargetCrdsNeedNoFieldSelectors(t *testing.T) {
	for _, mode := range []cluster.Mode{cluster.ModeLightweight, cluster.ModeProduction} {
		for _, kind := range targetKinds(mode) {
			crd, err := targetCrd(mode, kind)
			if err != nil {
				t.Fatal(err)
			}
			for _, version := range crd.Spec.Versions {
				if len(version.SelectableFields) > 0 {
					t.Errorf("the %s CRD of %s mode declares selectableFields, which DANM 4.4 no longer needs", kind.Kind, mode)
				}
			}
		}
	}
}

func TestPreflightChecksAndRecordsTailoredWorkloads(t *testing.T) {
	fixture := healthyFixture(cluster.ModeLightweight)
	netwatcher := healthyDeployment("nw")
	netwatcher.Namespace = "danm"
	fixture.workloads = []k8sruntime.Object{netwatcher, healthyDeployment("danm-webhook-deployment"),
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "registry", Namespace: "danm"}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "registry", Namespace: state.DefaultNamespace}}}
	handle, out := fixture.handle(t)
	handle.Images.PullSecret = "registry"
	handle.Workloads = cluster.WorkloadConfig{Overrides: map[string]cluster.WorkloadRef{
		ComponentNetwatcher: {Kind: cluster.WorkloadDeployment, Namespace: "danm", Name: "nw"},
	}}
	if err := execute(t, handle); err != nil {
		t.Fatalf("preflight failed on tailored workloads: %v\n%s", err, out.String())
	}
	record, _ := handle.State.Load(context.Background(), PlanID)
	recorded := record.Workloads[ComponentNetwatcher]
	if cluster.RecordedWorkload(recorded).String() != "Deployment/danm/nw" || !recorded.Present {
		t.Fatalf("netwatcher is recorded as %+v", recorded)
	}
	if record.Workloads[ComponentSvcwatcher].Present {
		t.Fatalf("the missing svcwatcher is recorded as present")
	}
	report, err := engine.NewRunner(New(), handle, &engine.TokenConfirmer{Out: io.Discard}).Report(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	rendered := &bytes.Buffer{}
	report.Render(rendered)
	if !strings.Contains(rendered.String(), "netwatcher: Deployment/danm/nw (required)") || !strings.Contains(rendered.String(), "svcwatcher: DaemonSet/kube-system/svcwatcher (optional, not deployed)") {
		t.Fatalf("the recorded workloads are not reported:\n%s", rendered.String())
	}
	runner := engine.NewRunner(New(), handle, &engine.TokenConfirmer{Supplied: "verify-source-cluster", Out: io.Discard})
	if err := runner.Rollback(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	record, _ = handle.State.Load(context.Background(), PlanID)
	if record.Workloads != nil {
		t.Fatalf("the workloads are still recorded after rollback: %+v", record.Workloads)
	}
}

func TestPreflightFailsOnMissingTailoredWorkload(t *testing.T) {
	handle, _ := healthyFixture(cluster.ModeLightweight).handle(t)
	handle.Workloads = cluster.WorkloadConfig{Overrides: map[string]cluster.WorkloadRef{
		ComponentWebhook: {Kind: cluster.WorkloadDaemonSet, Namespace: "danm", Name: "webhook"},
	}}
	expectFailure(t, execute(t, handle), "DaemonSet/danm/webhook of webhook is not deployed")
}

func TestPreflightRefusesUndeclaredWorkloads(t *testing.T) {
	handle, _ := healthyFixture(cluster.ModeLightweight).handle(t)
	handle.Workloads = cluster.WorkloadConfig{Overrides: map[string]cluster.WorkloadRef{
		"svcwatchr": {Kind: cluster.WorkloadDaemonSet, Namespace: "danm", Name: "svcwatcher"},
	}}
	expectFailure(t, execute(t, handle), "svcwatchr")
}

func TestPreflightAcceptsAndRecordsPrunedFields(t *testing.T) {
	stray := network(kindDanmNet, "app", "stray", nil, map[string]any{"host_device": "ens4", "legacy_flag": true})
	fixture := healthyFixture(cluster.ModeLightweight).add(kindDanmNet, stray)
	fixture.options.AcceptDefaultMtu = true
	fixture.options.AcceptPrunedFields = true
	handle, out := fixture.handle(t)
	if err := execute(t, handle); err != nil {
		t.Fatalf("preflight failed although pruned fields were accepted: %v\n%s", err, out.String())
	}
	if !strings.Contains(out.String(), "WARNING: DanmNet app/stray (spec.Options.legacy_flag) loses fields") {
		t.Fatalf("the accepted pruned field is not reported:\n%s", out.String())
	}
	record, _ := handle.State.Load(context.Background(), PlanID)
	if !record.PrunedFieldsAccepted || record.PrunedObjects != 1 {
		t.Fatalf("the acceptance is recorded as %v with %d objects", record.PrunedFieldsAccepted, record.PrunedObjects)
	}
	runner := engine.NewRunner(New(), handle, &engine.TokenConfirmer{Supplied: "verify-source-cluster", Out: io.Discard})
	if err := runner.Rollback(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	record, _ = handle.State.Load(context.Background(), PlanID)
	if record.PrunedFieldsAccepted || record.PrunedObjects != 0 {
		t.Fatalf("the acceptance survived the rollback of step 1")
	}
}

func TestForceDoesNotAcceptPrunedFields(t *testing.T) {
	stray := network(kindDanmNet, "app", "stray", nil, map[string]any{"host_device": "ens4", "legacy_flag": true})
	fixture := healthyFixture(cluster.ModeLightweight).add(kindDanmNet, stray)
	fixture.options.AcceptDefaultMtu = true
	fixture.options.Force = true
	handle, _ := fixture.handle(t)
	expectFailure(t, execute(t, handle), "app/stray", "spec.Options.legacy_flag")
}
