package v43to44

import (
	"strings"
	"testing"

	danmtypes "github.com/danm-cni/danm/crd/apis/danm/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func reservedIp(namespace, name, networkType, network string, sourcePods string, addresses ...string) *danmtypes.ReservedIP {
	rip := &danmtypes.ReservedIP{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
		Spec:       danmtypes.ReservedIPSpec{Network: danmtypes.RipNetworkSelector{Name: network, Type: networkType}},
	}
	if sourcePods != "" {
		rip.Annotations = map[string]string{SourcePodsKey: sourcePods}
	}
	for _, address := range addresses {
		rip.Spec.Ips = append(rip.Spec.Ips, danmtypes.RipIP{Address: address})
	}
	return rip
}

func TestNewInterfaces(t *testing.T) {
	rips := []*danmtypes.ReservedIP{
		reservedIp("app", "db", "TenantNetwork", "internal", `[{"namespace":"app","name":"db-0"}]`, "10.0.0.5", "fd00::5"),
		reservedIp("app", "lb", "ClusterNetwork", "external", "", "10.1.0.7"),
		reservedIp("other", "db", "TenantNetwork", "internal", `[{"namespace":"other","name":"db-0"}]`, "10.0.0.9"),
		reservedIp("app", "twin-a", "TenantNetwork", "shared", "", "10.2.0.1"),
		reservedIp("app", "twin-b", "TenantNetwork", "shared", "", "10.2.0.1"),
	}
	testCases := []struct {
		name     string
		old      string
		pod      string
		expected string
		note     string
	}{
		{name: "no ReservedIP involved", old: `[{"tenantNetwork":"internal","ip":"10.0.0.6/24"},{"network":"flannel"}]`, pod: "web-1",
			expected: `[{"tenantNetwork":"internal","ip":"10.0.0.6/24"},{"network":"flannel"}]`},
		{name: "formatting is kept on a plain copy", old: "[ {\"tenantNetwork\" : \"internal\", \"ip\": \"dynamic\"} ]", pod: "web-1",
			expected: "[ {\"tenantNetwork\" : \"internal\", \"ip\": \"dynamic\"} ]"},
		{name: "literal address with prefix", old: `[{"tenantNetwork":"internal","ip":"10.0.0.5/24","proutes":{"10.9.0.0/16":"10.0.0.1"}}]`, pod: "web-1",
			expected: `[{"tenantNetwork":"internal","ip":"db","proutes":{"10.9.0.0/16":"10.0.0.1"}}]`},
		{name: "literal IPv6 address", old: `[{"tenantNetwork":"internal","ip":"none","ip6":"fd00:0::5/64"}]`, pod: "",
			expected: `[{"tenantNetwork":"internal","ip":"none","ip6":"db"}]`},
		{name: "dynamic request of a source Pod", old: `[{"tenantNetwork":"internal","ip":"dynamic","ip6":"dynamic"}]`, pod: "db-0",
			expected: `[{"tenantNetwork":"internal","ip":"db","ip6":"db"}]`},
		{name: "dynamic request of another Pod", old: `[{"tenantNetwork":"internal","ip":"dynamic"}]`, pod: "db-1",
			expected: `[{"tenantNetwork":"internal","ip":"dynamic"}]`},
		{name: "dynamic request without a Pod name", old: `[{"tenantNetwork":"internal","ip":"dynamic"}]`, pod: "",
			expected: `[{"tenantNetwork":"internal","ip":"dynamic"}]`},
		{name: "ClusterNetwork entry", old: `[{"clusterNetwork":"external","ip":"10.1.0.7"}]`, pod: "web-1",
			expected: `[{"clusterNetwork":"external","ip":"lb"}]`},
		{name: "network of the wrong kind", old: `[{"tenantNetwork":"external","ip":"10.1.0.7"}]`, pod: "web-1",
			expected: `[{"tenantNetwork":"external","ip":"10.1.0.7"}]`},
		{name: "DanmNet entry", old: `[{"network":"internal","ip":"10.0.0.5"}]`, pod: "web-1",
			expected: `[{"network":"internal","ip":"10.0.0.5"}]`},
		{name: "ambiguous match", old: `[{"tenantNetwork":"shared","ip":"10.2.0.1"}]`, pod: "web-1",
			expected: `[{"tenantNetwork":"shared","ip":"10.2.0.1"}]`, note: "twin-a, twin-b"},
		{name: "not a list", old: `{"network":"internal"}`, pod: "web-1", expected: `{"network":"internal"}`},
		{name: "not JSON", old: `internal`, pod: "web-1", expected: `internal`},
		{name: "unknown fields are kept in order", old: `[{"zeta":1,"tenantNetwork":"internal","ip":"10.0.0.5","alpha":{"b":2,"a":1}}]`, pod: "web-1",
			expected: `[{"zeta":1,"tenantNetwork":"internal","ip":"db","alpha":{"b":2,"a":1}}]`},
	}
	for _, testCase := range testCases {
		value, notes := NewInterfaces(testCase.old, "app", testCase.pod, rips)
		if value != testCase.expected {
			t.Errorf("%s: got %s, expected %s", testCase.name, value, testCase.expected)
		}
		if testCase.note != "" && (len(notes) != 1 || !strings.Contains(notes[0], testCase.note)) {
			t.Errorf("%s: notes %v do not mention %s", testCase.name, notes, testCase.note)
		}
		if testCase.note == "" && len(notes) != 0 {
			t.Errorf("%s: unexpected notes %v", testCase.name, notes)
		}
	}
}

func TestNewInterfacesWithoutReservedIps(t *testing.T) {
	old := `[{"tenantNetwork":"internal","ip":"10.0.0.5/24"}]`
	if value, _ := NewInterfaces(old, "app", "db-0", nil); value != old {
		t.Fatalf("got %s without any ReservedIP", value)
	}
}
