package v43to44

import (
	"fmt"
	"net"
	"sort"
	"strings"

	danmtypes "github.com/danm-cni/danm/crd/apis/danm/v1"
	"github.com/danm-cni/danm/pkg/confman"
	"github.com/danm-cni/danm/pkg/ipam"
	"k8s.io/apimachinery/pkg/util/validation"
)

const (
	RipTagV4       = "dma.danm.io/reserved-ip-v4"
	RipTagV6       = "dma.danm.io/reserved-ip-v6"
	SharedRipTagV4 = "dma.danm.io/shared-reserved-ip-v4"
	SharedRipTagV6 = "dma.danm.io/shared-reserved-ip-v6"
)

var RipTagKeys = []string{RipTagV4, RipTagV6, SharedRipTagV4, SharedRipTagV6}

type ipFamily int

const (
	familyV4 ipFamily = iota
	familyV6
)

func (family ipFamily) String() string {
	if family == familyV6 {
		return "IPv6"
	}
	return "IPv4"
}

func (family ipFamily) tagKeys() (string, string) {
	if family == familyV6 {
		return RipTagV6, SharedRipTagV6
	}
	return RipTagV4, SharedRipTagV4
}

type networkKey struct {
	Kind      string
	Namespace string
	Name      string
}

func (key networkKey) String() string {
	return key.Kind + " " + objectName(key.Namespace, key.Name)
}

func keyOfNetwork(network *danmtypes.DanmNet) networkKey {
	return networkKey{Kind: network.Kind, Namespace: network.Namespace, Name: network.Name}
}

func networkOfEp(ep *danmtypes.DanmEp) networkKey {
	if ep.Spec.ApiType == kindClusterNetwork.Kind {
		return networkKey{Kind: ep.Spec.ApiType, Name: ep.Spec.NetworkName}
	}
	return networkKey{Kind: ep.Spec.ApiType, Namespace: ep.Namespace, Name: ep.Spec.NetworkName}
}

type plannedAddress struct {
	IP     net.IP
	Shared bool
	Users  []string
}

type plannedRip struct {
	Namespace string
	Name      string
	Network   networkKey
	Addresses [2]*plannedAddress
	Sources   []string
}

func isTagged(ep *danmtypes.DanmEp) bool {
	for _, key := range RipTagKeys {
		if _, found := ep.Annotations[key]; found {
			return true
		}
	}
	return false
}

func planReservedIps(eps []*danmtypes.DanmEp, networks map[networkKey]*danmtypes.DanmNet, tenantConfigs []*danmtypes.TenantConfig) ([]*plannedRip, []string) {
	planner := &ripPlanner{networks: networks, tenantConfigs: tenantConfigs, rips: map[string]*plannedRip{}}
	sorted := append([]*danmtypes.DanmEp{}, eps...)
	sort.Slice(sorted, func(i, j int) bool {
		return objectName(sorted[i].Namespace, sorted[i].Name) < objectName(sorted[j].Namespace, sorted[j].Name)
	})
	for _, ep := range sorted {
		for _, family := range []ipFamily{familyV4, familyV6} {
			planner.addTag(ep, family)
		}
	}
	return planner.finish()
}

type ripPlanner struct {
	networks      map[networkKey]*danmtypes.DanmNet
	tenantConfigs []*danmtypes.TenantConfig
	rips          map[string]*plannedRip
	problems      []string
}

func (planner *ripPlanner) fail(format string, args ...any) {
	planner.problems = append(planner.problems, fmt.Sprintf(format, args...))
}

func (planner *ripPlanner) addTag(ep *danmtypes.DanmEp, family ipFamily) {
	epName := objectName(ep.Namespace, ep.Name)
	plainKey, sharedKey := family.tagKeys()
	plainName, plainTagged := ep.Annotations[plainKey]
	sharedName, sharedTagged := ep.Annotations[sharedKey]
	if plainTagged && sharedTagged {
		planner.fail("DanmEp %s carries both %s and %s, at most one tag per address family is allowed", epName, plainKey, sharedKey)
		return
	}
	if !plainTagged && !sharedTagged {
		return
	}
	name, key := plainName, plainKey
	if sharedTagged {
		name, key = sharedName, sharedKey
	}
	if problems := ripNameProblems(name); len(problems) > 0 {
		planner.fail("DanmEp %s names ReservedIP %q in %s, which is not a valid ReservedIP name: %s", epName, name, key, strings.Join(problems, "; "))
		return
	}
	network, valid := planner.network(ep, key)
	if !valid {
		return
	}
	ip, valid := planner.address(ep, network, family, key)
	if !valid {
		return
	}
	ripKey := objectName(ep.Namespace, name)
	rip, found := planner.rips[ripKey]
	if !found {
		rip = &plannedRip{Namespace: ep.Namespace, Name: name, Network: keyOfNetwork(network)}
		planner.rips[ripKey] = rip
	}
	if rip.Network != keyOfNetwork(network) {
		planner.fail("ReservedIP %s is named by DanmEps of different networks: %s is on %s, %s is on %s", ripKey, epName, keyOfNetwork(network), strings.Join(rip.Sources, ", "), rip.Network)
		return
	}
	address := rip.Addresses[family]
	if address == nil {
		rip.Addresses[family] = &plannedAddress{IP: ip, Shared: sharedTagged, Users: []string{epName}}
	} else {
		if !address.IP.Equal(ip) {
			planner.fail("ReservedIP %s is named for different %s addresses: %s holds %s, %s holds %s", ripKey, family, epName, ip, strings.Join(address.Users, ", "), address.IP)
			return
		}
		if address.Shared != sharedTagged {
			planner.fail("ReservedIP %s is named both as shared and as not shared for its %s address %s, by %s and %s", ripKey, family, ip, epName, strings.Join(address.Users, ", "))
			return
		}
		address.Users = append(address.Users, epName)
	}
	if len(rip.Sources) == 0 || rip.Sources[len(rip.Sources)-1] != epName {
		rip.Sources = append(rip.Sources, epName)
	}
}

func (planner *ripPlanner) network(ep *danmtypes.DanmEp, key string) (*danmtypes.DanmNet, bool) {
	epName := objectName(ep.Namespace, ep.Name)
	if ep.Spec.ApiType != kindTenantNetwork.Kind && ep.Spec.ApiType != kindClusterNetwork.Kind {
		planner.fail("DanmEp %s carries %s, but it is connected to a %s, and ReservedIPs exist only on TenantNetworks and ClusterNetworks", epName, key, orDash(ep.Spec.ApiType))
		return nil, false
	}
	network, found := planner.networks[networkOfEp(ep)]
	if !found {
		planner.fail("DanmEp %s carries %s, but its network %s does not exist", epName, key, networkOfEp(ep))
		return nil, false
	}
	if problems := validation.IsValidLabelValue(network.Name); len(problems) > 0 {
		planner.fail("DanmEp %s carries %s, but the name of its network %s is not a valid label value, which a ReservedIP needs: %s", epName, key, keyOfNetwork(network), strings.Join(problems, "; "))
		return nil, false
	}
	if network.Kind == kindClusterNetwork.Kind && !planner.hasRipPrivilege(network.Name) {
		planner.fail("DanmEp %s carries %s, but its network %s is not granted the %s privilege in the allowedClusterNetworks of the TenantConfig", epName, key, keyOfNetwork(network), confman.ReservedIpPrivilege)
		return nil, false
	}
	return network, true
}

func (planner *ripPlanner) hasRipPrivilege(clusterNetwork string) bool {
	if len(planner.tenantConfigs) == 0 {
		return false
	}
	for _, allowed := range planner.tenantConfigs[0].AllowedCNets {
		if allowed.Name != clusterNetwork {
			continue
		}
		for _, privilege := range allowed.Privileges {
			if privilege == confman.ReservedIpPrivilege {
				return true
			}
		}
	}
	return false
}

func (planner *ripPlanner) address(ep *danmtypes.DanmEp, network *danmtypes.DanmNet, family ipFamily, key string) (net.IP, bool) {
	epName := objectName(ep.Namespace, ep.Name)
	raw, cidr, alloc := ep.Spec.Iface.Address, network.Spec.Options.Cidr, network.Spec.Options.Alloc
	if family == familyV6 {
		raw, cidr, alloc = ep.Spec.Iface.AddressIPv6, network.Spec.Options.Net6, network.Spec.Options.Alloc6
	}
	if raw == "" || raw == ipam.NoneAllocType {
		planner.fail("DanmEp %s carries %s, but it holds no %s address", epName, key, family)
		return nil, false
	}
	ip := parseAddress(raw)
	if ip == nil || (ip.To4() != nil) != (family == familyV4) {
		planner.fail("DanmEp %s carries %s, but its %s address %q cannot be parsed", epName, key, family, raw)
		return nil, false
	}
	if alloc == "" {
		planner.fail("DanmEp %s carries %s, but its network %s has no %s allocation, and a ReservedIP needs an L3 network", epName, key, keyOfNetwork(network), family)
		return nil, false
	}
	_, subnet, err := net.ParseCIDR(cidr)
	if err != nil || !subnet.Contains(ip) {
		planner.fail("DanmEp %s carries %s, but its %s address %s is outside of the CIDR %q of its network %s", epName, key, family, ip, cidr, keyOfNetwork(network))
		return nil, false
	}
	return ip, true
}

func (planner *ripPlanner) finish() ([]*plannedRip, []string) {
	rips := make([]*plannedRip, 0, len(planner.rips))
	for _, rip := range planner.rips {
		rips = append(rips, rip)
	}
	sort.Slice(rips, func(i, j int) bool {
		return objectName(rips[i].Namespace, rips[i].Name) < objectName(rips[j].Namespace, rips[j].Name)
	})
	claims := map[string]string{}
	for _, rip := range rips {
		ripKey := objectName(rip.Namespace, rip.Name)
		for family, address := range rip.Addresses {
			if address == nil {
				continue
			}
			if !address.Shared && len(address.Users) > 1 {
				planner.fail("ReservedIP %s is not shared, but %d DanmEps name it for their %s address: %s", ripKey, len(address.Users), ipFamily(family), strings.Join(address.Users, ", "))
			}
			claim := rip.Network.String() + " " + address.IP.String()
			if claimant, claimed := claims[claim]; claimed {
				planner.fail("ReservedIPs %s and %s both claim the address %s on %s", claimant, ripKey, address.IP, rip.Network)
				continue
			}
			claims[claim] = ripKey
		}
	}
	return rips, planner.problems
}

func ripNameProblems(name string) []string {
	if name == ipam.DynamicAllocType {
		return []string{"it is reserved for dynamic allocation"}
	}
	problems := validation.IsDNS1123Subdomain(name)
	return append(problems, validation.IsValidLabelValue(name)...)
}

func parseAddress(raw string) net.IP {
	if ip, _, err := net.ParseCIDR(raw); err == nil {
		return ip
	}
	return net.ParseIP(raw)
}
