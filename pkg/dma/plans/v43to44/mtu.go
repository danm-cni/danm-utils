package v43to44

import (
	"fmt"
	"io"
	"strconv"
	"strings"
	"text/tabwriter"

	danmtypes "github.com/danm-cni/danm/crd/apis/danm/v1"
	"github.com/danm-cni/danm/pkg/mtu"
)

const (
	MtuTagKey = "dma.danm.io/mtu"
	MinMtu    = 68
	MinMtuV6  = 1280
	MaxMtu    = 65535
)

func isIpvlan(network *danmtypes.DanmNet) bool {
	networkType := strings.ToLower(network.Spec.NetworkType)
	return networkType == "" || networkType == "ipvlan"
}

func legacyMtu(network *danmtypes.DanmNet) string {
	switch strings.ToLower(network.Spec.NetworkType) {
	case "", "ipvlan":
		return "parent device"
	case "macvlan":
		return strconv.Itoa(mtu.DefaultMtu)
	}
	return "delegate default"
}

func encapOverhead(network *danmtypes.DanmNet) int {
	if network.Spec.Options.Vxlan == 0 {
		return 0
	}
	if network.Spec.Options.Net6 != "" {
		return mtu.VxlanOverheadV6
	}
	return mtu.VxlanOverhead
}

func ParseMtuTag(network *danmtypes.DanmNet) (int, bool, error) {
	value, tagged := network.Annotations[MtuTagKey]
	if !tagged {
		return 0, false, nil
	}
	parsed, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil {
		return 0, true, fmt.Errorf("%s %s has a %s annotation of %q, which is not an integer", network.Kind, objectName(network.Namespace, network.Name), MtuTagKey, value)
	}
	minimum := MinMtu
	if network.Spec.Options.Net6 != "" {
		minimum = MinMtuV6
	}
	maximum := MaxMtu - encapOverhead(network)
	if parsed < minimum || parsed > maximum {
		return 0, true, fmt.Errorf("%s %s has a %s annotation of %d, outside of the %d to %d range the network can carry", network.Kind, objectName(network.Namespace, network.Name), MtuTagKey, parsed, minimum, maximum)
	}
	return parsed, true, nil
}

type mtuRisk struct {
	Network *danmtypes.DanmNet
	Tag     int
	Tagged  bool
	Err     error
}

func (risk mtuRisk) AtRisk() bool {
	return risk.Err == nil && !risk.Tagged && isIpvlan(risk.Network)
}

func (risk mtuRisk) newMtu() string {
	if risk.Err != nil {
		return "-"
	}
	if risk.Tagged {
		return strconv.Itoa(risk.Tag)
	}
	return strconv.Itoa(mtu.DefaultMtu)
}

func (risk mtuRisk) verdict() string {
	switch {
	case risk.Err != nil:
		return "INVALID TAG"
	case risk.AtRisk():
		return "AT RISK"
	}
	return "none"
}

func classifyMtu(networks []*danmtypes.DanmNet) []mtuRisk {
	risks := make([]mtuRisk, 0, len(networks))
	for _, network := range networks {
		tag, tagged, err := ParseMtuTag(network)
		risks = append(risks, mtuRisk{Network: network, Tag: tag, Tagged: tagged, Err: err})
	}
	return risks
}

func writeMtuTable(out io.Writer, risks []mtuRisk) {
	writer := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(writer, "KIND\tNAME\tTYPE\tDEVICE\tVXLAN\t4.3 MTU\tTAG\t4.4 MTU\tRISK")
	for _, risk := range risks {
		network := risk.Network
		tag := "-"
		if value, tagged := network.Annotations[MtuTagKey]; tagged {
			tag = value
		}
		fmt.Fprintf(writer, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			network.Kind, objectName(network.Namespace, network.Name), orDash(network.Spec.NetworkType), orDash(networkDevice(network)),
			orDash(vxlanText(network)), legacyMtu(network), tag, risk.newMtu(), risk.verdict())
	}
	writer.Flush()
}

func networkDevice(network *danmtypes.DanmNet) string {
	if network.Spec.Options.DevicePool != "" {
		return network.Spec.Options.DevicePool
	}
	return network.Spec.Options.Device
}

func vxlanText(network *danmtypes.DanmNet) string {
	if network.Spec.Options.Vxlan == 0 {
		return ""
	}
	return strconv.Itoa(network.Spec.Options.Vxlan)
}

func orDash(value string) string {
	if value == "" {
		return "-"
	}
	return value
}

func objectName(namespace, name string) string {
	if namespace == "" {
		return name
	}
	return namespace + "/" + name
}
