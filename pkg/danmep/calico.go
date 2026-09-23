package danmep

import (
	"fmt"
	"log"
	"net"
	"os/exec"

	danmipam "github.com/danm-cni/danm/pkg/ipam"
)

type calicoReleaseIPServiceImpl releaseIPServiceImplBase

func (h *calicoReleaseIPServiceImpl) IsIPAllocatedByMe(ip string) bool {
	return h.dnet == nil ||
		(!danmipam.WasIpAllocatedByDanm(ip, h.dnet) && h.ep.Spec.NetworkType == "calico")
}

func (h *calicoReleaseIPServiceImpl) ReleaseIP(ip string) error {
	parsedIp := net.ParseIP(ip)
	if parsedIp == nil {
		parsedIp, _, _ = net.ParseCIDR(ip)
	}
	cmd := exec.Command("calicoctl", "ipam", "release", fmt.Sprintf("--ip=%s", parsedIp))
	log.Printf("release calico managed IP: %s", cmd)

	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("could not release calico managed IP %s, because: %s | output: %s", ip, err, output)
	}
	return nil
}
