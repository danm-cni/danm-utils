package danmep

import (
	"net"

	danmipam "github.com/danm-cni/danm/pkg/ipam"
)

type danmReleaseIPServiceImpl releaseIPServiceImplBase

func (h *danmReleaseIPServiceImpl) IsIPAllocatedByMe(ip string) bool {
	return h.dnet == nil ||
		danmipam.WasIpAllocatedByDanm(ip, h.dnet) ||
		h.ep.Spec.NetworkType == "flannel"
}

func (h *danmReleaseIPServiceImpl) ReleaseIP(ip string) error {
	//TODO: this is just a quick fix of the interface being designed around the wrong assumption that different IPs belonging to the same interface could be allocated by different IPAMs
	//Whole thing would need to be refactored at some point, at least this design is not holding mainline ipam free algorithm hostage anymore
	netIp := net.ParseIP(ip)
	if netIp.To4() != nil {
		return danmipam.GarbageCollectIps(h.danmClient, *h.dnet, ip, "")
	} else {
		return danmipam.GarbageCollectIps(h.danmClient, *h.dnet, "", ip)
	}
}
