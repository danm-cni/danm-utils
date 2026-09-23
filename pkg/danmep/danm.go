package danmep

import (
	danmipam "github.com/danm-cni/danm/pkg/ipam"
)

type danmReleaseIPServiceImpl releaseIPServiceImplBase

func (h *danmReleaseIPServiceImpl) IsIPAllocatedByMe(ip string) bool {
	return h.dnet == nil ||
		danmipam.WasIpAllocatedByDanm(ip, h.dnet) ||
		h.ep.Spec.NetworkType == "flannel"
}

func (h *danmReleaseIPServiceImpl) ReleaseIP(ip string) error {
	return danmipam.Free(h.danmClient, *h.dnet, ip)
}
