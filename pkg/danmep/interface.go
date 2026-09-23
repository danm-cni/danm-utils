package danmep

import (
	"context"
	"fmt"

	danmtypes "github.com/danm-cni/danm/crd/apis/danm/v1"
	danmclientset "github.com/danm-cni/danm/crd/client/clientset/versioned"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// GetSupportedReleaseIPHandlers gets all supported static CNI implementation that cleaner can use
// when deleting dangling DanmEps
func GetSupportedReleaseIPHandlers(danmClient danmclientset.Interface, dnet *danmtypes.DanmNet, ep *danmtypes.DanmEp) []interface{} {
	return []interface{}{
		&danmReleaseIPServiceImpl{danmClient, dnet, ep},
		&calicoReleaseIPServiceImpl{danmClient, dnet, ep},
	}
}

// ReleaseIPInterface need to be implemented by every static CNI IPAM release plugin
// that want to invoked when Cleaner is cleaning up dangling DanmEps
type ReleaseIPInterface interface {
	ReleaseIP(ip string) error
	IsIPAllocatedByMe(ip string) bool
}

type releaseIPServiceImplBase struct {
	danmClient danmclientset.Interface
	dnet       *danmtypes.DanmNet
	ep         *danmtypes.DanmEp
}

// SelectReleaseIpServiceImplementation looks up and returns the
// first suitable ReleaseIP service implementation for given IP address
// returns nil when no suitable implementation found
func SelectReleaseIpServiceImplementation(danmClient danmclientset.Interface, dnet *danmtypes.DanmNet, ep *danmtypes.DanmEp, ip string) ReleaseIPInterface {
	for _, impl := range GetSupportedReleaseIPHandlers(danmClient, dnet, ep) {
		if service, ok := impl.(ReleaseIPInterface); ok && service.IsIPAllocatedByMe(ip) {
			return service
		}
	}
	return nil
}

// DeleteDanmEp selects an ReleaseIPService implementation for allocated IPv4 and IPv6 IP addresses in a
// given DanmEp and invokes them separately trying ot free up allocated IPs, after successfully freeing up
// all IPs, it deletes the DanmEp, returns error if unable to free any of the IPs or unable to delete DanmEp
func DeleteDanmEp(danmClient danmclientset.Interface, ep *danmtypes.DanmEp, dnet *danmtypes.DanmNet) error {
	if service := SelectReleaseIpServiceImplementation(danmClient, dnet, ep, ep.Spec.Iface.Address); service != nil {
		if err := service.ReleaseIP(ep.Spec.Iface.Address); err != nil {
			return fmt.Errorf("unable to release ipv4 IP because: %s", err)
		}
	} else {
		return fmt.Errorf("unable to release ipv4 IP because: no releaseIP Service selected")
	}
	if service := SelectReleaseIpServiceImplementation(danmClient, dnet, ep, ep.Spec.Iface.AddressIPv6); service != nil {
		if err := service.ReleaseIP(ep.Spec.Iface.AddressIPv6); err != nil {
			return fmt.Errorf("unable to release ipv6 IP because: %s", err)
		}
	} else {
		return fmt.Errorf("unable to release ipv6 IP because: no releaseIP Service selected")
	}
	return danmClient.DanmV1().DanmEps(ep.ObjectMeta.Namespace).Delete(context.TODO(), ep.ObjectMeta.Name, metav1.DeleteOptions{})
}
