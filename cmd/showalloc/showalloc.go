/*
© 2020 Nokia
Licensed under the BSD 3-Clause License
SPDX-License-Identifier: BSD-3-Clause
*/

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"

	danmtypes "github.com/danm-cni/danm/crd/apis/danm/v1"
	danmclientset "github.com/danm-cni/danm/crd/client/clientset/versioned"
	"github.com/danm-cni/danm/pkg/bitarray"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/clientcmd"
)

func addIP(ip net.IP, num uint32) net.IP { // last 4 bytes are considered only
	oldIP := ip.To16()
	newIP := make(net.IP, len(oldIP))
	copy(newIP, oldIP)
	v := uint32(oldIP[12])<<24 + uint32(oldIP[13])<<16 + uint32(oldIP[14])<<8 + uint32(oldIP[15])
	v += num
	newIP[15] = byte(v & 0xFF)
	newIP[14] = byte((v >> 8) & 0xFF)
	newIP[13] = byte((v >> 16) & 0xFF)
	newIP[12] = byte((v >> 24) & 0xFF)
	return newIP
}

func diffIP(ip1, ip2 net.IP) uint32 { // last 4 bytes are considered only
	i1 := ip1.To16()
	i2 := ip2.To16()
	u1 := uint32(i1[12])<<24 + uint32(i1[13])<<16 + uint32(i1[14])<<8 + uint32(i1[15])
	u2 := uint32(i2[12])<<24 + uint32(i2[13])<<16 + uint32(i2[14])<<8 + uint32(i2[15])
	return uint32(u2) - uint32(u1)
}

func findEpByIP(epList *danmtypes.DanmEpList, ip net.IP, v6 bool) *danmtypes.DanmEp {
	var epIP net.IP
	for _, ep := range epList.Items {
		if v6 {
			epIP, _, _ = net.ParseCIDR(ep.Spec.Iface.AddressIPv6) // safe to ignore err as it is a validated property
		} else {
			epIP, _, _ = net.ParseCIDR(ep.Spec.Iface.Address) // safe to ignore err as it is a validated property
		}
		if epIP.Equal(ip) {
			return &ep
		}
	}
	return nil
}

func main() {
	var (
		// flags
		kubeConfig, dnet, cnet, tnet, namespace string
		v6, showAll                             bool
		showCidrStr                             string
		// for showCIDR feature
		isShowCidrRequested bool = false
		showCidr            *net.IPNet
		// network parameters
		spec                                  danmtypes.DanmNetSpec
		kind, alloc, cidr, poolStart, poolEnd string
		// others
		i, iStart, iEnd uint32
		status          string
		err             error
	)

	flag.StringVar(&kubeConfig, "kubeconfig", "", "Absolute path to a valid kubeconfig file. Only required if ShowAlloc runs out-of-cluster.")
	flag.StringVar(&dnet, "dnet", "", "DanmNet name.")
	flag.StringVar(&cnet, "cnet", "", "ClusterNetwork name.")
	flag.StringVar(&tnet, "tnet", "", "TenantNetwork name.")
	flag.StringVar(&namespace, "n", "default", "Namespace for DanmNet and TenantNetwork.")
	flag.BoolVar(&v6, "6", false, "Switch to IPv6 mode. By default the IPv4 mode is active.")
	flag.StringVar(&showCidrStr, "showCIDR", "", "Show the specified sub-CIDR only.")
	flag.BoolVar(&showAll, "a", false, "Show `free` and `out of pool range` IPs as well. By default these are suppressed.")
	flag.Parse()

	if len(showCidrStr) > 0 {
		_, showCidr, err = net.ParseCIDR(showCidrStr)
		if err != nil {
			log.Fatalln("ERROR: the specified sub-CIDR is not valid because: " + err.Error())
		}
		isShowCidrRequested = true
	}

	cfg, err := clientcmd.BuildConfigFromFlags("", kubeConfig)
	if err != nil {
		log.Fatalln("ERROR: cannot build cluster config for K8s REST client because: " + err.Error())
	}
	danmClient, err := danmclientset.NewForConfig(cfg)
	if err != nil {
		log.Fatalln("ERROR: cannot build DANM REST client because: " + err.Error())
	}

	switch {
	case len(dnet) > 0:
		kind = "DanmNet"
		danmDNet, err := danmClient.DanmV1().DanmNets(namespace).Get(context.TODO(), dnet, metav1.GetOptions{})
		if err != nil {
			log.Fatalln("ERROR: missing DanmNet `" + dnet + "` in namespace `" + namespace + "`: " + err.Error())
		}
		spec = danmDNet.Spec
	case len(cnet) > 0:
		kind = "ClusterNetwork"
		danmCNet, err := danmClient.DanmV1().ClusterNetworks().Get(context.TODO(), cnet, metav1.GetOptions{})
		if err != nil {
			log.Fatalln("ERROR: missing ClusterNetwork `" + cnet + "`: " + err.Error())
		}
		spec = danmCNet.Spec
	case len(tnet) > 0:
		kind = "TenantNetwork"
		danmTNet, err := danmClient.DanmV1().TenantNetworks(namespace).Get(context.TODO(), tnet, metav1.GetOptions{})
		if err != nil {
			log.Fatalln("ERROR: missing TenantNetwork `" + tnet + "` in namespace `" + namespace + "`: " + err.Error())
		}
		spec = danmTNet.Spec
	default:
		log.Fatalln("ERROR: missing DanmNet/ClusterNetwork/TenantNetwork name. Specify one of these to show allocation map.")
	}

	if v6 {
		alloc = spec.Options.Alloc6
		cidr = spec.Options.Pool6.Cidr
		poolStart = spec.Options.Pool6.Start
		poolEnd = spec.Options.Pool6.End
	} else {
		alloc = spec.Options.Alloc
		cidr = spec.Options.Cidr
		poolStart = spec.Options.Pool.Start
		poolEnd = spec.Options.Pool.End
	}
	fmt.Println("DANM network Kind:", kind)
	if v6 {
		fmt.Println("       IP version: IPv6")
	} else {
		fmt.Println("       IP version: IPv4")
	}
	fmt.Println("             CIDR:", cidr)
	fmt.Println("       Pool start:", poolStart)
	fmt.Println("       Pool   end:", poolEnd)

	if len(alloc) == 0 || len(cidr) == 0 || len(poolStart) == 0 || len(poolEnd) == 0 {
		if v6 {
			log.Fatalln("ERROR: missing IPv6 network details")
		} else {
			log.Fatalln("ERROR: missing IPv4 network details")
		}
	}

	danmEpList, err := danmClient.DanmV1().DanmEps(namespace).List(context.TODO(), metav1.ListOptions{}) // any further filtering?
	if err != nil {
		log.Fatalln("ERROR: unable to list DanmEps in namespace `" + namespace + "`: " + err.Error())
	}

	ba := bitarray.NewBitArrayFromBase64(alloc)
	ip, ipnet, _ := net.ParseCIDR(cidr)          // safe to ignore err as it is a validated property
	ip0 := ip.Mask(ipnet.Mask)                   // first IP of CIDR
	iStart = diffIP(ip0, net.ParseIP(poolStart)) // safe to ignore err as it is a validated property
	iEnd = diffIP(ip0, net.ParseIP(poolEnd))     // safe to ignore err as it is a validated property

	fmt.Println("--------------------------------------------------------------")
	for i = 0; i < ba.Len(); i++ {
		ip = addIP(ip0, i)
		if isShowCidrRequested && !showCidr.Contains(ip) {
			continue
		}
		switch {
		case i == 0:
			status = "reserved (network base address)"
		case i == ba.Len()-1:
			status = "reserved (broadcast address)"
		case i < iStart, i > iEnd:
			if !showAll {
				continue
			}
			status = "out of pool range"
		case ba.Get(i): // is allocated?
			ep := findEpByIP(danmEpList, ip, v6)
			if ep != nil {
				status = fmt.Sprintf("allocated (Pod: %s)", ep.Spec.Pod)
			} else {
				status = "allocated (unknown Pod)" // allocated from another namespace?
			}
		default: // not allocated
			if !showAll {
				continue
			}
			status = "free"
		}
		fmt.Printf("%15s : %s\n", ip, status)
	}
}
