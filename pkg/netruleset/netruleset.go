package netruleset

import (
	"log"
	"strings"

	polv1 "github.com/danm-cni/danm-utils/crd/api/netpol/v1"
	"github.com/danm-cni/danm-utils/types/poltypes"
	danmv1 "github.com/danm-cni/danm/crd/apis/danm/v1"
	"github.com/danm-cni/danm/pkg/ipam"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/pkg/apis/networking"
)

type RuleParser func(address string, ports []networking.NetworkPolicyPort) []poltypes.NetRule

func NewNetRuleSet(polSet []polv1.DanmNetworkPolicy, depSet *poltypes.DanmEpSet) *poltypes.NetRuleSet {
	ruleSet := poltypes.NetRuleSet{Netns: depSet.PodEps[0].Spec.Netns}
	ruleSet.IngressV4Chain.Name = poltypes.IngressV4ChainName
	ruleSet.IngressV6Chain.Name = poltypes.IngressV6ChainName
	ruleSet.EgressV4Chain.Name = poltypes.EgressV4ChainName
	ruleSet.EgressV6Chain.Name = poltypes.EgressV6ChainName
	for _, policy := range polSet {
		//TODO: is it really necessary for Ingress / Egress to be list?
		// Format is kept to be consistent with upstream, but there is really no use-case for having multiple from/to sections in a network policy
		if len(policy.Spec.Ingress) > 0 {
			ingressV4Rules, ingressV6Rules := parsePolicyRules(depSet, policy.Spec.Ingress[0].From, policy.Spec.Ingress[0].Ports, newIngressNetRules)
			ruleSet.IngressV4Chain.Rules = append(ruleSet.IngressV4Chain.Rules, ingressV4Rules...)
			ruleSet.IngressV6Chain.Rules = append(ruleSet.IngressV6Chain.Rules, ingressV6Rules...)
		}
		if len(policy.Spec.Egress) > 0 {
			egressV4Rules, egressV6Rules := parsePolicyRules(depSet, policy.Spec.Egress[0].To, policy.Spec.Egress[0].Ports, newEgressNetRules)
			ruleSet.EgressV4Chain.Rules = append(ruleSet.EgressV4Chain.Rules, egressV4Rules...)
			ruleSet.EgressV6Chain.Rules = append(ruleSet.EgressV6Chain.Rules, egressV6Rules...)
		}
	}
	return &ruleSet
}

func parsePolicyRules(depSet *poltypes.DanmEpSet, peers []polv1.NetworkPolicyPeer, ports []networking.NetworkPolicyPort, parserFunc RuleParser) ([]poltypes.NetRule, []poltypes.NetRule) {
	v4Rules := make([]poltypes.NetRule, 0)
	v6Rules := make([]poltypes.NetRule, 0)
	for _, peer := range peers {
		//TODO: before we even get to rule filtering phase we should handle the corner-cases
		//1: peer list key is provided but empty list -> EVERYTHING is whitelisted
		//2: peer list is missing -> NOTHING is whitelisted
		//Only when peer list is provided and at least one selector is present we should progress to filtering
		podSelectedDeps := filterDepsByPodSelector(depSet, peer.PodSelector)
		networkSelectedDeps := filterDepsByNetworkSelector(depSet, peer.NetworkSelector)
		finalDeps := intersectDepSets(podSelectedDeps, networkSelectedDeps)
		depCache := make(poltypes.UidCache, 0)
		for _, dep := range finalDeps {
			if _, ok := depCache[dep.ObjectMeta.UID]; !ok {
				depCache[dep.ObjectMeta.UID] = true
				if dep.Spec.Iface.Address != "" && dep.Spec.Iface.Address != ipam.NoneAllocType {
					v4Rules = append(v4Rules, parserFunc(dep.Spec.Iface.Address, ports)...)
				}
				if dep.Spec.Iface.AddressIPv6 != "" && dep.Spec.Iface.AddressIPv6 != ipam.NoneAllocType {
					v6Rules = append(v6Rules, parserFunc(dep.Spec.Iface.AddressIPv6, ports)...)
				}
			}
		}
	}
	return v4Rules, v6Rules
}

func filterDepsByPodSelector(depSet *poltypes.DanmEpSet, podSelector metav1.LabelSelector) []danmv1.DanmEp {
	selectedDeps := make([]danmv1.DanmEp, 0)
	//Empty Pod selector in a non-empty peer means no Pods are whitelisted based on labels, whitelisting purely happens based on other selectors
	if len(podSelector.MatchLabels) == 0 {
		return selectedDeps
	}
	selectors, err := metav1.LabelSelectorAsMap(&podSelector)
	if err != nil {
		log.Println("WARNING: PodSelector parsing failed with error:" + err.Error() + ", ignoring related peers!")
		return selectedDeps
	}
	for key, value := range selectors {
		selectedDeps = append(selectedDeps, depSet.DanmEpsByLabel[key+value+poltypes.CustomBucketPostfix]...)
	}
	return selectedDeps
}

func filterDepsByNetworkSelector(depSet *poltypes.DanmEpSet, networkSelectors []polv1.NetworkSelector) []danmv1.DanmEp {
	selectedDeps := make([]danmv1.DanmEp, 0)
	//Empty network selector in a non-empty peer means we don't filter by networks, whitelisting happens purely based on other selectors
	if len(networkSelectors) == 0 {
		return selectedDeps
	}
	for _, netSelector := range networkSelectors {
		networkBucketName := netSelector.Name + netSelector.Type
		if netSelector.Type == "" {
			networkBucketName += poltypes.DanmNetKind
		}
		selectedDeps = append(selectedDeps, depSet.DanmEpsByNetwork[networkBucketName]...)
	}
	return selectedDeps
}

func intersectDepSets(firstSet, secondSet []danmv1.DanmEp) []danmv1.DanmEp {
	if len(firstSet) == 0 {
		return secondSet
	} else if len(secondSet) == 0 {
		return firstSet
	}
	secondSetIndex := make(poltypes.UidCache, 0)
	//Build up an index card from the second set to avoid iterating over it for every element in the first set
	for _, dep := range secondSet {
		if _, ok := secondSetIndex[dep.ObjectMeta.UID]; !ok {
			secondSetIndex[dep.ObjectMeta.UID] = true
		}
	}
	intersectedDeps := make([]danmv1.DanmEp, 0)
	for _, dep := range firstSet {
		if _, ok := secondSetIndex[dep.ObjectMeta.UID]; ok {
			intersectedDeps = append(intersectedDeps, dep)
		}
	}
	return intersectedDeps
}

func newIngressNetRules(address string, ports []networking.NetworkPolicyPort) []poltypes.NetRule {
	ingressRules := make([]poltypes.NetRule, 0)
	if len(ports) == 0 {
		universalRule := poltypes.NetRule{SourceIp: strings.Split(address, "/")[0]}
		ingressRules = append(ingressRules, universalRule)
		return ingressRules
	}
	for _, port := range ports {
		ingressRule := poltypes.NetRule{SourceIp: strings.Split(address, "/")[0], SourcePort: port.Port.StrVal, Protocol: string(*port.Protocol)}
		ingressRules = append(ingressRules, ingressRule)
	}
	return ingressRules
}

func newEgressNetRules(address string, ports []networking.NetworkPolicyPort) []poltypes.NetRule {
	egressRules := make([]poltypes.NetRule, 0)
	if len(ports) == 0 {
		universalRule := poltypes.NetRule{DestIp: strings.Split(address, "/")[0]}
		egressRules = append(egressRules, universalRule)
		return egressRules
	}
	for _, port := range ports {
		egressRule := poltypes.NetRule{DestIp: strings.Split(address, "/")[0], DestPort: port.Port.StrVal, Protocol: string(*port.Protocol)}
		egressRules = append(egressRules, egressRule)
	}
	return egressRules
}
