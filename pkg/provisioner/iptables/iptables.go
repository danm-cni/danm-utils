package iptables

import (
	"log"
	"runtime"

	"github.com/containernetworking/plugins/pkg/ns"
	"github.com/danm-cni/danm-utils/types/poltypes"
	corev1 "k8s.io/api/core/v1"
	k8stables "k8s.io/kubernetes/pkg/util/iptables"
)

var (
	DefaultInputRules = poltypes.NetRuleChain{
		Name: string(k8stables.ChainInput), Rules: []poltypes.NetRule{
			//Allow localhost communication by default
			poltypes.NetRule{SourceIface: "lo", Operation: poltypes.IptablesAccept},
			//Allow bi-directional communication through connections already established with trusted Egress entities
			poltypes.NetRule{State: poltypes.StateEstablishedRelated, Operation: poltypes.IptablesAccept},
			poltypes.NetRule{Operation: poltypes.IptablesReject},
		},
	}
	DefaultOutputRules = poltypes.NetRuleChain{
		Name: string(k8stables.ChainOutput), Rules: []poltypes.NetRule{
			//Allow localhost communication by default
			poltypes.NetRule{DestIface: "lo", Operation: poltypes.IptablesAccept},
			//Allow bi-directional communication through connections already established with trusted Ingress entities
			poltypes.NetRule{State: poltypes.StateEstablishedRelated, Operation: poltypes.IptablesAccept},
			//Allow outgoing cluster DNS communication by default. Can happen via either TCP, or UDP
			poltypes.NetRule{Protocol: "tcp", DestPort: "53", State: poltypes.StateNewEstablished, Operation: poltypes.IptablesAccept},
			poltypes.NetRule{Protocol: "udp", DestPort: "53", State: poltypes.StateNewEstablished, Operation: poltypes.IptablesAccept},
			poltypes.NetRule{Operation: poltypes.IptablesReject},
		},
	}
	DefaultForwardRules = poltypes.NetRuleChain{
		Name: string(k8stables.ChainForward), Rules: []poltypes.NetRule{
			poltypes.NetRule{Operation: poltypes.IptablesReject},
		},
	}
	JumpToV4IngressRule = poltypes.NetRuleChain{
		Name: string(k8stables.ChainInput), Rules: []poltypes.NetRule{
			poltypes.NetRule{Operation: poltypes.IngressV4ChainName},
		},
	}
	JumpToV4EgressRule = poltypes.NetRuleChain{
		Name: string(k8stables.ChainOutput), Rules: []poltypes.NetRule{
			poltypes.NetRule{Operation: poltypes.EgressV4ChainName},
		},
	}
	JumpToV6IngressRule = poltypes.NetRuleChain{
		Name: string(k8stables.ChainInput), Rules: []poltypes.NetRule{
			poltypes.NetRule{Operation: poltypes.IngressV6ChainName},
		},
	}
	JumpToV6EgressRule = poltypes.NetRuleChain{
		Name: string(k8stables.ChainOutput), Rules: []poltypes.NetRule{
			poltypes.NetRule{Operation: poltypes.EgressV6ChainName},
		},
	}
	DefaultReturnRule = poltypes.NetRule{
		Operation: poltypes.IptablesReturn,
	}
)

type IptablesProvisioner struct {
	V4Provisioner k8stables.Interface
	V6Provisioner k8stables.Interface
}

func NewIptablesProvisioner() *IptablesProvisioner {
	v4IptablesClient := k8stables.New(k8stables.ProtocolIPv4)
	v6IptablesClient := k8stables.New(k8stables.ProtocolIPv6)
	iptablesProv := IptablesProvisioner{V4Provisioner: v4IptablesClient, V6Provisioner: v6IptablesClient}
	return &iptablesProv
}

func (iptabProv *IptablesProvisioner) AddRulesToNewPod(ruleSet *poltypes.NetRuleSet, pod *corev1.Pod) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	origns, err := ns.GetCurrentNS()
	if err != nil {
		log.Println("Failed to get the current NS for Pod:" + pod.ObjectMeta.Name +
			" in ns:" + pod.ObjectMeta.Namespace + "because:" + err.Error())
		return
	}
	hns, err := ns.GetNS(ruleSet.Netns)
	if err != nil {
		log.Println("Failed to get into Pod's:" + pod.ObjectMeta.Name + " in ns:" + pod.ObjectMeta.Namespace +
			" netns:" + ruleSet.Netns + " cause of error:" + err.Error())
		return
	}
	defer func() {
		hns.Close()
		origns.Set()
	}()
	err = hns.Set()
	if err != nil {
		log.Println("failed to enter network namespace:" + ruleSet.Netns + " of Pod:" + pod.ObjectMeta.Name +
			" in ns:" + pod.ObjectMeta.Namespace + "because of error:" + err.Error())
		return
	}
	err = ensureChains(iptabProv, ruleSet, pod)
	if err != nil {
		log.Println("required filter chains could not be created for Pod:" + pod.ObjectMeta.Name +
			" in ns:" + pod.ObjectMeta.Namespace + " because of error:" + err.Error())
		return
	}
	provisionDynamicRules(iptabProv, ruleSet, pod)
	provisionDefaultRules(iptabProv, pod)
}

func ensureChains(iptablesProv *IptablesProvisioner, ruleSet *poltypes.NetRuleSet, pod *corev1.Pod) error {
	err := ensureChain(ruleSet.IngressV4Chain, JumpToV4IngressRule, iptablesProv.V4Provisioner, pod)
	if err != nil {
		return err
	}
	err = ensureChain(ruleSet.IngressV6Chain, JumpToV6IngressRule, iptablesProv.V6Provisioner, pod)
	if err != nil {
		return err
	}
	err = ensureChain(ruleSet.EgressV4Chain, JumpToV4EgressRule, iptablesProv.V4Provisioner, pod)
	if err != nil {
		return err
	}
	return ensureChain(ruleSet.EgressV6Chain, JumpToV6EgressRule, iptablesProv.V6Provisioner, pod)
}

func ensureChain(chain, jumpRule poltypes.NetRuleChain, provisioner k8stables.Interface, pod *corev1.Pod) error {
	var err error
	if len(chain.Rules) > 0 {
		_, err = provisioner.EnsureChain(k8stables.TableFilter, k8stables.Chain(chain.Name))
		provisioner.FlushChain(k8stables.TableFilter, k8stables.Chain(chain.Name))
		provisionRulesIntoChain(provisioner, jumpRule, pod)
	}
	return err
}

func provisionDynamicRules(iptablesProv *IptablesProvisioner, ruleSet *poltypes.NetRuleSet, pod *corev1.Pod) {
	provisionRulesIntoChain(iptablesProv.V4Provisioner, ruleSet.IngressV4Chain, pod)
	provisionRulesIntoChain(iptablesProv.V6Provisioner, ruleSet.IngressV6Chain, pod)
	provisionRulesIntoChain(iptablesProv.V4Provisioner, ruleSet.EgressV4Chain, pod)
	provisionRulesIntoChain(iptablesProv.V6Provisioner, ruleSet.EgressV6Chain, pod)
}

func provisionDefaultRules(iptablesProv *IptablesProvisioner, pod *corev1.Pod) {
	provisionRulesIntoChain(iptablesProv.V4Provisioner, DefaultInputRules, pod)
	provisionRulesIntoChain(iptablesProv.V4Provisioner, DefaultOutputRules, pod)
	provisionRulesIntoChain(iptablesProv.V4Provisioner, DefaultForwardRules, pod)
	provisionRulesIntoChain(iptablesProv.V6Provisioner, DefaultInputRules, pod)
	provisionRulesIntoChain(iptablesProv.V6Provisioner, DefaultOutputRules, pod)
	provisionRulesIntoChain(iptablesProv.V6Provisioner, DefaultForwardRules, pod)
}

func provisionRulesIntoChain(provisioner k8stables.Interface, rules poltypes.NetRuleChain, pod *corev1.Pod) {
	for _, rule := range rules.Rules {
		args := createArgsFromRule(rule)
		_, err := provisioner.EnsureRule(k8stables.Append, k8stables.TableFilter, k8stables.Chain(rules.Name), args...)
		if err != nil {
			log.Println("ERROR: provisioning iptables rule for Pod: " + pod.ObjectMeta.Name + " in ns: " + pod.ObjectMeta.Namespace + "with args:" + rule.String() +
				" into chain:" + rules.Name + " failed with error:" + err.Error())
		}
	}
	//We need to add a default "RETURN" rule to the end of our own chains
	if rules.Name != string(k8stables.ChainInput) && rules.Name != string(k8stables.ChainOutput) && rules.Name != string(k8stables.ChainForward) && len(rules.Rules) > 0 {
		args := createArgsFromRule(DefaultReturnRule)
		_, err := provisioner.EnsureRule(k8stables.Append, k8stables.TableFilter, k8stables.Chain(rules.Name), args...)
		if err != nil {
			log.Println("ERROR: provisioning iptables rule for Pod: " + pod.ObjectMeta.Name + " in ns: " + pod.ObjectMeta.Namespace + "with args:" + DefaultReturnRule.String() +
				" into chain:" + rules.Name + " failed with error:" + err.Error())
		}
	}
}

func createArgsFromRule(rule poltypes.NetRule) []string {
	args := make([]string, 0)
	if rule.Protocol != "" {
		args = append(args, "-p", rule.Protocol)
	}
	if rule.SourcePort != "" {
		args = append(args, "--sport", rule.SourcePort)
	}
	if rule.DestPort != "" {
		args = append(args, "--dport", rule.DestPort)
	}
	if rule.SourceIface != "" {
		args = append(args, "-i", rule.SourceIface)
	}
	if rule.DestIface != "" {
		args = append(args, "-o", rule.DestIface)
	}
	if rule.SourceIp != "" {
		args = append(args, "-s", rule.SourceIp)
	}
	if rule.DestIp != "" {
		args = append(args, "-d", rule.DestIp)
	}
	if rule.State != "" {
		args = append(args, "-m", "conntrack", "--ctstate", rule.State)
	}
	if rule.Operation != "" {
		args = append(args, "-j", rule.Operation)
	} else {
		args = append(args, "-j", poltypes.IptablesAccept)
	}
	return args
}
