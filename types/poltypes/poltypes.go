package poltypes

import (
	danmv1 "github.com/danm-cni/danm/crd/apis/danm/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

const (
	DefaultBucketName       = "default"
	CustomBucketPostfix     = "bucket42"
	IptablesReject          = "REJECT"
	IptablesAccept          = "ACCEPT"
	IptablesReturn          = "RETURN"
	IngressV4ChainName      = "DANM_INGRESS_V4"
	IngressV6ChainName      = "DANM_INGRESS_V6"
	EgressV4ChainName       = "DANM_EGRESS_V4"
	EgressV6ChainName       = "DANM_EGRESS_V6"
	StateEstablishedRelated = "ESTABLISHED,RELATED"
	StateNewEstablished     = "NEW,ESTABLISHED"
	DanmNetKind             = "DanmNet"
	ClusterNetworkKind      = "ClusterNetwork"
)

type RuleProvisioner interface {
	AddRulesToPod(*NetRuleSet, *corev1.Pod)
}

type UidCache map[types.UID]bool

type DanmEpBuckets map[string][]danmv1.DanmEp

type DanmEpSet struct {
	DanmEpsByLabel   DanmEpBuckets
	DanmEpsByNetwork DanmEpBuckets
	PodEps           []danmv1.DanmEp
}

type NetRuleSet struct {
	IngressV4Chain NetRuleChain
	IngressV6Chain NetRuleChain
	EgressV4Chain  NetRuleChain
	EgressV6Chain  NetRuleChain
	Netns          string
}

type NetRuleChain struct {
	Name  string
	Rules []NetRule
}

type NetRule struct {
	SourceIp    string
	SourcePort  string
	SourceIface string
	DestIp      string
	DestPort    string
	DestIface   string
	Protocol    string
	Operation   string
	State       string
}

func (rule NetRule) String() string {
	var ruleStr string
	if rule.Protocol != "" {
		ruleStr += "protocol:" + rule.Protocol
	}
	if rule.SourcePort != "" {
		ruleStr += " source port:" + rule.SourcePort
	}
	if rule.DestPort != "" {
		ruleStr += " dest port:" + rule.DestPort
	}
	if rule.SourceIface != "" {
		ruleStr += " source dev:" + rule.SourceIface
	}
	if rule.DestIface != "" {
		ruleStr += " dest dev:" + rule.DestIface
	}
	if rule.SourceIp != "" {
		ruleStr += " source IP:" + rule.SourceIp
	}
	if rule.DestIp != "" {
		ruleStr += " dest IP:" + rule.DestIp
	}
	if rule.Operation != "" {
		ruleStr += " op:" + rule.Operation
	}
	if rule.State != "" {
		ruleStr += " state:" + rule.State
	}
	return ruleStr
}
