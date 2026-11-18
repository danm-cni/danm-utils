package v43to44

import (
	"github.com/danm-cni/danm-utils/pkg/dma/cluster"
	"github.com/danm-cni/danm-utils/pkg/dma/engine"
)

const (
	copyNetworksStep = 6
	removeOldApiStep = 14
)

const (
	PlanID        = "4.3-to-4.4"
	DanmNamespace = "kube-system"
	TargetRelease = "4.4"
	PayloadName   = TargetRelease
)

const (
	ComponentWebhook    = "webhook"
	ComponentNetwatcher = "netwatcher"
	ComponentSvcwatcher = "svcwatcher"
	ComponentCleaner    = "cleaner"
)

var PayloadBinaries = []string{"danm", "fakeipam"}

var workloads = []cluster.ComponentWorkload{
	{Component: ComponentWebhook, Required: true, Default: cluster.WorkloadRef{Kind: cluster.WorkloadDeployment, Namespace: DanmNamespace, Name: "danm-webhook-deployment"}},
	{Component: ComponentNetwatcher, Required: true, Default: cluster.WorkloadRef{Kind: cluster.WorkloadDaemonSet, Namespace: DanmNamespace, Name: "netwatcher"}},
	{Component: ComponentSvcwatcher, Default: cluster.WorkloadRef{Kind: cluster.WorkloadDaemonSet, Namespace: DanmNamespace, Name: "svcwatcher"}},
	{Component: ComponentCleaner, Default: cluster.WorkloadRef{Kind: cluster.WorkloadDaemonSet, Namespace: DanmNamespace, Name: "danm-cleaner"}},
}

type plan struct {
	steps []engine.Step
}

func init() {
	engine.MustRegister(New())
}

func New() engine.Plan {
	return &plan{steps: []engine.Step{
		verifySourceCluster{},
		installNewCrds{},
		installNewRbac{},
		installAnnotationShim{},
		annotateExistingPods{},
		copyNetworkObjects{},
		copyDanmEps{},
		dualAnnotateServices{},
		replicateReservedIps{},
		deployNewWebhook{},
		rollControlPlane{},
		installCniBinaries{},
		resyncAndVerify{},
		removeOldApi{},
	}}
}

func (plan *plan) Meta() engine.PlanMeta {
	return engine.PlanMeta{
		ID:            PlanID,
		From:          "4.3",
		To:            TargetRelease,
		Description:   "Moves DANM from the danm.k8s.io API group to the danm.io API group without restarting DANM-attached Pods.",
		Components:    []string{ComponentWebhook, ComponentNetwatcher, ComponentSvcwatcher, ComponentCleaner},
		BinaryPayload: PayloadName,
		Workloads:     workloads,
	}
}

func (plan *plan) Steps() []engine.Step {
	return plan.steps
}

func (plan *plan) Advisories(rows []engine.StepRow) []string {
	copied, removed := false, false
	for _, row := range rows {
		switch row.Meta.ID {
		case copyNetworksStep:
			copied = row.Detected == engine.StateDone
		case removeOldApiStep:
			removed = row.Detected == engine.StateDone
		}
	}
	if copied && !removed {
		return []string{frozenGroupWarning}
	}
	return nil
}
