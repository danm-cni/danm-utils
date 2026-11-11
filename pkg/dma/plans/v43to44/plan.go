package v43to44

import (
	"context"
	"fmt"

	"github.com/danm-cni/danm-utils/pkg/dma/cluster"
	"github.com/danm-cni/danm-utils/pkg/dma/engine"
)

const (
	PlanID        = "4.3-to-4.4"
	DanmNamespace = "kube-system"
	PayloadName   = "4.4"
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
		pending(2, "install-new-crds", engine.PhaseDeploy, "Install the danm.io CRDs of the detected deployment mode next to the danm.k8s.io ones."),
		pending(3, "install-new-rbac", engine.PhaseDeploy, "Authorise the DANM components for the danm.io group alongside the danm.k8s.io one."),
		pending(4, "install-annotation-shim", engine.PhaseDeploy, "Deploy the webhook which adds danm.io/interfaces to every new Pod carrying danm.k8s.io/interfaces."),
		pending(5, "annotate-existing-pods", engine.PhaseConvert, "Add danm.io/interfaces to every existing Pod carrying only danm.k8s.io/interfaces."),
		pending(6, "copy-network-objects", engine.PhaseConvert, "Copy the network objects and TenantConfigs of the danm.k8s.io group into the danm.io group."),
		pending(7, "copy-danmeps", engine.PhaseConvert, "Copy the DanmEps of the danm.k8s.io group into the danm.io group."),
		pending(8, "dual-annotate-services", engine.PhaseConvert, "Add the danm.io Service annotations next to the danm.k8s.io ones."),
		pending(9, "replicate-reserved-ips", engine.PhaseConvert, "Create the ReservedIPs tagged on old-group DanmEps."),
		pending(10, "deploy-new-webhook", engine.PhaseCutover, "Roll the DANM webhook to 4.4 and point its configuration at the danm.io group."),
		pending(11, "roll-control-plane", engine.PhaseCutover, "Roll netwatcher, svcwatcher and cleaner to 4.4."),
		pending(12, "install-cni-binaries", engine.PhaseCutover, "Install the DANM 4.4 CNI binaries on every node."),
		pending(13, "resync-and-verify", engine.PhaseCutover, "Carry over the allocations made in the danm.k8s.io group since the copy, and verify the cluster."),
		pending(14, "remove-old-api", engine.PhaseCleanup, "Remove the danm.k8s.io webhook configuration, CRDs and RBAC, and the backed up CNI binaries."),
	}}
}

func (plan *plan) Meta() engine.PlanMeta {
	return engine.PlanMeta{
		ID:            PlanID,
		From:          "4.3",
		To:            "4.4",
		Description:   "Moves DANM from the danm.k8s.io API group to the danm.io API group without restarting DANM-attached Pods.",
		Components:    []string{ComponentWebhook, ComponentNetwatcher, ComponentSvcwatcher, ComponentCleaner},
		BinaryPayload: PayloadName,
		Workloads:     workloads,
	}
}

func (plan *plan) Steps() []engine.Step {
	return plan.steps
}

type pendingStep struct {
	meta engine.StepMeta
}

func pending(id int, name string, phase engine.Phase, description string) pendingStep {
	return pendingStep{meta: engine.StepMeta{ID: id, Name: name, Description: description, Phase: phase, Reversible: phase != engine.PhaseCleanup}}
}

func (step pendingStep) Meta() engine.StepMeta {
	return step.meta
}

func (step pendingStep) Detect(_ context.Context, _ *cluster.Handle) (engine.StepState, error) {
	return engine.StateNotStarted, nil
}

func (step pendingStep) Execute(_ context.Context, _ *cluster.Handle) error {
	return step.notImplemented()
}

func (step pendingStep) Rollback(_ context.Context, _ *cluster.Handle) error {
	return step.notImplemented()
}

func (step pendingStep) notImplemented() error {
	return fmt.Errorf("step %d (%s) is not implemented in this build of the assistant", step.meta.ID, step.meta.Name)
}
