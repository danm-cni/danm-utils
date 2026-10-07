// Package engine holds the plan and step abstractions of the DANM Migration Assistant, the
// registry of the pre-baked plans, and the runner which executes and rolls back their steps.
package engine

import (
	"context"
	"fmt"

	"github.com/danm-cni/danm-utils/pkg/dma/cluster"
)

// Phase groups the steps of a plan. Steps always run in phase order, and the cleanup phase is
// always the last one, because it is the phase holding the irreversible operations.
type Phase int

const (
	PhasePreflight Phase = iota
	PhaseDeploy
	PhaseConvert
	PhaseCutover
	PhaseCleanup
)

var phaseNames = map[Phase]string{
	PhasePreflight: "Preflight",
	PhaseDeploy:    "Deploy",
	PhaseConvert:   "Convert",
	PhaseCutover:   "Cutover",
	PhaseCleanup:   "Cleanup",
}

// String returns the human readable name of a phase.
func (phase Phase) String() string {
	if name, found := phaseNames[phase]; found {
		return name
	}
	return fmt.Sprintf("Phase(%d)", int(phase))
}

// StepState is what a step's own detector says about the cluster. It is always authoritative
// over the recorded state, so that a step interrupted halfway can never read as finished.
type StepState string

const (
	// StateNotStarted means the cluster shows no trace of this step having run.
	StateNotStarted StepState = "NotStarted"
	// StatePartial means the step left some but not all of its marks on the cluster.
	StatePartial StepState = "Partial"
	// StateDone means the cluster fully reflects this step.
	StateDone StepState = "Done"
)

// StepMeta describes a step to the operator reviewing a plan.
type StepMeta struct {
	ID          int
	Name        string
	Description string
	Phase       Phase
	// Reversible is false for steps which destroy something the assistant cannot put back.
	// An irreversible step demands the plan ID as its confirmation token, and can never be
	// rolled back.
	Reversible bool
}

// Step is one reviewable, individually executable unit of a migration plan.
//
// Detect must be cheap, read-only and safe to call at any time, including before the step has
// ever run and after it has been rolled back. Execute must be idempotent, so that re-running a
// step which failed halfway converges rather than doubling up. Rollback of an irreversible step
// is never called by the runner.
type Step interface {
	Meta() StepMeta
	Detect(ctx context.Context, handle *cluster.Handle) (StepState, error)
	Execute(ctx context.Context, handle *cluster.Handle) error
	Rollback(ctx context.Context, handle *cluster.Handle) error
}

// PlanMeta describes a migration between two DANM releases.
type PlanMeta struct {
	// ID is how the operator addresses the plan, conventionally "<from>-to-<to>".
	ID          string
	From        string
	To          string
	Description string
	// Components lists the image bearing workloads this plan rolls. The runner resolves all
	// of them before the plan starts, so that a bad image reference is found while nothing
	// has been changed yet.
	Components []string
	// BinaryPayload names the directory of CNI binaries inside the assistant's image which
	// this plan installs on the nodes. Empty when the plan installs no binaries.
	BinaryPayload string
}

// Plan is an ordered list of pre-baked migration steps between two DANM releases.
type Plan interface {
	Meta() PlanMeta
	Steps() []Step
}
