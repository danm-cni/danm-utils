package engine

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"strings"
	"testing"

	"github.com/danm-cni/danm-utils/pkg/dma/cluster"
	"github.com/danm-cni/danm-utils/pkg/dma/state"
	kubefake "k8s.io/client-go/kubernetes/fake"
)

type fakeStep struct {
	meta        StepMeta
	state       StepState
	executed    int
	rolledBack  int
	executeErr  error
	rollbackErr error
	detectErr   error
	noConverge  bool
}

func (step *fakeStep) Meta() StepMeta {
	return step.meta
}

func (step *fakeStep) Detect(_ context.Context, _ *cluster.Handle) (StepState, error) {
	if step.detectErr != nil {
		return StateNotStarted, step.detectErr
	}
	return step.state, nil
}

func (step *fakeStep) Execute(_ context.Context, _ *cluster.Handle) error {
	step.executed++
	if step.executeErr != nil {
		return step.executeErr
	}
	if step.noConverge {
		return nil
	}
	step.state = StateDone
	return nil
}

func (step *fakeStep) Rollback(_ context.Context, _ *cluster.Handle) error {
	step.rolledBack++
	if step.rollbackErr != nil {
		return step.rollbackErr
	}
	step.state = StateNotStarted
	return nil
}

type fakePlan struct {
	meta  PlanMeta
	steps []Step
}

func (plan *fakePlan) Meta() PlanMeta {
	return plan.meta
}

func (plan *fakePlan) Steps() []Step {
	return plan.steps
}

func step(id int, name string, phase Phase, reversible bool) *fakeStep {
	return &fakeStep{meta: StepMeta{ID: id, Name: name, Description: name + " description", Phase: phase, Reversible: reversible}, state: StateNotStarted}
}

func plan(steps ...Step) *fakePlan {
	return &fakePlan{meta: PlanMeta{ID: "4.3-to-4.4", From: "4.3", To: "4.4", Description: "test plan"}, steps: steps}
}

func handle() *cluster.Handle {
	client := kubefake.NewSimpleClientset()
	out := io.Discard
	return &cluster.Handle{
		Kube:      client,
		State:     state.NewStore(client, "kube-system", "dma-state"),
		Images:    cluster.ImageConfig{Tag: "latest", Overrides: map[string]cluster.ComponentImage{}},
		Namespace: "kube-system",
		Operator:  "tester",
		Log:       log.New(out, "", 0),
		Out:       out,
	}
}

func runner(plan Plan) (*Runner, *cluster.Handle) {
	handle := handle()
	return NewRunner(plan, handle, AlwaysConfirmed{}), handle
}

func TestValidateAcceptsAWellFormedPlan(t *testing.T) {
	err := Validate(plan(
		step(1, "preflight", PhasePreflight, true),
		step(2, "deploy", PhaseDeploy, true),
		step(3, "cleanup", PhaseCleanup, false),
	))
	if err != nil {
		t.Fatalf("a well formed plan was rejected: %v", err)
	}
}

func TestValidateRejectsMalformedPlans(t *testing.T) {
	cases := map[string]Plan{
		"no steps": plan(),
		"not numbered from one": plan(
			step(2, "preflight", PhasePreflight, true),
			step(3, "cleanup", PhaseCleanup, false),
		),
		"duplicate step names": plan(
			step(1, "same", PhasePreflight, true),
			step(2, "same", PhaseCleanup, false),
		),
		"phases out of order": plan(
			step(1, "cutover", PhaseCutover, true),
			step(2, "deploy", PhaseDeploy, true),
			step(3, "cleanup", PhaseCleanup, false),
		),
		"does not end in cleanup": plan(
			step(1, "preflight", PhasePreflight, true),
			step(2, "cutover", PhaseCutover, true),
		),
		"unnamed step": plan(
			step(1, "", PhasePreflight, true),
			step(2, "cleanup", PhaseCleanup, false),
		),
	}
	for name, malformed := range cases {
		t.Run(name, func(t *testing.T) {
			if err := Validate(malformed); err == nil {
				t.Fatalf("a malformed plan was accepted")
			}
		})
	}
}

func TestExecuteRecordsProgress(t *testing.T) {
	first := step(1, "first", PhasePreflight, true)
	testPlan := plan(first, step(2, "cleanup", PhaseCleanup, false))
	runner, handle := runner(testPlan)
	if err := runner.Execute(context.Background(), 1); err != nil {
		t.Fatalf("executing the first step failed: %v", err)
	}
	if first.executed != 1 {
		t.Fatalf("the step was executed %d times instead of once", first.executed)
	}
	record, err := handle.State.Load(context.Background(), "4.3-to-4.4")
	if err != nil {
		t.Fatalf("the recorded state cannot be read: %v", err)
	}
	if record == nil || record.LastExecutedStep != 1 {
		t.Fatalf("the last executed step was not recorded as 1: %+v", record)
	}
	if recorded := record.Step(1); recorded == nil || recorded.Status != state.StatusDone {
		t.Fatalf("the step was not recorded as done: %+v", recorded)
	}
}

func TestExecuteIsIdempotent(t *testing.T) {
	first := step(1, "first", PhasePreflight, true)
	runner, _ := runner(plan(first, step(2, "cleanup", PhaseCleanup, false)))
	if err := runner.Execute(context.Background(), 1); err != nil {
		t.Fatalf("executing the first step failed: %v", err)
	}
	if err := runner.Execute(context.Background(), 1); err != nil {
		t.Fatalf("re-executing a finished step failed: %v", err)
	}
	if first.executed != 1 {
		t.Fatalf("a finished step was executed again, %d times in total", first.executed)
	}
}

func TestExecuteRefusesToSkipPredecessors(t *testing.T) {
	second := step(2, "second", PhaseDeploy, true)
	runner, _ := runner(plan(step(1, "first", PhasePreflight, true), second, step(3, "cleanup", PhaseCleanup, false)))
	err := runner.Execute(context.Background(), 2)
	if err == nil {
		t.Fatalf("a step was executed before its predecessor")
	}
	if !strings.Contains(err.Error(), "run it before step 2") {
		t.Fatalf("the error does not name the missing predecessor: %v", err)
	}
	if second.executed != 0 {
		t.Fatalf("the step ran despite the refusal")
	}
}

func TestExecuteSkipsPredecessorsWhenForced(t *testing.T) {
	second := step(2, "second", PhaseDeploy, true)
	runner, handle := runner(plan(step(1, "first", PhasePreflight, true), second, step(3, "cleanup", PhaseCleanup, false)))
	handle.Options.Force = true
	if err := runner.Execute(context.Background(), 2); err != nil {
		t.Fatalf("a forced execution was refused: %v", err)
	}
	if second.executed != 1 {
		t.Fatalf("the forced step did not run")
	}
}

func TestExecuteRecordsAFailure(t *testing.T) {
	first := step(1, "first", PhasePreflight, true)
	first.executeErr = errors.New("the API server said no")
	runner, handle := runner(plan(first, step(2, "cleanup", PhaseCleanup, false)))
	if err := runner.Execute(context.Background(), 1); err == nil {
		t.Fatalf("a failing step reported success")
	}
	record, err := handle.State.Load(context.Background(), "4.3-to-4.4")
	if err != nil {
		t.Fatalf("the recorded state cannot be read: %v", err)
	}
	recorded := record.Step(1)
	if recorded == nil || recorded.Status != state.StatusFailed {
		t.Fatalf("the failure was not recorded: %+v", recorded)
	}
	if !strings.Contains(recorded.Error, "the API server said no") {
		t.Fatalf("the cause of the failure was not recorded: %+v", recorded)
	}
	if record.LastExecutedStep != 0 {
		t.Fatalf("a failed step was recorded as the last executed one")
	}
}

func TestExecuteFailsWhenTheStepDoesNotConverge(t *testing.T) {
	first := step(1, "first", PhasePreflight, true)
	first.noConverge = true
	runner, handle := runner(plan(first, step(2, "cleanup", PhaseCleanup, false)))
	err := runner.Execute(context.Background(), 1)
	if err == nil {
		t.Fatalf("a step which left the cluster unchanged reported success")
	}
	if !strings.Contains(err.Error(), "still reads as") {
		t.Fatalf("the error does not explain that the cluster did not change: %v", err)
	}
	record, loadErr := handle.State.Load(context.Background(), "4.3-to-4.4")
	if loadErr != nil {
		t.Fatalf("the recorded state cannot be read: %v", loadErr)
	}
	if recorded := record.Step(1); recorded == nil || recorded.Status != state.StatusFailed {
		t.Fatalf("the non-convergence was not recorded as a failure: %+v", recorded)
	}
}

func TestExecuteDemandsThePlanIdForAnIrreversibleStep(t *testing.T) {
	cleanup := step(2, "cleanup", PhaseCleanup, false)
	testPlan := plan(step(1, "first", PhasePreflight, true), cleanup)
	handle := handle()
	runner := NewRunner(testPlan, handle, &TokenConfirmer{Supplied: "cleanup"})
	testPlan.steps[0].(*fakeStep).state = StateDone
	err := runner.Execute(context.Background(), 2)
	if !errors.Is(err, ErrNotConfirmed) {
		t.Fatalf("the step name was accepted as the token of an irreversible step: %v", err)
	}
	runner = NewRunner(testPlan, handle, &TokenConfirmer{Supplied: "4.3-to-4.4"})
	if err := runner.Execute(context.Background(), 2); err != nil {
		t.Fatalf("the plan ID was rejected as the token of an irreversible step: %v", err)
	}
}

func TestRollbackUndoesTheLastExecutedStep(t *testing.T) {
	first := step(1, "first", PhasePreflight, true)
	second := step(2, "second", PhaseDeploy, true)
	runner, handle := runner(plan(first, second, step(3, "cleanup", PhaseCleanup, false)))
	if err := runner.Execute(context.Background(), 1); err != nil {
		t.Fatalf("executing the first step failed: %v", err)
	}
	if err := runner.Execute(context.Background(), 2); err != nil {
		t.Fatalf("executing the second step failed: %v", err)
	}
	if err := runner.Rollback(context.Background(), 2); err != nil {
		t.Fatalf("rolling back the second step failed: %v", err)
	}
	if second.rolledBack != 1 {
		t.Fatalf("the second step was rolled back %d times instead of once", second.rolledBack)
	}
	record, err := handle.State.Load(context.Background(), "4.3-to-4.4")
	if err != nil {
		t.Fatalf("the recorded state cannot be read: %v", err)
	}
	if record.LastExecutedStep != 1 {
		t.Fatalf("the last executed step did not move back to 1: %+v", record)
	}
	if recorded := record.Step(2); recorded == nil || recorded.Status != state.StatusRolledBack {
		t.Fatalf("the rollback was not recorded: %+v", recorded)
	}
}

func TestRollbackIsStrictlyLastExecuted(t *testing.T) {
	runner, _ := runner(plan(
		step(1, "first", PhasePreflight, true),
		step(2, "second", PhaseDeploy, true),
		step(3, "cleanup", PhaseCleanup, false),
	))
	if err := runner.Execute(context.Background(), 1); err != nil {
		t.Fatalf("executing the first step failed: %v", err)
	}
	if err := runner.Execute(context.Background(), 2); err != nil {
		t.Fatalf("executing the second step failed: %v", err)
	}
	err := runner.Rollback(context.Background(), 1)
	if err == nil {
		t.Fatalf("a step below the last executed one was rolled back")
	}
	if !strings.Contains(err.Error(), "step 2") {
		t.Fatalf("the error does not name the only step which may be rolled back: %v", err)
	}
}

func TestRollbackRefusesAnIrreversibleStep(t *testing.T) {
	runner, _ := runner(plan(step(1, "first", PhasePreflight, true), step(2, "cleanup", PhaseCleanup, false)))
	if err := runner.Execute(context.Background(), 1); err != nil {
		t.Fatalf("executing the first step failed: %v", err)
	}
	if err := runner.Rollback(context.Background(), 2); err == nil {
		t.Fatalf("an irreversible step was rolled back")
	}
}

func TestDryRunChangesNothing(t *testing.T) {
	first := step(1, "first", PhasePreflight, true)
	runner, handle := runner(plan(first, step(2, "cleanup", PhaseCleanup, false)))
	handle.Options.DryRun = true
	if err := runner.Execute(context.Background(), 1); err != nil {
		t.Fatalf("a dry run failed: %v", err)
	}
	if first.executed != 0 {
		t.Fatalf("a dry run executed the step")
	}
	record, err := handle.State.Load(context.Background(), "4.3-to-4.4")
	if err != nil {
		t.Fatalf("the recorded state cannot be read: %v", err)
	}
	if record != nil {
		t.Fatalf("a dry run recorded state: %+v", record)
	}
}

func TestReportShowsEveryStep(t *testing.T) {
	first := step(1, "first", PhasePreflight, true)
	runner, _ := runner(plan(first, step(2, "cleanup", PhaseCleanup, false)))
	if err := runner.Execute(context.Background(), 1); err != nil {
		t.Fatalf("executing the first step failed: %v", err)
	}
	report, err := runner.Report(context.Background())
	if err != nil {
		t.Fatalf("the report cannot be built: %v", err)
	}
	if len(report.Rows) != 2 {
		t.Fatalf("the report has %d rows instead of 2", len(report.Rows))
	}
	if report.Rows[0].Detected != StateDone || report.Rows[1].Detected != StateNotStarted {
		t.Fatalf("the report does not reflect the cluster: %+v", report.Rows)
	}
	rendered := &bytes.Buffer{}
	report.Render(rendered)
	if !strings.Contains(rendered.String(), "first") || !strings.Contains(rendered.String(), "cleanup") {
		t.Fatalf("the rendered report does not name every step:\n%s", rendered.String())
	}
}

func TestRegistryRejectsDuplicates(t *testing.T) {
	registry := NewRegistry()
	testPlan := plan(step(1, "first", PhasePreflight, true), step(2, "cleanup", PhaseCleanup, false))
	if err := registry.Register(testPlan); err != nil {
		t.Fatalf("a valid plan was rejected: %v", err)
	}
	if err := registry.Register(testPlan); err == nil {
		t.Fatalf("the same plan was registered twice")
	}
	found, err := registry.Find("4.3", "4.4")
	if err != nil {
		t.Fatalf("a registered plan cannot be found by release: %v", err)
	}
	if found.Meta().ID != "4.3-to-4.4" {
		t.Fatalf("the wrong plan was found: %s", found.Meta().ID)
	}
}
