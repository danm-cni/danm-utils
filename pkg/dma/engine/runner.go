package engine

import (
	"context"
	"fmt"

	"github.com/danm-cni/danm-utils/pkg/dma/cluster"
	"github.com/danm-cni/danm-utils/pkg/dma/state"
)

// Runner executes and rolls back the steps of a single plan against a single cluster.
type Runner struct {
	Plan    Plan
	Handle  *cluster.Handle
	Confirm Confirmer
}

// NewRunner returns a runner for the given plan.
func NewRunner(plan Plan, handle *cluster.Handle, confirmer Confirmer) *Runner {
	return &Runner{Plan: plan, Handle: handle, Confirm: confirmer}
}

// Execute runs one step of the plan, after confirming it with the operator.
//
// A step whose detector already reports it as done is a no-op, so repeating a command is
// harmless. A step whose predecessors are not all done is refused, unless --force was given.
func (runner *Runner) Execute(ctx context.Context, stepID int) error {
	step, err := runner.Step(stepID)
	if err != nil {
		return err
	}
	if err := runner.prepare(ctx); err != nil {
		return err
	}
	meta := step.Meta()
	detected, err := runner.detect(ctx, step)
	if err != nil {
		return err
	}
	if detected == StateDone {
		runner.Handle.Printf("Step %d (%s) is already done, nothing to execute.", meta.ID, meta.Name)
		return nil
	}
	if err := runner.verifyPredecessors(ctx, stepID); err != nil {
		return err
	}
	if runner.Handle.Options.DryRun {
		runner.Handle.Printf("Dry run: step %d (%s) of plan %s would be executed now.", meta.ID, meta.Name, runner.Plan.Meta().ID)
		runner.Handle.Printf("  %s", meta.Description)
		return nil
	}
	if err := runner.Confirm.Confirm(runner.executePrompt(step, detected), runner.executeToken(step)); err != nil {
		return err
	}
	if err := runner.record(ctx, step, func(record *state.StepRecord) {
		record.Status = state.StatusRunning
		record.StartedAt = state.Now()
		record.FinishedAt = ""
		record.Error = ""
		record.Operator = runner.Handle.Operator
	}); err != nil {
		return err
	}
	stepCtx, cancel := runner.stepContext(ctx)
	defer cancel()
	execErr := step.Execute(stepCtx, runner.Handle)
	if execErr != nil {
		runner.fail(ctx, step, execErr)
		return fmt.Errorf("step %d (%s) failed: %w", meta.ID, meta.Name, execErr)
	}
	after, err := step.Detect(ctx, runner.Handle)
	if err != nil {
		runner.fail(ctx, step, err)
		return fmt.Errorf("step %d (%s) ran but its state could not be verified: %w", meta.ID, meta.Name, err)
	}
	if after != StateDone {
		verifyErr := fmt.Errorf("the step reported success but the cluster still reads as %s", after)
		runner.fail(ctx, step, verifyErr)
		return fmt.Errorf("step %d (%s) failed: %w", meta.ID, meta.Name, verifyErr)
	}
	if err := runner.record(ctx, step, func(record *state.StepRecord) {
		record.Status = state.StatusDone
		record.FinishedAt = state.Now()
		record.Error = ""
	}); err != nil {
		return err
	}
	if _, err := runner.Handle.State.Mutate(ctx, runner.Plan.Meta().ID, func(record *state.PlanRecord) error {
		record.LastExecutedStep = meta.ID
		return nil
	}); err != nil {
		return err
	}
	runner.Handle.Printf("Step %d (%s) is done.", meta.ID, meta.Name)
	return nil
}

// Rollback reverts one step of the plan.
//
// Only the most recently executed step may be rolled back, and only when it is reversible.
// Unwinding further means repeating the command for the step below.
func (runner *Runner) Rollback(ctx context.Context, stepID int) error {
	step, err := runner.Step(stepID)
	if err != nil {
		return err
	}
	if err := runner.prepare(ctx); err != nil {
		return err
	}
	meta := step.Meta()
	if !meta.Reversible {
		return fmt.Errorf("step %d (%s) is irreversible and cannot be rolled back", meta.ID, meta.Name)
	}
	planRecord, err := runner.Handle.State.Load(ctx, runner.Plan.Meta().ID)
	if err != nil {
		return err
	}
	if planRecord == nil {
		return fmt.Errorf("plan %s has no recorded progress, there is nothing to roll back", runner.Plan.Meta().ID)
	}
	if planRecord.LastExecutedStep != meta.ID && !runner.Handle.Options.Force {
		return fmt.Errorf("only the most recently executed step can be rolled back, which is step %d, not step %d", planRecord.LastExecutedStep, meta.ID)
	}
	detected, err := runner.detect(ctx, step)
	if err != nil {
		return err
	}
	if detected == StateNotStarted {
		runner.Handle.Printf("Step %d (%s) has left no trace on the cluster, nothing to roll back.", meta.ID, meta.Name)
		return nil
	}
	if runner.Handle.Options.DryRun {
		runner.Handle.Printf("Dry run: step %d (%s) of plan %s would be rolled back now.", meta.ID, meta.Name, runner.Plan.Meta().ID)
		return nil
	}
	if err := runner.Confirm.Confirm(runner.rollbackPrompt(step, detected), meta.Name); err != nil {
		return err
	}
	stepCtx, cancel := runner.stepContext(ctx)
	defer cancel()
	if err := step.Rollback(stepCtx, runner.Handle); err != nil {
		runner.fail(ctx, step, err)
		return fmt.Errorf("rolling back step %d (%s) failed: %w", meta.ID, meta.Name, err)
	}
	if err := runner.record(ctx, step, func(record *state.StepRecord) {
		record.Status = state.StatusRolledBack
		record.FinishedAt = state.Now()
		record.Error = ""
		record.Operator = runner.Handle.Operator
	}); err != nil {
		return err
	}
	if _, err := runner.Handle.State.Mutate(ctx, runner.Plan.Meta().ID, func(record *state.PlanRecord) error {
		record.LastExecutedStep = record.HighestDoneStepBelow(meta.ID)
		return nil
	}); err != nil {
		return err
	}
	runner.Handle.Printf("Step %d (%s) is rolled back.", meta.ID, meta.Name)
	return nil
}

// Step returns the step with the given ID.
func (runner *Runner) Step(stepID int) (Step, error) {
	steps := runner.Plan.Steps()
	if stepID < 1 || stepID > len(steps) {
		return nil, fmt.Errorf("plan %s has steps 1 to %d, there is no step %d", runner.Plan.Meta().ID, len(steps), stepID)
	}
	return steps[stepID-1], nil
}

func (runner *Runner) prepare(ctx context.Context) error {
	meta := runner.Plan.Meta()
	if _, err := runner.Handle.Images.ResolveAll(meta.Components); err != nil {
		return err
	}
	planRecord, err := runner.Handle.State.Load(ctx, meta.ID)
	if err != nil {
		return err
	}
	if planRecord == nil {
		return nil
	}
	mode, err := cluster.ParseMode(planRecord.Mode)
	if err != nil {
		return fmt.Errorf("the recorded state of plan %s is invalid: %w", meta.ID, err)
	}
	if mode != cluster.ModeUnknown {
		runner.Handle.Mode = mode
	}
	return nil
}

func (runner *Runner) verifyPredecessors(ctx context.Context, stepID int) error {
	for _, step := range runner.Plan.Steps() {
		meta := step.Meta()
		if meta.ID >= stepID {
			break
		}
		detected, err := step.Detect(ctx, runner.Handle)
		if err != nil {
			return fmt.Errorf("cannot determine the state of step %d (%s): %w", meta.ID, meta.Name, err)
		}
		if detected == StateDone {
			continue
		}
		if runner.Handle.Options.Force {
			runner.Handle.Printf("WARNING: step %d (%s) reads as %s, continuing because --force was given.", meta.ID, meta.Name, detected)
			continue
		}
		return fmt.Errorf("step %d (%s) reads as %s, run it before step %d", meta.ID, meta.Name, detected, stepID)
	}
	return nil
}

func (runner *Runner) detect(ctx context.Context, step Step) (StepState, error) {
	meta := step.Meta()
	detected, err := step.Detect(ctx, runner.Handle)
	if err != nil {
		return StateNotStarted, fmt.Errorf("cannot determine the state of step %d (%s): %w", meta.ID, meta.Name, err)
	}
	return detected, nil
}

func (runner *Runner) record(ctx context.Context, step Step, mutator func(*state.StepRecord)) error {
	meta := step.Meta()
	planMeta := runner.Plan.Meta()
	_, err := runner.Handle.State.Mutate(ctx, planMeta.ID, func(planRecord *state.PlanRecord) error {
		planRecord.From = planMeta.From
		planRecord.To = planMeta.To
		planRecord.BinaryPayload = planMeta.BinaryPayload
		if runner.Handle.Mode != cluster.ModeUnknown {
			planRecord.Mode = string(runner.Handle.Mode)
		}
		stepRecord := planRecord.Step(meta.ID)
		if stepRecord == nil {
			stepRecord = &state.StepRecord{ID: meta.ID, Name: meta.Name, Status: state.StatusNotStarted}
		}
		stepRecord.Name = meta.Name
		mutator(stepRecord)
		planRecord.UpsertStep(*stepRecord)
		return nil
	})
	return err
}

func (runner *Runner) fail(ctx context.Context, step Step, cause error) {
	err := runner.record(ctx, step, func(record *state.StepRecord) {
		record.Status = state.StatusFailed
		record.FinishedAt = state.Now()
		record.Error = cause.Error()
	})
	if err != nil {
		runner.Handle.Printf("WARNING: the failure of step %d could not be recorded: %s", step.Meta().ID, err.Error())
	}
}

func (runner *Runner) stepContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if runner.Handle.Options.StepTimeout <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, runner.Handle.Options.StepTimeout)
}

func (runner *Runner) executeToken(step Step) string {
	meta := step.Meta()
	if !meta.Reversible {
		return runner.Plan.Meta().ID
	}
	return meta.Name
}

func (runner *Runner) executePrompt(step Step, detected StepState) string {
	meta := step.Meta()
	prompt := fmt.Sprintf("\nPlan %s, step %d of %d, phase %s\n  %s\n  %s\n  Current state: %s\n",
		runner.Plan.Meta().ID, meta.ID, len(runner.Plan.Steps()), meta.Phase, meta.Name, meta.Description, detected)
	if !meta.Reversible {
		return prompt + "\n  THIS STEP IS IRREVERSIBLE. It cannot be rolled back by this tool.\n"
	}
	return prompt
}

func (runner *Runner) rollbackPrompt(step Step, detected StepState) string {
	meta := step.Meta()
	return fmt.Sprintf("\nRolling back plan %s, step %d, phase %s\n  %s\n  %s\n  Current state: %s\n",
		runner.Plan.Meta().ID, meta.ID, meta.Phase, meta.Name, meta.Description, detected)
}
