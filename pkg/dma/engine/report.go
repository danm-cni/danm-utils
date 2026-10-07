package engine

import (
	"context"
	"fmt"
	"io"
	"sort"
	"text/tabwriter"

	"github.com/danm-cni/danm-utils/pkg/dma/cluster"
	"github.com/danm-cni/danm-utils/pkg/dma/state"
)

// StepRow is one line of a plan or status report: what the step is, what the cluster says about
// it, and what the assistant recorded the last time it was touched.
type StepRow struct {
	Meta     StepMeta
	Detected StepState
	Record   *state.StepRecord
}

// Report is the reviewable view of a plan against a live cluster.
type Report struct {
	Plan   PlanMeta
	Mode   cluster.Mode
	Images map[string]cluster.ComponentImage
	Record *state.PlanRecord
	Rows   []StepRow
}

// Report inspects the cluster and returns the current state of every step of the plan.
func (runner *Runner) Report(ctx context.Context) (*Report, error) {
	if err := runner.prepare(ctx); err != nil {
		return nil, err
	}
	planMeta := runner.Plan.Meta()
	planRecord, err := runner.Handle.State.Load(ctx, planMeta.ID)
	if err != nil {
		return nil, err
	}
	images, err := runner.Handle.Images.ResolveAll(planMeta.Components)
	if err != nil {
		return nil, err
	}
	report := &Report{Plan: planMeta, Mode: runner.Handle.Mode, Images: images, Record: planRecord}
	for _, step := range runner.Plan.Steps() {
		meta := step.Meta()
		detected, err := step.Detect(ctx, runner.Handle)
		if err != nil {
			return nil, fmt.Errorf("cannot determine the state of step %d (%s): %w", meta.ID, meta.Name, err)
		}
		row := StepRow{Meta: meta, Detected: detected}
		if planRecord != nil {
			row.Record = planRecord.Step(meta.ID)
		}
		report.Rows = append(report.Rows, row)
	}
	return report, nil
}

// Render writes the report as a table.
func (report *Report) Render(out io.Writer) {
	fmt.Fprintf(out, "Plan %s: DANM %s to DANM %s\n", report.Plan.ID, report.Plan.From, report.Plan.To)
	if report.Plan.Description != "" {
		fmt.Fprintf(out, "%s\n", report.Plan.Description)
	}
	fmt.Fprintf(out, "Deployment mode: %s\n", modeText(report.Mode))
	if report.Plan.BinaryPayload != "" {
		fmt.Fprintf(out, "CNI binary payload: %s\n", report.Plan.BinaryPayload)
	}
	report.renderImages(out)
	if report.Record != nil {
		fmt.Fprintf(out, "Last executed step: %s\n", lastExecutedText(report.Record))
	}
	fmt.Fprintln(out)
	writer := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(writer, "STEP\tPHASE\tNAME\tREVERSIBLE\tCLUSTER\tRECORDED\tWHEN")
	for _, row := range report.Rows {
		fmt.Fprintf(writer, "%d\t%s\t%s\t%s\t%s\t%s\t%s\n",
			row.Meta.ID, row.Meta.Phase, row.Meta.Name, reversibleText(row.Meta.Reversible),
			row.Detected, recordedText(row.Record), whenText(row.Record))
	}
	writer.Flush()
	report.renderErrors(out)
}

// RenderDetailed writes the report as a table followed by the description of every step.
func (report *Report) RenderDetailed(out io.Writer) {
	report.Render(out)
	fmt.Fprintln(out)
	for _, row := range report.Rows {
		fmt.Fprintf(out, "%d. %s [%s]\n   %s\n", row.Meta.ID, row.Meta.Name, row.Meta.Phase, row.Meta.Description)
	}
}

func (report *Report) renderImages(out io.Writer) {
	if len(report.Images) == 0 {
		return
	}
	components := make([]string, 0, len(report.Images))
	for component := range report.Images {
		components = append(components, component)
	}
	sort.Strings(components)
	fmt.Fprintln(out, "Resolved images:")
	for _, component := range components {
		image := report.Images[component]
		fmt.Fprintf(out, "  %s: %s (%s)\n", component, image.Image, image.PullPolicy)
	}
}

func (report *Report) renderErrors(out io.Writer) {
	for _, row := range report.Rows {
		if row.Record == nil || row.Record.Error == "" {
			continue
		}
		fmt.Fprintf(out, "\nStep %d (%s) last failed with:\n  %s\n", row.Meta.ID, row.Meta.Name, row.Record.Error)
	}
}

func modeText(mode cluster.Mode) string {
	if mode == cluster.ModeUnknown {
		return "not detected yet"
	}
	return string(mode)
}

func reversibleText(reversible bool) string {
	if reversible {
		return "yes"
	}
	return "NO"
}

func recordedText(record *state.StepRecord) string {
	if record == nil {
		return string(state.StatusNotStarted)
	}
	return string(record.Status)
}

func whenText(record *state.StepRecord) string {
	if record == nil {
		return "-"
	}
	if record.FinishedAt != "" {
		return record.FinishedAt
	}
	if record.StartedAt != "" {
		return record.StartedAt
	}
	return "-"
}

func lastExecutedText(record *state.PlanRecord) string {
	if record.LastExecutedStep == 0 {
		return "none"
	}
	step := record.Step(record.LastExecutedStep)
	if step == nil {
		return fmt.Sprintf("%d", record.LastExecutedStep)
	}
	return fmt.Sprintf("%d (%s)", step.ID, step.Name)
}
