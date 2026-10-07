// Command dma is the DANM Migration Assistant: it orchestrates the in-place migration of a DANM
// installation from one major release to another, one reviewable step at a time.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/danm-cni/danm-utils/pkg/dma/cluster"
	"github.com/danm-cni/danm-utils/pkg/dma/engine"
	"github.com/spf13/cobra"
)

var (
	version, commitHash string
)

type globalFlags struct {
	kubeconfig       string
	namespace        string
	stateName        string
	configName       string
	payloadRoot      string
	imageOverrides   []string
	mode             string
	dryRun           bool
	force            bool
	acceptDefaultMtu bool
	noCanary         bool
	binaryImage      string
	stepTimeout      time.Duration
	rolloutTimeout   time.Duration
}

func main() {
	if err := newRootCommand().Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "ERROR: "+err.Error())
		if errors.Is(err, engine.ErrNotConfirmed) {
			os.Exit(2)
		}
		os.Exit(1)
	}
}

func newRootCommand() *cobra.Command {
	flags := &globalFlags{}
	root := &cobra.Command{
		Use:   "dma",
		Short: "DANM Migration Assistant",
		Long: "The DANM Migration Assistant migrates a DANM installation in place from one major\n" +
			"release to another. A migration is a plan of pre-baked steps which are reviewed first,\n" +
			"then executed or rolled back one at a time, each behind a typed confirmation.",
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	root.PersistentFlags().StringVar(&flags.kubeconfig, "kubeconf", "", "absolute path to a kubeconfig file, only needed when the assistant runs outside of the cluster")
	root.PersistentFlags().StringVar(&flags.namespace, "namespace", "kube-system", "namespace holding the assistant's own state and configuration")
	root.PersistentFlags().StringVar(&flags.stateName, "state-configmap", "dma-state", "name of the ConfigMap the assistant records plan progress in")
	root.PersistentFlags().StringVar(&flags.configName, "config-configmap", "dma-config", "name of the ConfigMap the assistant reads its own configuration from")
	root.PersistentFlags().StringVar(&flags.payloadRoot, "payload-root", "/payload", "directory inside the assistant's image holding the CNI binaries of each target release")
	root.PersistentFlags().StringArrayVar(&flags.imageOverrides, "image", nil, "full image URL override for one component, as component=image, repeatable")
	root.PersistentFlags().StringVar(&flags.mode, "mode", "", "override the detected DANM deployment mode, either lightweight or production")
	root.PersistentFlags().BoolVar(&flags.dryRun, "dry-run", false, "describe what would happen without changing anything")
	root.PersistentFlags().BoolVar(&flags.force, "force", false, "relax the step ordering and last-executed checks, never a confirmation")
	root.PersistentFlags().BoolVar(&flags.acceptDefaultMtu, "accept-default-mtu", false, "accept that untagged networks are migrated with the default MTU of 1500")
	root.PersistentFlags().BoolVar(&flags.noCanary, "no-canary", false, "skip the single node trial run before a fleet wide CNI binary rollout")
	root.PersistentFlags().StringVar(&flags.binaryImage, "binary-image", "", "image to copy the CNI binaries from, instead of the assistant's own image")
	root.PersistentFlags().DurationVar(&flags.stepTimeout, "step-timeout", 30*time.Minute, "how long a single step may run")
	root.PersistentFlags().DurationVar(&flags.rolloutTimeout, "rollout-timeout", 10*time.Minute, "how long a step waits for a patched workload before reverting it")
	root.AddCommand(
		newPlansCommand(),
		newPlanCommand(flags),
		newStatusCommand(flags),
		newExecuteCommand(flags),
		newRollbackCommand(flags),
		newVersionCommand(),
	)
	return root
}

func newPlansCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "plans",
		Short: "list the migration plans this build knows about",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			plans := engine.Default.All()
			if len(plans) == 0 {
				fmt.Fprintln(command.OutOrStdout(), "This build contains no migration plans.")
				return nil
			}
			for _, plan := range plans {
				meta := plan.Meta()
				fmt.Fprintf(command.OutOrStdout(), "%s\tDANM %s to DANM %s\t%d steps\n", meta.ID, meta.From, meta.To, len(plan.Steps()))
				if meta.Description != "" {
					fmt.Fprintf(command.OutOrStdout(), "  %s\n", meta.Description)
				}
			}
			return nil
		},
	}
}

func newPlanCommand(flags *globalFlags) *cobra.Command {
	var planID, from, to string
	command := &cobra.Command{
		Use:   "plan",
		Short: "list the steps of a migration plan and their state in this cluster",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			plan, err := resolvePlan(planID, from, to)
			if err != nil {
				return err
			}
			runner, err := newRunner(command.Context(), plan, flags, "")
			if err != nil {
				return err
			}
			report, err := runner.Report(command.Context())
			if err != nil {
				return err
			}
			report.RenderDetailed(command.OutOrStdout())
			return nil
		},
	}
	addPlanSelectors(command, &planID, &from, &to)
	return command
}

func newStatusCommand(flags *globalFlags) *cobra.Command {
	var planID, from, to string
	command := &cobra.Command{
		Use:   "status",
		Short: "show the progress of a migration plan",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			plan, err := resolvePlan(planID, from, to)
			if err != nil {
				return err
			}
			runner, err := newRunner(command.Context(), plan, flags, "")
			if err != nil {
				return err
			}
			report, err := runner.Report(command.Context())
			if err != nil {
				return err
			}
			report.Render(command.OutOrStdout())
			return nil
		},
	}
	addPlanSelectors(command, &planID, &from, &to)
	return command
}

func newExecuteCommand(flags *globalFlags) *cobra.Command {
	var planID, from, to, confirm string
	var step int
	command := &cobra.Command{
		Use:   "execute",
		Short: "execute one step of a migration plan",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			plan, err := resolvePlan(planID, from, to)
			if err != nil {
				return err
			}
			runner, err := newRunner(command.Context(), plan, flags, confirm)
			if err != nil {
				return err
			}
			return runner.Execute(command.Context(), step)
		},
	}
	addPlanSelectors(command, &planID, &from, &to)
	command.Flags().IntVar(&step, "step", 0, "number of the step to execute")
	command.Flags().StringVar(&confirm, "confirm", "", "confirmation token, for running without a terminal")
	command.MarkFlagRequired("step")
	return command
}

func newRollbackCommand(flags *globalFlags) *cobra.Command {
	var planID, from, to, confirm string
	var step int
	command := &cobra.Command{
		Use:   "rollback",
		Short: "roll back the most recently executed step of a migration plan",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			plan, err := resolvePlan(planID, from, to)
			if err != nil {
				return err
			}
			runner, err := newRunner(command.Context(), plan, flags, confirm)
			if err != nil {
				return err
			}
			return runner.Rollback(command.Context(), step)
		},
	}
	addPlanSelectors(command, &planID, &from, &to)
	command.Flags().IntVar(&step, "step", 0, "number of the step to roll back")
	command.Flags().StringVar(&confirm, "confirm", "", "confirmation token, for running without a terminal")
	command.MarkFlagRequired("step")
	return command
}

func newVersionCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "print the build information of this binary",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			fmt.Fprintln(command.OutOrStdout(), "DANM Migration Assistant release: "+orUnknown(version))
			fmt.Fprintln(command.OutOrStdout(), "DANM Migration Assistant commit: "+orUnknown(commitHash))
			return nil
		},
	}
}

func addPlanSelectors(command *cobra.Command, planID, from, to *string) {
	command.Flags().StringVar(planID, "plan", "", "ID of the plan, as listed by \"dma plans\"")
	command.Flags().StringVar(from, "from", "", "DANM release to migrate from, an alternative to --plan")
	command.Flags().StringVar(to, "to", "", "DANM release to migrate to, an alternative to --plan")
}

func resolvePlan(planID, from, to string) (engine.Plan, error) {
	if planID != "" {
		return engine.Default.Get(planID)
	}
	if from != "" && to != "" {
		return engine.Default.Find(from, to)
	}
	return nil, errors.New("name the plan either with --plan, or with both --from and --to")
}

func newRunner(ctx context.Context, plan engine.Plan, flags *globalFlags, confirm string) (*engine.Runner, error) {
	overrides := map[string]cluster.ComponentImage{}
	for _, raw := range flags.imageOverrides {
		component, image, err := cluster.ParseOverride(raw)
		if err != nil {
			return nil, err
		}
		overrides[component] = image
	}
	mode, err := cluster.ParseMode(flags.mode)
	if err != nil {
		return nil, err
	}
	handle, err := cluster.New(ctx, cluster.Config{
		Kubeconfig:  flags.kubeconfig,
		Namespace:   flags.namespace,
		StateName:   flags.stateName,
		ConfigName:  flags.configName,
		PayloadRoot: flags.payloadRoot,
		Overrides:   overrides,
		Options: cluster.Options{
			DryRun:           flags.dryRun,
			Force:            flags.force,
			AcceptDefaultMtu: flags.acceptDefaultMtu,
			NoCanary:         flags.noCanary,
			BinaryImage:      flags.binaryImage,
			StepTimeout:      flags.stepTimeout,
			RolloutTimeout:   flags.rolloutTimeout,
		},
		Out: os.Stdout,
	})
	if err != nil {
		return nil, err
	}
	handle.Mode = mode
	return engine.NewRunner(plan, handle, confirmer(confirm)), nil
}

func confirmer(supplied string) engine.Confirmer {
	if supplied != "" {
		return &engine.TokenConfirmer{Supplied: supplied, Out: os.Stdout}
	}
	return engine.NewPromptConfirmer(os.Stdout)
}

func orUnknown(value string) string {
	if value == "" {
		return "unknown"
	}
	return value
}
