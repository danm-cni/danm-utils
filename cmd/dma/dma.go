// Command dma is the DANM Migration Assistant: it orchestrates the in-place migration of a DANM
// installation from one major release to another, one reviewable step at a time.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/danm-cni/danm-utils/pkg/dma/cluster"
	"github.com/danm-cni/danm-utils/pkg/dma/engine"
	"github.com/danm-cni/danm-utils/pkg/dma/plans/v43to44"
	"github.com/spf13/cobra"
	rbacv1 "k8s.io/api/rbac/v1"
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
	workloads        []string
	cniSubject       string
	mode             string
	dryRun           bool
	force            bool
	acceptDefaultMtu bool
	acceptPruned     bool
	noCanary         bool
	binaryImage      string
	shimImage        string
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
	root.PersistentFlags().StringArrayVar(&flags.workloads, "workload", nil, "workload of one component, as component=<Kind>/<namespace>/<name> where Kind is DaemonSet or Deployment, repeatable")
	root.PersistentFlags().StringVar(&flags.cniSubject, "cni-subject", "", "identity the DANM CNI binary authenticates as, as ServiceAccount/<namespace>/<name> or User/<name>")
	root.PersistentFlags().StringVar(&flags.mode, "mode", "", "override the detected DANM deployment mode, either lightweight or production")
	root.PersistentFlags().BoolVar(&flags.dryRun, "dry-run", false, "describe what would happen without changing anything")
	root.PersistentFlags().BoolVar(&flags.force, "force", false, "run a reversible step before its predecessors are done, never relaxes a confirmation, a rollback or an irreversible step")
	root.PersistentFlags().BoolVar(&flags.acceptDefaultMtu, "accept-default-mtu", false, "accept that untagged networks are migrated with the default MTU of 1500")
	root.PersistentFlags().BoolVar(&flags.acceptPruned, "accept-pruned-fields", false, "accept that fields the target schema does not know are dropped from the migrated objects, recorded once by preflight")
	root.PersistentFlags().BoolVar(&flags.noCanary, "no-canary", false, "skip the single node trial run before a fleet wide CNI binary rollout")
	root.PersistentFlags().StringVar(&flags.binaryImage, "binary-image", "", "image to copy the CNI binaries from, instead of the assistant's own image")
	root.PersistentFlags().StringVar(&flags.shimImage, "shim-image", "", "image of the annotation shim, instead of the image of the assistant's own Pod")
	root.PersistentFlags().DurationVar(&flags.stepTimeout, "step-timeout", 30*time.Minute, "how long a single step may run")
	root.PersistentFlags().DurationVar(&flags.rolloutTimeout, "rollout-timeout", 10*time.Minute, "how long a step waits for a patched workload before reverting it")
	root.AddCommand(
		newPlansCommand(),
		newPlanCommand(flags),
		newStatusCommand(flags),
		newExecuteCommand(flags),
		newRollbackCommand(flags),
		newShimCommand(flags),
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

func newShimCommand(flags *globalFlags) *cobra.Command {
	shim := &cobra.Command{
		Use:   "shim",
		Short: "manage the annotation shim which outlives the 4.3 to 4.4 migration plan",
	}
	shim.AddCommand(newShimServeCommand(flags), newShimStatusCommand(flags), newShimRemoveCommand(flags))
	return shim
}

func newShimServeCommand(flags *globalFlags) *cobra.Command {
	var tlsDir, mode string
	var port int
	command := &cobra.Command{
		Use:    "serve",
		Short:  "serve the annotation shim webhook",
		Args:   cobra.NoArgs,
		Hidden: true,
		RunE: func(command *cobra.Command, _ []string) error {
			parsed, err := cluster.ParseMode(mode)
			if err != nil {
				return err
			}
			ctx, stop := signal.NotifyContext(command.Context(), os.Interrupt, syscall.SIGTERM)
			defer stop()
			return v43to44.ServeShim(ctx, v43to44.ShimServerConfig{Kubeconfig: flags.kubeconfig, Mode: parsed, TlsDir: tlsDir, Port: port})
		},
	}
	command.Flags().StringVar(&tlsDir, "tls-dir", v43to44.ShimTlsDir, "directory holding tls.crt and tls.key")
	command.Flags().StringVar(&mode, "mode", "", "DANM deployment mode, lightweight or production")
	command.Flags().IntVar(&port, "port", v43to44.ShimPort, "port to serve HTTPS on")
	return command
}

func newShimStatusCommand(flags *globalFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "list the Pod templates which still depend on the annotation shim",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			handle, err := newHandle(command.Context(), flags)
			if err != nil {
				return err
			}
			templates, err := v43to44.ShimStatus(command.Context(), handle)
			if err != nil {
				return err
			}
			out := command.OutOrStdout()
			if len(templates) == 0 {
				fmt.Fprintf(out, "No Pod template carries only %s, the annotation shim can be removed once the plan is complete.\n", v43to44.OldInterfacesKey)
				return nil
			}
			fmt.Fprintf(out, "%d Pod templates carry only %s. Add %s with the value shown to each:\n", len(templates), v43to44.OldInterfacesKey, v43to44.NewInterfacesKey)
			for _, template := range templates {
				fmt.Fprintf(out, "%s %s/%s\n  %s: %s\n", template.Kind, template.Namespace, template.Name, v43to44.NewInterfacesKey, template.Value)
			}
			return nil
		},
	}
}

func newShimRemoveCommand(flags *globalFlags) *cobra.Command {
	var confirm string
	command := &cobra.Command{
		Use:   "remove",
		Short: "remove the annotation shim after the migration plan is complete",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			handle, err := newHandle(command.Context(), flags)
			if err != nil {
				return err
			}
			return v43to44.RemoveShim(command.Context(), handle, confirmer(confirm))
		},
	}
	command.Flags().StringVar(&confirm, "confirm", "", "confirmation token, for running without a terminal")
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
	handle, err := newHandle(ctx, flags)
	if err != nil {
		return nil, err
	}
	return engine.NewRunner(plan, handle, confirmer(confirm)), nil
}

func newHandle(ctx context.Context, flags *globalFlags) (*cluster.Handle, error) {
	overrides := map[string]cluster.ComponentImage{}
	for _, raw := range flags.imageOverrides {
		component, image, err := cluster.ParseOverride(raw)
		if err != nil {
			return nil, err
		}
		overrides[component] = image
	}
	workloads := map[string]cluster.WorkloadRef{}
	for _, raw := range flags.workloads {
		component, ref, err := cluster.ParseWorkloadOverride(raw)
		if err != nil {
			return nil, err
		}
		workloads[component] = ref
	}
	var cniSubject *rbacv1.Subject
	if flags.cniSubject != "" {
		subject, err := cluster.ParseSubject(flags.cniSubject)
		if err != nil {
			return nil, err
		}
		cniSubject = &subject
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
		Workloads:   workloads,
		CniSubject:  cniSubject,
		Options: cluster.Options{
			DryRun:             flags.dryRun,
			Force:              flags.force,
			AcceptDefaultMtu:   flags.acceptDefaultMtu,
			AcceptPrunedFields: flags.acceptPruned,
			NoCanary:           flags.noCanary,
			BinaryImage:        flags.binaryImage,
			ShimImage:          flags.shimImage,
			StepTimeout:        flags.stepTimeout,
			RolloutTimeout:     flags.rolloutTimeout,
		},
		Out: os.Stdout,
	})
	if err != nil {
		return nil, err
	}
	handle.Mode = mode
	return handle, nil
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
