package v43to44

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/danm-cni/danm-utils/pkg/dma/cluster"
	"github.com/danm-cni/danm-utils/pkg/dma/engine"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
)

var (
	crdPollInterval = time.Second
	crdWaitTimeout  = 30 * time.Second
)

var newKinds = []apiKind{kindDanmEp, kindDanmNet, kindTenantNetwork, kindClusterNetwork, kindTenantConfig, kindReservedIP}

type installNewCrds struct{}

func (installNewCrds) Meta() engine.StepMeta {
	return engine.StepMeta{
		ID:          2,
		Name:        "install-new-crds",
		Description: "Install the danm.io CRDs of the detected deployment mode next to the danm.k8s.io ones.",
		Phase:       engine.PhaseDeploy,
		Reversible:  true,
	}
}

func (installNewCrds) Detect(ctx context.Context, handle *cluster.Handle) (engine.StepState, error) {
	installed, err := installedNewCrds(ctx, handle)
	if err != nil {
		return engine.StateNotStarted, err
	}
	if len(installed) == 0 {
		return engine.StateNotStarted, nil
	}
	if handle.Mode == cluster.ModeUnknown {
		return engine.StatePartial, nil
	}
	targets := targetKinds(handle.Mode)
	if len(installed) != len(targets) {
		return engine.StatePartial, nil
	}
	for _, kind := range targets {
		crd, found := installed[kind]
		if !found || !crdCondition(crd, apiextensionsv1.Established) {
			return engine.StatePartial, nil
		}
	}
	return engine.StateDone, nil
}

func (step installNewCrds) Execute(ctx context.Context, handle *cluster.Handle) error {
	if handle.Mode == cluster.ModeUnknown {
		return errors.New("the deployment mode is unknown, run step 1 first or name the mode with --mode")
	}
	targets := targetKinds(handle.Mode)
	manifests := make(map[apiKind]*apiextensionsv1.CustomResourceDefinition, len(targets))
	for _, kind := range targets {
		crd, err := targetCrd(handle.Mode, kind)
		if err != nil {
			return err
		}
		manifests[kind] = crd
	}
	missing, err := step.verifyNewGroup(ctx, handle, targets)
	if err != nil {
		return err
	}
	for _, kind := range missing {
		crd := manifests[kind].DeepCopy()
		labels := crd.GetLabels()
		if labels == nil {
			labels = map[string]string{}
		}
		labels[PlanLabel] = PlanID
		crd.SetLabels(labels)
		if _, err := handle.ApiExt.ApiextensionsV1().CustomResourceDefinitions().Create(ctx, crd, metav1.CreateOptions{}); err != nil {
			return revertNewCrds(handle, fmt.Errorf("cannot create CRD %s: %w", kind.NewCrdName(), err))
		}
		handle.Printf("Created CRD %s.", kind.NewCrdName())
	}
	if err := waitForEstablished(ctx, handle, targets); err != nil {
		return revertNewCrds(handle, err)
	}
	handle.Printf("The danm.io CRDs of %s mode are established.", handle.Mode)
	return nil
}

func (installNewCrds) Rollback(ctx context.Context, handle *cluster.Handle) error {
	return deleteNewCrds(ctx, handle)
}

func (installNewCrds) verifyNewGroup(ctx context.Context, handle *cluster.Handle, targets []apiKind) ([]apiKind, error) {
	wanted := make(map[apiKind]bool, len(targets))
	for _, kind := range targets {
		wanted[kind] = true
	}
	problems := []string{}
	missing := []apiKind{}
	for _, kind := range newKinds {
		crd, err := handle.ApiExt.ApiextensionsV1().CustomResourceDefinitions().Get(ctx, kind.NewCrdName(), metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			if wanted[kind] {
				missing = append(missing, kind)
			}
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("cannot read CRD %s: %w", kind.NewCrdName(), err)
		}
		switch {
		case !wanted[kind]:
			problems = append(problems, fmt.Sprintf("CRD %s belongs to the other deployment mode than %s", kind.NewCrdName(), handle.Mode))
		case crd.GetLabels()[PlanLabel] != PlanID:
			problems = append(problems, fmt.Sprintf("CRD %s already exists and was not installed by plan %s", kind.NewCrdName(), PlanID))
		}
	}
	if len(problems) > 0 {
		return nil, fmt.Errorf("the danm.io group cannot be installed, remove or explain these CRDs first:\n  - %s", strings.Join(problems, "\n  - "))
	}
	return missing, nil
}

func installedNewCrds(ctx context.Context, handle *cluster.Handle) (map[apiKind]*apiextensionsv1.CustomResourceDefinition, error) {
	installed := map[apiKind]*apiextensionsv1.CustomResourceDefinition{}
	for _, kind := range newKinds {
		crd, err := handle.ApiExt.ApiextensionsV1().CustomResourceDefinitions().Get(ctx, kind.NewCrdName(), metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("cannot read CRD %s: %w", kind.NewCrdName(), err)
		}
		if crd.GetLabels()[PlanLabel] == PlanID {
			installed[kind] = crd
		}
	}
	return installed, nil
}

func crdCondition(crd *apiextensionsv1.CustomResourceDefinition, conditionType apiextensionsv1.CustomResourceDefinitionConditionType) bool {
	for _, condition := range crd.Status.Conditions {
		if condition.Type == conditionType {
			return condition.Status == apiextensionsv1.ConditionTrue
		}
	}
	return false
}

func namesRejected(crd *apiextensionsv1.CustomResourceDefinition) string {
	for _, condition := range crd.Status.Conditions {
		if condition.Type == apiextensionsv1.NamesAccepted && condition.Status == apiextensionsv1.ConditionFalse {
			return condition.Message
		}
	}
	return ""
}

func waitForEstablished(ctx context.Context, handle *cluster.Handle, targets []apiKind) error {
	pending := []string{}
	err := wait.PollUntilContextTimeout(ctx, crdPollInterval, crdWaitTimeout, true, func(ctx context.Context) (bool, error) {
		pending = pending[:0]
		for _, kind := range targets {
			crd, err := handle.ApiExt.ApiextensionsV1().CustomResourceDefinitions().Get(ctx, kind.NewCrdName(), metav1.GetOptions{})
			if err != nil {
				return false, fmt.Errorf("cannot read CRD %s: %w", kind.NewCrdName(), err)
			}
			if message := namesRejected(crd); message != "" {
				return false, fmt.Errorf("the names of CRD %s are not accepted: %s", kind.NewCrdName(), message)
			}
			if !crdCondition(crd, apiextensionsv1.Established) {
				pending = append(pending, kind.NewCrdName())
			}
		}
		return len(pending) == 0, nil
	})
	if wait.Interrupted(err) {
		return fmt.Errorf("CRDs %s are not established after %s", strings.Join(pending, ", "), crdWaitTimeout)
	}
	return err
}

func revertNewCrds(handle *cluster.Handle, cause error) error {
	ctx, cancel := context.WithTimeout(context.Background(), crdWaitTimeout)
	defer cancel()
	handle.Printf("Removing the danm.io CRDs installed so far, as the set could not be installed in full: %s", cause.Error())
	if err := deleteNewCrds(ctx, handle); err != nil {
		return fmt.Errorf("%w, and removing the partially installed set failed as well: %w", cause, err)
	}
	return cause
}

func deleteNewCrds(ctx context.Context, handle *cluster.Handle) error {
	installed, err := installedNewCrds(ctx, handle)
	if err != nil {
		return err
	}
	populated := []string{}
	for _, kind := range newKinds {
		if _, found := installed[kind]; !found {
			continue
		}
		list, err := handle.Dynamic.Resource(kind.NewResource()).List(ctx, metav1.ListOptions{Limit: 1})
		if err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("cannot verify that CRD %s holds no objects: %w", kind.NewCrdName(), err)
		}
		if err == nil && len(list.Items) > 0 {
			populated = append(populated, kind.NewCrdName())
		}
	}
	if len(populated) > 0 {
		return fmt.Errorf("CRDs %s hold objects, deleting them would delete the objects as well; roll back the steps which created those objects first", strings.Join(populated, ", "))
	}
	for _, kind := range newKinds {
		if _, found := installed[kind]; !found {
			continue
		}
		err := handle.ApiExt.ApiextensionsV1().CustomResourceDefinitions().Delete(ctx, kind.NewCrdName(), metav1.DeleteOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("cannot delete CRD %s: %w", kind.NewCrdName(), err)
		}
		handle.Printf("Deleted CRD %s.", kind.NewCrdName())
	}
	return wait.PollUntilContextTimeout(ctx, crdPollInterval, crdWaitTimeout, true, func(ctx context.Context) (bool, error) {
		remaining, err := installedNewCrds(ctx, handle)
		if err != nil {
			return false, err
		}
		return len(remaining) == 0, nil
	})
}
