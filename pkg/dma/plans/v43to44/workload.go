package v43to44

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/danm-cni/danm-utils/pkg/dma/cluster"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
)

type workload struct {
	ref        cluster.WorkloadRef
	deployment *appsv1.Deployment
	daemonSet  *appsv1.DaemonSet
}

func recordedWorkload(ctx context.Context, handle *cluster.Handle, component string) (cluster.WorkloadRef, bool, error) {
	record, err := handle.State.Load(ctx, PlanID)
	if err != nil || record == nil {
		return cluster.WorkloadRef{}, false, err
	}
	stored, found := record.Workloads[component]
	if !found {
		return cluster.WorkloadRef{}, false, nil
	}
	return cluster.RecordedWorkload(stored), stored.Present, nil
}

func loadWorkload(ctx context.Context, handle *cluster.Handle, ref cluster.WorkloadRef) (*workload, error) {
	if ref.Kind == cluster.WorkloadDaemonSet {
		daemonSet, err := handle.Kube.AppsV1().DaemonSets(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
		if err != nil {
			return nil, fmt.Errorf("cannot read %s: %w", ref, err)
		}
		return &workload{ref: ref, daemonSet: daemonSet}, nil
	}
	deployment, err := handle.Kube.AppsV1().Deployments(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("cannot read %s: %w", ref, err)
	}
	return &workload{ref: ref, deployment: deployment}, nil
}

func (object *workload) template() *corev1.PodTemplateSpec {
	if object.daemonSet != nil {
		return &object.daemonSet.Spec.Template
	}
	return &object.deployment.Spec.Template
}

func (object *workload) selector() *metav1.LabelSelector {
	if object.daemonSet != nil {
		return object.daemonSet.Spec.Selector
	}
	return object.deployment.Spec.Selector
}

func (object *workload) problem() string {
	if object.daemonSet != nil {
		return daemonSetProblem(object.daemonSet)
	}
	return deploymentProblem(object.deployment)
}

func (object *workload) spec() (string, error) {
	var encoded []byte
	var err error
	if object.daemonSet != nil {
		encoded, err = json.Marshal(object.daemonSet.Spec)
	} else {
		encoded, err = json.Marshal(object.deployment.Spec)
	}
	if err != nil {
		return "", fmt.Errorf("the spec of %s cannot be serialised: %w", object.ref, err)
	}
	return string(encoded), nil
}

func (object *workload) setSpec(raw string) error {
	if object.daemonSet != nil {
		spec := appsv1.DaemonSetSpec{}
		if err := json.Unmarshal([]byte(raw), &spec); err != nil {
			return fmt.Errorf("the recorded spec of %s is corrupted: %w", object.ref, err)
		}
		object.daemonSet.Spec = spec
		return nil
	}
	spec := appsv1.DeploymentSpec{}
	if err := json.Unmarshal([]byte(raw), &spec); err != nil {
		return fmt.Errorf("the recorded spec of %s is corrupted: %w", object.ref, err)
	}
	object.deployment.Spec = spec
	return nil
}

func (object *workload) update(ctx context.Context, handle *cluster.Handle) error {
	var err error
	if object.daemonSet != nil {
		object.daemonSet, err = handle.Kube.AppsV1().DaemonSets(object.ref.Namespace).Update(ctx, object.daemonSet, metav1.UpdateOptions{})
	} else {
		object.deployment, err = handle.Kube.AppsV1().Deployments(object.ref.Namespace).Update(ctx, object.deployment, metav1.UpdateOptions{})
	}
	if err != nil {
		return fmt.Errorf("cannot update %s: %w", object.ref, err)
	}
	return nil
}

func componentContainer(template *corev1.PodTemplateSpec, component string) (int, error) {
	named := []int{}
	for index, candidate := range template.Spec.Containers {
		if candidate.Name == component {
			return index, nil
		}
		if strings.Contains(candidate.Name, component) {
			named = append(named, index)
		}
	}
	if len(named) == 1 {
		return named[0], nil
	}
	if len(named) == 0 && len(template.Spec.Containers) == 1 {
		return 0, nil
	}
	names := make([]string, 0, len(template.Spec.Containers))
	for _, candidate := range template.Spec.Containers {
		names = append(names, candidate.Name)
	}
	if len(named) > 1 {
		return 0, fmt.Errorf("several containers could be the one of %s, name one of them exactly %s: %s", component, component, strings.Join(names, ", "))
	}
	return 0, fmt.Errorf("no container is named after %s, the template runs %s", component, strings.Join(names, ", "))
}

func containerImage(template *corev1.PodTemplateSpec, component string) (string, error) {
	index, err := componentContainer(template, component)
	if err != nil {
		return "", err
	}
	return template.Spec.Containers[index].Image, nil
}

func setContainerImage(template *corev1.PodTemplateSpec, component string, image cluster.ComponentImage) error {
	index, err := componentContainer(template, component)
	if err != nil {
		return err
	}
	template.Spec.Containers[index].Image = image.Image
	template.Spec.Containers[index].ImagePullPolicy = image.PullPolicy
	return nil
}

func waitForWorkload(ctx context.Context, handle *cluster.Handle, ref cluster.WorkloadRef) error {
	timeout := handle.Options.RolloutTimeout
	if timeout <= 0 {
		timeout = 10 * time.Minute
	}
	problem := ""
	err := wait.PollUntilContextTimeout(ctx, workloadPollInterval, timeout, true, func(ctx context.Context) (bool, error) {
		object, err := loadWorkload(ctx, handle, ref)
		if apierrors.IsNotFound(err) {
			return false, err
		}
		if err != nil {
			return false, nil
		}
		problem = object.problem()
		return problem == "", nil
	})
	if wait.Interrupted(err) {
		return fmt.Errorf("%s is not ready after %s: %s", ref, timeout, orNone(problem))
	}
	return err
}

var workloadPollInterval = 2 * time.Second
