package v43to44

import (
	"context"
	"fmt"
	"sort"

	"github.com/danm-cni/danm-utils/pkg/dma/cluster"
	"github.com/danm-cni/danm-utils/pkg/dma/engine"
	danmtypes "github.com/danm-cni/danm/crd/apis/danm/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

const RemoveShimToken = "remove-annotation-shim"

type ShimTemplate struct {
	Kind      string
	Namespace string
	Name      string
	Value     string
}

func ShimStatus(ctx context.Context, handle *cluster.Handle) ([]ShimTemplate, error) {
	templates, err := unconvertedTemplates(ctx, handle)
	if err != nil {
		return nil, err
	}
	rips, err := allReservedIps(ctx, handle)
	if err != nil {
		return nil, err
	}
	for index := range templates {
		templates[index].Value, _ = NewInterfaces(templates[index].Value, templates[index].Namespace, "", rips)
	}
	return templates, nil
}

func RemoveShim(ctx context.Context, handle *cluster.Handle, confirmer engine.Confirmer) error {
	steps := New().Steps()
	cleanup := steps[len(steps)-1]
	detected, err := cleanup.Detect(ctx, handle)
	if err != nil {
		return err
	}
	if detected != engine.StateDone {
		return fmt.Errorf("step %d (%s) reads as %s; during the plan the shim is removed by rolling back step 4, even with --force", cleanup.Meta().ID, cleanup.Meta().Name, detected)
	}
	templates, err := unconvertedTemplates(ctx, handle)
	if err != nil {
		return err
	}
	if len(templates) > 0 {
		return fmt.Errorf("%d Pod templates still carry only %s, \"dma shim status\" lists them; the shim stays until they carry %s", len(templates), OldInterfacesKey, NewInterfacesKey)
	}
	if handle.Options.DryRun {
		handle.Printf("Dry run: the annotation shim would be removed now.")
		return nil
	}
	prompt := "\nRemoving the annotation shim. Pods created afterwards from a template carrying only " + OldInterfacesKey + " get no DANM networks.\n"
	if err := confirmer.Confirm(prompt, RemoveShimToken); err != nil {
		return err
	}
	if err := deleteShim(ctx, handle); err != nil {
		return err
	}
	handle.Printf("The annotation shim is removed.")
	return nil
}

func allReservedIps(ctx context.Context, handle *cluster.Handle) ([]*danmtypes.ReservedIP, error) {
	list, err := handle.Dynamic.Resource(kindReservedIP.NewResource()).List(ctx, metav1.ListOptions{})
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("cannot list %s: %w", kindReservedIP.NewResource(), err)
	}
	rips := make([]*danmtypes.ReservedIP, 0, len(list.Items))
	for _, item := range list.Items {
		rip := &danmtypes.ReservedIP{}
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(item.Object, rip); err != nil {
			return nil, fmt.Errorf("ReservedIP %s/%s cannot be read: %w", item.GetNamespace(), item.GetName(), err)
		}
		rips = append(rips, rip)
	}
	return rips, nil
}

func unconverted(kind string, meta metav1.ObjectMeta, template corev1.PodTemplateSpec, templates []ShimTemplate) []ShimTemplate {
	old, hasOld := template.Annotations[OldInterfacesKey]
	if _, hasNew := template.Annotations[NewInterfacesKey]; !hasOld || hasNew {
		return templates
	}
	return append(templates, ShimTemplate{Kind: kind, Namespace: meta.Namespace, Name: meta.Name, Value: old})
}

func unconvertedTemplates(ctx context.Context, handle *cluster.Handle) ([]ShimTemplate, error) {
	templates := []ShimTemplate{}
	listOptions := metav1.ListOptions{}
	deployments, err := handle.Kube.AppsV1().Deployments("").List(ctx, listOptions)
	if err != nil {
		return nil, fmt.Errorf("cannot list Deployments: %w", err)
	}
	for _, item := range deployments.Items {
		templates = unconverted("Deployment", item.ObjectMeta, item.Spec.Template, templates)
	}
	statefulSets, err := handle.Kube.AppsV1().StatefulSets("").List(ctx, listOptions)
	if err != nil {
		return nil, fmt.Errorf("cannot list StatefulSets: %w", err)
	}
	for _, item := range statefulSets.Items {
		templates = unconverted("StatefulSet", item.ObjectMeta, item.Spec.Template, templates)
	}
	daemonSets, err := handle.Kube.AppsV1().DaemonSets("").List(ctx, listOptions)
	if err != nil {
		return nil, fmt.Errorf("cannot list DaemonSets: %w", err)
	}
	for _, item := range daemonSets.Items {
		templates = unconverted("DaemonSet", item.ObjectMeta, item.Spec.Template, templates)
	}
	cronJobs, err := handle.Kube.BatchV1().CronJobs("").List(ctx, listOptions)
	if err != nil {
		return nil, fmt.Errorf("cannot list CronJobs: %w", err)
	}
	for _, item := range cronJobs.Items {
		templates = unconverted("CronJob", item.ObjectMeta, item.Spec.JobTemplate.Spec.Template, templates)
	}
	replicaSets, err := handle.Kube.AppsV1().ReplicaSets("").List(ctx, listOptions)
	if err != nil {
		return nil, fmt.Errorf("cannot list ReplicaSets: %w", err)
	}
	for _, item := range replicaSets.Items {
		if len(item.OwnerReferences) == 0 {
			templates = unconverted("ReplicaSet", item.ObjectMeta, item.Spec.Template, templates)
		}
	}
	jobs, err := handle.Kube.BatchV1().Jobs("").List(ctx, listOptions)
	if err != nil {
		return nil, fmt.Errorf("cannot list Jobs: %w", err)
	}
	for _, item := range jobs.Items {
		if len(item.OwnerReferences) == 0 {
			templates = unconverted("Job", item.ObjectMeta, item.Spec.Template, templates)
		}
	}
	sort.SliceStable(templates, func(left, right int) bool {
		if templates[left].Namespace != templates[right].Namespace {
			return templates[left].Namespace < templates[right].Namespace
		}
		if templates[left].Kind != templates[right].Kind {
			return templates[left].Kind < templates[right].Kind
		}
		return templates[left].Name < templates[right].Name
	})
	return templates, nil
}
