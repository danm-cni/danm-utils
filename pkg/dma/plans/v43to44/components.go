package v43to44

import (
	"context"
	"embed"
	"fmt"
	"strings"

	"github.com/danm-cni/danm-utils/pkg/dma/cluster"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"
)

const (
	ControlPlaneLabel     = "node-role.kubernetes.io/control-plane"
	componentManifestDir  = "manifests/components"
	componentDocSeparator = "\n---"
)

//go:embed manifests/components
var componentManifests embed.FS

type componentManifest struct {
	account    *corev1.ServiceAccount
	deployment *appsv1.Deployment
	daemonSet  *appsv1.DaemonSet
}

func loadComponentManifest(component string) (*componentManifest, error) {
	raw, err := componentManifests.ReadFile(componentManifestDir + "/" + component + ".yaml")
	if err != nil {
		return nil, fmt.Errorf("the assistant carries no manifest for %s: %w", component, err)
	}
	manifest := &componentManifest{}
	for _, document := range strings.Split(string(raw), componentDocSeparator) {
		if strings.TrimSpace(document) == "" {
			continue
		}
		meta := &metav1.TypeMeta{}
		if err := yaml.Unmarshal([]byte(document), meta); err != nil {
			return nil, fmt.Errorf("the embedded manifest of %s is malformed: %w", component, err)
		}
		switch meta.Kind {
		case "ServiceAccount":
			manifest.account = &corev1.ServiceAccount{}
			err = yaml.UnmarshalStrict([]byte(document), manifest.account)
		case string(cluster.WorkloadDeployment):
			manifest.deployment = &appsv1.Deployment{}
			err = yaml.UnmarshalStrict([]byte(document), manifest.deployment)
		case string(cluster.WorkloadDaemonSet):
			manifest.daemonSet = &appsv1.DaemonSet{}
			err = yaml.UnmarshalStrict([]byte(document), manifest.daemonSet)
		default:
			err = fmt.Errorf("it carries an unexpected %s", meta.Kind)
		}
		if err != nil {
			return nil, fmt.Errorf("the embedded manifest of %s is malformed: %w", component, err)
		}
	}
	if manifest.account == nil || (manifest.deployment == nil && manifest.daemonSet == nil) {
		return nil, fmt.Errorf("the embedded manifest of %s carries no ServiceAccount or no workload", component)
	}
	return manifest, nil
}

func (manifest *componentManifest) kind() cluster.WorkloadKind {
	if manifest.daemonSet != nil {
		return cluster.WorkloadDaemonSet
	}
	return cluster.WorkloadDeployment
}

func (manifest *componentManifest) template() *corev1.PodTemplateSpec {
	if manifest.daemonSet != nil {
		return &manifest.daemonSet.Spec.Template
	}
	return &manifest.deployment.Spec.Template
}

func applyComponentTemplate(component string, template *corev1.PodTemplateSpec) error {
	manifest, err := loadComponentManifest(component)
	if err != nil {
		return err
	}
	desired := manifest.template().Spec
	if desired.PriorityClassName != "" {
		template.Spec.PriorityClassName = desired.PriorityClassName
	}
	if len(desired.NodeSelector) > 0 {
		template.Spec.NodeSelector = desired.NodeSelector
	}
	if len(desired.Tolerations) > 0 {
		template.Spec.Tolerations = desired.Tolerations
	}
	if desired.TerminationGracePeriodSeconds != nil {
		grace := *desired.TerminationGracePeriodSeconds
		template.Spec.TerminationGracePeriodSeconds = &grace
	}
	return nil
}

func componentTemplateIsCurrent(component string, template *corev1.PodTemplateSpec) (bool, error) {
	applied := template.DeepCopy()
	if err := applyComponentTemplate(component, applied); err != nil {
		return false, err
	}
	return equality.Semantic.DeepEqual(applied.Spec, template.Spec), nil
}

func deployComponent(ctx context.Context, handle *cluster.Handle, component string, ref cluster.WorkloadRef, image cluster.ComponentImage) error {
	manifest, err := loadComponentManifest(component)
	if err != nil {
		return err
	}
	if manifest.kind() != ref.Kind {
		return fmt.Errorf("%s is configured as a %s, but the assistant deploys %s as a %s", component, ref.Kind, component, manifest.kind())
	}
	if err := ensureComponentAccount(ctx, handle, manifest.account.Name, ref.Namespace); err != nil {
		return err
	}
	template := manifest.template()
	if err := setContainerImage(template, component, image); err != nil {
		return fmt.Errorf("the embedded manifest of %s cannot be deployed: %w", component, err)
	}
	template.Spec.ServiceAccountName = manifest.account.Name
	if handle.Images.PullSecret != "" {
		template.Spec.ImagePullSecrets = []corev1.LocalObjectReference{{Name: handle.Images.PullSecret}}
	}
	if manifest.daemonSet != nil {
		manifest.daemonSet.ObjectMeta = metav1.ObjectMeta{Namespace: ref.Namespace, Name: ref.Name, Labels: manifest.daemonSet.Labels}
		_, err = handle.Kube.AppsV1().DaemonSets(ref.Namespace).Create(ctx, manifest.daemonSet, metav1.CreateOptions{})
	} else {
		manifest.deployment.ObjectMeta = metav1.ObjectMeta{Namespace: ref.Namespace, Name: ref.Name, Labels: manifest.deployment.Labels}
		_, err = handle.Kube.AppsV1().Deployments(ref.Namespace).Create(ctx, manifest.deployment, metav1.CreateOptions{})
	}
	if err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("cannot create %s: %w", ref, err)
	}
	handle.Printf("%s is deployed as %s, running %s.", component, ref, image.Image)
	return nil
}

func ensureComponentAccount(ctx context.Context, handle *cluster.Handle, name, namespace string) error {
	accounts := handle.Kube.CoreV1().ServiceAccounts(namespace)
	_, err := accounts.Get(ctx, name, metav1.GetOptions{})
	if !apierrors.IsNotFound(err) {
		if err != nil {
			return fmt.Errorf("cannot read ServiceAccount %s: %w", objectName(namespace, name), err)
		}
		return nil
	}
	account := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name}}
	if _, err := accounts.Create(ctx, account, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("cannot create ServiceAccount %s: %w", objectName(namespace, name), err)
	}
	return nil
}

func componentAccountName(component string) (string, error) {
	manifest, err := loadComponentManifest(component)
	if err != nil {
		return "", err
	}
	return manifest.account.Name, nil
}

func warnMissingControlPlaneNodes(ctx context.Context, handle *cluster.Handle, component string) error {
	manifest, err := loadComponentManifest(component)
	if err != nil {
		return err
	}
	selector := manifest.template().Spec.NodeSelector
	if len(selector) == 0 {
		return nil
	}
	nodes, err := handle.Kube.CoreV1().Nodes().List(ctx, metav1.ListOptions{LabelSelector: labelSelector(selector)})
	if err != nil {
		return fmt.Errorf("cannot list the nodes of the cluster: %w", err)
	}
	if len(nodes.Items) == 0 {
		handle.Warnf("no node carries %s, so the Pods of %s stay unschedulable until one does", labelSelector(selector), component)
	}
	return nil
}

func labelSelector(selector map[string]string) string {
	terms := make([]string, 0, len(selector))
	for key, value := range selector {
		if value == "" {
			terms = append(terms, key)
			continue
		}
		terms = append(terms, key+"="+value)
	}
	return strings.Join(terms, ",")
}
