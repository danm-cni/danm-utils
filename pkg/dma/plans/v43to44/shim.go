package v43to44

import (
	"bytes"
	"context"
	"embed"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"text/template"
	"time"

	"github.com/danm-cni/danm-utils/pkg/dma/cluster"
	"github.com/danm-cni/danm-utils/pkg/dma/engine"
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/yaml"
)

const (
	ShimName          = "dma-shim"
	ShimTlsSecretName = "dma-shim-tls"
	ShimRbacName      = "danm:dma-shim"
	ShimWebhookName   = "interfaces-shim.dma.danm.io"
	PodNameEnv        = "DMA_POD_NAME"
	PodNamespaceEnv   = "DMA_POD_NAMESPACE"
	DmaContainerName  = "dma"
	shimManifestDir   = "manifests/shim"
)

//go:embed manifests/shim
var shimManifests embed.FS

var (
	shimPollInterval = 2 * time.Second
	shimNow          = time.Now
)

type shimManifestValues struct {
	Namespace  string
	Image      string
	PullPolicy string
	PullSecret string
	Mode       string
	CaBundle   string
}

type installAnnotationShim struct{}

func (installAnnotationShim) Meta() engine.StepMeta {
	return engine.StepMeta{
		ID:          4,
		Name:        "install-annotation-shim",
		Description: "Deploy the webhook which adds danm.io/interfaces to every new Pod carrying danm.k8s.io/interfaces.",
		Phase:       engine.PhaseDeploy,
		Reversible:  true,
	}
}

func (installAnnotationShim) Detect(ctx context.Context, handle *cluster.Handle) (engine.StepState, error) {
	present, complete, err := shimPresence(ctx, handle)
	if err != nil {
		return engine.StateNotStarted, err
	}
	if present == 0 {
		return engine.StateNotStarted, nil
	}
	if !complete {
		return engine.StatePartial, nil
	}
	return engine.StateDone, nil
}

func (installAnnotationShim) Execute(ctx context.Context, handle *cluster.Handle) error {
	if handle.Mode == cluster.ModeUnknown {
		return errors.New("the deployment mode is unknown, run step 1 first or name the mode with --mode")
	}
	image, err := shimImage(ctx, handle)
	if err != nil {
		return err
	}
	if err := ensureShimServiceAccount(ctx, handle); err != nil {
		return err
	}
	if handle.Mode == cluster.ModeProduction {
		rules, err := shimRules(handle)
		if err != nil {
			return err
		}
		if err := ensureClusterRole(ctx, handle, ShimRbacName, rules); err != nil {
			return err
		}
		subject := rbacv1.Subject{Kind: rbacv1.ServiceAccountKind, Namespace: handle.Namespace, Name: ShimName}
		if err := ensureClusterRoleBinding(ctx, handle, ShimRbacName, subject); err != nil {
			return err
		}
	}
	ca, err := ensureShimSecret(ctx, handle)
	if err != nil {
		return err
	}
	if err := ensureShimService(ctx, handle); err != nil {
		return err
	}
	if err := ensureShimDeployment(ctx, handle, image); err != nil {
		return err
	}
	if err := waitForShim(ctx, handle); err != nil {
		return err
	}
	if err := ensureShimWebhook(ctx, handle, ca); err != nil {
		return err
	}
	handle.Printf("The annotation shim runs %s and admits every new Pod.", image.Image)
	return nil
}

func (installAnnotationShim) Rollback(ctx context.Context, handle *cluster.Handle) error {
	return deleteShim(ctx, handle)
}

func shimDnsName(handle *cluster.Handle) string {
	return ShimName + "." + handle.Namespace + ".svc"
}

func shimValues(handle *cluster.Handle) shimManifestValues {
	return shimManifestValues{Namespace: handle.Namespace, Mode: string(handle.Mode)}
}

func shimManifest[T any](file, name string, values shimManifestValues) (*T, error) {
	raw, err := shimManifests.ReadFile(shimManifestDir + "/" + file)
	if err != nil {
		return nil, fmt.Errorf("the assistant carries no shim manifest %s: %w", file, err)
	}
	parsed, err := template.New(file).Option("missingkey=error").Parse(string(raw))
	if err != nil {
		return nil, fmt.Errorf("the embedded shim manifest %s is malformed: %w", file, err)
	}
	rendered := &bytes.Buffer{}
	if err := parsed.Execute(rendered, values); err != nil {
		return nil, fmt.Errorf("cannot render the embedded shim manifest %s: %w", file, err)
	}
	meta := &metav1.PartialObjectMetadata{}
	if err := yaml.Unmarshal(rendered.Bytes(), meta); err != nil {
		return nil, fmt.Errorf("the embedded shim manifest %s is malformed: %w", file, err)
	}
	if meta.Name != name {
		return nil, fmt.Errorf("the embedded shim manifest %s is named %s instead of %s", file, meta.Name, name)
	}
	object := new(T)
	if err := yaml.UnmarshalStrict(rendered.Bytes(), object); err != nil {
		return nil, fmt.Errorf("the embedded shim manifest %s is malformed: %w", file, err)
	}
	return object, nil
}

func shimRules(handle *cluster.Handle) ([]rbacv1.PolicyRule, error) {
	role, err := shimManifest[rbacv1.ClusterRole]("clusterrole.yaml", ShimRbacName, shimValues(handle))
	if err != nil {
		return nil, err
	}
	return role.Rules, nil
}

func shimImage(ctx context.Context, handle *cluster.Handle) (cluster.ComponentImage, error) {
	policy := handle.Images.PullPolicy
	if policy == "" {
		policy = corev1.PullIfNotPresent
	}
	if handle.Options.ShimImage != "" {
		return cluster.ComponentImage{Image: handle.Options.ShimImage, PullPolicy: policy}, nil
	}
	name, namespace := os.Getenv(PodNameEnv), os.Getenv(PodNamespaceEnv)
	if name == "" || namespace == "" {
		return cluster.ComponentImage{}, fmt.Errorf("the image of the assistant is unknown, as %s and %s are not set; name it with --shim-image", PodNameEnv, PodNamespaceEnv)
	}
	pod, err := handle.Kube.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return cluster.ComponentImage{}, fmt.Errorf("cannot read the assistant's own Pod %s/%s: %w", namespace, name, err)
	}
	for _, container := range pod.Spec.Containers {
		if container.Name == DmaContainerName || len(pod.Spec.Containers) == 1 {
			return cluster.ComponentImage{Image: container.Image, PullPolicy: policy}, nil
		}
	}
	return cluster.ComponentImage{}, fmt.Errorf("the assistant's own Pod %s/%s has no container named %s; name the image with --shim-image", namespace, name, DmaContainerName)
}

func ensureShimServiceAccount(ctx context.Context, handle *cluster.Handle) error {
	desired, err := shimManifest[corev1.ServiceAccount]("serviceaccount.yaml", ShimName, shimValues(handle))
	if err != nil {
		return err
	}
	accounts := handle.Kube.CoreV1().ServiceAccounts(handle.Namespace)
	_, err = accounts.Get(ctx, ShimName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		_, err = accounts.Create(ctx, desired, metav1.CreateOptions{})
	}
	if err != nil {
		return fmt.Errorf("cannot create ServiceAccount %s/%s: %w", handle.Namespace, ShimName, err)
	}
	return nil
}

func ensureShimSecret(ctx context.Context, handle *cluster.Handle) ([]byte, error) {
	desired, err := shimManifest[corev1.Secret]("secret.yaml", ShimTlsSecretName, shimValues(handle))
	if err != nil {
		return nil, err
	}
	secrets := handle.Kube.CoreV1().Secrets(handle.Namespace)
	existing, err := secrets.Get(ctx, ShimTlsSecretName, metav1.GetOptions{})
	exists := err == nil
	if err != nil && !apierrors.IsNotFound(err) {
		return nil, fmt.Errorf("cannot read Secret %s/%s: %w", handle.Namespace, ShimTlsSecretName, err)
	}
	if exists && shimCertificatesUsable(existing, shimDnsName(handle), shimNow()) {
		return existing.Data[CaCertKey], nil
	}
	certificates, err := generateShimCertificates([]string{shimDnsName(handle), ShimName + "." + handle.Namespace + ".svc.cluster.local"}, shimNow())
	if err != nil {
		return nil, err
	}
	data := map[string][]byte{corev1.TLSCertKey: certificates.cert, corev1.TLSPrivateKeyKey: certificates.key, CaCertKey: certificates.ca}
	if exists {
		existing.Data = data
		existing.Type = desired.Type
		if _, err := secrets.Update(ctx, existing, metav1.UpdateOptions{}); err != nil {
			return nil, fmt.Errorf("cannot update Secret %s/%s: %w", handle.Namespace, ShimTlsSecretName, err)
		}
		return certificates.ca, nil
	}
	desired.Data = data
	if _, err := secrets.Create(ctx, desired, metav1.CreateOptions{}); err != nil {
		return nil, fmt.Errorf("cannot create Secret %s/%s: %w", handle.Namespace, ShimTlsSecretName, err)
	}
	return certificates.ca, nil
}

func ensureShimService(ctx context.Context, handle *cluster.Handle) error {
	desired, err := shimManifest[corev1.Service]("service.yaml", ShimName, shimValues(handle))
	if err != nil {
		return err
	}
	services := handle.Kube.CoreV1().Services(handle.Namespace)
	existing, err := services.Get(ctx, ShimName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		if _, err := services.Create(ctx, desired, metav1.CreateOptions{}); err != nil {
			return fmt.Errorf("cannot create Service %s/%s: %w", handle.Namespace, ShimName, err)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("cannot read Service %s/%s: %w", handle.Namespace, ShimName, err)
	}
	if equality.Semantic.DeepEqual(existing.Spec.Selector, desired.Spec.Selector) && equality.Semantic.DeepEqual(existing.Spec.Ports, desired.Spec.Ports) {
		return nil
	}
	existing.Spec.Selector = desired.Spec.Selector
	existing.Spec.Ports = desired.Spec.Ports
	if _, err := services.Update(ctx, existing, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("cannot update Service %s/%s: %w", handle.Namespace, ShimName, err)
	}
	return nil
}

func shimDeployment(handle *cluster.Handle, image cluster.ComponentImage) (*appsv1.Deployment, error) {
	values := shimValues(handle)
	values.Image = image.Image
	values.PullPolicy = string(image.PullPolicy)
	values.PullSecret = handle.Images.PullSecret
	return shimManifest[appsv1.Deployment]("deployment.yaml", ShimName, values)
}

func ensureShimDeployment(ctx context.Context, handle *cluster.Handle, image cluster.ComponentImage) error {
	desired, err := shimDeployment(handle, image)
	if err != nil {
		return err
	}
	deployments := handle.Kube.AppsV1().Deployments(handle.Namespace)
	existing, err := deployments.Get(ctx, ShimName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		if _, err := deployments.Create(ctx, desired, metav1.CreateOptions{}); err != nil {
			return fmt.Errorf("cannot create Deployment %s/%s: %w", handle.Namespace, ShimName, err)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("cannot read Deployment %s/%s: %w", handle.Namespace, ShimName, err)
	}
	existing.Spec = desired.Spec
	if _, err := deployments.Update(ctx, existing, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("cannot update Deployment %s/%s: %w", handle.Namespace, ShimName, err)
	}
	return nil
}

func shimAvailable(deployment *appsv1.Deployment) bool {
	replicas := int32(1)
	if deployment.Spec.Replicas != nil {
		replicas = *deployment.Spec.Replicas
	}
	return deployment.Status.ObservedGeneration >= deployment.Generation &&
		deployment.Status.UpdatedReplicas == replicas &&
		deployment.Status.AvailableReplicas == replicas &&
		deployment.Status.UnavailableReplicas == 0
}

func waitForShim(ctx context.Context, handle *cluster.Handle) error {
	timeout := handle.Options.RolloutTimeout
	if timeout <= 0 {
		timeout = 10 * time.Minute
	}
	err := wait.PollUntilContextTimeout(ctx, shimPollInterval, timeout, true, func(ctx context.Context) (bool, error) {
		deployment, err := handle.Kube.AppsV1().Deployments(handle.Namespace).Get(ctx, ShimName, metav1.GetOptions{})
		if err != nil {
			return false, fmt.Errorf("cannot read Deployment %s/%s: %w", handle.Namespace, ShimName, err)
		}
		return shimAvailable(deployment), nil
	})
	if wait.Interrupted(err) {
		return fmt.Errorf("Deployment %s/%s is not available after %s, the webhook configuration was not created", handle.Namespace, ShimName, timeout)
	}
	return err
}

func shimWebhooks(handle *cluster.Handle, ca []byte) ([]admissionregistrationv1.MutatingWebhook, error) {
	values := shimValues(handle)
	values.CaBundle = base64.StdEncoding.EncodeToString(ca)
	configuration, err := shimManifest[admissionregistrationv1.MutatingWebhookConfiguration]("webhook.yaml", ShimName, values)
	if err != nil {
		return nil, err
	}
	return configuration.Webhooks, nil
}

func ensureShimWebhook(ctx context.Context, handle *cluster.Handle, ca []byte) error {
	webhooks, err := shimWebhooks(handle, ca)
	if err != nil {
		return err
	}
	configurations := handle.Kube.AdmissionregistrationV1().MutatingWebhookConfigurations()
	existing, err := configurations.Get(ctx, ShimName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		configuration := &admissionregistrationv1.MutatingWebhookConfiguration{ObjectMeta: metav1.ObjectMeta{Name: ShimName}, Webhooks: webhooks}
		if _, err := configurations.Create(ctx, configuration, metav1.CreateOptions{}); err != nil {
			return fmt.Errorf("cannot create MutatingWebhookConfiguration %s: %w", ShimName, err)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("cannot read MutatingWebhookConfiguration %s: %w", ShimName, err)
	}
	existing.Webhooks = webhooks
	if _, err := configurations.Update(ctx, existing, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("cannot update MutatingWebhookConfiguration %s: %w", ShimName, err)
	}
	return nil
}

func shimWebhookCurrent(handle *cluster.Handle, configuration *admissionregistrationv1.MutatingWebhookConfiguration, ca []byte) bool {
	if len(configuration.Webhooks) != 1 {
		return false
	}
	webhook := configuration.Webhooks[0]
	service := webhook.ClientConfig.Service
	return webhook.Name == ShimWebhookName && service != nil && service.Namespace == handle.Namespace && service.Name == ShimName &&
		len(ca) > 0 && bytes.Equal(webhook.ClientConfig.CABundle, ca)
}

func shimPresence(ctx context.Context, handle *cluster.Handle) (int, bool, error) {
	present := 0
	missing := 0
	lookup := func(err error, what string, required bool) (bool, error) {
		if apierrors.IsNotFound(err) {
			if required {
				missing++
			}
			return false, nil
		}
		if err != nil {
			return false, fmt.Errorf("cannot read %s: %w", what, err)
		}
		present++
		return true, nil
	}
	production := handle.Mode == cluster.ModeProduction
	_, err := handle.Kube.CoreV1().ServiceAccounts(handle.Namespace).Get(ctx, ShimName, metav1.GetOptions{})
	if _, err := lookup(err, "ServiceAccount "+ShimName, true); err != nil {
		return 0, false, err
	}
	_, err = handle.Kube.RbacV1().ClusterRoles().Get(ctx, ShimRbacName, metav1.GetOptions{})
	if _, err := lookup(err, "ClusterRole "+ShimRbacName, production); err != nil {
		return 0, false, err
	}
	_, err = handle.Kube.RbacV1().ClusterRoleBindings().Get(ctx, ShimRbacName, metav1.GetOptions{})
	if _, err := lookup(err, "ClusterRoleBinding "+ShimRbacName, production); err != nil {
		return 0, false, err
	}
	secret, err := handle.Kube.CoreV1().Secrets(handle.Namespace).Get(ctx, ShimTlsSecretName, metav1.GetOptions{})
	secretFound, err := lookup(err, "Secret "+ShimTlsSecretName, true)
	if err != nil {
		return 0, false, err
	}
	_, err = handle.Kube.CoreV1().Services(handle.Namespace).Get(ctx, ShimName, metav1.GetOptions{})
	if _, err := lookup(err, "Service "+ShimName, true); err != nil {
		return 0, false, err
	}
	deployment, err := handle.Kube.AppsV1().Deployments(handle.Namespace).Get(ctx, ShimName, metav1.GetOptions{})
	deploymentFound, err := lookup(err, "Deployment "+ShimName, true)
	if err != nil {
		return 0, false, err
	}
	configuration, err := handle.Kube.AdmissionregistrationV1().MutatingWebhookConfigurations().Get(ctx, ShimName, metav1.GetOptions{})
	configurationFound, err := lookup(err, "MutatingWebhookConfiguration "+ShimName, true)
	if err != nil {
		return 0, false, err
	}
	complete := missing == 0 && handle.Mode != cluster.ModeUnknown
	if deploymentFound && !shimAvailable(deployment) {
		complete = false
	}
	if configurationFound && (!secretFound || !shimWebhookCurrent(handle, configuration, secret.Data[CaCertKey])) {
		complete = false
	}
	return present, complete, nil
}

func deleteShim(ctx context.Context, handle *cluster.Handle) error {
	deletions := []struct {
		what   string
		delete func() error
	}{
		{"MutatingWebhookConfiguration " + ShimName, func() error {
			return handle.Kube.AdmissionregistrationV1().MutatingWebhookConfigurations().Delete(ctx, ShimName, metav1.DeleteOptions{})
		}},
		{"Deployment " + ShimName, func() error {
			return handle.Kube.AppsV1().Deployments(handle.Namespace).Delete(ctx, ShimName, metav1.DeleteOptions{})
		}},
		{"Service " + ShimName, func() error {
			return handle.Kube.CoreV1().Services(handle.Namespace).Delete(ctx, ShimName, metav1.DeleteOptions{})
		}},
		{"Secret " + ShimTlsSecretName, func() error {
			return handle.Kube.CoreV1().Secrets(handle.Namespace).Delete(ctx, ShimTlsSecretName, metav1.DeleteOptions{})
		}},
		{"ClusterRoleBinding " + ShimRbacName, func() error {
			return handle.Kube.RbacV1().ClusterRoleBindings().Delete(ctx, ShimRbacName, metav1.DeleteOptions{})
		}},
		{"ClusterRole " + ShimRbacName, func() error {
			return handle.Kube.RbacV1().ClusterRoles().Delete(ctx, ShimRbacName, metav1.DeleteOptions{})
		}},
		{"ServiceAccount " + ShimName, func() error {
			return handle.Kube.CoreV1().ServiceAccounts(handle.Namespace).Delete(ctx, ShimName, metav1.DeleteOptions{})
		}},
	}
	for _, deletion := range deletions {
		err := deletion.delete()
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return fmt.Errorf("cannot delete %s: %w", deletion.what, err)
		}
		handle.Printf("Deleted %s.", deletion.what)
	}
	return nil
}
