package v43to44

import (
	"bytes"
	"context"
	"embed"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"text/template"

	"github.com/danm-cni/danm-utils/pkg/dma/cluster"
	"github.com/danm-cni/danm-utils/pkg/dma/engine"
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"
)

const (
	WebhookConfigurationName = "danm-webhook-config"
	webhookManifestDir       = "manifests/webhook"
	webhookSpecKey           = "webhook-workload"
	webhookConfigurationKey  = "webhook-configuration"
)

//go:embed manifests/webhook
var webhookManifests embed.FS

type webhookManifestValues struct {
	Service   string
	Namespace string
	CaBundle  string
}

type deployNewWebhook struct{}

func (deployNewWebhook) Meta() engine.StepMeta {
	return engine.StepMeta{
		ID:          10,
		Name:        "deploy-new-webhook",
		Description: "Roll the DANM webhook to 4.4 and point its configuration at the danm.io group.",
		Phase:       engine.PhaseCutover,
		Reversible:  true,
	}
}

func (deployNewWebhook) Detect(ctx context.Context, handle *cluster.Handle) (engine.StepState, error) {
	ref, present, err := recordedWorkload(ctx, handle, ComponentWebhook)
	if err != nil || !present {
		return engine.StateNotStarted, err
	}
	image, err := handle.Images.Resolve(ComponentWebhook)
	if err != nil {
		return engine.StateNotStarted, err
	}
	object, err := loadWorkload(ctx, handle, ref)
	if apierrors.IsNotFound(err) {
		return engine.StateNotStarted, nil
	}
	if err != nil {
		return engine.StateNotStarted, err
	}
	running, err := containerImage(object.template(), ComponentWebhook)
	current, err := componentTemplateIsCurrent(ComponentWebhook, object.template())
	if err != nil {
		return engine.StateNotStarted, err
	}
	rolled := running == image.Image && current
	configured, err := webhookConfigurationCurrent(ctx, handle)
	if err != nil {
		return engine.StateNotStarted, err
	}
	switch {
	case rolled && configured && object.problem() == "":
		return engine.StateDone, nil
	case !rolled && !configured:
		return engine.StateNotStarted, nil
	}
	return engine.StatePartial, nil
}

func (deployNewWebhook) Execute(ctx context.Context, handle *cluster.Handle) error {
	ref, present, err := recordedWorkload(ctx, handle, ComponentWebhook)
	if err != nil {
		return err
	}
	if !present {
		return errors.New("step 1 recorded no webhook workload, run it before this step")
	}
	image, err := handle.Images.Resolve(ComponentWebhook)
	if err != nil {
		return err
	}
	existing, err := discoverWebhookConfiguration(ctx, handle)
	if err != nil {
		return err
	}
	service, namespace, caBundle, err := webhookClientConfig(existing)
	if err != nil {
		return err
	}
	if err := snapshotWebhookConfiguration(ctx, handle, existing); err != nil {
		return err
	}
	object, err := loadWorkload(ctx, handle, ref)
	if err != nil {
		return err
	}
	if err := snapshotWorkloadSpec(ctx, handle, object, webhookSpecKey); err != nil {
		return err
	}
	if err := setContainerImage(object.template(), ComponentWebhook, image); err != nil {
		return fmt.Errorf("%s cannot be rolled: %w", ref, err)
	}
	if err := applyComponentTemplate(ComponentWebhook, object.template()); err != nil {
		return err
	}
	if err := warnMissingControlPlaneNodes(ctx, handle, ComponentWebhook); err != nil {
		return err
	}
	if err := object.update(ctx, handle); err != nil {
		return err
	}
	handle.Printf("%s now runs %s, waiting for it to become ready.", ref, image.Image)
	if err := waitForWorkload(ctx, handle, ref); err != nil {
		if revertErr := revertWorkloadSpec(ctx, handle, ref, webhookSpecKey); revertErr != nil {
			return fmt.Errorf("%w, and the revert failed as well: %s", err, revertErr.Error())
		}
		handle.Printf("%s is reverted to the spec recorded before this step.", ref)
		return err
	}
	webhooks, err := webhookConfiguration(service, namespace, caBundle)
	if err != nil {
		return err
	}
	existing.Webhooks = webhooks
	if _, err := handle.Kube.AdmissionregistrationV1().MutatingWebhookConfigurations().Update(ctx, existing, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("cannot update MutatingWebhookConfiguration %s: %w", existing.Name, err)
	}
	handle.Printf("MutatingWebhookConfiguration %s now validates the %s group.", existing.Name, NewGroup)
	return nil
}

func (deployNewWebhook) Rollback(ctx context.Context, handle *cluster.Handle) error {
	snapshot, found, err := handle.LoadSnapshot(ctx, webhookConfigurationKey)
	if err != nil {
		return err
	}
	if found {
		configuration := &admissionregistrationv1.MutatingWebhookConfiguration{}
		if err := json.Unmarshal([]byte(snapshot), configuration); err != nil {
			return fmt.Errorf("the recorded webhook configuration is corrupted: %w", err)
		}
		existing, err := handle.Kube.AdmissionregistrationV1().MutatingWebhookConfigurations().Get(ctx, configuration.Name, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("cannot read MutatingWebhookConfiguration %s: %w", configuration.Name, err)
		}
		existing.Webhooks = configuration.Webhooks
		if _, err := handle.Kube.AdmissionregistrationV1().MutatingWebhookConfigurations().Update(ctx, existing, metav1.UpdateOptions{}); err != nil {
			return fmt.Errorf("cannot update MutatingWebhookConfiguration %s: %w", configuration.Name, err)
		}
		handle.Printf("MutatingWebhookConfiguration %s is restored.", configuration.Name)
	}
	ref, present, err := recordedWorkload(ctx, handle, ComponentWebhook)
	if err != nil || !present {
		return err
	}
	if err := revertWorkloadSpec(ctx, handle, ref, webhookSpecKey); err != nil {
		return err
	}
	return waitForWorkload(ctx, handle, ref)
}

func snapshotWorkloadSpec(ctx context.Context, handle *cluster.Handle, object *workload, key string) error {
	if _, found, err := handle.LoadSnapshot(ctx, key); err != nil || found {
		return err
	}
	spec, err := object.spec()
	if err != nil {
		return err
	}
	return handle.SaveSnapshot(ctx, key, spec)
}

func revertWorkloadSpec(ctx context.Context, handle *cluster.Handle, ref cluster.WorkloadRef, key string) error {
	snapshot, found, err := handle.LoadSnapshot(ctx, key)
	if err != nil || !found {
		return err
	}
	object, err := loadWorkload(ctx, handle, ref)
	if err != nil {
		return err
	}
	if err := object.setSpec(snapshot); err != nil {
		return err
	}
	return object.update(ctx, handle)
}

func snapshotWebhookConfiguration(ctx context.Context, handle *cluster.Handle, configuration *admissionregistrationv1.MutatingWebhookConfiguration) error {
	if _, found, err := handle.LoadSnapshot(ctx, webhookConfigurationKey); err != nil || found {
		return err
	}
	encoded, err := json.Marshal(&admissionregistrationv1.MutatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{Name: configuration.Name},
		Webhooks:   configuration.Webhooks,
	})
	if err != nil {
		return fmt.Errorf("the webhook configuration cannot be serialised: %w", err)
	}
	return handle.SaveSnapshot(ctx, webhookConfigurationKey, string(encoded))
}

func webhookClientConfig(configuration *admissionregistrationv1.MutatingWebhookConfiguration) (string, string, []byte, error) {
	for _, webhook := range configuration.Webhooks {
		service := webhook.ClientConfig.Service
		if service == nil || len(webhook.ClientConfig.CABundle) == 0 {
			continue
		}
		return service.Name, service.Namespace, webhook.ClientConfig.CABundle, nil
	}
	return "", "", nil, fmt.Errorf("MutatingWebhookConfiguration %s names no Service with a CA bundle, and this step keeps the certificates of the 4.3 webhook", configuration.Name)
}

func webhookConfiguration(service, namespace string, caBundle []byte) ([]admissionregistrationv1.MutatingWebhook, error) {
	values := webhookManifestValues{Service: service, Namespace: namespace, CaBundle: base64.StdEncoding.EncodeToString(caBundle)}
	raw, err := webhookManifests.ReadFile(webhookManifestDir + "/webhookconfiguration.yaml")
	if err != nil {
		return nil, fmt.Errorf("the assistant carries no webhook configuration manifest: %w", err)
	}
	parsed, err := template.New("webhookconfiguration.yaml").Option("missingkey=error").Parse(string(raw))
	if err != nil {
		return nil, fmt.Errorf("the embedded webhook configuration manifest is malformed: %w", err)
	}
	rendered := &bytes.Buffer{}
	if err := parsed.Execute(rendered, values); err != nil {
		return nil, fmt.Errorf("cannot render the embedded webhook configuration manifest: %w", err)
	}
	configuration := &admissionregistrationv1.MutatingWebhookConfiguration{}
	if err := yaml.UnmarshalStrict(rendered.Bytes(), configuration); err != nil {
		return nil, fmt.Errorf("the embedded webhook configuration manifest is malformed: %w", err)
	}
	if configuration.Name != WebhookConfigurationName {
		return nil, fmt.Errorf("the embedded webhook configuration manifest is named %s instead of %s", configuration.Name, WebhookConfigurationName)
	}
	return configuration.Webhooks, nil
}

func discoverWebhookConfiguration(ctx context.Context, handle *cluster.Handle) (*admissionregistrationv1.MutatingWebhookConfiguration, error) {
	configurations, err := handle.Kube.AdmissionregistrationV1().MutatingWebhookConfigurations().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("cannot list the MutatingWebhookConfigurations of the cluster: %w", err)
	}
	candidates := []*admissionregistrationv1.MutatingWebhookConfiguration{}
	names := []string{}
	for index := range configurations.Items {
		if !validatesDanmGroups(&configurations.Items[index]) {
			continue
		}
		candidates = append(candidates, &configurations.Items[index])
		names = append(names, configurations.Items[index].Name)
	}
	switch len(candidates) {
	case 1:
		return candidates[0], nil
	case 0:
		return nil, fmt.Errorf("no MutatingWebhookConfiguration validates the %s or %s group, and the 4.3 webhook configuration, %s by default, is what this step replaces", OldGroup, NewGroup, WebhookConfigurationName)
	}
	for _, candidate := range candidates {
		if candidate.Name == WebhookConfigurationName {
			return candidate, nil
		}
	}
	return nil, fmt.Errorf("%d MutatingWebhookConfigurations validate the %s or %s group, and none is named %s, so this step cannot tell which one is DANM's: %s",
		len(candidates), OldGroup, NewGroup, WebhookConfigurationName, strings.Join(names, ", "))
}

func validatesDanmGroups(configuration *admissionregistrationv1.MutatingWebhookConfiguration) bool {
	for _, webhook := range configuration.Webhooks {
		for _, rule := range webhook.Rules {
			for _, group := range rule.APIGroups {
				if group == OldGroup || group == NewGroup {
					return true
				}
			}
		}
	}
	return false
}

func webhookConfigurationCurrent(ctx context.Context, handle *cluster.Handle) (bool, error) {
	existing, err := discoverWebhookConfiguration(ctx, handle)
	if err != nil {
		return false, nil
	}
	service, namespace, caBundle, err := webhookClientConfig(existing)
	if err != nil {
		return false, nil
	}
	desired, err := webhookConfiguration(service, namespace, caBundle)
	if err != nil {
		return false, err
	}
	return equality.Semantic.DeepEqual(existing.Webhooks, desired), nil
}
