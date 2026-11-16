package v43to44

import (
	"bytes"
	"context"
	"io"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/danm-cni/danm-utils/pkg/dma/cluster"
	"github.com/danm-cni/danm-utils/pkg/dma/engine"
	"github.com/danm-cni/danm-utils/pkg/dma/state"
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	kubefake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func webhookDeployment(image string) *appsv1.Deployment {
	replicas := int32(1)
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: DanmNamespace, Name: "danm-webhook-deployment"},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{
				{Name: "danm-webhook", Image: image},
				{Name: "sidecar", Image: "registry.example.com/sidecar:1.0"},
			}}},
		},
		Status: appsv1.DeploymentStatus{UpdatedReplicas: 1, AvailableReplicas: 1, ReadyReplicas: 1, Replicas: 1},
	}
}

func legacyWebhookConfiguration() *admissionregistrationv1.MutatingWebhookConfiguration {
	policy := admissionregistrationv1.Fail
	sideEffects := admissionregistrationv1.SideEffectClassNone
	return &admissionregistrationv1.MutatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{Name: WebhookConfigurationName},
		Webhooks: []admissionregistrationv1.MutatingWebhook{{
			Name: "danm-netvalidation.danm.k8s.io",
			ClientConfig: admissionregistrationv1.WebhookClientConfig{
				Service:  &admissionregistrationv1.ServiceReference{Namespace: DanmNamespace, Name: "danm-webhook-svc"},
				CABundle: []byte("legacy-ca"),
			},
			Rules: []admissionregistrationv1.RuleWithOperations{{
				Operations: []admissionregistrationv1.OperationType{admissionregistrationv1.Create},
				Rule:       admissionregistrationv1.Rule{APIGroups: []string{OldGroup}, APIVersions: []string{Version}, Resources: []string{"tenantnetworks"}},
			}},
			FailurePolicy:           &policy,
			SideEffects:             &sideEffects,
			AdmissionReviewVersions: []string{"v1beta1"},
		}},
	}
}

func webhookHandle(t *testing.T, ready bool, objects ...k8sruntime.Object) *cluster.Handle {
	t.Helper()
	interval := workloadPollInterval
	workloadPollInterval = 5 * time.Millisecond
	t.Cleanup(func() { workloadPollInterval = interval })
	kube := kubefake.NewSimpleClientset(objects...)
	kube.PrependReactor("update", "deployments", func(action k8stesting.Action) (bool, k8sruntime.Object, error) {
		deployment := action.(k8stesting.UpdateAction).GetObject().(*appsv1.Deployment)
		if ready {
			deployment.Status = appsv1.DeploymentStatus{UpdatedReplicas: 1, AvailableReplicas: 1, ReadyReplicas: 1, Replicas: 1}
		} else {
			deployment.Status = appsv1.DeploymentStatus{UpdatedReplicas: 1, UnavailableReplicas: 1, Replicas: 1}
		}
		return false, nil, nil
	})
	handle := &cluster.Handle{
		Kube:      kube,
		State:     state.NewStore(kube, state.DefaultNamespace, state.DefaultName),
		Namespace: state.DefaultNamespace,
		Mode:      cluster.ModeProduction,
		Images:    cluster.ImageConfig{RegistryPrefix: "registry.example.com/", Tag: "4.4"},
		Operator:  "tester",
		Options:   cluster.Options{RolloutTimeout: 50 * time.Millisecond},
		Log:       log.New(io.Discard, "", 0),
		Out:       &bytes.Buffer{},
	}
	if _, err := handle.State.Mutate(context.Background(), PlanID, func(record *state.PlanRecord) error {
		record.Workloads = map[string]state.WorkloadRecord{
			ComponentWebhook: cluster.RecordWorkload(cluster.WorkloadRef{Kind: cluster.WorkloadDeployment, Namespace: DanmNamespace, Name: "danm-webhook-deployment"}, true, false),
		}
		record.UpsertStep(state.StepRecord{ID: 10, Name: "deploy-new-webhook", Status: state.StatusRunning})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	handle.BeginStep(PlanID, 10)
	return handle
}

func detectWebhookStep(t *testing.T, handle *cluster.Handle) engine.StepState {
	t.Helper()
	detected, err := deployNewWebhook{}.Detect(context.Background(), handle)
	if err != nil {
		t.Fatalf("detecting step 10 failed: %v", err)
	}
	return detected
}

func readWebhookConfiguration(t *testing.T, handle *cluster.Handle) *admissionregistrationv1.MutatingWebhookConfiguration {
	t.Helper()
	configuration, err := handle.Kube.AdmissionregistrationV1().MutatingWebhookConfigurations().Get(context.Background(), WebhookConfigurationName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return configuration
}

func TestDeployNewWebhook(t *testing.T) {
	handle := webhookHandle(t, true, webhookDeployment("registry.example.com/webhook:4.3"), legacyWebhookConfiguration())
	ctx := context.Background()
	if detectWebhookStep(t, handle) != engine.StateNotStarted {
		t.Fatal("step 10 does not read as not started before it ran")
	}
	if err := (deployNewWebhook{}).Execute(ctx, handle); err != nil {
		t.Fatal(err)
	}
	if detectWebhookStep(t, handle) != engine.StateDone {
		t.Fatal("step 10 does not read as done after it ran")
	}
	deployment, err := handle.Kube.AppsV1().Deployments(DanmNamespace).Get(ctx, "danm-webhook-deployment", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if deployment.Spec.Template.Spec.Containers[0].Image != "registry.example.com/webhook:4.4" {
		t.Fatalf("the webhook runs %s", deployment.Spec.Template.Spec.Containers[0].Image)
	}
	if deployment.Spec.Template.Spec.Containers[1].Image != "registry.example.com/sidecar:1.0" {
		t.Fatalf("a container beside the webhook was rolled: %s", deployment.Spec.Template.Spec.Containers[1].Image)
	}
	configuration := readWebhookConfiguration(t, handle)
	if len(configuration.Webhooks) != 4 {
		t.Fatalf("the configuration carries %d hooks", len(configuration.Webhooks))
	}
	names := []string{}
	for _, webhook := range configuration.Webhooks {
		names = append(names, webhook.Name)
		if webhook.Rules[0].APIGroups[0] != NewGroup {
			t.Fatalf("hook %s is scoped to %v", webhook.Name, webhook.Rules[0].APIGroups)
		}
		if string(webhook.ClientConfig.CABundle) != "legacy-ca" {
			t.Fatalf("hook %s carries the CA bundle %q", webhook.Name, webhook.ClientConfig.CABundle)
		}
		if webhook.ClientConfig.Service.Name != "danm-webhook-svc" || webhook.ClientConfig.Service.Namespace != DanmNamespace {
			t.Fatalf("hook %s points at %+v", webhook.Name, webhook.ClientConfig.Service)
		}
		if webhook.SideEffects == nil || len(webhook.AdmissionReviewVersions) == 0 {
			t.Fatalf("hook %s has no side effects or review versions", webhook.Name)
		}
	}
	joined := strings.Join(names, ",")
	if joined != "danm-netvalidation.danm.io,danm-configvalidation.danm.io,danm-netdeletion.danm.io,reservedipvalidation.danm.io" {
		t.Fatalf("the hooks are %s", joined)
	}
	if err := (deployNewWebhook{}).Rollback(ctx, handle); err != nil {
		t.Fatal(err)
	}
	if detectWebhookStep(t, handle) != engine.StateNotStarted {
		t.Fatal("step 10 does not read as not started after its rollback")
	}
	restored := readWebhookConfiguration(t, handle)
	if len(restored.Webhooks) != 1 || restored.Webhooks[0].Name != "danm-netvalidation.danm.k8s.io" {
		t.Fatalf("the rollback left the hooks at %+v", restored.Webhooks)
	}
	deployment, err = handle.Kube.AppsV1().Deployments(DanmNamespace).Get(ctx, "danm-webhook-deployment", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if deployment.Spec.Template.Spec.Containers[0].Image != "registry.example.com/webhook:4.3" {
		t.Fatalf("the rollback left the webhook at %s", deployment.Spec.Template.Spec.Containers[0].Image)
	}
}

func TestDeployNewWebhookRevertsOnATimeout(t *testing.T) {
	handle := webhookHandle(t, false, webhookDeployment("registry.example.com/webhook:4.3"), legacyWebhookConfiguration())
	ctx := context.Background()
	err := (deployNewWebhook{}).Execute(ctx, handle)
	if err == nil || !strings.Contains(err.Error(), "not ready") {
		t.Fatalf("a webhook which never became ready was accepted: %v", err)
	}
	deployment, getErr := handle.Kube.AppsV1().Deployments(DanmNamespace).Get(ctx, "danm-webhook-deployment", metav1.GetOptions{})
	if getErr != nil {
		t.Fatal(getErr)
	}
	if deployment.Spec.Template.Spec.Containers[0].Image != "registry.example.com/webhook:4.3" {
		t.Fatalf("the failed roll was not reverted, the webhook runs %s", deployment.Spec.Template.Spec.Containers[0].Image)
	}
	if configuration := readWebhookConfiguration(t, handle); len(configuration.Webhooks) != 1 {
		t.Fatal("the configuration was replaced although the roll failed")
	}
}

func TestDeployNewWebhookDiscoversARenamedConfiguration(t *testing.T) {
	configuration := legacyWebhookConfiguration()
	configuration.Name = "acme-danm-admission"
	handle := webhookHandle(t, true, webhookDeployment("registry.example.com/webhook:4.3"), configuration)
	ctx := context.Background()
	if err := (deployNewWebhook{}).Execute(ctx, handle); err != nil {
		t.Fatal(err)
	}
	if detectWebhookStep(t, handle) != engine.StateDone {
		t.Fatal("step 10 does not read as done after it rolled a renamed configuration")
	}
	renamed, err := handle.Kube.AdmissionregistrationV1().MutatingWebhookConfigurations().Get(ctx, "acme-danm-admission", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(renamed.Webhooks) != 4 {
		t.Fatalf("the renamed configuration carries %d hooks", len(renamed.Webhooks))
	}
	if err := (deployNewWebhook{}).Rollback(ctx, handle); err != nil {
		t.Fatal(err)
	}
	restored, err := handle.Kube.AdmissionregistrationV1().MutatingWebhookConfigurations().Get(ctx, "acme-danm-admission", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(restored.Webhooks) != 1 {
		t.Fatalf("the rollback left the renamed configuration at %+v", restored.Webhooks)
	}
}

func TestDeployNewWebhookRefusesAnAmbiguousConfiguration(t *testing.T) {
	first := legacyWebhookConfiguration()
	first.Name = "acme-danm-admission"
	second := legacyWebhookConfiguration()
	second.Name = "other-danm-admission"
	handle := webhookHandle(t, true, webhookDeployment("registry.example.com/webhook:4.3"), first, second)
	err := (deployNewWebhook{}).Execute(context.Background(), handle)
	if err == nil || !strings.Contains(err.Error(), "cannot tell which one") {
		t.Fatalf("two DANM webhook configurations were accepted: %v", err)
	}
}

func TestDeployNewWebhookRollsARenamedContainer(t *testing.T) {
	deployment := webhookDeployment("registry.example.com/webhook:4.3")
	deployment.Spec.Template.Spec.Containers[0].Name = "acme-webhook-server"
	handle := webhookHandle(t, true, deployment, legacyWebhookConfiguration())
	ctx := context.Background()
	if err := (deployNewWebhook{}).Execute(ctx, handle); err != nil {
		t.Fatal(err)
	}
	rolled, err := handle.Kube.AppsV1().Deployments(DanmNamespace).Get(ctx, "danm-webhook-deployment", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if rolled.Spec.Template.Spec.Containers[0].Image != "registry.example.com/webhook:4.4" {
		t.Fatalf("the renamed container runs %s", rolled.Spec.Template.Spec.Containers[0].Image)
	}
}

func TestDeployNewWebhookNeedsACaBundle(t *testing.T) {
	configuration := legacyWebhookConfiguration()
	configuration.Webhooks[0].ClientConfig.CABundle = nil
	handle := webhookHandle(t, true, webhookDeployment("registry.example.com/webhook:4.3"), configuration)
	err := (deployNewWebhook{}).Execute(context.Background(), handle)
	if err == nil || !strings.Contains(err.Error(), "CA bundle") {
		t.Fatalf("a configuration without a CA bundle was accepted: %v", err)
	}
}
