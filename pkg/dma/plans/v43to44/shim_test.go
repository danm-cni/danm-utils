package v43to44

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/danm-cni/danm-utils/pkg/dma/cluster"
	"github.com/danm-cni/danm-utils/pkg/dma/engine"
	"github.com/danm-cni/danm-utils/pkg/dma/state"
	danmtypes "github.com/danm-cni/danm/crd/apis/danm/v1"
	admissionv1 "k8s.io/api/admission/v1"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	kubefake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func shimHandle(t *testing.T, mode cluster.Mode, available bool, objects ...k8sruntime.Object) *cluster.Handle {
	interval := shimPollInterval
	shimPollInterval = 5 * time.Millisecond
	t.Cleanup(func() { shimPollInterval = interval })
	kube := kubefake.NewSimpleClientset(objects...)
	markAvailable := func(action k8stesting.Action) (bool, k8sruntime.Object, error) {
		var deployment *appsv1.Deployment
		switch typed := action.(type) {
		case k8stesting.CreateAction:
			deployment = typed.GetObject().(*appsv1.Deployment)
		case k8stesting.UpdateAction:
			deployment = typed.GetObject().(*appsv1.Deployment)
		}
		if available && deployment != nil {
			deployment.Status = appsv1.DeploymentStatus{UpdatedReplicas: *deployment.Spec.Replicas, AvailableReplicas: *deployment.Spec.Replicas}
		}
		return false, nil, nil
	}
	kube.PrependReactor("create", "deployments", markAvailable)
	kube.PrependReactor("update", "deployments", markAvailable)
	listKinds := map[schema.GroupVersionResource]string{kindReservedIP.NewResource(): "ReservedIPList"}
	return &cluster.Handle{
		Kube:      kube,
		Dynamic:   dynamicfake.NewSimpleDynamicClientWithCustomListKinds(k8sruntime.NewScheme(), listKinds),
		State:     state.NewStore(kube, state.DefaultNamespace, state.DefaultName),
		Namespace: state.DefaultNamespace,
		Mode:      mode,
		Operator:  "tester",
		Options:   cluster.Options{ShimImage: "registry.example.com/dma:1.0", RolloutTimeout: 50 * time.Millisecond},
		Log:       log.New(io.Discard, "", 0),
		Out:       &bytes.Buffer{},
	}
}

func detectShim(t *testing.T, handle *cluster.Handle) engine.StepState {
	t.Helper()
	detected, err := installAnnotationShim{}.Detect(context.Background(), handle)
	if err != nil {
		t.Fatalf("detecting step 4 failed: %v", err)
	}
	return detected
}

func TestInstallAnnotationShimInProductionMode(t *testing.T) {
	handle := shimHandle(t, cluster.ModeProduction, true)
	ctx := context.Background()
	if detectShim(t, handle) != engine.StateNotStarted {
		t.Fatal("step 4 does not read as not started on a clean cluster")
	}
	if err := (installAnnotationShim{}).Execute(ctx, handle); err != nil {
		t.Fatal(err)
	}
	if detectShim(t, handle) != engine.StateDone {
		t.Fatal("step 4 does not read as done after it ran")
	}
	secret, err := handle.Kube.CoreV1().Secrets(handle.Namespace).Get(ctx, ShimTlsSecretName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !shimCertificatesUsable(secret, "dma-shim.kube-system.svc", time.Now()) {
		t.Fatal("the generated certificate does not serve the shim's Service")
	}
	configuration, err := handle.Kube.AdmissionregistrationV1().MutatingWebhookConfigurations().Get(ctx, ShimName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	webhook := configuration.Webhooks[0]
	if string(*webhook.FailurePolicy) != "Ignore" || !bytes.Equal(webhook.ClientConfig.CABundle, secret.Data[CaCertKey]) || webhook.Rules[0].Operations[0] != "CREATE" {
		t.Fatalf("the webhook configuration is not the expected one: %+v", webhook)
	}
	deployment, err := handle.Kube.AppsV1().Deployments(handle.Namespace).Get(ctx, ShimName, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	container := deployment.Spec.Template.Spec.Containers[0]
	if container.Image != "registry.example.com/dma:1.0" || !strings.Contains(strings.Join(container.Args, " "), "--mode production") {
		t.Fatalf("the shim container is %s %v", container.Image, container.Args)
	}
	if _, err := handle.Kube.RbacV1().ClusterRoles().Get(ctx, ShimRbacName, metav1.GetOptions{}); err != nil {
		t.Fatalf("the shim's ClusterRole is missing: %v", err)
	}
	if err := (installAnnotationShim{}).Execute(ctx, handle); err != nil {
		t.Fatal(err)
	}
	again, _ := handle.Kube.CoreV1().Secrets(handle.Namespace).Get(ctx, ShimTlsSecretName, metav1.GetOptions{})
	if !bytes.Equal(again.Data[CaCertKey], secret.Data[CaCertKey]) {
		t.Fatal("a usable certificate was regenerated on a rerun")
	}
	if err := (installAnnotationShim{}).Rollback(ctx, handle); err != nil {
		t.Fatal(err)
	}
	if detectShim(t, handle) != engine.StateNotStarted {
		t.Fatal("step 4 does not read as not started after its rollback")
	}
}

func TestInstallAnnotationShimInLightweightMode(t *testing.T) {
	handle := shimHandle(t, cluster.ModeLightweight, true)
	if err := (installAnnotationShim{}).Execute(context.Background(), handle); err != nil {
		t.Fatal(err)
	}
	if detectShim(t, handle) != engine.StateDone {
		t.Fatal("step 4 does not read as done in lightweight mode")
	}
	if _, err := handle.Kube.RbacV1().ClusterRoles().Get(context.Background(), ShimRbacName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("a ReservedIP ClusterRole was created in lightweight mode: %v", err)
	}
}

func TestInstallAnnotationShimNeverPointsAtAnUnavailableShim(t *testing.T) {
	handle := shimHandle(t, cluster.ModeProduction, false)
	err := (installAnnotationShim{}).Execute(context.Background(), handle)
	if err == nil || !strings.Contains(err.Error(), "not available") {
		t.Fatalf("an unavailable shim returned %v", err)
	}
	if _, err := handle.Kube.AdmissionregistrationV1().MutatingWebhookConfigurations().Get(context.Background(), ShimName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("the webhook configuration exists although the shim is unavailable: %v", err)
	}
	if detectShim(t, handle) != engine.StatePartial {
		t.Fatal("a half installed shim does not read as partial")
	}
}

func TestInstallAnnotationShimRegeneratesAStaleCertificate(t *testing.T) {
	handle := shimHandle(t, cluster.ModeLightweight, true)
	now := shimNow
	t.Cleanup(func() { shimNow = now })
	shimNow = func() time.Time { return time.Now().Add(-shimCertValidity + 24*time.Hour) }
	ctx := context.Background()
	if err := (installAnnotationShim{}).Execute(ctx, handle); err != nil {
		t.Fatal(err)
	}
	stale, _ := handle.Kube.CoreV1().Secrets(handle.Namespace).Get(ctx, ShimTlsSecretName, metav1.GetOptions{})
	shimNow = now
	if detectShim(t, handle) != engine.StateDone {
		t.Fatal("the shim does not read as done before its certificate is renewed")
	}
	if err := (installAnnotationShim{}).Execute(ctx, handle); err != nil {
		t.Fatal(err)
	}
	renewed, _ := handle.Kube.CoreV1().Secrets(handle.Namespace).Get(ctx, ShimTlsSecretName, metav1.GetOptions{})
	configuration, _ := handle.Kube.AdmissionregistrationV1().MutatingWebhookConfigurations().Get(ctx, ShimName, metav1.GetOptions{})
	if bytes.Equal(renewed.Data[CaCertKey], stale.Data[CaCertKey]) || !bytes.Equal(configuration.Webhooks[0].ClientConfig.CABundle, renewed.Data[CaCertKey]) {
		t.Fatal("a certificate expiring within a year was not renewed, or the webhook kept the old CA")
	}
}

func TestShimImageComesFromTheOwnPod(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "dma-0", Namespace: "kube-system"},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "sidecar", Image: "proxy"}, {Name: DmaContainerName, Image: "registry.example.com/dma:2.0"}}},
	}
	handle := shimHandle(t, cluster.ModeLightweight, true, pod)
	handle.Options.ShimImage = ""
	if _, err := shimImage(context.Background(), handle); err == nil || !strings.Contains(err.Error(), "--shim-image") {
		t.Fatalf("an unknown own Pod returned %v", err)
	}
	t.Setenv(PodNameEnv, "dma-0")
	t.Setenv(PodNamespaceEnv, "kube-system")
	image, err := shimImage(context.Background(), handle)
	if err != nil || image.Image != "registry.example.com/dma:2.0" {
		t.Fatalf("the own image resolved to %v, %v", image, err)
	}
}

func TestInstallAnnotationShimNeedsTheMode(t *testing.T) {
	handle := shimHandle(t, cluster.ModeUnknown, true)
	if err := (installAnnotationShim{}).Execute(context.Background(), handle); err == nil || !strings.Contains(err.Error(), "mode is unknown") {
		t.Fatalf("installing without a mode returned %v", err)
	}
}

type staticRips struct {
	rips   []*danmtypes.ReservedIP
	synced bool
}

func (source staticRips) ReservedIPs(string) ([]*danmtypes.ReservedIP, bool) {
	return source.rips, source.synced
}

func review(t *testing.T, handler http.Handler, pod *corev1.Pod) *admissionv1.AdmissionResponse {
	t.Helper()
	raw, err := json.Marshal(pod)
	if err != nil {
		t.Fatal(err)
	}
	request := admissionv1.AdmissionReview{
		TypeMeta: metav1.TypeMeta{APIVersion: "admission.k8s.io/v1", Kind: "AdmissionReview"},
		Request:  &admissionv1.AdmissionRequest{UID: types.UID("uid-1"), Namespace: "app", Object: k8sruntime.RawExtension{Raw: raw}},
	}
	body, _ := json.Marshal(request)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, ShimMutatePath, bytes.NewReader(body)))
	response := admissionv1.AdmissionReview{}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil || response.Response == nil {
		t.Fatalf("the shim answered %d %s", recorder.Code, recorder.Body.String())
	}
	if response.Response.UID != "uid-1" || !response.Response.Allowed {
		t.Fatalf("the shim did not allow the Pod: %+v", response.Response)
	}
	return response.Response
}

func patchOf(t *testing.T, response *admissionv1.AdmissionResponse) []jsonPatchOperation {
	t.Helper()
	if response.Patch == nil {
		return nil
	}
	operations := []jsonPatchOperation{}
	if err := json.Unmarshal(response.Patch, &operations); err != nil {
		t.Fatal(err)
	}
	return operations
}

func TestShimCopiesTheOldAnnotation(t *testing.T) {
	rips := []*danmtypes.ReservedIP{reservedIp("app", "db", "TenantNetwork", "internal", "", "10.0.0.5")}
	handler := shimHandler{source: staticRips{rips: rips, synced: true}, log: log.New(io.Discard, "", 0)}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "db-0", Annotations: map[string]string{
		OldInterfacesKey: `[{"tenantNetwork":"internal","ip":"10.0.0.5/24"},{"tenantNetwork":"internal","ip":"10.0.0.6/24"}]`,
	}}}
	operations := patchOf(t, review(t, handler, pod))
	if len(operations) != 2 || operations[0].Path != "/metadata/annotations/danm.io~1interfaces" || operations[1].Path != "/metadata/annotations/dma.danm.io~1interfaces-copied" {
		t.Fatalf("the patch is %+v", operations)
	}
	if operations[0].Value != `[{"tenantNetwork":"internal","ip":"db"},{"tenantNetwork":"internal","ip":"10.0.0.6/24"}]` || operations[1].Value != CopiedByShim {
		t.Fatalf("the patch sets %v and %v", operations[0].Value, operations[1].Value)
	}
	unsynced := shimHandler{source: staticRips{rips: rips}, log: log.New(io.Discard, "", 0)}
	operations = patchOf(t, review(t, unsynced, pod))
	if operations[0].Value != pod.Annotations[OldInterfacesKey] {
		t.Fatalf("an unsynced shim did not copy verbatim: %v", operations[0].Value)
	}
}

func TestShimLeavesOtherPodsAlone(t *testing.T) {
	handler := shimHandler{source: noReservedIPs{}, log: log.New(io.Discard, "", 0)}
	for _, annotations := range []map[string]string{
		nil,
		{"other": "x"},
		{OldInterfacesKey: `[{"network":"a"}]`, NewInterfacesKey: `[{"network":"b"}]`},
	} {
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "web", Annotations: annotations}}
		if response := review(t, handler, pod); response.Patch != nil {
			t.Errorf("Pod with annotations %v was patched: %s", annotations, response.Patch)
		}
	}
}

func templated(annotations map[string]string) corev1.PodTemplateSpec {
	return corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Annotations: annotations}}
}

func TestShimStatusListsUnconvertedTemplates(t *testing.T) {
	old := map[string]string{OldInterfacesKey: `[{"tenantNetwork":"internal","ip":"dynamic"}]`}
	both := map[string]string{OldInterfacesKey: `[{"network":"a"}]`, NewInterfacesKey: `[{"network":"a"}]`}
	owner := []metav1.OwnerReference{{Kind: "Deployment", Name: "web", APIVersion: "apps/v1", UID: "1"}}
	handle := shimHandle(t, cluster.ModeProduction, true,
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "app"}, Spec: appsv1.DeploymentSpec{Template: templated(old)}},
		&appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "done", Namespace: "app"}, Spec: appsv1.DeploymentSpec{Template: templated(both)}},
		&appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{Name: "web-1", Namespace: "app", OwnerReferences: owner}, Spec: appsv1.ReplicaSetSpec{Template: templated(old)}},
		&appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{Name: "bare", Namespace: "app"}, Spec: appsv1.ReplicaSetSpec{Template: templated(old)}},
		&batchv1.CronJob{ObjectMeta: metav1.ObjectMeta{Name: "nightly", Namespace: "ops"}, Spec: batchv1.CronJobSpec{JobTemplate: batchv1.JobTemplateSpec{Spec: batchv1.JobSpec{Template: templated(old)}}}},
	)
	templates, err := ShimStatus(context.Background(), handle)
	if err != nil {
		t.Fatal(err)
	}
	names := []string{}
	for _, template := range templates {
		names = append(names, template.Kind+" "+template.Namespace+"/"+template.Name)
	}
	if strings.Join(names, ",") != "Deployment app/web,ReplicaSet app/bare,CronJob ops/nightly" {
		t.Fatalf("listed %v", names)
	}
}

func TestRemoveShimRefusesDuringThePlan(t *testing.T) {
	handle := shimHandle(t, cluster.ModeLightweight, true)
	if err := (installAnnotationShim{}).Execute(context.Background(), handle); err != nil {
		t.Fatal(err)
	}
	handle.Options.Force = true
	err := RemoveShim(context.Background(), handle, &engine.TokenConfirmer{Supplied: RemoveShimToken, Out: io.Discard})
	if err == nil || !strings.Contains(err.Error(), "rolling back step 4") {
		t.Fatalf("removing the shim during the plan returned %v", err)
	}
	if detectShim(t, handle) != engine.StateDone {
		t.Fatal("the shim was touched although its removal was refused")
	}
}
