package v43to44

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"text/template"
	"time"

	"github.com/danm-cni/danm-utils/pkg/dma/cluster"
	"github.com/danm-cni/danm-utils/pkg/dma/engine"
	danmtypes "github.com/danm-cni/danm/crd/apis/danm/v1"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/yaml"
)

const (
	CniCopierName   = "dma-cni"
	CniCanaryName   = "dma-cni-canary"
	CniHostPath     = "/opt/cni/bin"
	cniManifestDir  = "manifests/cni"
	cniInstalledKey = "cni-binaries"
	cniModeInstall  = "install"
	cniModeRestore  = "restore"
)

//go:embed manifests/cni
var cniManifests embed.FS

var cniPollInterval = 2 * time.Second

type cniManifestValues struct {
	Namespace  string
	Image      string
	PullPolicy string
	PullSecret string
	Payload    string
	Suffix     string
	Mode       string
	NodeName   string
	HostPath   string
}

type installCniBinaries struct{}

func (installCniBinaries) Meta() engine.StepMeta {
	return engine.StepMeta{
		ID:          12,
		Name:        "install-cni-binaries",
		Description: "Install the DANM 4.4 CNI binaries on every node.",
		Phase:       engine.PhaseCutover,
		Reversible:  true,
	}
}

func (installCniBinaries) Detect(ctx context.Context, handle *cluster.Handle) (engine.StepState, error) {
	running, err := cniObjectsPresent(ctx, handle)
	if err != nil {
		return engine.StateNotStarted, err
	}
	_, installed, err := handle.LoadSnapshot(ctx, cniInstalledKey)
	if err != nil {
		return engine.StateNotStarted, err
	}
	switch {
	case installed && !running:
		return engine.StateDone, nil
	case !installed && !running:
		return engine.StateNotStarted, nil
	}
	return engine.StatePartial, nil
}

func (installCniBinaries) Execute(ctx context.Context, handle *cluster.Handle) error {
	image, err := cniImage(ctx, handle)
	if err != nil {
		return err
	}
	if err := deleteCniObjects(ctx, handle); err != nil {
		return err
	}
	if !handle.Options.NoCanary {
		if err := runCanary(ctx, handle, image); err != nil {
			return err
		}
	} else {
		handle.Warnf("the canary node is skipped, the binaries are installed on every node at once")
	}
	if err := runCopier(ctx, handle, image, cniModeInstall); err != nil {
		return err
	}
	if err := handle.SaveSnapshot(ctx, cniInstalledKey, cniBackupSuffix()); err != nil {
		return err
	}
	handle.Printf("The DANM %s CNI binaries are installed on every node, the previous ones are kept as %s.", TargetRelease, cniBackupSuffix())
	return nil
}

func (installCniBinaries) Rollback(ctx context.Context, handle *cluster.Handle) error {
	image, err := cniImage(ctx, handle)
	if err != nil {
		return err
	}
	if err := deleteCniObjects(ctx, handle); err != nil {
		return err
	}
	if err := runCopier(ctx, handle, image, cniModeRestore); err != nil {
		return err
	}
	handle.Printf("The CNI binaries backed up as %s are restored on every node.", cniBackupSuffix())
	return nil
}

func cniBackupSuffix() string {
	return ".dma-bak-" + PlanID
}

func cniImage(ctx context.Context, handle *cluster.Handle) (cluster.ComponentImage, error) {
	policy := handle.Images.PullPolicy
	if policy == "" {
		policy = corev1.PullIfNotPresent
	}
	if handle.Options.BinaryImage != "" {
		return cluster.ComponentImage{Image: handle.Options.BinaryImage, PullPolicy: policy}, nil
	}
	return ownImage(ctx, handle)
}

func cniValues(handle *cluster.Handle, image cluster.ComponentImage, mode string) cniManifestValues {
	return cniManifestValues{
		Namespace:  handle.Namespace,
		Image:      image.Image,
		PullPolicy: string(image.PullPolicy),
		PullSecret: handle.Images.PullSecret,
		Payload:    handle.PayloadDir(PayloadName),
		Suffix:     cniBackupSuffix(),
		Mode:       mode,
		HostPath:   CniHostPath,
	}
}

func cniManifest[T any](file, name string, values cniManifestValues) (*T, error) {
	raw, err := cniManifests.ReadFile(cniManifestDir + "/" + file)
	if err != nil {
		return nil, fmt.Errorf("the assistant carries no CNI manifest %s: %w", file, err)
	}
	parsed, err := template.New(file).Option("missingkey=error").Parse(string(raw))
	if err != nil {
		return nil, fmt.Errorf("the embedded CNI manifest %s is malformed: %w", file, err)
	}
	rendered := &bytes.Buffer{}
	if err := parsed.Execute(rendered, values); err != nil {
		return nil, fmt.Errorf("cannot render the embedded CNI manifest %s: %w", file, err)
	}
	meta := &metav1.PartialObjectMetadata{}
	if err := yaml.Unmarshal(rendered.Bytes(), meta); err != nil {
		return nil, fmt.Errorf("the embedded CNI manifest %s is malformed: %w", file, err)
	}
	if meta.Name != name {
		return nil, fmt.Errorf("the embedded CNI manifest %s is named %s instead of %s", file, meta.Name, name)
	}
	object := new(T)
	if err := yaml.UnmarshalStrict(rendered.Bytes(), object); err != nil {
		return nil, fmt.Errorf("the embedded CNI manifest %s is malformed: %w", file, err)
	}
	return object, nil
}

func withPullSecret(template *corev1.PodTemplateSpec, secret string) {
	if secret == "" {
		return
	}
	template.Spec.ImagePullSecrets = []corev1.LocalObjectReference{{Name: secret}}
}

func runCopier(ctx context.Context, handle *cluster.Handle, image cluster.ComponentImage, mode string) error {
	desired, err := cniManifest[appsv1.DaemonSet]("copier.yaml", CniCopierName, cniValues(handle, image, mode))
	if err != nil {
		return err
	}
	withPullSecret(&desired.Spec.Template, handle.Images.PullSecret)
	daemonSets := handle.Kube.AppsV1().DaemonSets(handle.Namespace)
	if _, err := daemonSets.Create(ctx, desired, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("cannot create DaemonSet %s: %w", objectName(handle.Namespace, CniCopierName), err)
	}
	handle.Printf("The CNI binary copier runs on every node in %s mode.", mode)
	waitErr := waitForWorkload(ctx, handle, cluster.WorkloadRef{Kind: cluster.WorkloadDaemonSet, Namespace: handle.Namespace, Name: CniCopierName})
	if waitErr != nil {
		handle.Warnf("the CNI binary copier is left in place for inspection, delete DaemonSet %s once the problem is understood", objectName(handle.Namespace, CniCopierName))
		return waitErr
	}
	return deleteCniObjects(ctx, handle)
}

func runCanary(ctx context.Context, handle *cluster.Handle, image cluster.ComponentImage) error {
	node, err := canaryNode(ctx, handle)
	if err != nil {
		return err
	}
	network, namespace, err := canaryNetwork(ctx, handle)
	if err != nil {
		return err
	}
	values := cniValues(handle, image, cniModeInstall)
	values.NodeName = node
	job, err := cniManifest[batchv1.Job]("canary.yaml", CniCanaryName, values)
	if err != nil {
		return err
	}
	withPullSecret(&job.Spec.Template, handle.Images.PullSecret)
	if _, err := handle.Kube.BatchV1().Jobs(handle.Namespace).Create(ctx, job, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("cannot create Job %s: %w", objectName(handle.Namespace, CniCanaryName), err)
	}
	handle.Printf("The CNI binaries are installed on the canary node %s.", node)
	if err := waitForCanaryJob(ctx, handle); err != nil {
		return err
	}
	if err := runCanaryPod(ctx, handle, image, node, network, namespace); err != nil {
		return err
	}
	handle.Printf("The canary Pod of node %s received an address from %s, the rollout continues.", node, network)
	return deleteCniObjects(ctx, handle)
}

func runCanaryPod(ctx context.Context, handle *cluster.Handle, image cluster.ComponentImage, node, network, namespace string) error {
	values := cniValues(handle, image, cniModeInstall)
	values.NodeName = node
	values.Namespace = namespace
	pod, err := cniManifest[corev1.Pod]("canarypod.yaml", CniCanaryName, values)
	if err != nil {
		return err
	}
	if handle.Images.PullSecret != "" {
		pod.Spec.ImagePullSecrets = []corev1.LocalObjectReference{{Name: handle.Images.PullSecret}}
	}
	interfaces, err := canaryInterfaces(handle.Mode, network)
	if err != nil {
		return err
	}
	pod.Annotations = map[string]string{NewInterfacesKey: interfaces}
	pods := handle.Kube.CoreV1().Pods(namespace)
	if _, err := pods.Create(ctx, pod, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("cannot create the canary Pod %s: %w", objectName(namespace, CniCanaryName), err)
	}
	defer pods.Delete(context.WithoutCancel(ctx), CniCanaryName, metav1.DeleteOptions{})
	problem := ""
	err = wait.PollUntilContextTimeout(ctx, cniPollInterval, handle.Options.RolloutTimeout, true, func(ctx context.Context) (bool, error) {
		running, err := pods.Get(ctx, CniCanaryName, metav1.GetOptions{})
		if err != nil {
			return false, nil
		}
		problem = canaryProblem(running)
		return running.Status.Phase == corev1.PodRunning, nil
	})
	if wait.Interrupted(err) {
		return fmt.Errorf("the canary Pod on node %s did not start with the DANM %s binaries: %s", node, TargetRelease, orNone(problem))
	}
	if err != nil {
		return err
	}
	return verifyCanaryAddress(ctx, handle, namespace, network)
}

func canaryProblem(pod *corev1.Pod) string {
	for _, condition := range pod.Status.Conditions {
		if condition.Status != corev1.ConditionTrue && condition.Message != "" {
			return condition.Message
		}
	}
	return string(pod.Status.Phase)
}

func verifyCanaryAddress(ctx context.Context, handle *cluster.Handle, namespace, network string) error {
	found := false
	err := eachObject(ctx, handle, kindDanmEp.NewResource(), func(item *unstructured.Unstructured) error {
		endpoint := &danmtypes.DanmEp{}
		if err := decodeObjectInto(kindDanmEp, item, endpoint); err != nil {
			return err
		}
		if endpoint.Namespace != namespace || endpoint.Spec.Pod != CniCanaryName || endpoint.Spec.NetworkName != network {
			return nil
		}
		if endpoint.Spec.Iface.Address == "" {
			return fmt.Errorf("the canary Pod is attached to %s without an address, the DANM %s CNI binaries are not working", network, TargetRelease)
		}
		found = true
		return nil
	})
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("the canary Pod received no DanmEp in the %s group, the DANM %s CNI binaries are not working", NewGroup, TargetRelease)
	}
	return nil
}

func canaryInterfaces(mode cluster.Mode, network string) (string, error) {
	entry := map[string]string{"network": network}
	if mode == cluster.ModeProduction {
		entry = map[string]string{tenantNetworkKey: network}
	}
	encoded, err := json.Marshal([]map[string]string{entry})
	if err != nil {
		return "", fmt.Errorf("the canary Pod annotation cannot be built: %w", err)
	}
	return string(encoded), nil
}

func canaryNode(ctx context.Context, handle *cluster.Handle) (string, error) {
	nodes, err := handle.Kube.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return "", fmt.Errorf("cannot list the nodes of the cluster: %w", err)
	}
	for _, node := range nodes.Items {
		if node.Spec.Unschedulable {
			continue
		}
		for _, condition := range node.Status.Conditions {
			if condition.Type == corev1.NodeReady && condition.Status == corev1.ConditionTrue {
				return node.Name, nil
			}
		}
	}
	return "", errors.New("no node of the cluster is ready and schedulable, there is nowhere to try the new binaries")
}

func canaryNetwork(ctx context.Context, handle *cluster.Handle) (string, string, error) {
	kind := kindDanmNet
	if handle.Mode == cluster.ModeProduction {
		kind = kindTenantNetwork
	}
	name, namespace := "", ""
	err := eachObject(ctx, handle, kind.NewResource(), func(item *unstructured.Unstructured) error {
		if name != "" {
			return nil
		}
		network := &danmtypes.DanmNet{}
		if err := decodeObjectInto(kind, item, network); err != nil {
			return err
		}
		if network.Spec.Options.Alloc == "" || network.Spec.Options.Cidr == "" {
			return nil
		}
		name, namespace = network.Name, network.Namespace
		return nil
	})
	if err != nil {
		return "", "", err
	}
	if name == "" {
		return "", "", fmt.Errorf("the %s group holds no %s with an IPv4 allocation, so the new binaries cannot be tried on a canary Pod; run with --no-canary to skip the trial", NewGroup, kind.Kind)
	}
	return name, namespace, nil
}

func waitForCanaryJob(ctx context.Context, handle *cluster.Handle) error {
	timeout := handle.Options.RolloutTimeout
	if timeout <= 0 {
		timeout = 10 * time.Minute
	}
	err := wait.PollUntilContextTimeout(ctx, cniPollInterval, timeout, true, func(ctx context.Context) (bool, error) {
		job, err := handle.Kube.BatchV1().Jobs(handle.Namespace).Get(ctx, CniCanaryName, metav1.GetOptions{})
		if err != nil {
			return false, nil
		}
		if job.Status.Failed > 0 {
			return false, fmt.Errorf("the CNI binary copier failed on the canary node, inspect Job %s", objectName(handle.Namespace, CniCanaryName))
		}
		return job.Status.Succeeded > 0, nil
	})
	if wait.Interrupted(err) {
		return fmt.Errorf("the CNI binary copier did not finish on the canary node within %s", timeout)
	}
	return err
}

func cniObjectsPresent(ctx context.Context, handle *cluster.Handle) (bool, error) {
	_, err := handle.Kube.AppsV1().DaemonSets(handle.Namespace).Get(ctx, CniCopierName, metav1.GetOptions{})
	if err == nil {
		return true, nil
	}
	if !apierrors.IsNotFound(err) {
		return false, fmt.Errorf("cannot read DaemonSet %s: %w", objectName(handle.Namespace, CniCopierName), err)
	}
	_, err = handle.Kube.BatchV1().Jobs(handle.Namespace).Get(ctx, CniCanaryName, metav1.GetOptions{})
	if err == nil {
		return true, nil
	}
	if !apierrors.IsNotFound(err) {
		return false, fmt.Errorf("cannot read Job %s: %w", objectName(handle.Namespace, CniCanaryName), err)
	}
	return false, nil
}

func deleteCniObjects(ctx context.Context, handle *cluster.Handle) error {
	policy := metav1.DeletePropagationForeground
	options := metav1.DeleteOptions{PropagationPolicy: &policy}
	if err := handle.Kube.AppsV1().DaemonSets(handle.Namespace).Delete(ctx, CniCopierName, options); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("cannot delete DaemonSet %s: %w", objectName(handle.Namespace, CniCopierName), err)
	}
	if err := handle.Kube.BatchV1().Jobs(handle.Namespace).Delete(ctx, CniCanaryName, options); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("cannot delete Job %s: %w", objectName(handle.Namespace, CniCanaryName), err)
	}
	return waitForCniObjectsGone(ctx, handle)
}

func waitForCniObjectsGone(ctx context.Context, handle *cluster.Handle) error {
	err := wait.PollUntilContextTimeout(ctx, cniPollInterval, time.Minute, true, func(ctx context.Context) (bool, error) {
		present, err := cniObjectsPresent(ctx, handle)
		return !present, err
	})
	if wait.Interrupted(err) {
		return fmt.Errorf("the CNI binary copier objects of %s are not gone yet", handle.Namespace)
	}
	return err
}
