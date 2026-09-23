package depset

import (
	"context"
	"log"

	"github.com/danm-cni/danm-utils/types/poltypes"
	danmv1 "github.com/danm-cni/danm/crd/apis/danm/v1"
	danmclientset "github.com/danm-cni/danm/crd/client/clientset/versioned"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func NewDanmEpSet(danmClient danmclientset.Interface, pod *corev1.Pod) *poltypes.DanmEpSet {
	var depSet poltypes.DanmEpSet
	deps, err := danmClient.DanmV1().DanmEps(pod.ObjectMeta.Namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		log.Println("ERROR: can't list DANM DanmEps API because:" + err.Error())
		return &depSet
	}
	depSet.DanmEpsByLabel, depSet.DanmEpsByNetwork, depSet.PodEps = sortDeps(deps.Items, pod)
	return &depSet
}

func sortDeps(deps []danmv1.DanmEp, pod *corev1.Pod) (poltypes.DanmEpBuckets, poltypes.DanmEpBuckets, []danmv1.DanmEp) {
	depPodLabelBuckets := make(poltypes.DanmEpBuckets, 0)
	depNetworkBuckets := make(poltypes.DanmEpBuckets, 0)
	depUidCache := make(map[string]poltypes.UidCache, 0)
	podEps := make([]danmv1.DanmEp, 0)
	for _, dep := range deps {
		if dep.Spec.PodUID == pod.ObjectMeta.UID {
			podEps = append(podEps, dep)
		}
		networkBucketName := dep.Spec.NetworkName + dep.Spec.ApiType
		if dep.Spec.ApiType == "" {
			networkBucketName += poltypes.DanmNetKind
		}
		depNetworkBuckets[networkBucketName] = append(depNetworkBuckets[networkBucketName], dep)
		for key, value := range dep.ObjectMeta.Labels {
			if _, ok := depUidCache[key+value+poltypes.CustomBucketPostfix][dep.ObjectMeta.UID]; !ok {
				if depUidCache[key+value+poltypes.CustomBucketPostfix] == nil {
					cache := make(poltypes.UidCache, 0)
					depUidCache[key+value+poltypes.CustomBucketPostfix] = cache
				}
				depUidCache[key+value+poltypes.CustomBucketPostfix][dep.ObjectMeta.UID] = true
				depPodLabelBuckets[key+value+poltypes.CustomBucketPostfix] = append(depPodLabelBuckets[key+value+poltypes.CustomBucketPostfix], dep)
			}
		}
	}
	return depPodLabelBuckets, depNetworkBuckets, podEps
}
