package cleaner

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/danm-cni/danm-utils/pkg/services"
	danmv1 "github.com/danm-cni/danm/crd/apis/danm/v1"
	danmclientset "github.com/danm-cni/danm/crd/client/clientset/versioned"
	danmscheme "github.com/danm-cni/danm/crd/client/clientset/versioned/scheme"
	danminformers "github.com/danm-cni/danm/crd/client/informers/externalversions/danm/v1"
	danmlisters "github.com/danm-cni/danm/crd/client/listers/danm/v1"
	"github.com/danm-cni/danm/pkg/danmep"
	"github.com/danm-cni/danm/pkg/ipam"
	"github.com/danm-cni/danm/pkg/netcontrol"
	corev1 "k8s.io/api/core/v1"
	k8serr "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	coreinformers "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes/scheme"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
)

const (
	podUIDIndex = "poduid"
	gracePeriod = 1 * time.Minute
	flannelType = "flannel"
)

type Cleaner struct {
	term           atomic.Pointer[term]
	PodLister      corelisters.PodLister
	PodSynced      cache.InformerSynced
	danmepInformer cache.SharedIndexInformer
	DanmEpLister   danmlisters.DanmEpLister
	DanmEpSynced   cache.InformerSynced
}

type term struct {
	stopCh     <-chan struct{}
	danmClient danmclientset.Interface
	workqueue  workqueue.TypedRateLimitingInterface[string]
}

func New(podInformer coreinformers.PodInformer, danmepInformer danminformers.DanmEpInformer) (*Cleaner, error) {
	if err := danmscheme.AddToScheme(scheme.Scheme); err != nil {
		return nil, fmt.Errorf("DANM API types could not be registered because: %w", err)
	}
	cleaner := &Cleaner{
		PodLister:      podInformer.Lister(),
		PodSynced:      podInformer.Informer().HasSynced,
		danmepInformer: danmepInformer.Informer(),
		DanmEpLister:   danmepInformer.Lister(),
		DanmEpSynced:   danmepInformer.Informer().HasSynced,
	}
	err := cleaner.danmepInformer.AddIndexers(cache.Indexers{podUIDIndex: indexByPodUID})
	if err != nil {
		return nil, fmt.Errorf("DanmEp indexers could not be added because: %w", err)
	}
	_, err = podInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		DeleteFunc: cleaner.delPod,
	})
	if err != nil {
		return nil, fmt.Errorf("Pod event handler could not be registered because: %w", err)
	}
	return cleaner, nil
}

func indexByPodUID(obj interface{}) ([]string, error) {
	dep, ok := obj.(*danmv1.DanmEp)
	if !ok {
		return nil, nil
	}
	return []string{podKey(dep.Namespace, dep.Spec.PodUID)}, nil
}

func podKey(namespace string, uid types.UID) string {
	return namespace + "/" + string(uid)
}

// WarmUp blocks until every cache of the Cleaner is synced, or stopCh closes.
func (c *Cleaner) WarmUp(stopCh <-chan struct{}) error {
	if ok := cache.WaitForCacheSync(stopCh, c.PodSynced, c.DanmEpSynced); !ok {
		return errors.New("failed to wait for caches to sync")
	}
	return nil
}

func (c *Cleaner) Run(stopCh <-chan struct{}, danmClient danmclientset.Interface, threadiness int) {
	defer runtime.HandleCrash()
	t := &term{
		stopCh:     stopCh,
		danmClient: danmClient,
		workqueue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{Name: "Endpoints"}),
	}
	c.term.Store(t)
	defer t.workqueue.ShutDown()
	defer c.term.CompareAndSwap(t, nil)
	log.Printf("INFO: Starting Cleaner event handler threads")
	for i := 0; i < threadiness; i++ {
		go wait.Until(func() { c.runWorker(t) }, time.Second, stopCh)
	}
	log.Printf("INFO: Starting Cleaner's periodic cleanup")
	go wait.Until(func() { c.cleanDanglingEps(t) }, 30*time.Second, stopCh)
	<-stopCh
	log.Printf("INFO: Shutting down Cleaner's event handler threads")
}

func (c *Cleaner) activeTerm() *term {
	t := c.term.Load()
	if t == nil || t.isOver() {
		return nil
	}
	return t
}

func (t *term) isOver() bool {
	select {
	case <-t.stopCh:
		return true
	default:
		return false
	}
}

func (c *Cleaner) cleanDanglingEps(t *term) {
	if t.isOver() {
		return
	}
	danmeps, err := c.filterDanmeps()
	if err != nil {
		log.Printf("WARNING: Periodic cleaning failed with error: %s", err.Error())
		return
	}
	podCache := make(map[types.UID]bool, 0)
	for _, dep := range danmeps {
		if doesPodExist, ok := podCache[dep.Spec.PodUID]; ok {
			if !doesPodExist {
				c.deleteInterface(t, dep)
			}
			continue
		}
		pod, err := c.PodLister.Pods(dep.ObjectMeta.Namespace).Get(dep.Spec.Pod)
		if k8serr.IsNotFound(err) || (err == nil && pod.ObjectMeta.UID != dep.Spec.PodUID) {
			c.deleteInterface(t, dep)
			podCache[dep.Spec.PodUID] = false
		} else {
			podCache[dep.Spec.PodUID] = true
		}
	}
}

func (c *Cleaner) filterDanmeps() ([]*danmv1.DanmEp, error) {
	danmeps, err := c.DanmEpLister.List(labels.Everything())
	if err != nil {
		return nil, err
	}
	var valids []*danmv1.DanmEp
	for _, dep := range danmeps {
		if isInGracePeriod(dep) {
			continue
		}
		valids = append(valids, dep)
	}
	return valids, nil
}

func isInGracePeriod(dep *danmv1.DanmEp) bool {
	return dep.GetCreationTimestamp().Time.After(time.Now().UTC().Add(-gracePeriod))
}

func (c *Cleaner) runWorker(t *term) {
	for c.processNextWorkItem(t) {
	}
}

func (c *Cleaner) processNextWorkItem(t *term) bool {
	key, shutdown := t.workqueue.Get()
	if shutdown {
		return false
	}
	defer t.workqueue.Done(key)
	if err := c.handleKey(t, key); err != nil {
		runtime.HandleError(fmt.Errorf("ERROR: could not process item: %s because: %w", key, err))
		return true
	}
	t.workqueue.Forget(key)
	return true
}

func (c *Cleaner) handleKey(t *term, key string) error {
	objs, err := c.danmepInformer.GetIndexer().ByIndex(podUIDIndex, key)
	if err != nil {
		return err
	}
	for _, obj := range objs {
		dep, ok := obj.(*danmv1.DanmEp)
		if !ok || isInGracePeriod(dep) {
			continue
		}
		c.deleteInterface(t, dep)
	}
	return nil
}

func (c *Cleaner) deleteInterface(t *term, ep *danmv1.DanmEp) {
	log.Printf("INFO: Cleaner freeing IPs belonging to interface: %s of Pod: %s/%s", ep.Spec.Iface.Name, ep.Namespace, ep.Spec.Pod)
	cachedUID := ep.UID
	ep, err := t.danmClient.DanmV1().DanmEps(ep.Namespace).Get(context.TODO(), ep.Name, metav1.GetOptions{})
	if err != nil || ep.UID != cachedUID {
		return
	}
	netInfo, err := netcontrol.GetNetworkFromEp(t.danmClient, ep)
	if err != nil {
		log.Printf("WARNING: DanmEp '%s' in namespace '%s' could not be cleaned as its network could not be GET from K8s API server: %s",
			ep.ObjectMeta.Name, ep.ObjectMeta.Namespace, err)
		return
	}
	//Rules for thee, but not for me
	if (ipam.WasIpAllocatedByDanm(ep.Spec.Iface.Address, netInfo) && ipam.WasIpAllocatedByDanm(ep.Spec.Iface.AddressIPv6, netInfo)) || ep.Spec.NetworkType == flannelType {
		err = danmep.DeleteDanmEp(t.danmClient, ep, netInfo)
	} else {
		err = services.DeleteDanmEp(t.danmClient, ep, netInfo)
	}
	if err != nil {
		log.Printf("WARNING: DanmEp '%s' in namespace '%s' with network type '%s' could not be cleaned because of error: %s",
			ep.ObjectMeta.Name, ep.ObjectMeta.Namespace, ep.Spec.NetworkType, err)
	}
}

func (c *Cleaner) delPod(obj interface{}) {
	t := c.activeTerm()
	if t == nil {
		return
	}
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			return
		}
		pod, ok = tombstone.Obj.(*corev1.Pod)
		if !ok {
			return
		}
	}
	t.workqueue.Add(podKey(pod.Namespace, pod.UID))
}
