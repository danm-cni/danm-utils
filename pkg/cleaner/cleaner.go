package cleaner

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	cleaner "github.com/danm-cni/danm-utils/pkg/danmep"
	danmv1 "github.com/danm-cni/danm/crd/apis/danm/v1"
	danmclientset "github.com/danm-cni/danm/crd/client/clientset/versioned"
	danmscheme "github.com/danm-cni/danm/crd/client/clientset/versioned/scheme"
	"github.com/danm-cni/danm/pkg/danmep"
	"github.com/danm-cni/danm/pkg/netcontrol"
	corev1 "k8s.io/api/core/v1"
	k8serr "k8s.io/apimachinery/pkg/api/errors"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	coreinformers "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes/scheme"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
)

type Cleaner struct {
	DanmClient  danmclientset.Interface
	Initialized bool
	PodLister   corelisters.PodLister
	PodSynced   cache.InformerSynced
	Workqueue   workqueue.RateLimitingInterface
}

func New(
	danmClient danmclientset.Interface,
	podInformer coreinformers.PodInformer) *Cleaner {
	danmscheme.AddToScheme(scheme.Scheme)
	cleaner := &Cleaner{
		DanmClient:  danmClient,
		Initialized: false,
		PodLister:   podInformer.Lister(),
		PodSynced:   podInformer.Informer().HasSynced,
		Workqueue:   workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "Endpoints"),
	}
	podInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		UpdateFunc: cleaner.updatePod,
		DeleteFunc: cleaner.delPod,
	})
	return cleaner
}

func (c *Cleaner) Initialize() bool {
	interval := 100
	timeout := 10000
	for timer := 0; timer <= timeout; timer = timer + interval {
		//This can be easily expanded if we need to add more Informers to Cleaner in the future
		if c.PodSynced() {
			break
		}
		time.Sleep(time.Duration(interval) * time.Millisecond)
	}
	c.Initialized = true
	return true
}

func PeriodicCleanup(danmClient danmclientset.Interface, podLister corelisters.PodLister, stopCh <-chan struct{}) {
	go cleanupOnTick(danmClient, podLister)
	log.Println("INFO: Successfully started Cleaner's periodic worker thread")
	<-stopCh
	log.Println("INFO: Shutting down Cleaner's periodic worker thread")
}

func cleanupOnTick(danmClient danmclientset.Interface, podLister corelisters.PodLister) {
	timeForCleanup := time.NewTicker(10 * time.Second)
	for {
		select {
		case <-timeForCleanup.C:
			danmeps, err := danmep.FindByPodName(danmClient, "", "")
			if err != nil {
				log.Println("WARNING: Periodic cleaning failed with error:" + err.Error())
				continue
			}
			cleanDanglingEps(danmClient, danmeps, podLister)
		}
	}
}

func cleanDanglingEps(danmClient danmclientset.Interface, danmeps []danmv1.DanmEp, podLister corelisters.PodLister) {
	podCache := make(map[types.UID]bool, 0)
	for _, dep := range danmeps {
		//We have already checked this Pod
		if doesPodExist, ok := podCache[dep.Spec.PodUID]; ok {
			if !doesPodExist {
				log.Println("INFO: Cleaner freeing IPs belonging to interface:" + dep.Spec.Iface.Name + " of Pod:" + dep.Spec.Pod)
				deleteInterface(danmClient, dep)
			}
			continue
		}
		pod, err := podLister.Pods(dep.ObjectMeta.Namespace).Get(dep.Spec.Pod)
		//Statefulset, or non-controlled Pods can be re-instantiated with the same name
		//A Pod is considered non-existent if it does not exist OR it exist but with a different UID
		if k8serr.IsNotFound(err) || (err == nil && pod.ObjectMeta.UID != dep.Spec.PodUID) {
			log.Println("INFO: Cleaner freeing IPs belonging to interface:" + dep.Spec.Iface.Name + " of Pod:" + dep.Spec.Pod)
			deleteInterface(danmClient, dep)
			podCache[dep.Spec.PodUID] = false
		} else {
			podCache[dep.Spec.PodUID] = true
		}
	}
}

func (c *Cleaner) Run(threadiness int, stopCh <-chan struct{}) error {
	defer runtime.HandleCrash()
	defer c.Workqueue.ShutDown()
	log.Println("INFO: starting Cleaner")
	log.Println("INFO: waiting for Cleaner to synchronize cache")
	if ok := cache.WaitForCacheSync(stopCh, c.PodSynced); !ok {
		return errors.New("synching Cleaner's cache failed")
	}
	log.Println("INFO: Starting Cleaner event handler threads")
	for i := 0; i < threadiness; i++ {
		go wait.Until(c.runWorker, time.Second, stopCh)
	}
	log.Println("INFO: Successfully started Cleaner's event handler threads")
	<-stopCh
	log.Println("INFO: Shutting down Cleaner's event handler threads")
	return nil
}

func (c *Cleaner) runWorker() {
	for c.processNextWorkItem() {
	}
}

func (c *Cleaner) processNextWorkItem() bool {
	obj, shutdown := c.Workqueue.Get()
	if shutdown {
		return false
	}
	err := c.processItemInQueue(obj)
	if err != nil {
		runtime.HandleError(err)
	}
	return true
}

func (c *Cleaner) processItemInQueue(obj interface{}) error {
	defer c.Workqueue.Done(obj)
	var key string
	var ok bool
	if key, ok = obj.(string); !ok {
		c.Workqueue.Forget(obj)
		runtime.HandleError(fmt.Errorf("WARNING: Cannot decode work item from queue because instead string type we got %#v", obj))
		return nil
	}
	if err := c.handleKey(key); err != nil {
		return errors.New("ERROR: could not process item:" + key + " because:" + err.Error())
	}
	c.Workqueue.Forget(obj)
	return nil
}

func (c *Cleaner) handleKey(key string) error {
	ns, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		log.Println("WARNING: Dropping work item from because its key:" + key + " could not be broken up into API object identifiers due to error:" + err.Error())
		return nil
	}
	deps, err := danmep.FindByPodName(c.DanmClient, name, ns)
	if err != nil {
		return err
	}
	//Check if the specified DanmEp (if any) actually exists in the namespace
	for _, dep := range deps {
		log.Println("INFO: Cleaner freeing IPs belonging to interface:" + dep.Spec.Iface.Name + " of Pod:" + dep.Spec.Pod)
		deleteInterface(c.DanmClient, dep)
	}
	return nil
}

func deleteInterface(danmClient danmclientset.Interface, ep danmv1.DanmEp) {
	//We give time for DANM to execute normal CNI operation
	//We want to avoid possible interference, and with it exotic race conditions
	//TODO: this quite possibly needs to be more sophisticated than this :)
	time.Sleep(1 * time.Second)
	_, err := danmClient.DanmV1().DanmEps(ep.ObjectMeta.Namespace).Get(context.TODO(), ep.ObjectMeta.Name, meta_v1.GetOptions{})
	if err != nil {
		//Problem solved itself in the meantime
		return
	}
	netInfo, err := netcontrol.GetNetworkFromEp(danmClient, &ep)
	if err != nil {
		log.Printf(
			"WARNING: DanmEp '%s' in namespace '%s' could not be cleaned as its network could not be GET from K8s API server: %s",
			ep.ObjectMeta.Name, ep.ObjectMeta.Namespace, err)
		return
	}
	err = cleaner.DeleteDanmEp(danmClient, &ep, netInfo)
	if err != nil {
		log.Printf(
			"WARNING: Danmep '%s' in namespace '%s' with network type '%s' could not be cleaned because of error: %s",
			ep.ObjectMeta.Name, ep.ObjectMeta.Namespace, ep.Spec.NetworkType, err)
	}
}

func (c *Cleaner) updatePod(old, new interface{}) {
	oldPod := old.(*corev1.Pod)
	newPod := new.(*corev1.Pod)
	if oldPod.ResourceVersion == newPod.ResourceVersion {
		return
	}
	// If pod was ready but the current status is not ready then we are interested in it...
	//TODO: what happens when a Node loses connection, but comes back before Pod timeout?
	//      does the Pod changes state? do we accidentally cleanup?
	if !isPodRunning(newPod) && isPodRunning(oldPod) {
		c.enqueuePod(new)
	}
}

func isPodRunning(pod *corev1.Pod) bool {
	return pod.Status.Phase == corev1.PodRunning
}

func (c *Cleaner) enqueuePod(obj interface{}) {
	var key string
	var err error
	if key, err = cache.MetaNamespaceKeyFunc(obj); err != nil {
		log.Println("WARNING: Could not schedule Pod for automatic cleanup because:" + err.Error())
		return
	}
	c.Workqueue.Add(key)
}

func (c *Cleaner) delPod(obj interface{}) {
	if _, ok := obj.(meta_v1.Object); !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			return
		}
		_, ok = tombstone.Obj.(meta_v1.Object)
		if !ok {
			return
		}
	}
	c.enqueuePod(obj)
}
