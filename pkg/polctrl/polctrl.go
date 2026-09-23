package polctrl

import (
	"context"
	"errors"
	"io"
	"log"
	"os"
	"time"

	polclientset "github.com/danm-cni/danm-utils/crd/client/clientset/versioned"
	polinformers "github.com/danm-cni/danm-utils/crd/client/informers/externalversions"
	"github.com/danm-cni/danm-utils/pkg/depset"
	"github.com/danm-cni/danm-utils/pkg/netruleset"
	"github.com/danm-cni/danm-utils/pkg/polset"
	"github.com/danm-cni/danm-utils/pkg/provisioner/iptables"
	"github.com/danm-cni/danm-utils/types/poltypes"
	danmclientset "github.com/danm-cni/danm/crd/client/clientset/versioned"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubeinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
)

const (
	MaxRetryCount      = 5
	ShortRetryInterval = 100
	LongRetryInterval  = 500
	NodeNameEnv        = "NODE_NAME"
)

var (
	ControllerNode = os.Getenv(NodeNameEnv)
)

type NetPolControl struct {
	PolicyController cache.SharedIndexInformer
	PodController    cache.SharedIndexInformer
	PolicyClient     polclientset.Interface
	DanmClient       danmclientset.Interface
	StopChan         *chan struct{}
}

func NewNetPolControl(cfg *rest.Config, stopChan *chan struct{}) (*NetPolControl, error) {
	polControl := &NetPolControl{StopChan: stopChan}
	polClient, err := polclientset.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	danmClient, err := danmclientset.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	polControl.PolicyClient = polClient
	polControl.DanmClient = danmClient
	for i := 0; i < MaxRetryCount; i++ {
		log.Println("INFO: Trying to discover DanmNetworkPolicy API in the cluster...")
		_, err = polControl.PolicyClient.NetpolV1().DanmNetworkPolicies("").List(context.TODO(), metav1.ListOptions{})
		if err != nil {
			log.Println("INFO: DanmNetworkPolicy discovery query failed with error:" + err.Error())
			time.Sleep(ShortRetryInterval * time.Millisecond)
		} else {
			log.Println("INFO: DanmNetworkPolicy API seems to be installed in the cluster!")
			polControl.createPolicyController()
			break
		}
	}
	if polControl.PolicyController == nil {
		return nil, errors.New("DanmNetworkPolicy API is not installed in the cluster, DANM Network Policy Controller cannot start!")
	}
	polControl.createPodController(cfg)
	return polControl, nil
}

func (netpolController *NetPolControl) Run() {
	go netpolController.PolicyController.Run(*netpolController.StopChan)
	go netpolController.PodController.Run(*netpolController.StopChan)
}

func (netpolController *NetPolControl) WatchErrorHandler(r *cache.Reflector, err error) {
	if apierrors.IsResourceExpired(err) || apierrors.IsGone(err) || err == io.EOF {
		log.Println("INFO: One of the API watchers closed gracefully, re-establishing connection")
		return
	}
	//The default K8s client retry mechanism expires after a certain amount of time, and just gives-up
	//It is better to shutdown the whole process now and freshly re-build the watchers, than risking becoming a permanent zombie
	*netpolController.StopChan <- struct{}{}
	//Give some time for gracefully terminating the connections
	time.Sleep(5 * time.Second)
	log.Println("ERROR: One of the API watchers closed unexpectedly with error:" + err.Error() + " shutting down DANM Network Policy Controller!!")
	os.Exit(0)
}

func (netpolCtrl *NetPolControl) createPolicyController() {
	netpolInformerFactory := polinformers.NewSharedInformerFactory(netpolCtrl.PolicyClient, time.Second*30)
	polController := netpolInformerFactory.Netpol().V1().DanmNetworkPolicies().Informer()
	polController.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    AddNetPol,
		UpdateFunc: UpdateNetPol,
		DeleteFunc: DeleteNetPol,
	})
	polController.SetWatchErrorHandler(netpolCtrl.WatchErrorHandler)
	netpolCtrl.PolicyController = polController
}

func (netpolCtrl *NetPolControl) createPodController(cfg *rest.Config) {
	kubeClient, _ := kubernetes.NewForConfig(cfg)
	kubeInformerFactory := kubeinformers.NewSharedInformerFactory(kubeClient, time.Second*30)
	podController := kubeInformerFactory.Core().V1().Pods().Informer()
	podController.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    netpolCtrl.AddPod,
		UpdateFunc: netpolCtrl.UpdatePod,
	})
	podController.SetWatchErrorHandler(netpolCtrl.WatchErrorHandler)
	netpolCtrl.PodController = podController
}

// TODO: implement event handlers
func AddNetPol(netpol interface{})                  {}
func UpdateNetPol(oldNetpol, newNetpol interface{}) {}
func DeleteNetPol(netpol interface{})               {}

func (netpolCtrl *NetPolControl) AddPod(pod interface{}) {
	podObj := pod.(*corev1.Pod)
	if podObj.Spec.NodeName != ControllerNode {
		return
	}
	policySet := polset.NewPolicySet(netpolCtrl.PolicyClient, podObj.ObjectMeta.Namespace)
	applicablePols := policySet.FilterApplicablePolicies(podObj)
	//By K8s documentation a Pod is only considered isolated if there is any network policy selecting it
	if len(applicablePols) == 0 {
		return
	}
	var depSet *poltypes.DanmEpSet
	for i := 0; i < MaxRetryCount; i++ {
		depSet = depset.NewDanmEpSet(netpolCtrl.DanmClient, podObj)
		//CNI might just be creating the DanmEps for the Pod
		//To be on the safe side we need to retry a couple of times before we can decide we have an error
		if len(depSet.PodEps) != 0 {
			break
		}
		time.Sleep(LongRetryInterval * time.Millisecond)
	}
	if len(depSet.PodEps) == 0 {
		log.Println("ERROR: DanmNetworkPolicy provisioning is impossible for Pod:" + podObj.ObjectMeta.Name + " in namespace:" +
			podObj.ObjectMeta.Name + " becuase its networking is not managed by DANM!")
		return
	}
	//Kubernetes doesn't remember the netns of the Pod, but we do. We need to read it from one of the DanmEps belonging to the Pod
	netRuleSet := netruleset.NewNetRuleSet(applicablePols, depSet)
	//TODOD: make this configurable once we have multiple executors to choose from
	ruleProvisioner := iptables.NewIptablesProvisioner()
	go ruleProvisioner.AddRulesToNewPod(netRuleSet, podObj)
}
func (netpolCtrl *NetPolControl) UpdatePod(oldPod, newPod interface{}) {
	oldPodObj := oldPod.(*corev1.Pod)
	newPodObj := newPod.(*corev1.Pod)
	if oldPodObj.Spec.NodeName != newPodObj.Spec.NodeName {
		netpolCtrl.AddPod(newPod)
	}
}
