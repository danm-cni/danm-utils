package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/danm-cni/danm-utils/pkg/cleaner"
	danmclientset "github.com/danm-cni/danm/crd/client/clientset/versioned"
	corev1 "k8s.io/api/core/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	kubeinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/transport"
)

var (
	kubeConf string
)

func main() {
	flag.StringVar(&kubeConf, "kubeconf", "", "Absolute path to a valid kubeconf file. Only required if Cleaner runs out-of-cluster.")
	flag.Parse()
	cfg, err := clientcmd.BuildConfigFromFlags("", kubeConf)
	if err != nil {
		log.Println("ERROR: cannot build cluster config for K8s REST client because:" + err.Error())
		os.Exit(1)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg.Wrap(transport.ContextCanceller(ctx, fmt.Errorf("the leader is shutting down")))
	kubeClient, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		log.Println("ERROR: cannot build K8s REST client because:" + err.Error())
		os.Exit(1)
	}
	danmClient, err := danmclientset.NewForConfig(cfg)
	if err != nil {
		log.Println("ERROR: cannot build DANM REST client because:" + err.Error())
		os.Exit(1)
	}
	kubeInformerFactory := kubeinformers.NewSharedInformerFactory(kubeClient, time.Second*30)
	mrHandy := cleaner.New(danmClient,
		kubeInformerFactory.Core().V1().Pods())
	cleanStuff := func(ctx context.Context) {
		go kubeInformerFactory.Start(ctx.Done())
		if !mrHandy.Initialize() {
			log.Println("ERROR: Cleaner timed-out synching its cache, retrying!")
			os.Exit(1)
		}
		go cleaner.PeriodicCleanup(mrHandy.DanmClient, mrHandy.PodLister, ctx.Done())
		if err = mrHandy.Run(10, ctx.Done()); err != nil {
			log.Println("ERROR: Cleaner failed with:" + err.Error())
			os.Exit(1)
		}
	}
	rl, err := resourcelock.New(resourcelock.LeasesResourceLock,
		"kube-system",
		"danm-cleaner",
		kubeClient.CoreV1(),
		kubeClient.CoordinationV1(),
		resourcelock.ResourceLockConfig{
			Identity:      GetHostname(),
			EventRecorder: createRecorder(kubeClient, "danm-cleaner"),
		})
	if err != nil {
		log.Println("ERROR: Cannot create resource lock because:" + err.Error())
		os.Exit(1)
	}
	leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{
		Lock:          rl,
		LeaseDuration: 10 * time.Second,
		RenewDeadline: 5 * time.Second,
		RetryPeriod:   3 * time.Second,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: cleanStuff,
			OnStoppedLeading: func() {
				utilruntime.HandleError(errors.New("WARNING: Cleaner cluster lost its leader"))
			},
		},
	})
	log.Println("WARNING: instance lost its lease, restarting!")
}

func GetHostname() string {
	ret, _ := os.Hostname()
	return ret
}

func createRecorder(kubeClient *kubernetes.Clientset, comp string) record.EventRecorder {
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: v1core.New(kubeClient.CoreV1().RESTClient()).Events("kube-system")})
	return eventBroadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: comp})
}
