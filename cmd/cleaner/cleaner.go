package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/danm-cni/danm-utils/pkg/cleaner"
	danmclientset "github.com/danm-cni/danm/crd/client/clientset/versioned"
	danminformers "github.com/danm-cni/danm/crd/client/informers/externalversions"
	corev1 "k8s.io/api/core/v1"
	kubeinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/transport"
)

var (
	kubeConf string
)

// main runs Cleaner as a leader elected controller.
// The informers run for the whole process lifetime, so a standby instance is warm when it gets elected.
// An instance losing its lease does not exit, but goes back to standby and waits for the next election.
func main() {
	flag.StringVar(&kubeConf, "kubeconf", "", "Absolute path to a valid kubeconf file. Only required if Cleaner runs out-of-cluster.")
	flag.Parse()
	cfg, err := clientcmd.BuildConfigFromFlags("", kubeConf)
	if err != nil {
		log.Fatalf("ERROR: cannot build cluster config for K8s REST client because: %v", err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	kubeClient, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		log.Fatalf("ERROR: cannot build K8s REST client because: %v", err)
	}
	danmClient, err := danmclientset.NewForConfig(cfg)
	if err != nil {
		log.Fatalf("ERROR: cannot build DANM REST client because: %v", err)
	}
	kubeInformerFactory := kubeinformers.NewSharedInformerFactory(kubeClient, time.Second*30)
	danmInformerFactory := danminformers.NewSharedInformerFactory(danmClient, time.Second*30)
	mrHandy, err := cleaner.New(kubeInformerFactory.Core().V1().Pods(), danmInformerFactory.Danm().V1().DanmEps())
	if err != nil {
		log.Fatalf("ERROR: Cleaner could not be created because: %v", err)
	}
	kubeInformerFactory.Start(ctx.Done())
	danmInformerFactory.Start(ctx.Done())
	if err := mrHandy.WarmUp(ctx.Done()); err != nil {
		log.Fatalf("ERROR: Cleaner caches could not be synced because: %v", err)
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
		log.Fatalf("ERROR: Cannot create resource lock because: %v", err)
	}
	electionConfig := leaderelection.LeaderElectionConfig{
		Lock:          rl,
		LeaseDuration: 10 * time.Second,
		RenewDeadline: 5 * time.Second,
		RetryPeriod:   3 * time.Second,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(termCtx context.Context) {
				lead(termCtx, cfg, mrHandy)
			},
			OnStoppedLeading: func() {
				log.Println("INFO: Cleaner instance is not the leader, waiting for the next election")
			},
		},
	}
	for ctx.Err() == nil {
		leaderelection.RunOrDie(ctx, electionConfig)
	}
	log.Println("INFO: Cleaner is shutting down")
}

// lead runs Cleaner for one leadership term.
// termCtx is cancelled by leader election when the lease is lost, and the DANM client of the term is fenced by it,
// so a demoted leader cannot write to the API server anymore.
// The leader election itself uses an unfenced client, so it can keep competing for the lease.
func lead(termCtx context.Context, cfg *rest.Config, mrHandy *cleaner.Cleaner) {
	termCfg := rest.CopyConfig(cfg)
	termCfg.Wrap(transport.ContextCanceller(termCtx, fmt.Errorf("Cleaner is not the leader anymore, or is shutting down")))
	danmClient, err := danmclientset.NewForConfig(termCfg)
	if err != nil {
		log.Fatalf("ERROR: cannot build DANM REST client of the leader because: %v", err)
	}
	log.Println("INFO: Cleaner instance became the leader")
	mrHandy.Run(termCtx.Done(), danmClient, 10)
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
