package main

import (
	"flag"
	"log"
	"os"

	"github.com/danm-cni/danm-utils/pkg/polctrl"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

var (
	version, commitHash string
)

func getClientConfig(kubeConfig *string) (*rest.Config, error) {
	if kubeConfig != nil {
		return clientcmd.BuildConfigFromFlags("", *kubeConfig)
	}
	return rest.InClusterConfig()
}

func main() {
	printVersion := flag.Bool("version", false, "prints Git version information of the binary to standard out")
	flag.Parse()
	if *printVersion {
		log.Println("DANM Netpol binary was built from release: " + version)
		log.Println("DANM Netpol binary was built from commit: " + commitHash)
		return
	}
	log.SetOutput(os.Stdout)
	log.Println("INFO: Starting DANM Network Policy Controller...")
	kubeConfig := flag.String("kubeconf", "", "Path to a kube config. Only required if out-of-cluster.")
	flag.Parse()
	config, err := getClientConfig(kubeConfig)
	if err != nil {
		log.Println("ERROR: Parsing kubeconfig failed with error:" + err.Error() + " , exiting")
		os.Exit(-1)
	}
	stopCh := make(chan struct{})
	netPolicer, err := polctrl.NewNetPolControl(config, &stopCh)
	if err != nil {
		log.Println("ERROR: Creation of Network Policy Controller failed with error:" + err.Error() + " , exiting")
		os.Exit(-1)
	}
	netPolicer.Run()
	select {}
}
