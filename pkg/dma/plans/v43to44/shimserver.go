package v43to44

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/danm-cni/danm-utils/pkg/dma/cluster"
	danmtypes "github.com/danm-cni/danm/crd/apis/danm/v1"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/tools/cache"
)

const (
	ShimPort        = 8443
	ShimTlsDir      = "/etc/dma-shim"
	ShimMutatePath  = "/mutate"
	ShimHealthPath  = "/healthz"
	maxReviewLength = 4 << 20
)

type ShimServerConfig struct {
	Kubeconfig string
	Mode       cluster.Mode
	TlsDir     string
	Port       int
}

type ripSource interface {
	ReservedIPs(namespace string) ([]*danmtypes.ReservedIP, bool)
}

type noReservedIPs struct{}

func (noReservedIPs) ReservedIPs(string) ([]*danmtypes.ReservedIP, bool) {
	return nil, true
}

type informerReservedIPs struct {
	informer cache.SharedIndexInformer
	lister   cache.GenericLister
}

func (source *informerReservedIPs) ReservedIPs(namespace string) ([]*danmtypes.ReservedIP, bool) {
	if !source.informer.HasSynced() {
		return nil, false
	}
	objects, err := source.lister.ByNamespace(namespace).List(labels.Everything())
	if err != nil {
		return nil, false
	}
	rips := make([]*danmtypes.ReservedIP, 0, len(objects))
	for _, object := range objects {
		unstructuredObject, err := runtime.DefaultUnstructuredConverter.ToUnstructured(object)
		if err != nil {
			continue
		}
		rip := &danmtypes.ReservedIP{}
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(unstructuredObject, rip); err != nil {
			continue
		}
		rips = append(rips, rip)
	}
	return rips, true
}

func ServeShim(ctx context.Context, config ShimServerConfig) error {
	if config.TlsDir == "" {
		config.TlsDir = ShimTlsDir
	}
	if config.Port == 0 {
		config.Port = ShimPort
	}
	var source ripSource = noReservedIPs{}
	if config.Mode == cluster.ModeProduction {
		restConfig, err := cluster.RestConfig(config.Kubeconfig)
		if err != nil {
			return err
		}
		client, err := dynamic.NewForConfig(restConfig)
		if err != nil {
			return fmt.Errorf("cannot build the dynamic client: %w", err)
		}
		factory := dynamicinformer.NewDynamicSharedInformerFactory(client, 10*time.Minute)
		informer := factory.ForResource(kindReservedIP.NewResource())
		factory.Start(ctx.Done())
		source = &informerReservedIPs{informer: informer.Informer(), lister: informer.Lister()}
	}
	mux := http.NewServeMux()
	mux.HandleFunc(ShimHealthPath, func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusOK)
	})
	mux.Handle(ShimMutatePath, shimHandler{source: source, log: log.Default()})
	server := &http.Server{Addr: fmt.Sprintf(":%d", config.Port), Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		server.Shutdown(shutdown)
	}()
	log.Printf("annotation shim serving in %s mode on port %d", orUnknownMode(config.Mode), config.Port)
	err := server.ListenAndServeTLS(filepath.Join(config.TlsDir, "tls.crt"), filepath.Join(config.TlsDir, "tls.key"))
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func orUnknownMode(mode cluster.Mode) string {
	if mode == cluster.ModeUnknown {
		return "unknown"
	}
	return string(mode)
}

type shimHandler struct {
	source ripSource
	log    *log.Logger
}

func (handler shimHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	body, err := io.ReadAll(io.LimitReader(request.Body, maxReviewLength))
	if err != nil {
		http.Error(writer, "cannot read the request", http.StatusBadRequest)
		return
	}
	review := &admissionv1.AdmissionReview{}
	if err := json.Unmarshal(body, review); err != nil || review.Request == nil {
		http.Error(writer, "the request is not an AdmissionReview", http.StatusBadRequest)
		return
	}
	response := handler.admit(review.Request)
	review.Response = response
	review.Request = nil
	encoded, err := json.Marshal(review)
	if err != nil {
		http.Error(writer, "cannot encode the response", http.StatusInternalServerError)
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	writer.Write(encoded)
}

func (handler shimHandler) admit(request *admissionv1.AdmissionRequest) *admissionv1.AdmissionResponse {
	response := &admissionv1.AdmissionResponse{UID: request.UID, Allowed: true}
	pod := &corev1.Pod{}
	if err := json.Unmarshal(request.Object.Raw, pod); err != nil {
		handler.log.Printf("cannot decode the Pod of request %s: %s", request.UID, err.Error())
		return response
	}
	namespace := request.Namespace
	if namespace == "" {
		namespace = pod.Namespace
	}
	patch, notes := ShimPatch(pod, namespace, handler.source)
	for _, note := range notes {
		handler.log.Print(note)
	}
	if patch == nil {
		return response
	}
	encoded, err := json.Marshal(patch)
	if err != nil {
		handler.log.Printf("cannot encode the patch of Pod %s/%s: %s", namespace, pod.Name, err.Error())
		return response
	}
	patchType := admissionv1.PatchTypeJSONPatch
	response.Patch = encoded
	response.PatchType = &patchType
	return response
}

type jsonPatchOperation struct {
	Op    string `json:"op"`
	Path  string `json:"path"`
	Value any    `json:"value"`
}

func ShimPatch(pod *corev1.Pod, namespace string, source ripSource) ([]jsonPatchOperation, []string) {
	old, hasOld := pod.Annotations[OldInterfacesKey]
	if !hasOld {
		return nil, nil
	}
	if _, hasNew := pod.Annotations[NewInterfacesKey]; hasNew {
		return nil, nil
	}
	rips, synced := source.ReservedIPs(namespace)
	notes := []string{}
	if !synced {
		rips = nil
		notes = append(notes, fmt.Sprintf("the ReservedIP cache is not synced, Pod %s/%s gets a verbatim copy", namespace, pod.Name))
	}
	value, rewriteNotes := NewInterfaces(old, namespace, pod.Name, rips)
	notes = append(notes, rewriteNotes...)
	return []jsonPatchOperation{
		{Op: "add", Path: "/metadata/annotations/" + escapeJsonPointer(NewInterfacesKey), Value: value},
		{Op: "add", Path: "/metadata/annotations/" + escapeJsonPointer(CopiedMarkerKey), Value: CopiedByShim},
	}, notes
}

func escapeJsonPointer(key string) string {
	return strings.ReplaceAll(strings.ReplaceAll(key, "~", "~0"), "/", "~1")
}
