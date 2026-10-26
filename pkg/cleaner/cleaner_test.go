package cleaner

import (
	"testing"
	"time"

	danmv1 "github.com/danm-cni/danm/crd/apis/danm/v1"
	danmfake "github.com/danm-cni/danm/crd/client/clientset/versioned/fake"
	danminformers "github.com/danm-cni/danm/crd/client/informers/externalversions"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	coreinformers "k8s.io/client-go/informers"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
)

type fakeQueue struct {
	workqueue.TypedRateLimitingInterface[string]
	added []string
}

func (f *fakeQueue) Add(item string) {
	f.added = append(f.added, item)
}

func newTestCleaner(t *testing.T) *Cleaner {
	kubeInformerFactory := coreinformers.NewSharedInformerFactory(k8sfake.NewClientset(), 0)
	danmInformerFactory := danminformers.NewSharedInformerFactory(danmfake.NewSimpleClientset(), 0)
	c, err := New(kubeInformerFactory.Core().V1().Pods(), danmInformerFactory.Danm().V1().DanmEps())
	require.NoError(t, err)
	return c
}

func startTerm(t *testing.T, c *Cleaner) *fakeQueue {
	stopCh := make(chan struct{})
	t.Cleanup(func() { close(stopCh) })
	queue := &fakeQueue{}
	c.term.Store(&term{stopCh: stopCh, workqueue: queue})
	return queue
}

func TestDelPod(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: meta_v1.ObjectMeta{
			Namespace: "ns",
			Name:      "pod",
			UID:       "uid",
		},
	}
	obj := &meta_v1.ObjectMeta{
		Namespace: "ns",
		Name:      "pod",
		UID:       "uid",
	}
	tests := []struct {
		name         string
		obj          interface{}
		expectedQLen int
		expectedKey  string
	}{
		{
			name:         "got pod",
			obj:          pod,
			expectedQLen: 1,
			expectedKey:  "ns/uid",
		},
		{
			name:         "got pod tombstone",
			obj:          cache.DeletedFinalStateUnknown{Obj: pod},
			expectedQLen: 1,
			expectedKey:  "ns/uid",
		},
		{
			name:         "got non pod tombstone",
			obj:          cache.DeletedFinalStateUnknown{Obj: obj},
			expectedQLen: 0,
		},
		{
			name:         "got unexpected type object",
			obj:          obj,
			expectedQLen: 0,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := newTestCleaner(t)
			queue := startTerm(t, c)
			c.delPod(tc.obj)
			assert.Equal(t, tc.expectedQLen, len(queue.added))
			if tc.expectedQLen > 0 {
				assert.Equal(t, tc.expectedKey, queue.added[0])
			}
		})
	}
}

func TestDelPodWithoutLeadership(t *testing.T) {
	pod := &corev1.Pod{ObjectMeta: meta_v1.ObjectMeta{Namespace: "ns", Name: "pod", UID: "uid"}}
	c := newTestCleaner(t)
	c.delPod(pod)
	stopCh := make(chan struct{})
	queue := &fakeQueue{}
	c.term.Store(&term{stopCh: stopCh, workqueue: queue})
	close(stopCh)
	c.delPod(pod)
	assert.Empty(t, queue.added)
}

func TestFilterDanmepsEmpty(t *testing.T) {
	c := newTestCleaner(t)
	deps, err := c.filterDanmeps()
	require.NoError(t, err)
	assert.Empty(t, deps)
}

func TestFilterDanmeps(t *testing.T) {
	dep1 := &danmv1.DanmEp{
		ObjectMeta: meta_v1.ObjectMeta{
			Namespace:         "ns",
			Name:              "dep1",
			UID:               "uid",
			CreationTimestamp: meta_v1.Time{Time: time.Now().UTC().Add(-10 * time.Minute)},
		},
	}
	dep2 := &danmv1.DanmEp{
		ObjectMeta: meta_v1.ObjectMeta{
			Namespace:         "ns",
			Name:              "dep2",
			UID:               "uid",
			CreationTimestamp: meta_v1.Time{Time: time.Now().UTC().Add(-10 * time.Second)},
		},
	}
	c := newTestCleaner(t)
	store := c.danmepInformer.GetStore()
	require.NoError(t, store.Add(dep1))
	require.NoError(t, store.Add(dep2))
	deps, err := c.filterDanmeps()
	require.NoError(t, err)
	assert.Equal(t, []*danmv1.DanmEp{dep1}, deps)
}

func TestPodUIDIndex(t *testing.T) {
	dep := &danmv1.DanmEp{
		ObjectMeta: meta_v1.ObjectMeta{Namespace: "ns", Name: "dep"},
		Spec:       danmv1.DanmEpSpec{PodUID: "uid"},
	}
	c := newTestCleaner(t)
	require.NoError(t, c.danmepInformer.GetStore().Add(dep))
	objs, err := c.danmepInformer.GetIndexer().ByIndex(podUIDIndex, "ns/uid")
	require.NoError(t, err)
	assert.Equal(t, []interface{}{dep}, objs)
}
