package state

import (
	"context"
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubefake "k8s.io/client-go/kubernetes/fake"
)

func TestLoadReturnsNothingForAnUnknownPlan(t *testing.T) {
	store := NewStore(kubefake.NewSimpleClientset(), "kube-system", "dma-state")
	record, err := store.Load(context.Background(), "4.3-to-4.4")
	if err != nil {
		t.Fatalf("loading an unknown plan failed: %v", err)
	}
	if record != nil {
		t.Fatalf("an unknown plan returned a record: %+v", record)
	}
}

func TestMutateCreatesAndUpdatesTheConfigMap(t *testing.T) {
	client := kubefake.NewSimpleClientset()
	store := NewStore(client, "kube-system", "dma-state")
	_, err := store.Mutate(context.Background(), "4.3-to-4.4", func(record *PlanRecord) error {
		record.From = "4.3"
		record.To = "4.4"
		record.Mode = "production"
		record.UpsertStep(StepRecord{ID: 1, Name: "preflight", Status: StatusDone})
		record.LastExecutedStep = 1
		return nil
	})
	if err != nil {
		t.Fatalf("the first write failed: %v", err)
	}
	_, err = store.Mutate(context.Background(), "4.3-to-4.4", func(record *PlanRecord) error {
		record.UpsertStep(StepRecord{ID: 2, Name: "install-new-crds", Status: StatusDone})
		record.LastExecutedStep = 2
		return nil
	})
	if err != nil {
		t.Fatalf("the second write failed: %v", err)
	}
	record, err := store.Load(context.Background(), "4.3-to-4.4")
	if err != nil {
		t.Fatalf("the record cannot be read back: %v", err)
	}
	if record.Mode != "production" || record.LastExecutedStep != 2 || len(record.Steps) != 2 {
		t.Fatalf("the record was not merged correctly: %+v", record)
	}
	if record.UpdatedAt == "" {
		t.Fatalf("the record carries no update timestamp")
	}
	configMap, err := client.CoreV1().ConfigMaps("kube-system").Get(context.Background(), "dma-state", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("the ConfigMap was not created: %v", err)
	}
	if configMap.Labels["app.kubernetes.io/managed-by"] != "dma" {
		t.Fatalf("the ConfigMap is not labelled as managed by the assistant: %+v", configMap.Labels)
	}
}

func TestMutateKeepsPlansApart(t *testing.T) {
	store := NewStore(kubefake.NewSimpleClientset(), "kube-system", "dma-state")
	for _, planID := range []string{"4.3-to-4.4", "4.4-to-4.5"} {
		if _, err := store.Mutate(context.Background(), planID, func(record *PlanRecord) error {
			record.LastExecutedStep = 1
			return nil
		}); err != nil {
			t.Fatalf("writing plan %s failed: %v", planID, err)
		}
	}
	records, err := store.List(context.Background())
	if err != nil {
		t.Fatalf("the plans cannot be listed: %v", err)
	}
	if len(records) != 2 || records[0].PlanID != "4.3-to-4.4" || records[1].PlanID != "4.4-to-4.5" {
		t.Fatalf("the plans were not kept apart: %+v", records)
	}
}

func TestMutatePropagatesTheMutatorError(t *testing.T) {
	store := NewStore(kubefake.NewSimpleClientset(), "kube-system", "dma-state")
	refused := errors.New("the step refused to record itself")
	_, err := store.Mutate(context.Background(), "4.3-to-4.4", func(_ *PlanRecord) error { return refused })
	if err == nil {
		t.Fatalf("a refusing mutator was treated as a success")
	}
	record, loadErr := store.Load(context.Background(), "4.3-to-4.4")
	if loadErr != nil {
		t.Fatalf("the state cannot be read: %v", loadErr)
	}
	if record != nil {
		t.Fatalf("a refused mutation was written: %+v", record)
	}
}

func TestLoadRejectsCorruptedState(t *testing.T) {
	client := kubefake.NewSimpleClientset(&corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "dma-state", Namespace: "kube-system"},
		Data:       map[string]string{"4.3-to-4.4": "this is not JSON"},
	})
	store := NewStore(client, "kube-system", "dma-state")
	if _, err := store.Load(context.Background(), "4.3-to-4.4"); err == nil {
		t.Fatalf("corrupted state was accepted")
	}
}

func TestHighestDoneStepBelow(t *testing.T) {
	record := &PlanRecord{Steps: []StepRecord{
		{ID: 1, Status: StatusDone},
		{ID: 2, Status: StatusDone},
		{ID: 3, Status: StatusRolledBack},
		{ID: 4, Status: StatusDone},
	}}
	if highest := record.HighestDoneStepBelow(4); highest != 2 {
		t.Fatalf("the highest finished step below 4 is %d instead of 2", highest)
	}
	if highest := record.HighestDoneStepBelow(1); highest != 0 {
		t.Fatalf("the highest finished step below 1 is %d instead of 0", highest)
	}
}
