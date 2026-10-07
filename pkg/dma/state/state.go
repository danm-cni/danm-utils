// Package state persists the progress of DANM Migration Assistant plans into a Kubernetes
// ConfigMap, so that a plan survives restarts of the Pod the assistant runs in.
package state

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"
)

const (
	// DefaultNamespace is where the assistant keeps all of its own objects.
	DefaultNamespace = "kube-system"
	// DefaultName is the name of the ConfigMap holding the state of every known plan.
	DefaultName = "dma-state"
)

// StepStatus is the recorded outcome of the last operation performed on a step.
// It records intent and history; it is never the authority on what the cluster actually
// looks like, which is what a step's own detector is for.
type StepStatus string

const (
	StatusNotStarted StepStatus = "NotStarted"
	StatusRunning    StepStatus = "Running"
	StatusDone       StepStatus = "Done"
	StatusFailed     StepStatus = "Failed"
	StatusRolledBack StepStatus = "RolledBack"
)

// StepRecord is everything the assistant remembers about a single step of a plan.
type StepRecord struct {
	ID         int        `json:"id"`
	Name       string     `json:"name"`
	Status     StepStatus `json:"status"`
	StartedAt  string     `json:"startedAt,omitempty"`
	FinishedAt string     `json:"finishedAt,omitempty"`
	Operator   string     `json:"operator,omitempty"`
	Error      string     `json:"error,omitempty"`
	// Snapshots holds the serialised form of objects this step mutated in place, keyed by
	// an identifier the step chooses. Steps which only create objects store nothing here.
	Snapshots map[string]string `json:"snapshots,omitempty"`
}

// PlanRecord is the persisted state of one migration plan.
type PlanRecord struct {
	PlanID string `json:"planId"`
	From   string `json:"from"`
	To     string `json:"to"`
	// Mode is the detected DANM deployment mode. It is written once by the preflight step and
	// read by every later step, so that the mode cannot drift in the middle of a plan.
	Mode          string            `json:"mode,omitempty"`
	Images        map[string]string `json:"images,omitempty"`
	BinaryPayload string            `json:"binaryPayload,omitempty"`
	// LastExecutedStep is the ID of the most recently executed step, and the only step
	// rollback will accept.
	LastExecutedStep int          `json:"lastExecutedStep"`
	Steps            []StepRecord `json:"steps,omitempty"`
	UpdatedAt        string       `json:"updatedAt,omitempty"`
}

// Step returns the record of the given step, or nil when the step was never touched.
func (p *PlanRecord) Step(id int) *StepRecord {
	for i := range p.Steps {
		if p.Steps[i].ID == id {
			return &p.Steps[i]
		}
	}
	return nil
}

// UpsertStep inserts or replaces the record of a step, keeping the list ordered by step ID.
func (p *PlanRecord) UpsertStep(record StepRecord) *StepRecord {
	if existing := p.Step(record.ID); existing != nil {
		*existing = record
		return existing
	}
	p.Steps = append(p.Steps, record)
	sort.Slice(p.Steps, func(i, j int) bool { return p.Steps[i].ID < p.Steps[j].ID })
	return p.Step(record.ID)
}

// HighestDoneStepBelow returns the ID of the highest numbered step below the given one which is
// still recorded as done, or zero when there is none. Rollback uses it to move the
// last-executed pointer backwards.
func (p *PlanRecord) HighestDoneStepBelow(id int) int {
	highest := 0
	for _, step := range p.Steps {
		if step.ID < id && step.Status == StatusDone && step.ID > highest {
			highest = step.ID
		}
	}
	return highest
}

// Store reads and writes plan records in a single ConfigMap, one key per plan.
type Store struct {
	client    kubernetes.Interface
	namespace string
	name      string
}

// NewStore returns a store backed by the named ConfigMap. The ConfigMap is created on first write.
func NewStore(client kubernetes.Interface, namespace, name string) *Store {
	if namespace == "" {
		namespace = DefaultNamespace
	}
	if name == "" {
		name = DefaultName
	}
	return &Store{client: client, namespace: namespace, name: name}
}

// Load returns the record of a plan, or nil when the plan has no recorded progress yet.
func (s *Store) Load(ctx context.Context, planID string) (*PlanRecord, error) {
	configMap, err := s.configMap(ctx)
	if err != nil {
		return nil, err
	}
	if configMap == nil {
		return nil, nil
	}
	return decode(configMap, planID)
}

// List returns the records of every plan the store knows about, ordered by plan ID.
func (s *Store) List(ctx context.Context) ([]PlanRecord, error) {
	configMap, err := s.configMap(ctx)
	if err != nil {
		return nil, err
	}
	if configMap == nil {
		return nil, nil
	}
	records := make([]PlanRecord, 0, len(configMap.Data))
	for planID := range configMap.Data {
		record, err := decode(configMap, planID)
		if err != nil {
			return nil, err
		}
		if record != nil {
			records = append(records, *record)
		}
	}
	sort.Slice(records, func(i, j int) bool { return records[i].PlanID < records[j].PlanID })
	return records, nil
}

// Mutate applies the given change to a plan's record and writes it back, retrying on a
// concurrent update of the ConfigMap. The mutator receives the current record, or a freshly
// initialised one when the plan has no recorded progress yet.
func (s *Store) Mutate(ctx context.Context, planID string, mutator func(*PlanRecord) error) (*PlanRecord, error) {
	var result *PlanRecord
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		configMap, err := s.configMap(ctx)
		if err != nil {
			return err
		}
		created := false
		if configMap == nil {
			configMap = s.emptyConfigMap()
			created = true
		}
		record, err := decode(configMap, planID)
		if err != nil {
			return err
		}
		if record == nil {
			record = &PlanRecord{PlanID: planID}
		}
		if err := mutator(record); err != nil {
			return err
		}
		record.PlanID = planID
		record.UpdatedAt = Now()
		encoded, err := json.MarshalIndent(record, "", "  ")
		if err != nil {
			return fmt.Errorf("cannot serialise state of plan %s: %w", planID, err)
		}
		if configMap.Data == nil {
			configMap.Data = map[string]string{}
		}
		configMap.Data[planID] = string(encoded)
		if created {
			_, err = s.client.CoreV1().ConfigMaps(s.namespace).Create(ctx, configMap, metav1.CreateOptions{})
		} else {
			_, err = s.client.CoreV1().ConfigMaps(s.namespace).Update(ctx, configMap, metav1.UpdateOptions{})
		}
		if err != nil {
			return err
		}
		result = record
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("cannot persist state of plan %s: %w", planID, err)
	}
	return result, nil
}

// Delete removes all recorded progress of a plan.
func (s *Store) Delete(ctx context.Context, planID string) error {
	_, err := s.Mutate(ctx, planID, func(record *PlanRecord) error {
		*record = PlanRecord{PlanID: planID}
		return nil
	})
	return err
}

func (s *Store) configMap(ctx context.Context) (*corev1.ConfigMap, error) {
	configMap, err := s.client.CoreV1().ConfigMaps(s.namespace).Get(ctx, s.name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("cannot read state ConfigMap %s/%s: %w", s.namespace, s.name, err)
	}
	return configMap, nil
}

func (s *Store) emptyConfigMap() *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      s.name,
			Namespace: s.namespace,
			Labels:    map[string]string{"app.kubernetes.io/managed-by": "dma"},
		},
		Data: map[string]string{},
	}
}

func decode(configMap *corev1.ConfigMap, planID string) (*PlanRecord, error) {
	raw, found := configMap.Data[planID]
	if !found || raw == "" {
		return nil, nil
	}
	record := &PlanRecord{}
	if err := json.Unmarshal([]byte(raw), record); err != nil {
		return nil, fmt.Errorf("state of plan %s is corrupted: %w", planID, err)
	}
	return record, nil
}

// Now returns the timestamp format used throughout the recorded state.
func Now() string {
	return time.Now().UTC().Format(time.RFC3339)
}
