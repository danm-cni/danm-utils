package engine

import (
	"fmt"
	"sort"
	"strings"
	"sync"
)

// Registry holds the pre-baked plans the assistant was built with.
type Registry struct {
	mutex sync.RWMutex
	plans map[string]Plan
}

// Default is the registry every plan package registers itself into from its init function.
var Default = NewRegistry()

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{plans: map[string]Plan{}}
}

// Register validates a plan and adds it to the registry.
func (registry *Registry) Register(plan Plan) error {
	if err := Validate(plan); err != nil {
		return err
	}
	registry.mutex.Lock()
	defer registry.mutex.Unlock()
	id := plan.Meta().ID
	if _, found := registry.plans[id]; found {
		return fmt.Errorf("plan %s is registered twice", id)
	}
	registry.plans[id] = plan
	return nil
}

// Get returns the plan with the given ID.
func (registry *Registry) Get(id string) (Plan, error) {
	registry.mutex.RLock()
	defer registry.mutex.RUnlock()
	plan, found := registry.plans[id]
	if !found {
		return nil, fmt.Errorf("there is no migration plan called %s, run \"dma plans\" to see the known ones", id)
	}
	return plan, nil
}

// Find returns the plan migrating between the two given releases.
func (registry *Registry) Find(from, to string) (Plan, error) {
	registry.mutex.RLock()
	defer registry.mutex.RUnlock()
	for _, plan := range registry.plans {
		meta := plan.Meta()
		if meta.From == from && meta.To == to {
			return plan, nil
		}
	}
	return nil, fmt.Errorf("there is no migration plan from %s to %s, run \"dma plans\" to see the known ones", from, to)
}

// All returns every registered plan, ordered by plan ID.
func (registry *Registry) All() []Plan {
	registry.mutex.RLock()
	defer registry.mutex.RUnlock()
	plans := make([]Plan, 0, len(registry.plans))
	for _, plan := range registry.plans {
		plans = append(plans, plan)
	}
	sort.Slice(plans, func(i, j int) bool { return plans[i].Meta().ID < plans[j].Meta().ID })
	return plans
}

// MustRegister adds a plan to the default registry, and panics when the plan is malformed.
// Plans call it from their init function, so a malformed plan fails the binary at start-up
// rather than in the middle of a migration.
func MustRegister(plan Plan) {
	if err := Default.Register(plan); err != nil {
		panic("dma: " + err.Error())
	}
}

// Validate checks the structural invariants every plan has to satisfy: contiguous step numbering
// starting at one, unique and non-empty step names, steps ordered by phase, and a cleanup phase
// which is always the last one.
func Validate(plan Plan) error {
	if plan == nil {
		return fmt.Errorf("a nil plan cannot be registered")
	}
	meta := plan.Meta()
	if strings.TrimSpace(meta.ID) == "" {
		return fmt.Errorf("a plan without an ID cannot be registered")
	}
	if strings.TrimSpace(meta.From) == "" || strings.TrimSpace(meta.To) == "" {
		return fmt.Errorf("plan %s does not name both the release it migrates from and the one it migrates to", meta.ID)
	}
	steps := plan.Steps()
	if len(steps) == 0 {
		return fmt.Errorf("plan %s has no steps", meta.ID)
	}
	names := map[string]bool{}
	previousPhase := PhasePreflight
	for index, step := range steps {
		if step == nil {
			return fmt.Errorf("plan %s has a nil step at position %d", meta.ID, index+1)
		}
		stepMeta := step.Meta()
		if stepMeta.ID != index+1 {
			return fmt.Errorf("plan %s numbers its steps incorrectly: step at position %d has ID %d", meta.ID, index+1, stepMeta.ID)
		}
		if strings.TrimSpace(stepMeta.Name) == "" {
			return fmt.Errorf("plan %s has an unnamed step %d", meta.ID, stepMeta.ID)
		}
		if names[stepMeta.Name] {
			return fmt.Errorf("plan %s uses the step name %s more than once", meta.ID, stepMeta.Name)
		}
		names[stepMeta.Name] = true
		if index > 0 && stepMeta.Phase < previousPhase {
			return fmt.Errorf("plan %s is not ordered by phase: step %d is in phase %s, after a step in phase %s", meta.ID, stepMeta.ID, stepMeta.Phase, previousPhase)
		}
		previousPhase = stepMeta.Phase
	}
	if previousPhase != PhaseCleanup {
		return fmt.Errorf("plan %s does not end in the cleanup phase", meta.ID)
	}
	return nil
}
