package model

type Request struct {
	Namespace string `json:"namespace"`
	Pod       string `json:"pod"`
	Branch    string `json:"branch,omitempty"`
}

type Check struct {
	ID          string   `json:"id"`
	Stage       string   `json:"stage"`
	Name        string   `json:"name"`
	Passed      bool     `json:"passed"`
	Determinate bool     `json:"determinate"`
	Skipped     bool     `json:"skipped,omitempty"`
	Reason      string   `json:"reason"`
	Evidence    any      `json:"evidence,omitempty"`
	Source      []string `json:"source,omitempty"`
}

type Outcome string

const (
	OutcomePass    Outcome = "pass"
	OutcomeFail    Outcome = "fail"
	OutcomeUnknown Outcome = "unknown"
	OutcomeSkipped Outcome = "skipped"
)

type StageState struct {
	Outcome    Outcome `json:"outcome"`
	Conclusion string  `json:"conclusion"`
	SkipReason string  `json:"skipReason,omitempty"`
}

type ResourcePair struct {
	Free         *float64 `json:"free,omitempty"`
	Total        *float64 `json:"total,omitempty"`
	Used         *float64 `json:"used,omitempty"`
	Releasing    *float64 `json:"releasing,omitempty"`
	Insufficient bool     `json:"insufficient,omitempty"`
}

type NodeResult struct {
	Name        string                  `json:"name"`
	Passed      bool                    `json:"passed"`
	Determinate bool                    `json:"determinate"`
	Resources   map[string]ResourcePair `json:"resources,omitempty"`
	Checks      []Check                 `json:"checks"`
}

type QueueResourceValue struct {
	Capability *float64 `json:"capability,omitempty"`
	Deserved   *float64 `json:"deserved,omitempty"`
	Allocated  *float64 `json:"allocated,omitempty"`
	Request    *float64 `json:"request,omitempty"`
	Inqueue    *float64 `json:"inqueue,omitempty"`
	Elastic    *float64 `json:"elastic,omitempty"`
	Candidate  *float64 `json:"candidate,omitempty"`
	Available  *float64 `json:"available,omitempty"`
	Required   *float64 `json:"required,omitempty"`
}

type QueueSummary struct {
	Name               string                        `json:"name"`
	State              string                        `json:"state,omitempty"`
	Strategy           string                        `json:"strategy,omitempty"`
	Formula            string                        `json:"formula,omitempty"`
	FormulaNote        string                        `json:"formulaNote,omitempty"`
	RuntimeSource      string                        `json:"runtimeSource,omitempty"`
	RuntimeDeterminate bool                          `json:"runtimeDeterminate"`
	RuntimeReason      string                        `json:"runtimeReason,omitempty"`
	Resources          map[string]QueueResourceValue `json:"resources,omitempty"`
}

type PodGroupSummary struct {
	Namespace         string             `json:"namespace"`
	Name              string             `json:"name"`
	Target            bool               `json:"target,omitempty"`
	Phase             string             `json:"phase,omitempty"`
	PriorityClassName string             `json:"priorityClassName,omitempty"`
	Priority          *int32             `json:"priority,omitempty"`
	CreatedAt         string             `json:"createdAt,omitempty"`
	AgeSeconds        int64              `json:"ageSeconds,omitempty"`
	MinMember         int32              `json:"minMember,omitempty"`
	Resources         map[string]float64 `json:"resources,omitempty"`
}

type EnqueueReport struct {
	State     StageState        `json:"state"`
	Queue     QueueSummary      `json:"queue"`
	Checks    []Check           `json:"checks"`
	PodGroups []PodGroupSummary `json:"podGroups"`
}

type WorkloadRow struct {
	Namespace  string             `json:"namespace"`
	Pod        string             `json:"pod"`
	Containers []string           `json:"containers"`
	Resources  map[string]float64 `json:"resources,omitempty"`
	Checks     []Check            `json:"checks"`
}

type JobValidReport struct {
	State StageState    `json:"state"`
	Rows  []WorkloadRow `json:"rows"`
}

type AllocateReport struct {
	State StageState   `json:"state"`
	Nodes []NodeResult `json:"nodes"`
}

type Diagnosis struct {
	RootCause   string   `json:"rootCause"`
	Suggestions []string `json:"suggestions"`
}

type ConfiguredPlugin struct {
	Name              string          `json:"name"`
	Tier              int             `json:"tier"`
	Order             int             `json:"order"`
	ArgumentKeys      []string        `json:"argumentKeys,omitempty"`
	OptionKeys        []string        `json:"optionKeys,omitempty"`
	ExplicitArguments map[string]bool `json:"explicitArguments,omitempty"`
	ExplicitOptions   map[string]bool `json:"explicitOptions,omitempty"`
}

type PluginTier struct {
	Order   int                `json:"order"`
	Plugins []ConfiguredPlugin `json:"plugins"`
}

type SchedulerPolicy struct {
	Determinate              bool         `json:"determinate"`
	ActiveDeterminate        bool         `json:"activeDeterminate"`
	Reason                   string       `json:"reason,omitempty"`
	SchedulerNamespace       string       `json:"schedulerNamespace,omitempty"`
	SchedulerPod             string       `json:"schedulerPod,omitempty"`
	SchedulerUID             string       `json:"schedulerUID,omitempty"`
	SchedulerConfPath        string       `json:"schedulerConfPath,omitempty"`
	ConfigMapNamespace       string       `json:"configMapNamespace,omitempty"`
	ConfigMapName            string       `json:"configMapName,omitempty"`
	ConfigMapKey             string       `json:"configMapKey,omitempty"`
	ConfigMapUID             string       `json:"configMapUID,omitempty"`
	ConfigMapResourceVersion string       `json:"configMapResourceVersion,omitempty"`
	Actions                  []string     `json:"actions,omitempty"`
	Tiers                    []PluginTier `json:"tiers,omitempty"`
}

type PluginHookReport struct {
	Action             string   `json:"action"`
	Phase              int      `json:"phase"`
	Tier               int      `json:"tier"`
	Order              int      `json:"order"`
	Plugin             string   `json:"plugin"`
	Hook               string   `json:"hook"`
	Enabled            bool     `json:"enabled"`
	EnabledDeterminate bool     `json:"enabledDeterminate"`
	Passed             bool     `json:"passed"`
	Determinate        bool     `json:"determinate"`
	Reason             string   `json:"reason"`
	Source             []string `json:"source,omitempty"`
}

type ActionReport struct {
	Name       string             `json:"name"`
	Order      int                `json:"order"`
	Configured bool               `json:"configured"`
	Checks     []Check            `json:"checks"`
	Plugins    []PluginHookReport `json:"plugins,omitempty"`
	Nodes      []NodeResult       `json:"nodes,omitempty"`
}

type Report struct {
	Request              Request            `json:"request"`
	Scheduler            any                `json:"scheduler"`
	Policy               SchedulerPolicy    `json:"policy"`
	Checks               []Check            `json:"checks"`
	Nodes                []NodeResult       `json:"nodes"`
	Actions              []ActionReport     `json:"actions"`
	Passed               bool               `json:"passed"`
	Conclusion           string             `json:"conclusion"`
	Diagnosis            Diagnosis          `json:"diagnosis"`
	Enqueue              EnqueueReport      `json:"enqueue"`
	JobValid             JobValidReport     `json:"jobValid"`
	Allocate             AllocateReport     `json:"allocate"`
	SourceFallback       bool               `json:"sourceFallback"`
	LLM                  string             `json:"llm,omitempty"`
	PluginHooks          []PluginHookReport `json:"-"`
	HooksInspected       bool               `json:"-"`
	PredicateDefaults    map[string]bool    `json:"-"`
	PredicateDefaultsErr string             `json:"-"`
}
