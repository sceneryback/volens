package agent

import (
	"errors"
	"strings"
	"testing"

	"github.com/volcano-sh/volens/internal/agent/model"
	"github.com/volcano-sh/volens/internal/cluster"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestBuildPresentationSkipsJobValidAndAllocateAfterKnownEnqueueFailure(t *testing.T) {
	report := Report{
		Checks: []Check{
			model.Known(
				"job.enqueue.evidence",
				"enqueue",
				"Job entered the queue",
				false,
				"queue resource quota insufficient",
				nil,
			),
			model.Known(
				"plugins.gang.min-member",
				"jobValid",
				"Gang minMember",
				true,
				"would pass if reached",
				nil,
			),
		},
		Nodes: []NodeResult{
			{
				Name:        "node-a",
				Passed:      true,
				Determinate: true,
				Checks: []Check{
					model.Known(
						"node.resources",
						"allocate",
						"Node resources",
						true,
						"would pass if reached",
						nil,
					),
				},
			},
		},
	}
	target := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "job-a-0",
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "worker"}},
		},
	}
	stopReason := "enqueue 明确失败：队列资源不足"

	buildPresentation(&report, presentationEvidence{
		Pod:             target,
		Tasks:           []corev1.Pod{*target.DeepCopy()},
		QueueName:       "batch",
		EnqueueStopped:  true,
		AllocateStopped: true,
		StopReason:      stopReason,
	})

	if report.JobValid.State.Outcome != model.OutcomeSkipped {
		t.Fatalf("JobValid state=%+v", report.JobValid.State)
	}

	if report.JobValid.State.SkipReason != stopReason {
		t.Fatalf("JobValid skip reason=%q want=%q", report.JobValid.State.SkipReason, stopReason)
	}

	if len(report.JobValid.Rows) != 1 || len(report.JobValid.Rows[0].Checks) != 1 {
		t.Fatalf("JobValid rows=%+v", report.JobValid.Rows)
	}

	jobValidCheck := report.JobValid.Rows[0].Checks[0]
	if !jobValidCheck.Skipped || jobValidCheck.Stage != "jobValid" {
		t.Fatalf("JobValid check=%+v", jobValidCheck)
	}

	if report.Allocate.State.Outcome != model.OutcomeSkipped {
		t.Fatalf("allocate state=%+v", report.Allocate.State)
	}

	if report.Allocate.State.SkipReason != stopReason {
		t.Fatalf("allocate skip reason=%q want=%q", report.Allocate.State.SkipReason, stopReason)
	}

	if len(report.Allocate.Nodes) != 0 {
		t.Fatalf("skipped allocate must not expose nodes: %+v", report.Allocate.Nodes)
	}
}

func TestFinishReportSkipsAllSchedulerStagesAfterPreflightStops(t *testing.T) {
	tests := []struct {
		name  string
		check Check
	}{
		{
			name: "known invalid target",
			check: model.Known(
				"task.pending",
				"preflight",
				"Pod is Pending",
				false,
				"phase=Running",
				nil,
			),
		},
		{
			name: "target lookup unavailable",
			check: model.Unknown(
				"task.load",
				"preflight",
				"Target Pod loaded from informer cache",
				"informer cache unavailable",
				nil,
				nil,
			),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			report := Report{
				Checks: []Check{test.check},
			}
			evidence := stoppedAtPreflight(test.check)

			finishReport(&report, evidence)

			stages := map[string]StageState{
				"enqueue":  report.Enqueue.State,
				"jobValid": report.JobValid.State,
				"allocate": report.Allocate.State,
			}

			for stage, state := range stages {
				if state.Outcome != model.OutcomeSkipped {
					t.Fatalf("%s state=%+v", stage, state)
				}

				if state.SkipReason != evidence.StopReason {
					t.Fatalf(
						"%s skip reason=%q want=%q",
						stage,
						state.SkipReason,
						evidence.StopReason,
					)
				}
			}

			if len(report.Allocate.Nodes) != 0 || len(report.Nodes) != 0 {
				t.Fatalf("preflight stop exposed nodes: allocate=%+v report=%+v", report.Allocate.Nodes, report.Nodes)
			}

			for _, check := range report.Checks {
				if check.ID == "allocate.nodes" {
					t.Fatalf("preflight stop manufactured node availability: %+v", check)
				}
			}
		})
	}
}

func TestDiagnoseReportUsesPreflightUnknownOnlyWhenAllStagesSkipped(t *testing.T) {
	preflightUnknown := model.Unknown(
		"task.load",
		"preflight",
		"Target Pod loaded from informer cache",
		"informer cache unavailable",
		nil,
		nil,
	)
	skipped := StageState{Outcome: model.OutcomeSkipped}
	report := Report{
		Checks:   []Check{preflightUnknown},
		Enqueue:  EnqueueReport{State: skipped},
		JobValid: JobValidReport{State: skipped},
		Allocate: AllocateReport{State: skipped},
	}

	diagnosis := diagnoseReport(report)

	if !strings.Contains(diagnosis.RootCause, "前置证据不足") ||
		!strings.Contains(diagnosis.RootCause, preflightUnknown.Name) ||
		!strings.Contains(diagnosis.RootCause, preflightUnknown.Reason) {
		t.Fatalf("diagnosis=%+v", diagnosis)
	}

	if len(diagnosis.Suggestions) != 2 ||
		!strings.Contains(diagnosis.Suggestions[0], "重试") ||
		!strings.Contains(diagnosis.Suggestions[0], "informer") ||
		!strings.Contains(diagnosis.Suggestions[1], "Kubernetes API") {
		t.Fatalf("suggestions=%+v", diagnosis.Suggestions)
	}

	report.Allocate.State.Outcome = model.OutcomeUnknown
	diagnosis = diagnoseReport(report)

	if strings.Contains(diagnosis.RootCause, "前置证据不足") {
		t.Fatalf("non-skipped stages must retain the normal unknown diagnosis: %+v", diagnosis)
	}
}

func TestPluginHookChecksPreservesTierOrderAndMapsEnableState(t *testing.T) {
	hooks := []PluginHookReport{
		{
			Action:             "allocate",
			Tier:               0,
			Order:              0,
			Plugin:             "disabled",
			Hook:               "AddJobValidFn",
			Enabled:            false,
			EnabledDeterminate: true,
			Determinate:        true,
			Passed:             false,
			Reason:             "enable switch is false",
		},
		{
			Action:             "allocate",
			Tier:               0,
			Order:              1,
			Plugin:             "enable-unknown",
			Hook:               "AddJobValidFn",
			Enabled:            true,
			EnabledDeterminate: false,
			Determinate:        true,
			Passed:             true,
			Reason:             "selected-branch default is unknown",
		},
		{
			Action:             "allocate",
			Tier:               1,
			Order:              0,
			Plugin:             "result-unknown",
			Hook:               "AddJobValidFn",
			Enabled:            true,
			EnabledDeterminate: true,
			Determinate:        false,
			Reason:             "plugin-private session state is unavailable",
		},
		{
			Action:             "allocate",
			Tier:               1,
			Order:              1,
			Plugin:             "known-failure",
			Hook:               "AddJobValidFn",
			Enabled:            true,
			EnabledDeterminate: true,
			Determinate:        true,
			Passed:             false,
			Reason:             "hook rejected the job",
		},
		{
			Action:             "enqueue",
			Tier:               0,
			Order:              0,
			Plugin:             "wrong-action",
			Hook:               "AddJobValidFn",
			Enabled:            true,
			EnabledDeterminate: true,
			Determinate:        true,
			Passed:             true,
		},
	}

	checks := pluginHookChecks(
		hooks,
		map[string]bool{"allocate": true},
		map[string]bool{"AddJobValidFn": true},
		"jobValid",
	)

	if len(checks) != 4 {
		t.Fatalf("checks=%+v", checks)
	}

	wantNames := []string{
		"T1.1 disabled JobValidFn",
		"T1.2 enable-unknown JobValidFn",
		"T2.1 result-unknown JobValidFn",
		"T2.2 known-failure JobValidFn",
	}

	for index, want := range wantNames {
		if checks[index].Name != want || checks[index].Stage != "jobValid" {
			t.Fatalf("checks[%d]=%+v want name=%q", index, checks[index], want)
		}
	}

	if !checks[0].Skipped || !checks[0].Determinate || !checks[0].Passed {
		t.Fatalf("disabled hook=%+v", checks[0])
	}

	if checks[1].Skipped || checks[1].Determinate || checks[1].Passed {
		t.Fatalf("unknown enable state=%+v", checks[1])
	}

	if checks[2].Skipped || checks[2].Determinate || checks[2].Passed {
		t.Fatalf("unknown hook result=%+v", checks[2])
	}

	if checks[3].Skipped || !checks[3].Determinate || checks[3].Passed {
		t.Fatalf("known hook failure=%+v", checks[3])
	}
}

func TestQueueResourceValuesMergesSnapshotAndCandidateWithAvailable(t *testing.T) {
	snapshot := cluster.QueueSnapshot{
		QueueName: "batch",
		Resources: map[string]cluster.QueueSnapshotResource{
			"cpu": {
				Capability: testFloat64Pointer(100),
				Deserved:   testFloat64Pointer(80),
				Allocated:  testFloat64Pointer(60),
				Request:    testFloat64Pointer(97),
				Inqueue:    testFloat64Pointer(30),
			},
			"memory": {
				Capability: testFloat64Pointer(64),
				Allocated:  testFloat64Pointer(32),
				Inqueue:    testFloat64Pointer(8),
			},
		},
	}
	group := cluster.PodGroup{
		MinResources: corev1.ResourceList{
			corev1.ResourceCPU:                    resource.MustParse("12"),
			corev1.ResourceName("huawei.com/npu"): resource.MustParse("8"),
			corev1.ResourceName("requests.cpu"):   resource.MustParse("14"),
		},
	}

	values := queueResourceValues(snapshot, group)

	if len(values) != 3 {
		t.Fatalf("resources=%+v", values)
	}

	cpu := values["cpu"]
	if cpu.Capability == nil || *cpu.Capability != 100 ||
		cpu.Deserved == nil || *cpu.Deserved != 80 ||
		cpu.Allocated == nil || *cpu.Allocated != 60 ||
		cpu.Request == nil || *cpu.Request != 97 ||
		cpu.Inqueue == nil || *cpu.Inqueue != 30 {
		t.Fatalf("cpu snapshot fields were not preserved: %+v", cpu)
	}

	if cpu.Candidate == nil || *cpu.Candidate != 14 {
		t.Fatalf("cpu candidate=%+v", cpu.Candidate)
	}

	if cpu.Available == nil || *cpu.Available != 10 {
		t.Fatalf("cpu conservative available=%+v want=10", cpu.Available)
	}

	memory := values["memory"]
	if memory.Candidate != nil || memory.Available != nil {
		t.Fatalf("snapshot-only memory dimension=%+v", memory)
	}

	npu := values["huawei.com/npu"]
	if npu.Candidate == nil || *npu.Candidate != 8 {
		t.Fatalf("npu candidate=%+v", npu.Candidate)
	}

	if npu.Available != nil {
		t.Fatalf("missing runtime NPU snapshot must not manufacture available capacity: %+v", npu)
	}
}

func TestQueueResourceValuesCollapsesRequestAliases(t *testing.T) {
	snapshot := cluster.QueueSnapshot{
		Resources: map[string]cluster.QueueSnapshotResource{
			"pods": {
				Capability: testFloat64Pointer(100),
			},
			"requests.pods": {
				Allocated: testFloat64Pointer(8),
				Inqueue:   testFloat64Pointer(2),
			},
			"nvidia.com/gpu": {
				Capability: testFloat64Pointer(8),
			},
			"requests.nvidia.com/gpu": {
				Allocated: testFloat64Pointer(4),
			},
		},
	}
	group := cluster.PodGroup{
		MinResources: corev1.ResourceList{
			corev1.ResourceName("requests.pods"):           resource.MustParse("1"),
			corev1.ResourceName("requests.nvidia.com/gpu"): resource.MustParse("4"),
		},
	}

	values := queueResourceValues(snapshot, group)
	if len(values) != 2 {
		t.Fatalf("resources=%+v", values)
	}

	if values["pods"].Capability == nil || values["pods"].Allocated == nil ||
		values["nvidia.com/gpu"].Capability == nil || values["nvidia.com/gpu"].Candidate == nil {
		t.Fatalf("aliases were not merged field by field: %+v", values)
	}
}

func TestQueueCapacityCheckRejectsExceededDimension(t *testing.T) {
	queue := QueueSummary{
		Strategy: "capacity",
		Resources: map[string]QueueResourceValue{
			"nvidia.com/gpu": {
				Capability: testFloat64Pointer(8),
				Allocated:  testFloat64Pointer(0),
				Inqueue:    testFloat64Pointer(0),
				Elastic:    testFloat64Pointer(0),
				Candidate:  testFloat64Pointer(16),
				Available:  testFloat64Pointer(8),
				Required:   testFloat64Pointer(16),
			},
		},
	}

	check := queueCapacityCheck(queue, nil, true)
	if !check.Determinate || check.Passed || check.Skipped ||
		!strings.Contains(check.Reason, "nvidia.com/gpu") {
		t.Fatalf("capacity rejection=%+v", check)
	}
}

func TestQueueCapacityCheckRemainsUnknownWhenLocalCacheSnapshotFails(t *testing.T) {
	check := queueCapacityCheck(
		QueueSummary{
			Strategy: "proportion",
			Resources: map[string]QueueResourceValue{
				"cpu": {
					Capability: testFloat64Pointer(100),
					Allocated:  testFloat64Pointer(60),
					Inqueue:    testFloat64Pointer(20),
					Candidate:  testFloat64Pointer(10),
					Available:  testFloat64Pointer(20),
				},
			},
		},
		errors.New("cache dump parse failed"),
		true,
	)

	if check.Determinate || check.Passed || !strings.Contains(check.Reason, "local scheduler cache snapshot was unavailable") {
		t.Fatalf("cache snapshot capacity check=%+v", check)
	}
}

func TestQueueCapacityCheckRequiresParsedPolicy(t *testing.T) {
	queue := QueueSummary{
		Strategy: "proportion",
		Resources: map[string]QueueResourceValue{
			"cpu": {
				Capability: testFloat64Pointer(100),
				Allocated:  testFloat64Pointer(60),
				Inqueue:    testFloat64Pointer(20),
				Candidate:  testFloat64Pointer(10),
				Available:  testFloat64Pointer(20),
			},
		},
	}

	check := queueCapacityCheck(queue, nil, false)
	if check.Determinate || check.Passed || check.Skipped ||
		!strings.Contains(check.Reason, "could not be parsed") {
		t.Fatalf("unconfirmed queue capacity check=%+v", check)
	}

	policy := SchedulerPolicy{
		Determinate:       true,
		ActiveDeterminate: false,
		Tiers: []PluginTier{
			{Plugins: []ConfiguredPlugin{{Name: "proportion"}}},
		},
	}
	if strategy := queueStrategy(policy); strategy != "proportion" {
		t.Fatalf("strategy=%q", strategy)
	}
}

func TestDiagnoseReportPrioritizesKnownEnqueueFailure(t *testing.T) {
	report := Report{
		Enqueue: EnqueueReport{
			Queue: QueueSummary{Name: "batch"},
			Checks: []Check{
				model.Known(
					"queue.enqueue-capacity",
					"enqueue",
					"Queue has room for candidate",
					false,
					"candidate cpu exceeds conservative available capacity",
					nil,
				),
			},
		},
		JobValid: JobValidReport{
			Rows: []WorkloadRow{
				{
					Pod: "job-a-0",
					Checks: []Check{
						model.Known(
							"plugins.gang.min-member",
							"jobValid",
							"Gang minMember",
							false,
							"not enough tasks",
							nil,
						),
					},
				},
			},
		},
		Allocate: AllocateReport{
			Nodes: []NodeResult{
				{
					Name: "node-a",
					Checks: []Check{
						model.Known(
							"node.resources",
							"allocate",
							"Node resources",
							false,
							"insufficient cpu",
							nil,
						),
					},
				},
			},
		},
	}

	diagnosis := diagnoseReport(report)

	if diagnosis.RootCause != "队列当前没有足够的可入队资源" {
		t.Fatalf("diagnosis=%+v", diagnosis)
	}

	if len(diagnosis.Suggestions) < 2 ||
		!strings.Contains(diagnosis.Suggestions[0], "释放") ||
		!strings.Contains(diagnosis.Suggestions[1], "等待") {
		t.Fatalf("enqueue suggestions=%+v", diagnosis.Suggestions)
	}
}

func TestBuildAllocatePresentationAppliesPredicateArgumentsPerColumn(t *testing.T) {
	report := Report{
		Policy: SchedulerPolicy{
			ActiveDeterminate: true,
			Tiers: []PluginTier{
				{
					Plugins: []ConfiguredPlugin{
						{
							Name: "predicates",
							ExplicitArguments: map[string]bool{
								"predicate.NodeAffinityEnable": false,
								"predicate.ProportionalEnable": true,
							},
						},
					},
				},
			},
		},
		HooksInspected: true,
		PredicateDefaults: map[string]bool{
			"predicate.TaintTolerationEnable": true,
		},
		PluginHooks: []PluginHookReport{
			{
				Action:             "allocate",
				Plugin:             "predicates",
				Hook:               "AddPredicateFn",
				Enabled:            true,
				EnabledDeterminate: true,
				Reason:             "enablePredicate=true",
			},
		},
		Nodes: []NodeResult{
			{
				Name: "node-a",
				Checks: []Check{
					model.Known(
						"node.affinity",
						"allocate",
						"Required node affinity",
						false,
						"local projection failed",
						nil,
					),
					model.Known(
						"node.taints",
						"allocate",
						"Taints tolerated",
						true,
						"local projection passed",
						nil,
					),
					model.Skipped(
						"node.proportional",
						"allocate",
						"Proportional resource predicate",
						"default false",
						nil,
					),
				},
			},
		},
	}

	allocate := buildAllocatePresentation(report, presentationEvidence{})
	if len(allocate.Nodes) != 1 {
		t.Fatalf("nodes=%+v", allocate.Nodes)
	}

	affinity := checkByID(t, allocate.Nodes[0].Checks, "node.affinity")
	if !affinity.Skipped || !affinity.Determinate || !affinity.Passed {
		t.Fatalf("disabled affinity=%+v", affinity)
	}

	taints := checkByID(t, allocate.Nodes[0].Checks, "node.taints")
	if taints.Skipped || !taints.Determinate || !taints.Passed {
		t.Fatalf("enabled taints=%+v", taints)
	}

	proportional := checkByID(t, allocate.Nodes[0].Checks, "node.proportional")
	if proportional.Skipped || proportional.Determinate || proportional.Passed {
		t.Fatalf("enabled but unreproduced proportional=%+v", proportional)
	}

	if allocate.State.Outcome != model.OutcomeUnknown {
		t.Fatalf("allocate state=%+v", allocate.State)
	}
}

func TestBuildAllocatePresentationKeepsDeterminatePredicateProjectionWhenPolicyIsNotProvenActive(t *testing.T) {
	evidence := map[string]string{"node": "node-a", "projection": "failed"}
	report := Report{
		Policy: SchedulerPolicy{
			ActiveDeterminate: false,
			Tiers: []PluginTier{
				{
					Plugins: []ConfiguredPlugin{
						{
							Name: "predicates",
							ExplicitArguments: map[string]bool{
								"predicate.NodeAffinityEnable": true,
							},
						},
					},
				},
			},
		},
		HooksInspected: true,
		PluginHooks: []PluginHookReport{
			{
				Action:             "allocate",
				Plugin:             "predicates",
				Hook:               "AddPredicateFn",
				Enabled:            true,
				EnabledDeterminate: true,
			},
		},
		Nodes: []NodeResult{
			{
				Name: "node-a",
				Checks: []Check{
					model.Known(
						"node.affinity",
						"allocate",
						"Required node affinity",
						false,
						"local projection failed",
						nil,
					),
				},
			},
		},
	}
	report.Nodes[0].Checks[0].Evidence = evidence

	allocate := buildAllocatePresentation(report, presentationEvidence{})
	affinity := checkByID(t, allocate.Nodes[0].Checks, "node.affinity")

	if !affinity.Determinate || affinity.Passed || affinity.Skipped {
		t.Fatalf("local Kubernetes-visible projection must remain determinate: %+v", affinity)
	}

	if affinity.Evidence == nil || !strings.Contains(affinity.Reason, "not proven") {
		t.Fatalf("projection evidence or active-policy reason was lost: %+v", affinity)
	}

	if !allocate.Nodes[0].Determinate || allocate.State.Outcome != model.OutcomeFail {
		t.Fatalf("node/stage outcome=%+v / %+v", allocate.Nodes[0], allocate.State)
	}
}

func TestBuildAllocatePresentationKeepsDefaultDisabledProportionalSkipped(t *testing.T) {
	report := Report{
		Policy: SchedulerPolicy{
			ActiveDeterminate: false,
			Tiers: []PluginTier{
				{
					Plugins: []ConfiguredPlugin{
						{Name: "predicates"},
					},
				},
			},
		},
		HooksInspected: true,
		PredicateDefaults: map[string]bool{
			"predicate.ProportionalEnable": false,
		},
		Nodes: []NodeResult{
			{
				Name: "node-a",
				Checks: []Check{
					model.Skipped(
						"node.proportional",
						"allocate",
						"Proportional resource predicate",
						"default false",
						nil,
					),
				},
			},
		},
	}

	allocate := buildAllocatePresentation(report, presentationEvidence{})
	proportional := checkByID(t, allocate.Nodes[0].Checks, "node.proportional")

	if !proportional.Skipped || !proportional.Determinate || !proportional.Passed {
		t.Fatalf("default-disabled proportional predicate should remain skipped: %+v", proportional)
	}
}

func TestBuildAllocatePresentationKeepsExplicitEnabledProportionalUnknown(t *testing.T) {
	report := Report{
		Policy: SchedulerPolicy{
			ActiveDeterminate: false,
			Tiers: []PluginTier{
				{
					Plugins: []ConfiguredPlugin{
						{
							Name: "predicates",
							ExplicitArguments: map[string]bool{
								"predicate.ProportionalEnable": true,
							},
						},
					},
				},
			},
		},
		HooksInspected: true,
		Nodes: []NodeResult{
			{
				Name: "node-a",
				Checks: []Check{
					model.Skipped(
						"node.proportional",
						"allocate",
						"Proportional resource predicate",
						"default false",
						nil,
					),
				},
			},
		},
	}

	allocate := buildAllocatePresentation(report, presentationEvidence{})
	proportional := checkByID(t, allocate.Nodes[0].Checks, "node.proportional")

	if proportional.Skipped || proportional.Determinate || proportional.Passed {
		t.Fatalf("explicit-enabled proportional predicate should remain unknown until runtime proof: %+v", proportional)
	}
}

func TestAllocateStateUsesNodeAlternativesRatherThanAnyFailedCell(t *testing.T) {
	report := Report{
		Nodes: []NodeResult{
			{
				Name:        "node-a",
				Passed:      false,
				Determinate: true,
				Checks: []Check{
					model.Known("custom.a", "allocate", "Filter A", false, "node-a failed", nil),
				},
			},
			{
				Name:        "node-b",
				Passed:      true,
				Determinate: true,
				Checks: []Check{
					model.Known("custom.a", "allocate", "Filter A", true, "node-b passed", nil),
				},
			},
		},
	}

	allocate := buildAllocatePresentation(report, presentationEvidence{})
	if allocate.State.Outcome != model.OutcomePass {
		t.Fatalf("one feasible node must pass the stage: %+v", allocate.State)
	}

	report.Nodes[1] = NodeResult{
		Name:        "node-b",
		Passed:      false,
		Determinate: true,
		Checks: []Check{
			model.Known("custom.b", "allocate", "Filter B", false, "node-b failed", nil),
		},
	}

	allocate = buildAllocatePresentation(report, presentationEvidence{})
	if allocate.State.Outcome != model.OutcomeFail {
		t.Fatalf("all determinately filtered nodes must fail the stage: %+v", allocate.State)
	}

	report.Allocate = allocate
	diagnosis := diagnoseReport(report)
	if !strings.Contains(diagnosis.RootCause, "各节点") {
		t.Fatalf("mixed-node diagnosis=%+v", diagnosis)
	}
}

func TestEnqueueEvidenceCanProveStageDespiteUnknownPerPluginVotes(t *testing.T) {
	checks := []Check{
		model.Known(
			"job.enqueue.evidence",
			"enqueue",
			"Enqueue evidence",
			true,
			"PodGroup phase=Inqueue",
			nil,
		),
		model.Unknown(
			"plugin.ascend.enqueue",
			"enqueue",
			"Ascend JobEnqueueable",
			"individual vote cannot be reconstructed",
			nil,
			nil,
		),
	}

	state := enqueueState(checks)
	if state.Outcome != model.OutcomePass || !strings.Contains(state.Conclusion, "已经通过") {
		t.Fatalf("state=%+v", state)
	}
}

func TestJobValidResolvesOneSidedGangPassFromVisibleTasks(t *testing.T) {
	gangCheck := model.Unknown(
		"plugins.gang.min-member",
		"jobValid",
		"Active gang minMember rule",
		"calculated visible tasks",
		map[string]any{"wouldPass": true},
		nil,
	)
	report := Report{
		Policy: SchedulerPolicy{
			Determinate:       true,
			ActiveDeterminate: true,
			Actions:           []string{"allocate"},
		},
		HooksInspected: true,
		Checks: []Check{
			gangCheck,
			model.Unknown(
				"plugins.job-valid",
				"jobValid",
				"Remaining active JobValid plugin hooks",
				"unknown until source inspection",
				nil,
				nil,
			),
		},
		PluginHooks: []PluginHookReport{
			{
				Action:             "allocate",
				Plugin:             "gang",
				Hook:               "AddJobValidFn",
				Enabled:            true,
				EnabledDeterminate: true,
			},
		},
	}

	jobValid := buildJobValidPresentation(report, presentationEvidence{})
	if jobValid.State.Outcome != model.OutcomePass {
		t.Fatalf("state=%+v", jobValid.State)
	}
}

func TestJobValidReportsVerifiedNoActiveHooks(t *testing.T) {
	report := Report{
		Policy: SchedulerPolicy{
			Determinate:       true,
			ActiveDeterminate: true,
			Actions:           []string{"allocate"},
		},
		HooksInspected: true,
		Checks: []Check{
			model.Unknown(
				"plugins.gang.min-member",
				"jobValid",
				"Active gang minMember rule",
				"gang activation was not known before source inspection",
				map[string]any{"wouldPass": false},
				nil,
			),
			model.Unknown(
				"plugins.job-valid",
				"jobValid",
				"Remaining active JobValid plugin hooks",
				"unknown until source inspection",
				nil,
				nil,
			),
		},
		PluginHooks: []PluginHookReport{
			{
				Action:             "allocate",
				Plugin:             "predicates",
				Hook:               "AddPredicateFn",
				Enabled:            true,
				EnabledDeterminate: true,
			},
		},
	}
	target := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "job-a-0"},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "worker"}}},
	}

	jobValid := buildJobValidPresentation(report, presentationEvidence{Pod: target})
	if jobValid.State.Outcome != model.OutcomePass {
		t.Fatalf("state=%+v", jobValid.State)
	}

	if len(jobValid.Rows) != 1 {
		t.Fatalf("rows=%+v", jobValid.Rows)
	}

	none := checkByID(t, jobValid.Rows[0].Checks, "plugins.job-valid.none")
	if !none.Determinate || !none.Passed || none.Skipped {
		t.Fatalf("verified-none check=%+v", none)
	}

	for _, check := range jobValid.Rows[0].Checks {
		if strings.HasPrefix(check.ID, "plugins.gang.") || check.ID == "plugins.job-valid" {
			t.Fatalf("inactive synthetic check remained: %+v", check)
		}
	}
}

func TestJobValidKeepsUnmatchedPluginUnknownBesideMatchedGang(t *testing.T) {
	report := Report{
		Policy: SchedulerPolicy{
			Determinate:       true,
			ActiveDeterminate: true,
			Actions:           []string{"allocate"},
		},
		HooksInspected: true,
		Checks: []Check{
			model.Unknown(
				"plugins.gang.min-member",
				"jobValid",
				"Active gang minMember rule",
				"visible tasks meet the minimum",
				map[string]any{"wouldPass": true},
				nil,
			),
			model.Unknown(
				"plugins.job-valid",
				"jobValid",
				"Remaining active JobValid plugin hooks",
				"unknown until source inspection",
				nil,
				nil,
			),
		},
		PluginHooks: []PluginHookReport{
			{
				Action:             "allocate",
				Tier:               0,
				Order:              0,
				Plugin:             "gang",
				Hook:               "AddJobValidFn",
				Enabled:            true,
				EnabledDeterminate: true,
			},
			{
				Action:      "allocate",
				Tier:        1,
				Order:       0,
				Plugin:      "external-private",
				Hook:        "source hook registration",
				Determinate: false,
				Reason:      "configured plugin source is unavailable",
			},
		},
	}
	target := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "job-a-0"},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "worker"}}},
	}

	jobValid := buildJobValidPresentation(report, presentationEvidence{Pod: target})
	if jobValid.State.Outcome != model.OutcomeUnknown {
		t.Fatalf("state=%+v", jobValid.State)
	}

	unmatched := checkByID(
		t,
		jobValid.Rows[0].Checks,
		"plugin.external-private.t2-p1.source-registration",
	)
	if unmatched.Determinate || unmatched.Passed || unmatched.Skipped {
		t.Fatalf("unmatched check=%+v", unmatched)
	}
}

func TestJobValidDoesNotResolveGangBeforeObservedPolicyIsProvenActive(t *testing.T) {
	report := Report{
		Policy: SchedulerPolicy{
			Determinate:       true,
			ActiveDeterminate: false,
			Actions:           []string{"allocate"},
		},
		HooksInspected: true,
		Checks: []Check{
			model.Unknown(
				"plugins.gang.min-member",
				"jobValid",
				"Active gang minMember rule",
				"visible tasks meet the minimum",
				map[string]any{"wouldPass": true},
				nil,
			),
			model.Unknown(
				"plugins.job-valid",
				"jobValid",
				"Remaining active JobValid plugin hooks",
				"observed policy is not proven active",
				nil,
				nil,
			),
		},
		PluginHooks: []PluginHookReport{
			{
				Action:             "allocate",
				Plugin:             "gang",
				Hook:               "AddJobValidFn",
				Enabled:            true,
				EnabledDeterminate: true,
			},
		},
	}
	target := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "job-a-0"},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "worker"}}},
	}

	jobValid := buildJobValidPresentation(report, presentationEvidence{Pod: target})
	if jobValid.State.Outcome != model.OutcomeUnknown {
		t.Fatalf("state=%+v", jobValid.State)
	}

	gang := checkByID(t, jobValid.Rows[0].Checks, "plugins.gang.min-member")
	if gang.Determinate || gang.Passed || gang.Skipped {
		t.Fatalf("unconfirmed gang projection=%+v", gang)
	}

	for _, check := range jobValid.Rows[0].Checks {
		if check.ID == "plugins.job-valid.none" {
			t.Fatalf("unconfirmed policy must not claim verified-none: %+v", check)
		}
	}
}

func TestEnqueueKeepsUnmatchedPluginBesideExactJobEnqueueableHook(t *testing.T) {
	report := Report{
		Policy: SchedulerPolicy{
			Determinate:       true,
			ActiveDeterminate: true,
			Actions:           []string{"enqueue", "allocate"},
			Tiers: []PluginTier{
				{Plugins: []ConfiguredPlugin{{Name: "proportion"}}},
				{Plugins: []ConfiguredPlugin{{Name: "external-private"}}},
			},
		},
		HooksInspected: true,
		Checks: []Check{
			model.Unknown(
				"plugins.job-enqueueable",
				"enqueue",
				"Active JobEnqueueable plugin hooks",
				"unknown until source inspection",
				nil,
				nil,
			),
		},
		PluginHooks: []PluginHookReport{
			{
				Action:             "enqueue",
				Tier:               0,
				Order:              0,
				Plugin:             "proportion",
				Hook:               "AddJobEnqueueableFn",
				Enabled:            true,
				EnabledDeterminate: true,
				Reason:             "runtime vote depends on Session state",
			},
			{
				Action:      "enqueue",
				Tier:        1,
				Order:       0,
				Plugin:      "external-private",
				Hook:        "source hook registration",
				Determinate: false,
				Reason:      "configured plugin source is unavailable",
			},
		},
	}
	evidence := presentationEvidence{
		PodGroup: cluster.PodGroup{
			MinResources: corev1.ResourceList{
				corev1.ResourceCPU: resource.MustParse("1"),
			},
		},
		QueueSnapshot: cluster.QueueSnapshot{
			Resources: map[string]cluster.QueueSnapshotResource{
				"cpu": {
					Capability: testFloat64Pointer(10),
					Allocated:  testFloat64Pointer(0),
					Inqueue:    testFloat64Pointer(0),
				},
			},
		},
	}

	enqueue := buildEnqueuePresentation(report, evidence)
	checkByID(t, enqueue.Checks, "queue.enqueue-capacity")
	for _, check := range enqueue.Checks {
		if check.ID == "plugin.proportion.t1-p1.addjobenqueueablefn" {
			t.Fatalf("locally reproduced proportion hook must not be duplicated: %+v", check)
		}
	}
	unmatched := checkByID(
		t,
		enqueue.Checks,
		"plugin.external-private.t2-p1.source-registration",
	)
	if unmatched.Determinate || unmatched.Passed || unmatched.Skipped {
		t.Fatalf("unmatched check=%+v", unmatched)
	}

	for _, check := range enqueue.Checks {
		if check.ID == "plugins.job-enqueueable" {
			t.Fatalf("generic check must be replaced by exact inventory: %+v", enqueue.Checks)
		}
	}
}

func TestDiagnoseGenericEnqueueFailurePreservesPluginReason(t *testing.T) {
	check := model.Known(
		"job.enqueue.evidence",
		"enqueue",
		"Enqueue evidence",
		false,
		"FailedEnqueue Ascend topology does not satisfy the job",
		nil,
	)

	diagnosis := diagnosisForFailure(check, "batch")
	if !strings.Contains(diagnosis.RootCause, "Ascend topology") ||
		strings.Contains(diagnosis.RootCause, "队列当前没有足够") {
		t.Fatalf("diagnosis=%+v", diagnosis)
	}

	if len(diagnosis.Suggestions) == 0 ||
		!strings.Contains(diagnosis.Suggestions[0], "JobEnqueueable") {
		t.Fatalf("suggestions=%+v", diagnosis.Suggestions)
	}
}

func TestDiagnoseExplicitQueueQuotaEnqueueFailureUsesCapacityAdvice(t *testing.T) {
	check := model.Known(
		"job.enqueue.evidence",
		"enqueue",
		"Enqueue evidence",
		false,
		"Unschedulable queue resource quota insufficient",
		nil,
	)

	diagnosis := diagnosisForFailure(check, "batch")
	if diagnosis.RootCause != "队列当前没有足够的可入队资源" {
		t.Fatalf("diagnosis=%+v", diagnosis)
	}
}

func checkByID(t *testing.T, checks []Check, id string) Check {
	t.Helper()

	for _, check := range checks {
		if check.ID == id {
			return check
		}
	}

	t.Fatalf("missing check %q in %+v", id, checks)

	return Check{}
}

func testFloat64Pointer(value float64) *float64 {
	return &value
}
