package filter

import (
	"fmt"
	"sort"

	"github.com/volcano-sh/volens/internal/agent/model"
	"github.com/volcano-sh/volens/internal/cluster"
	corev1 "k8s.io/api/core/v1"
)

var allocateSources = []string{
	"pkg/scheduler/actions/allocate/allocate.go",
	"pkg/scheduler/framework/session_plugins.go:PrePredicateFn",
	"pkg/scheduler/framework/session_plugins.go:PredicateFn",
}

type Input struct {
	Pod        *corev1.Pod
	Nodes      []corev1.Node
	NodesErr   error
	Dump       cluster.CacheDump
	CaptureErr error
}

type Result struct {
	CacheCheck      model.Check
	Nodes           []model.NodeResult
	AllocationCheck model.Check
	PluginCheck     model.Check
}

func Evaluate(input Input) Result {
	cacheNodes := input.Dump.Nodes
	if cacheNodes == nil {
		cacheNodes = map[string]cluster.CacheNode{}
	}

	expected := eligibleReadyNodeNames(input.Nodes, input.Pod)
	matched, missing := matchCacheNodes(cacheNodes, expected)

	result := Result{
		CacheCheck: cacheEvidence(input.Dump, input.CaptureErr, matched, missing, len(expected)),
		Nodes:      make([]model.NodeResult, 0, len(input.Nodes)),
	}

	for i := range input.Nodes {
		cacheNode, found := cacheNodes[input.Nodes[i].Name]

		result.Nodes = append(result.Nodes, evaluateNode(input.Pod, &input.Nodes[i], cacheNode, found))
	}

	sort.Slice(result.Nodes, func(i, j int) bool {
		return result.Nodes[i].Name < result.Nodes[j].Name
	})

	result.AllocationCheck = allocationCheck(result.Nodes, input.NodesErr)
	result.PluginCheck = model.Unknown(
		"plugins.predicates",
		"allocate",
		"Active PrePredicate and Predicate plugin hooks",
		"enabled predicates and plugin-private Session state cannot be reproduced from Kubernetes objects and the node cache dump alone",
		nil,
		allocateSources,
	)

	return result
}

func cacheEvidence(
	dump cluster.CacheDump,
	captureErr error,
	matched map[string]cluster.CacheNode,
	missing []string,
	expected int,
) model.Check {
	if captureErr != nil {
		return model.Unknown(
			"cache.capture",
			"allocate",
			"Volcano scheduler cache captured",
			captureErr.Error(),
			dump.Nodes,
			[]string{"SIGUSR2 Node (...) cache dump"},
		)
	}

	if len(missing) > 0 {
		return model.Unknown(
			"cache.capture",
			"allocate",
			"Volcano scheduler cache captured",
			fmt.Sprintf("cache dump is missing eligible Ready nodes: %v", missing),
			matched,
			[]string{"SIGUSR2 Node (...) cache dump"},
		)
	}

	check := model.Known(
		"cache.capture",
		"allocate",
		"Volcano scheduler cache captured",
		true,
		fmt.Sprintf(
			"captured %d scheduler cache nodes; matched %d/%d eligible Ready nodes from %s/%s",
			len(dump.Nodes),
			len(matched),
			expected,
			dump.Scheduler.Namespace,
			dump.Scheduler.Name,
		),
		[]string{"SIGUSR2 Node (...) cache dump"},
	)
	check.Evidence = matched

	return check
}

func allocationCheck(nodes []model.NodeResult, nodesErr error) model.Check {
	if nodesErr != nil {
		return model.Unknown(
			"allocate.nodes",
			"allocate",
			"At least one node passes common filters",
			"list Kubernetes nodes from informer cache: "+nodesErr.Error(),
			nil,
			allocateSources,
		)
	}

	passed := 0
	unknown := false

	for _, node := range nodes {
		if node.Passed {
			passed++
		}

		unknown = unknown || !node.Determinate
	}

	reason := fmt.Sprintf("%d/%d nodes passed common filters", passed, len(nodes))

	if passed == 0 && unknown {
		return model.Unknown(
			"allocate.nodes",
			"allocate",
			"At least one node passes common filters",
			reason+"; one or more nodes lack Volcano cache evidence",
			nil,
			allocateSources,
		)
	}

	return model.Known(
		"allocate.nodes",
		"allocate",
		"At least one node passes common filters",
		passed > 0,
		reason,
		allocateSources,
	)
}
