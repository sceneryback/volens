package cluster

import (
	"fmt"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
)

var allocatedTaskStatuses = map[string]bool{
	"Bound":     true,
	"Binding":   true,
	"Running":   true,
	"Allocated": true,
}

// BuildQueueSnapshot reconstructs the queue attributes used by proportion and
// capacity from one SIGUSR2 cache dump plus informer-backed Queue/PodGroup data.
// Both plugins use the same realCapability and JobEnqueueable quota formula.
func BuildQueueSnapshot(
	dump CacheDump,
	queues []Queue,
	podGroups []PodGroup,
	queueName string,
	strategy string,
) (QueueSnapshot, error) {
	result, err := buildQueueSnapshot(dump, queues, podGroups, queueName)
	result.Strategy = strategy

	if err != nil {
		return result, err
	}

	if strategy == "capacity" {
		populateCapacityDeserved(&result, queues, queueName)
	}

	return result, nil
}

// BuildProportionQueueSnapshot is kept for callers that only need the common
// proportion/capacity enqueue attributes.
func BuildProportionQueueSnapshot(
	dump CacheDump,
	queues []Queue,
	podGroups []PodGroup,
	queueName string,
) (QueueSnapshot, error) {
	return BuildQueueSnapshot(dump, queues, podGroups, queueName, "proportion")
}

func buildQueueSnapshot(
	dump CacheDump,
	queues []Queue,
	podGroups []PodGroup,
	queueName string,
) (QueueSnapshot, error) {
	result := newQueueSnapshot(queueName)
	result.Source = queueCacheDumpSource

	if queueName == "" {
		return result, fmt.Errorf("queue name is required")
	}

	queueByName := make(map[string]Queue, len(queues))
	totalGuarantee := map[string]float64{}

	for _, queue := range queues {
		queueByName[queue.Name] = queue
		addResourceValues(totalGuarantee, resourceListValues(queue.Guarantee))
	}

	queue, found := queueByName[queueName]
	if !found {
		return result, fmt.Errorf("queue %s is unavailable from informer cache", queueName)
	}

	totalResource := totalNodeAllocatable(dump.Nodes)
	realCapability := cloneResourceValues(totalResource)
	subResourceValues(realCapability, totalGuarantee)
	addResourceValues(realCapability, resourceListValues(queue.Guarantee))
	minResourceValues(realCapability, resourceListValues(queue.Capability))

	for name, value := range realCapability {
		result.setResourceValue(name, "capacity", value)
	}

	podGroupByKey := make(map[string]PodGroup, len(podGroups))
	for _, podGroup := range podGroups {
		podGroupByKey[podGroup.Namespace+"/"+podGroup.Name] = podGroup
	}

	for _, job := range dump.Jobs {
		if job.Queue != queueName {
			continue
		}

		key := job.Namespace + "/" + job.Name
		podGroup, hasPodGroup := podGroupByKey[key]
		allocated, request, allocatedTasks := jobTaskResources(job)

		for name, value := range allocated {
			result.addResourceValue(name, "allocated", value)
		}

		for name, value := range request {
			result.addResourceValue(name, "request", value)
		}

		if !hasPodGroup {
			continue
		}

		minResources := resourceListValues(podGroup.MinResources)

		switch strings.ToLower(podGroup.Phase) {
		case "inqueue":
			for name, value := range minResources {
				result.addResourceValue(name, "inqueue", value)
			}

		case "running":
			if allocatedTasks >= int(podGroup.MinMember) {
				reserved := positiveResourceDiff(minResources, allocated)
				for name, value := range reserved {
					result.addResourceValue(name, "inqueue", value)
				}
			}
		}

		elastic := positiveResourceDiff(allocated, minResources)
		for name, value := range elastic {
			result.addResourceValue(name, "elastic", value)
		}
	}

	return result, nil
}

func populateCapacityDeserved(snapshot *QueueSnapshot, queues []Queue, queueName string) {
	for _, queue := range queues {
		if queue.Name != queueName {
			continue
		}

		deserved := resourceListValues(queue.Deserved)
		guarantee := resourceListValues(queue.Guarantee)

		for name, resource := range snapshot.Resources {
			value := deserved[name]

			if resource.Capability != nil && value > *resource.Capability {
				value = *resource.Capability
			}

			if resource.Request != nil && value > *resource.Request {
				value = *resource.Request
			}

			if value < guarantee[name] {
				value = guarantee[name]
			}

			snapshot.setResourceValue(name, "deserved", value)
		}

		return
	}
}

func (m *QueueSnapshot) addResourceValue(resourceName, resourceSet string, value float64) {
	if value == 0 {
		return
	}

	snapshot := m.Resources[resourceName]
	current := 0.0

	switch resourceSet {
	case "capacity":
		if snapshot.Capability != nil {
			current = *snapshot.Capability
		}
		current += value
		snapshot.Capability = &current
	case "deserved":
		if snapshot.Deserved != nil {
			current = *snapshot.Deserved
		}
		current += value
		snapshot.Deserved = &current
	case "allocated":
		if snapshot.Allocated != nil {
			current = *snapshot.Allocated
		}
		current += value
		snapshot.Allocated = &current
	case "request":
		if snapshot.Request != nil {
			current = *snapshot.Request
		}
		current += value
		snapshot.Request = &current
	case "inqueue":
		if snapshot.Inqueue != nil {
			current = *snapshot.Inqueue
		}
		current += value
		snapshot.Inqueue = &current
	case "elastic":
		if snapshot.Elastic != nil {
			current = *snapshot.Elastic
		}
		current += value
		snapshot.Elastic = &current
	default:
		panic("unknown queue resource set " + resourceSet)
	}

	m.Resources[resourceName] = snapshot
}

func totalNodeAllocatable(nodes map[string]CacheNode) map[string]float64 {
	result := map[string]float64{}

	for _, node := range nodes {
		addResourceValues(result, node.Allocatable)
	}

	return result
}

func jobTaskResources(job CacheJob) (allocated, request map[string]float64, allocatedTasks int) {
	allocated = map[string]float64{}
	request = map[string]float64{}

	for _, task := range job.Tasks {
		if allocatedTaskStatuses[task.Status] {
			addResourceValues(allocated, task.Resreq)
			addResourceValues(request, task.Resreq)
			allocatedTasks++

			continue
		}

		if task.Status == "Pending" {
			addResourceValues(request, task.Resreq)
		}
	}

	return allocated, request, allocatedTasks
}

func addResourceValues(target map[string]float64, source map[string]float64) {
	for name, value := range source {
		target[name] += value
	}
}

func subResourceValues(target map[string]float64, source map[string]float64) {
	for name, value := range source {
		target[name] -= value
	}
}

func minResourceValues(target map[string]float64, limit map[string]float64) {
	for name, value := range limit {
		current, found := target[name]
		if !found || value < current {
			target[name] = value
		}
	}
}

func cloneResourceValues(source map[string]float64) map[string]float64 {
	result := make(map[string]float64, len(source))
	for name, value := range source {
		result[name] = value
	}

	return result
}

func positiveResourceDiff(left, right map[string]float64) map[string]float64 {
	names := make(map[string]struct{}, len(left)+len(right))
	for name := range left {
		names[name] = struct{}{}
	}
	for name := range right {
		names[name] = struct{}{}
	}

	ordered := make([]string, 0, len(names))
	for name := range names {
		ordered = append(ordered, name)
	}
	sort.Strings(ordered)

	result := map[string]float64{}
	for _, name := range ordered {
		value := left[name] - right[name]
		if value > 0 {
			result[name] = value
		}
	}

	return result
}

func resourceListValues(list corev1.ResourceList) map[string]float64 {
	result := map[string]float64{}
	requestBacked := map[string]bool{}

	for name, quantity := range list {
		originalName := string(name)
		resourceName := canonicalQueueResourceName(originalName)
		fromRequest := strings.HasPrefix(strings.ToLower(originalName), "requests.")

		if requestBacked[resourceName] && !fromRequest {
			continue
		}

		switch corev1.ResourceName(resourceName) {
		case corev1.ResourceCPU:
			result[resourceName] = float64(quantity.MilliValue()) / 1000
		case corev1.ResourceMemory:
			result[resourceName] = float64(quantity.Value())
		case corev1.ResourcePods:
			result[resourceName] = float64(quantity.Value())
		default:
			result[resourceName] = float64(quantity.MilliValue()) / 1000
		}

		requestBacked[resourceName] = fromRequest || requestBacked[resourceName]
	}

	return result
}

func canonicalQueueResourceName(name string) string {
	trimmed := strings.TrimSpace(name)
	lower := strings.ToLower(trimmed)

	if strings.HasPrefix(lower, "requests.") {
		return trimmed[len("requests."):]
	}

	return trimmed
}
