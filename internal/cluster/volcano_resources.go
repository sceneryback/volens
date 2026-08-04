package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const (
	podGroupNameAnnotation   = "scheduling.k8s.io/group-name"
	defaultVolcanoAPIVersion = "v1beta1"
)

var (
	podGroupGVR = schema.GroupVersionResource{
		Group:    "scheduling.volcano.sh",
		Version:  defaultVolcanoAPIVersion,
		Resource: "podgroups",
	}
	queueGVR = schema.GroupVersionResource{
		Group:    "scheduling.volcano.sh",
		Version:  defaultVolcanoAPIVersion,
		Resource: "queues",
	}

	ErrVolcanoResourceCacheUnavailable = errors.New("Volcano resource informer cache unavailable")
)

type PodGroupCondition struct {
	Type               string `json:"type"`
	Status             string `json:"status"`
	TransitionID       string `json:"transitionID,omitempty"`
	Reason             string `json:"reason,omitempty"`
	Message            string `json:"message,omitempty"`
	LastTransitionTime string `json:"lastTransitionTime,omitempty"`
}

type PodGroup struct {
	Namespace       string              `json:"namespace"`
	Name            string              `json:"name"`
	UID             string              `json:"uid"`
	ResourceVersion string              `json:"resourceVersion,omitempty"`
	Queue           string              `json:"queue"`
	MinMember       int32               `json:"minMember"`
	MinTaskMember   map[string]int32    `json:"minTaskMember,omitempty"`
	MinResources    corev1.ResourceList `json:"minResources,omitempty"`
	Phase           string              `json:"phase"`
	Conditions      []PodGroupCondition `json:"conditions,omitempty"`
	Running         int32               `json:"running,omitempty"`
	Succeeded       int32               `json:"succeeded,omitempty"`
	Failed          int32               `json:"failed,omitempty"`
}

type Queue struct {
	Name            string              `json:"name"`
	UID             string              `json:"uid"`
	ResourceVersion string              `json:"resourceVersion,omitempty"`
	State           string              `json:"state"`
	Type            string              `json:"type,omitempty"`
	Parent          string              `json:"parent,omitempty"`
	Weight          int32               `json:"weight,omitempty"`
	Reclaimable     bool                `json:"reclaimable,omitempty"`
	Capability      corev1.ResourceList `json:"capability,omitempty"`
	Deserved        corev1.ResourceList `json:"deserved,omitempty"`
	Allocated       corev1.ResourceList `json:"allocated,omitempty"`
	Pending         int32               `json:"pending,omitempty"`
	Inqueue         int32               `json:"inqueue,omitempty"`
	Running         int32               `json:"running,omitempty"`
	Completed       int32               `json:"completed,omitempty"`
	Unknown         int32               `json:"unknown,omitempty"`
}

func (m *Client) GetPodGroup(ctx context.Context, namespace, name string) (PodGroup, error) {
	if err := ctx.Err(); err != nil {
		return PodGroup{}, err
	}

	if m.podGroupLister == nil {
		return PodGroup{}, fmt.Errorf("get PodGroup %s/%s: %w", namespace, name, ErrVolcanoResourceCacheUnavailable)
	}

	object, err := m.podGroupLister.ByNamespace(namespace).Get(name)
	if err != nil {
		return PodGroup{}, fmt.Errorf("get PodGroup %s/%s from informer cache: %w", namespace, name, err)
	}

	podGroup, ok := object.(*unstructured.Unstructured)
	if !ok {
		return PodGroup{}, fmt.Errorf("PodGroup informer cache returned %T", object)
	}

	result, err := podGroupFromUnstructured(podGroup)
	if err != nil {
		return PodGroup{}, fmt.Errorf("decode PodGroup %s/%s from informer cache: %w", namespace, name, err)
	}

	return result, nil
}

func (m *Client) GetQueue(ctx context.Context, name string) (Queue, error) {
	if err := ctx.Err(); err != nil {
		return Queue{}, err
	}

	if m.queueLister == nil {
		return Queue{}, fmt.Errorf("get Queue %s: %w", name, ErrVolcanoResourceCacheUnavailable)
	}

	object, err := m.queueLister.Get(name)
	if err != nil {
		return Queue{}, fmt.Errorf("get Queue %s from informer cache: %w", name, err)
	}

	queue, ok := object.(*unstructured.Unstructured)
	if !ok {
		return Queue{}, fmt.Errorf("Queue informer cache returned %T", object)
	}

	result, err := queueFromUnstructured(queue)
	if err != nil {
		return Queue{}, fmt.Errorf("decode Queue %s from informer cache: %w", name, err)
	}

	return result, nil
}

func (m *Client) ListPodsForPodGroup(
	ctx context.Context,
	namespace string,
	podGroupName string,
) ([]corev1.Pod, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	objects, err := m.podIndexer.ByIndex(podGroupIndex, namespace+"/"+podGroupName)
	if err != nil {
		return nil, fmt.Errorf(
			"list Pods for PodGroup %s/%s from informer cache: %w",
			namespace,
			podGroupName,
			err,
		)
	}

	result := make([]corev1.Pod, 0, len(objects))

	for _, object := range objects {
		pod, ok := object.(*corev1.Pod)
		if !ok {
			continue
		}

		result = append(result, *pod.DeepCopy())
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})

	return result, nil
}

func podGroupIndexKeys(object any) ([]string, error) {
	pod, ok := object.(*corev1.Pod)
	if !ok {
		return nil, fmt.Errorf("pod group index received %T", object)
	}

	name := pod.Annotations[podGroupNameAnnotation]
	if name == "" {
		return nil, nil
	}

	return []string{pod.Namespace + "/" + name}, nil
}

func podGroupFromUnstructured(object *unstructured.Unstructured) (PodGroup, error) {
	if object == nil {
		return PodGroup{}, fmt.Errorf("object is nil")
	}

	minMember, err := int32At(object.Object, "spec", "minMember")
	if err != nil {
		return PodGroup{}, err
	}

	minTaskMember, err := int32MapAt(object.Object, "spec", "minTaskMember")
	if err != nil {
		return PodGroup{}, err
	}

	minResources, err := resourceListAt(object.Object, "spec", "minResources")
	if err != nil {
		return PodGroup{}, err
	}

	queue, err := stringAt(object.Object, "spec", "queue")
	if err != nil {
		return PodGroup{}, err
	}

	phase, err := stringAt(object.Object, "status", "phase")
	if err != nil {
		return PodGroup{}, err
	}

	conditions, err := podGroupConditionsAt(object.Object, "status", "conditions")
	if err != nil {
		return PodGroup{}, err
	}

	running, err := int32At(object.Object, "status", "running")
	if err != nil {
		return PodGroup{}, err
	}

	succeeded, err := int32At(object.Object, "status", "succeeded")
	if err != nil {
		return PodGroup{}, err
	}

	failed, err := int32At(object.Object, "status", "failed")
	if err != nil {
		return PodGroup{}, err
	}

	return PodGroup{
		Namespace:       object.GetNamespace(),
		Name:            object.GetName(),
		UID:             string(object.GetUID()),
		ResourceVersion: object.GetResourceVersion(),
		Queue:           queue,
		MinMember:       minMember,
		MinTaskMember:   minTaskMember,
		MinResources:    minResources,
		Phase:           phase,
		Conditions:      conditions,
		Running:         running,
		Succeeded:       succeeded,
		Failed:          failed,
	}, nil
}

func queueFromUnstructured(object *unstructured.Unstructured) (Queue, error) {
	if object == nil {
		return Queue{}, fmt.Errorf("object is nil")
	}

	state, err := stringAt(object.Object, "status", "state")
	if err != nil {
		return Queue{}, err
	}

	queueType, err := stringAt(object.Object, "spec", "type")
	if err != nil {
		return Queue{}, err
	}

	parent, err := stringAt(object.Object, "spec", "parent")
	if err != nil {
		return Queue{}, err
	}

	weight, err := int32At(object.Object, "spec", "weight")
	if err != nil {
		return Queue{}, err
	}

	reclaimable, err := boolAt(object.Object, "spec", "reclaimable")
	if err != nil {
		return Queue{}, err
	}

	capability, err := resourceListAt(object.Object, "spec", "capability")
	if err != nil {
		return Queue{}, err
	}

	deserved, err := resourceListAt(object.Object, "spec", "deserved")
	if err != nil {
		return Queue{}, err
	}

	allocated, err := resourceListAt(object.Object, "status", "allocated")
	if err != nil {
		return Queue{}, err
	}

	pending, err := int32At(object.Object, "status", "pending")
	if err != nil {
		return Queue{}, err
	}

	inqueue, err := int32At(object.Object, "status", "inqueue")
	if err != nil {
		return Queue{}, err
	}

	running, err := int32At(object.Object, "status", "running")
	if err != nil {
		return Queue{}, err
	}

	completed, err := int32At(object.Object, "status", "completed")
	if err != nil {
		return Queue{}, err
	}

	unknown, err := int32At(object.Object, "status", "unknown")
	if err != nil {
		return Queue{}, err
	}

	return Queue{
		Name:            object.GetName(),
		UID:             string(object.GetUID()),
		ResourceVersion: object.GetResourceVersion(),
		State:           state,
		Type:            queueType,
		Parent:          parent,
		Weight:          weight,
		Reclaimable:     reclaimable,
		Capability:      capability,
		Deserved:        deserved,
		Allocated:       allocated,
		Pending:         pending,
		Inqueue:         inqueue,
		Running:         running,
		Completed:       completed,
		Unknown:         unknown,
	}, nil
}

func stringAt(object map[string]any, fields ...string) (string, error) {
	value, found, err := unstructured.NestedFieldNoCopy(object, fields...)
	if err != nil {
		return "", fmt.Errorf("read %s: %w", fieldPath(fields), err)
	}

	if !found {
		return "", nil
	}

	result, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("%s has type %T, want string", fieldPath(fields), value)
	}

	return result, nil
}

func boolAt(object map[string]any, fields ...string) (bool, error) {
	value, found, err := unstructured.NestedFieldNoCopy(object, fields...)
	if err != nil {
		return false, fmt.Errorf("read %s: %w", fieldPath(fields), err)
	}

	if !found {
		return false, nil
	}

	result, ok := value.(bool)
	if !ok {
		return false, fmt.Errorf("%s has type %T, want bool", fieldPath(fields), value)
	}

	return result, nil
}

func int32At(object map[string]any, fields ...string) (int32, error) {
	value, found, err := unstructured.NestedFieldNoCopy(object, fields...)
	if err != nil {
		return 0, fmt.Errorf("read %s: %w", fieldPath(fields), err)
	}

	if !found {
		return 0, nil
	}

	result, err := valueAsInt32(value)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", fieldPath(fields), err)
	}

	return result, nil
}

func int32MapAt(object map[string]any, fields ...string) (map[string]int32, error) {
	value, found, err := unstructured.NestedFieldNoCopy(object, fields...)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", fieldPath(fields), err)
	}

	if !found {
		return nil, nil
	}

	source, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%s has type %T, want object", fieldPath(fields), value)
	}

	result := make(map[string]int32, len(source))

	for name, raw := range source {
		parsed, err := valueAsInt32(raw)
		if err != nil {
			return nil, fmt.Errorf("parse %s.%s: %w", fieldPath(fields), name, err)
		}

		result[name] = parsed
	}

	return result, nil
}

func resourceListAt(object map[string]any, fields ...string) (corev1.ResourceList, error) {
	value, found, err := unstructured.NestedFieldNoCopy(object, fields...)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", fieldPath(fields), err)
	}

	if !found {
		return nil, nil
	}

	source, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%s has type %T, want object", fieldPath(fields), value)
	}

	result := make(corev1.ResourceList, len(source))

	for name, raw := range source {
		quantityText, err := scalarText(raw)
		if err != nil {
			return nil, fmt.Errorf("parse %s.%s: %w", fieldPath(fields), name, err)
		}

		quantity, err := resource.ParseQuantity(quantityText)
		if err != nil {
			return nil, fmt.Errorf("parse %s.%s quantity %q: %w", fieldPath(fields), name, quantityText, err)
		}

		result[corev1.ResourceName(name)] = quantity
	}

	return result, nil
}

func podGroupConditionsAt(object map[string]any, fields ...string) ([]PodGroupCondition, error) {
	value, found, err := unstructured.NestedFieldNoCopy(object, fields...)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", fieldPath(fields), err)
	}

	if !found {
		return nil, nil
	}

	source, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("%s has type %T, want array", fieldPath(fields), value)
	}

	result := make([]PodGroupCondition, 0, len(source))

	for index, raw := range source {
		condition, ok := raw.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("%s[%d] has type %T, want object", fieldPath(fields), index, raw)
		}

		parsed, err := podGroupConditionFromMap(condition)
		if err != nil {
			return nil, fmt.Errorf("parse %s[%d]: %w", fieldPath(fields), index, err)
		}

		result = append(result, parsed)
	}

	return result, nil
}

func podGroupConditionFromMap(condition map[string]any) (PodGroupCondition, error) {
	conditionType, err := stringAt(condition, "type")
	if err != nil {
		return PodGroupCondition{}, err
	}

	status, err := stringAt(condition, "status")
	if err != nil {
		return PodGroupCondition{}, err
	}

	transitionID, err := stringAt(condition, "transitionID")
	if err != nil {
		return PodGroupCondition{}, err
	}

	reason, err := stringAt(condition, "reason")
	if err != nil {
		return PodGroupCondition{}, err
	}

	message, err := stringAt(condition, "message")
	if err != nil {
		return PodGroupCondition{}, err
	}

	lastTransitionTime, err := stringAt(condition, "lastTransitionTime")
	if err != nil {
		return PodGroupCondition{}, err
	}

	return PodGroupCondition{
		Type:               conditionType,
		Status:             status,
		TransitionID:       transitionID,
		Reason:             reason,
		Message:            message,
		LastTransitionTime: lastTransitionTime,
	}, nil
}

func valueAsInt32(value any) (int32, error) {
	var parsed int64

	switch typed := value.(type) {
	case int:
		parsed = int64(typed)
	case int8:
		parsed = int64(typed)
	case int16:
		parsed = int64(typed)
	case int32:
		parsed = int64(typed)
	case int64:
		parsed = typed
	case uint:
		if uint64(typed) > math.MaxInt64 {
			return 0, fmt.Errorf("value %v overflows int64", value)
		}

		parsed = int64(typed)
	case uint8:
		parsed = int64(typed)
	case uint16:
		parsed = int64(typed)
	case uint32:
		parsed = int64(typed)
	case uint64:
		if typed > math.MaxInt64 {
			return 0, fmt.Errorf("value %v overflows int64", value)
		}

		parsed = int64(typed)
	case float32:
		floatValue := float64(typed)

		if math.Trunc(floatValue) != floatValue {
			return 0, fmt.Errorf("value %v is not an integer", value)
		}

		parsed = int64(floatValue)
	case float64:
		if math.Trunc(typed) != typed {
			return 0, fmt.Errorf("value %v is not an integer", value)
		}

		parsed = int64(typed)
	case json.Number:
		valueAsInt64, err := typed.Int64()
		if err != nil {
			return 0, err
		}

		parsed = valueAsInt64
	case string:
		valueAsInt64, err := strconv.ParseInt(typed, 10, 64)
		if err != nil {
			return 0, err
		}

		parsed = valueAsInt64
	default:
		return 0, fmt.Errorf("value has type %T, want integer", value)
	}

	if parsed < math.MinInt32 || parsed > math.MaxInt32 {
		return 0, fmt.Errorf("value %d overflows int32", parsed)
	}

	return int32(parsed), nil
}

func scalarText(value any) (string, error) {
	switch typed := value.(type) {
	case string:
		return typed, nil
	case json.Number:
		return typed.String(), nil
	case int:
		return strconv.FormatInt(int64(typed), 10), nil
	case int8:
		return strconv.FormatInt(int64(typed), 10), nil
	case int16:
		return strconv.FormatInt(int64(typed), 10), nil
	case int32:
		return strconv.FormatInt(int64(typed), 10), nil
	case int64:
		return strconv.FormatInt(typed, 10), nil
	case uint:
		return strconv.FormatUint(uint64(typed), 10), nil
	case uint8:
		return strconv.FormatUint(uint64(typed), 10), nil
	case uint16:
		return strconv.FormatUint(uint64(typed), 10), nil
	case uint32:
		return strconv.FormatUint(uint64(typed), 10), nil
	case uint64:
		return strconv.FormatUint(typed, 10), nil
	case float32:
		return strconv.FormatFloat(float64(typed), 'g', -1, 32), nil
	case float64:
		return strconv.FormatFloat(typed, 'g', -1, 64), nil
	default:
		return "", fmt.Errorf("value has type %T, want string or number", value)
	}
}

func fieldPath(fields []string) string {
	result := ""

	for _, field := range fields {
		if result != "" {
			result += "."
		}

		result += field
	}

	return result
}
