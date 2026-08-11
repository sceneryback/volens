package cluster

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/yaml"
)

// SchedulerRuntimeConfig is the ordered scheduler policy declared by the
// ConfigMap mounted into the current scheduler leader Pod.
type SchedulerRuntimeConfig struct {
	Determinate              bool                           `json:"determinate"`
	Reason                   string                         `json:"reason,omitempty"`
	SchedulerNamespace       string                         `json:"schedulerNamespace,omitempty"`
	SchedulerPod             string                         `json:"schedulerPod,omitempty"`
	SchedulerUID             string                         `json:"schedulerUID,omitempty"`
	SchedulerConfPath        string                         `json:"schedulerConfPath,omitempty"`
	ConfigMapNamespace       string                         `json:"configMapNamespace,omitempty"`
	ConfigMapName            string                         `json:"configMapName,omitempty"`
	ConfigMapKey             string                         `json:"configMapKey,omitempty"`
	ConfigMapUID             string                         `json:"configMapUID,omitempty"`
	ConfigMapResourceVersion string                         `json:"configMapResourceVersion,omitempty"`
	Actions                  []string                       `json:"actions,omitempty"`
	Tiers                    []SchedulerTier                `json:"tiers,omitempty"`
	Configurations           []SchedulerPluginConfiguration `json:"configurations,omitempty"`
	Metrics                  map[string]any                 `json:"metrics,omitempty"`
}

// SchedulerTier preserves the plugin order used by a Volcano framework tier.
type SchedulerTier struct {
	Plugins []SchedulerPlugin `json:"plugins"`
}

// SchedulerPlugin contains one configured plugin. Options only contains
// values explicitly declared in the ConfigMap. Effective defaults remain
// branch-specific and must be resolved from the selected Volcano source.
type SchedulerPlugin struct {
	Name      string         `json:"name"`
	Arguments map[string]any `json:"arguments,omitempty"`
	Options   map[string]any `json:"options,omitempty"`
}

// SchedulerPluginConfiguration preserves entries from the top-level
// configurations section of the scheduler policy.
type SchedulerPluginConfiguration struct {
	Name      string         `json:"name"`
	Arguments map[string]any `json:"arguments,omitempty"`
	Options   map[string]any `json:"options,omitempty"`
}

type schedulerConfigMapReference struct {
	namespace string
	name      string
	key       string
}

type schedulerConfigurationDocument struct {
	Actions        string                  `json:"actions"`
	Tiers          []schedulerTierDocument `json:"tiers"`
	Configurations []map[string]any        `json:"configurations"`
	Metrics        map[string]any          `json:"metrics"`
}

type schedulerTierDocument struct {
	Plugins []map[string]any `json:"plugins"`
}

func (m *Client) GetVolcanoSchedulerConfiguration(
	ctx context.Context,
) (SchedulerRuntimeConfig, error) {
	if ctx == nil {
		return SchedulerRuntimeConfig{}, fmt.Errorf("scheduler configuration context is nil")
	}

	if err := ctx.Err(); err != nil {
		return SchedulerRuntimeConfig{}, err
	}

	scheduler, err := m.GetVolcanoScheduler(ctx)
	if err != nil {
		return SchedulerRuntimeConfig{}, err
	}

	return m.GetVolcanoSchedulerConfigurationFor(ctx, scheduler)
}

func (m *Client) GetVolcanoSchedulerConfigurationFor(
	ctx context.Context,
	scheduler Scheduler,
) (SchedulerRuntimeConfig, error) {
	if ctx == nil {
		return SchedulerRuntimeConfig{}, fmt.Errorf("scheduler configuration context is nil")
	}

	if err := ctx.Err(); err != nil {
		return SchedulerRuntimeConfig{}, err
	}

	if scheduler.Namespace == "" || scheduler.Name == "" || scheduler.Container == "" {
		return SchedulerRuntimeConfig{}, fmt.Errorf("scheduler identity is incomplete")
	}

	result := SchedulerRuntimeConfig{
		SchedulerNamespace: scheduler.Namespace,
		SchedulerPod:       scheduler.Name,
		SchedulerUID:       scheduler.UID,
	}

	pod, err := m.podLister.Pods(scheduler.Namespace).Get(scheduler.Name)
	if err != nil {
		result.Reason = fmt.Sprintf("read scheduler Pod from informer cache: %v", err)

		return result, nil
	}

	if scheduler.UID != "" && string(pod.UID) != scheduler.UID {
		result.Reason = fmt.Sprintf(
			"scheduler Pod UID changed while reading policy: discovered=%s cached=%s",
			scheduler.UID,
			pod.UID,
		)

		return result, nil
	}

	container := containerByName(pod, scheduler.Container)
	if container == nil {
		result.Reason = "scheduler container is unavailable in the informer cache"

		return result, nil
	}

	configurationPath, found, err := schedulerConfigurationPath(container)
	if err != nil {
		result.Reason = err.Error()

		return result, nil
	}

	if !found {
		result.Reason = "scheduler does not set --scheduler-conf; the active configuration comes from the branch-compiled DefaultSchedulerConf"

		return result, nil
	}

	result.SchedulerConfPath = configurationPath

	reference, err := schedulerConfigMapForPath(pod, container, configurationPath)
	if err != nil {
		result.Reason = err.Error()

		return result, nil
	}

	result.ConfigMapNamespace = reference.namespace
	result.ConfigMapName = reference.name
	result.ConfigMapKey = reference.key

	configMap, err := m.configMapLister.ConfigMaps(reference.namespace).Get(reference.name)
	if err != nil {
		result.Reason = fmt.Sprintf(
			"read scheduler ConfigMap %s/%s from informer cache: %v",
			reference.namespace,
			reference.name,
			err,
		)

		return result, nil
	}

	result.ConfigMapUID = string(configMap.UID)
	result.ConfigMapResourceVersion = configMap.ResourceVersion

	content, found := configMap.Data[reference.key]
	if !found {
		if binaryContent, binaryFound := configMap.BinaryData[reference.key]; binaryFound {
			content = string(binaryContent)
			found = true
		}
	}

	if !found {
		result.Reason = fmt.Sprintf(
			"scheduler ConfigMap %s/%s does not contain key %q",
			reference.namespace,
			reference.name,
			reference.key,
		)

		return result, nil
	}

	actions, tiers, configurations, metrics, err := parseSchedulerConfiguration(content)
	if err != nil {
		result.Reason = fmt.Sprintf(
			"parse scheduler ConfigMap %s/%s key %q: %v",
			reference.namespace,
			reference.name,
			reference.key,
			err,
		)

		return result, nil
	}

	result.Determinate = true
	result.Reason = "parsed the ConfigMap mounted by the current scheduler leader; ConfigMap projection and scheduler file reload are eventually consistent"
	result.Actions = actions
	result.Tiers = tiers
	result.Configurations = configurations
	result.Metrics = metrics

	return result, nil
}

func containerByName(pod *corev1.Pod, name string) *corev1.Container {
	if pod == nil {
		return nil
	}

	for index := range pod.Spec.Containers {
		if pod.Spec.Containers[index].Name == name {
			return &pod.Spec.Containers[index]
		}
	}

	return nil
}

func schedulerConfigurationPath(container *corev1.Container) (string, bool, error) {
	if container == nil {
		return "", false, fmt.Errorf("scheduler container is unavailable")
	}

	commandLine := append([]string(nil), container.Command...)
	commandLine = append(commandLine, container.Args...)
	fields := strings.Fields(strings.Join(commandLine, " "))
	paths := map[string]struct{}{}

	for index := 0; index < len(fields); index++ {
		field := trimShellToken(fields[index])
		value := ""

		if field == "--scheduler-conf" {
			if index+1 >= len(fields) {
				return "", false, fmt.Errorf("scheduler flag --scheduler-conf has no value")
			}

			index++
			value = trimShellToken(fields[index])
		} else if strings.HasPrefix(field, "--scheduler-conf=") {
			value = trimShellToken(strings.TrimPrefix(field, "--scheduler-conf="))
		} else {
			continue
		}

		if value == "" {
			return "", false, fmt.Errorf("scheduler flag --scheduler-conf has an empty value")
		}

		if strings.Contains(value, "$(") || strings.Contains(value, "${") {
			return "", false, fmt.Errorf("scheduler flag --scheduler-conf uses runtime variable expansion")
		}

		if !filepath.IsAbs(value) {
			return "", false, fmt.Errorf("scheduler flag --scheduler-conf path %q is not absolute", value)
		}

		paths[filepath.Clean(value)] = struct{}{}
	}

	if len(paths) == 0 {
		return "", false, nil
	}

	if len(paths) > 1 {
		return "", false, fmt.Errorf("scheduler command contains conflicting --scheduler-conf paths")
	}

	for path := range paths {
		return path, true, nil
	}

	return "", false, nil
}

func trimShellToken(value string) string {
	return strings.Trim(value, "\"'`;\\")
}

func schedulerConfigMapForPath(
	pod *corev1.Pod,
	container *corev1.Container,
	configurationPath string,
) (schedulerConfigMapReference, error) {
	if pod == nil || container == nil {
		return schedulerConfigMapReference{}, fmt.Errorf("scheduler Pod or container is unavailable")
	}

	mount := longestMatchingVolumeMount(container.VolumeMounts, configurationPath)
	if mount == nil {
		return schedulerConfigMapReference{}, fmt.Errorf(
			"scheduler configuration path %q is not backed by a container volume mount",
			configurationPath,
		)
	}

	volume := podVolumeByName(pod, mount.Name)
	if volume == nil {
		return schedulerConfigMapReference{}, fmt.Errorf(
			"scheduler volume mount %q has no matching Pod volume",
			mount.Name,
		)
	}

	if volume.ConfigMap == nil || volume.ConfigMap.Name == "" {
		return schedulerConfigMapReference{}, fmt.Errorf(
			"scheduler configuration volume %q is not backed by a named ConfigMap",
			volume.Name,
		)
	}

	projectedPath, err := projectedConfigurationPath(mount, configurationPath)
	if err != nil {
		return schedulerConfigMapReference{}, err
	}

	key, err := configMapKeyForProjectedPath(volume.ConfigMap, projectedPath)
	if err != nil {
		return schedulerConfigMapReference{}, err
	}

	return schedulerConfigMapReference{
		namespace: pod.Namespace,
		name:      volume.ConfigMap.Name,
		key:       key,
	}, nil
}

func longestMatchingVolumeMount(
	mounts []corev1.VolumeMount,
	configurationPath string,
) *corev1.VolumeMount {
	var selected *corev1.VolumeMount
	selectedLength := -1

	for index := range mounts {
		mount := &mounts[index]
		mountPath := filepath.Clean(mount.MountPath)

		if mount.SubPath != "" {
			if filepath.Clean(configurationPath) != mountPath {
				continue
			}
		} else {
			relative, err := filepath.Rel(mountPath, filepath.Clean(configurationPath))
			if err != nil || relative == "." || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
				continue
			}
		}

		if len(mountPath) > selectedLength {
			selected = mount
			selectedLength = len(mountPath)
		}
	}

	return selected
}

func podVolumeByName(pod *corev1.Pod, name string) *corev1.Volume {
	for index := range pod.Spec.Volumes {
		if pod.Spec.Volumes[index].Name == name {
			return &pod.Spec.Volumes[index]
		}
	}

	return nil
}

func projectedConfigurationPath(
	mount *corev1.VolumeMount,
	configurationPath string,
) (string, error) {
	if mount.SubPath != "" {
		return filepath.ToSlash(filepath.Clean(mount.SubPath)), nil
	}

	relative, err := filepath.Rel(filepath.Clean(mount.MountPath), filepath.Clean(configurationPath))
	if err != nil || relative == "." || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf(
			"scheduler configuration path %q is outside volume mount %q",
			configurationPath,
			mount.MountPath,
		)
	}

	return filepath.ToSlash(relative), nil
}

func configMapKeyForProjectedPath(
	source *corev1.ConfigMapVolumeSource,
	projectedPath string,
) (string, error) {
	if len(source.Items) == 0 {
		return projectedPath, nil
	}

	cleanPath := filepath.ToSlash(filepath.Clean(projectedPath))

	for _, item := range source.Items {
		if filepath.ToSlash(filepath.Clean(item.Path)) == cleanPath {
			if item.Key == "" {
				break
			}

			return item.Key, nil
		}
	}

	return "", fmt.Errorf(
		"scheduler configuration projected path %q is not mapped by the ConfigMap volume items",
		projectedPath,
	)
}

func parseSchedulerConfiguration(
	content string,
) (
	[]string,
	[]SchedulerTier,
	[]SchedulerPluginConfiguration,
	map[string]any,
	error,
) {
	document := schedulerConfigurationDocument{}

	if err := yaml.Unmarshal([]byte(content), &document); err != nil {
		return nil, nil, nil, nil, err
	}

	actions, err := parseSchedulerActions(document.Actions)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	tiers := make([]SchedulerTier, 0, len(document.Tiers))

	for tierIndex, tierDocument := range document.Tiers {
		tier := SchedulerTier{
			Plugins: make([]SchedulerPlugin, 0, len(tierDocument.Plugins)),
		}

		for pluginIndex, rawPlugin := range tierDocument.Plugins {
			plugin, err := parseSchedulerPlugin(rawPlugin)
			if err != nil {
				return nil, nil, nil, nil, fmt.Errorf(
					"tiers[%d].plugins[%d]: %w",
					tierIndex,
					pluginIndex,
					err,
				)
			}

			tier.Plugins = append(tier.Plugins, plugin)
		}

		tiers = append(tiers, tier)
	}

	configurations := make([]SchedulerPluginConfiguration, 0, len(document.Configurations))

	for index, rawConfiguration := range document.Configurations {
		configuration, err := parseSchedulerPluginConfiguration(rawConfiguration)
		if err != nil {
			return nil, nil, nil, nil, fmt.Errorf("configurations[%d]: %w", index, err)
		}

		configurations = append(configurations, configuration)
	}

	return actions, tiers, configurations, document.Metrics, nil
}

func parseSchedulerActions(value string) ([]string, error) {
	parts := strings.Split(value, ",")
	actions := make([]string, 0, len(parts))

	for _, part := range parts {
		action := strings.TrimSpace(part)
		if action == "" {
			return nil, fmt.Errorf("actions contains an empty action name")
		}

		actions = append(actions, action)
	}

	return actions, nil
}

func parseSchedulerPlugin(raw map[string]any) (SchedulerPlugin, error) {
	name, arguments, options, err := parseNamedSchedulerOptions(raw)
	if err != nil {
		return SchedulerPlugin{}, err
	}

	return SchedulerPlugin{
		Name:      name,
		Arguments: arguments,
		Options:   options,
	}, nil
}

func parseSchedulerPluginConfiguration(
	raw map[string]any,
) (SchedulerPluginConfiguration, error) {
	name, arguments, options, err := parseNamedSchedulerOptions(raw)
	if err != nil {
		return SchedulerPluginConfiguration{}, err
	}

	return SchedulerPluginConfiguration{
		Name:      name,
		Arguments: arguments,
		Options:   options,
	}, nil
}

func parseNamedSchedulerOptions(
	raw map[string]any,
) (string, map[string]any, map[string]any, error) {
	name, ok := raw["name"].(string)
	if !ok || strings.TrimSpace(name) == "" {
		return "", nil, nil, fmt.Errorf("name must be a non-empty string")
	}

	name = strings.TrimSpace(name)
	arguments := map[string]any(nil)

	if value, found := raw["arguments"]; found && value != nil {
		arguments, ok = value.(map[string]any)
		if !ok {
			return "", nil, nil, fmt.Errorf("arguments for %q must be a mapping", name)
		}
	}

	options := make(map[string]any, len(raw))

	for key, value := range raw {
		if key == "name" || key == "arguments" {
			continue
		}

		options[key] = value
	}

	if len(options) == 0 {
		options = nil
	}

	return name, arguments, options, nil
}
