package cluster

const queueCacheDumpSource = "volcano-cache-dump"

type QueueSnapshotResource struct {
	Capability *float64 `json:"capability,omitempty"`
	Deserved   *float64 `json:"deserved,omitempty"`
	Allocated  *float64 `json:"allocated,omitempty"`
	Request    *float64 `json:"request,omitempty"`
	Inqueue    *float64 `json:"inqueue,omitempty"`
	Elastic    *float64 `json:"elastic,omitempty"`
}

type QueueRuntimeResources = QueueSnapshotResource

type QueueSnapshot struct {
	QueueName string                           `json:"queueName"`
	Strategy  string                           `json:"strategy,omitempty"`
	Source    string                           `json:"source"`
	Resources map[string]QueueSnapshotResource `json:"resources"`
}

func newQueueSnapshot(queueName string) QueueSnapshot {
	return QueueSnapshot{
		QueueName: queueName,
		Source:    queueCacheDumpSource,
		Resources: make(map[string]QueueSnapshotResource),
	}
}

func (m *QueueSnapshot) setResourceValue(resourceName, resourceSet string, value float64) {
	snapshot := m.Resources[resourceName]

	switch resourceSet {
	case "capacity":
		snapshot.Capability = &value
	case "deserved":
		snapshot.Deserved = &value
	case "allocated":
		snapshot.Allocated = &value
	case "request":
		snapshot.Request = &value
	case "inqueue":
		snapshot.Inqueue = &value
	case "elastic":
		snapshot.Elastic = &value
	default:
		panic("unknown queue resource set " + resourceSet)
	}

	m.Resources[resourceName] = snapshot
}
