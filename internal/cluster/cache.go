package cluster

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

const NodeCacheDumpPrefix = "Node ("

const (
	nodeDumpStart        = "Dump of nodes info in scheduler cache"
	nodeDumpEnd          = "Dump of jobs info in scheduler cache"
	jobDumpStart         = nodeDumpEnd
	cacheDumpEnd         = "volcano scheduler memory stat:"
	cacheDumpTimeout     = 15 * time.Second
	cacheStreamCloseWait = 250 * time.Millisecond
)

type CacheNode struct {
	Name        string             `json:"name"`
	Allocatable map[string]float64 `json:"allocatable"`
	Idle        map[string]float64 `json:"idle"`
	Used        map[string]float64 `json:"used"`
	Releasing   map[string]float64 `json:"releasing"`
}

type CacheTask struct {
	UID       string             `json:"uid,omitempty"`
	Namespace string             `json:"namespace,omitempty"`
	Name      string             `json:"name,omitempty"`
	Job       string             `json:"job,omitempty"`
	Status    string             `json:"status,omitempty"`
	Resreq    map[string]float64 `json:"resreq,omitempty"`
}

type CacheJob struct {
	UID          string      `json:"uid,omitempty"`
	Namespace    string      `json:"namespace,omitempty"`
	Name         string      `json:"name,omitempty"`
	Queue        string      `json:"queue,omitempty"`
	MinAvailable int32       `json:"minAvailable,omitempty"`
	Tasks        []CacheTask `json:"tasks,omitempty"`
}

type CacheDump struct {
	Scheduler Scheduler            `json:"scheduler"`
	Nodes     map[string]CacheNode `json:"nodes"`
	Jobs      map[string]CacheJob  `json:"jobs,omitempty"`
}

type cacheCaptureResult struct {
	nodes        map[string]CacheNode
	jobs         map[string]CacheJob
	lastParseErr error
	err          error
}

type readStartNotifier struct {
	reader  io.Reader
	started chan struct{}
	once    sync.Once
}

func (r *readStartNotifier) Read(buffer []byte) (int, error) {
	r.once.Do(func() {
		close(r.started)
	})

	return r.reader.Read(buffer)
}

var cachePatterns = struct {
	node        *regexp.Regexp
	allocatable *regexp.Regexp
	idle        *regexp.Regexp
	used        *regexp.Regexp
	releasing   *regexp.Regexp
	job         *regexp.Regexp
	task        *regexp.Regexp
}{
	node:        regexp.MustCompile(`Node \((.*?)\)`),
	allocatable: regexp.MustCompile(`allocatable<(.*?)>`),
	idle:        regexp.MustCompile(`idle <(.*?)>`),
	used:        regexp.MustCompile(`used <(.*?)>`),
	releasing:   regexp.MustCompile(`releasing <(.*?)>`),
	job: regexp.MustCompile(
		`Job \((.*?)\): namespace (.*?) \((.*?)\), name (.*?), minAvailable ([0-9]+),`,
	),
	task: regexp.MustCompile(
		`Task \((.*?):(.*?)/(.*?)\): job (.*?), status (.*?), pri .*?, resreq (.*?), preemptable`,
	),
}

// ParseNodeCache parses Volcano's SIGUSR2 scheduler cache dump. Values are
// normalized to Kubernetes quantity semantics: CPU cores, bytes for
// memory/storage, and whole units for extended resources.
func ParseNodeCache(line string) (CacheNode, error) {
	name, err := capture(cachePatterns.node, line, "node")
	if err != nil {
		return CacheNode{}, err
	}

	allocValue, err := capture(cachePatterns.allocatable, line, "allocatable")
	if err != nil {
		return CacheNode{}, err
	}

	idleValue, err := capture(cachePatterns.idle, line, "idle")
	if err != nil {
		return CacheNode{}, err
	}

	usedValue, err := capture(cachePatterns.used, line, "used")
	if err != nil {
		return CacheNode{}, err
	}

	releasingValue, err := capture(cachePatterns.releasing, line, "releasing")
	if err != nil {
		return CacheNode{}, err
	}

	alloc, err := parseDumpResources(allocValue)
	if err != nil {
		return CacheNode{}, fmt.Errorf("allocatable: %w", err)
	}

	idle, err := parseDumpResources(idleValue)
	if err != nil {
		return CacheNode{}, fmt.Errorf("idle: %w", err)
	}

	used, err := parseDumpResources(usedValue)
	if err != nil {
		return CacheNode{}, fmt.Errorf("used: %w", err)
	}

	releasing, err := parseDumpResources(releasingValue)
	if err != nil {
		return CacheNode{}, fmt.Errorf("releasing: %w", err)
	}

	return CacheNode{
		Name:        name,
		Allocatable: alloc,
		Idle:        idle,
		Used:        used,
		Releasing:   releasing,
	}, nil
}

func ParseJobCache(line string) (CacheJob, error) {
	match := cachePatterns.job.FindStringSubmatch(line)
	if len(match) < 6 {
		return CacheJob{}, fmt.Errorf("cache dump missing job fields")
	}

	minAvailable, err := strconv.ParseInt(match[5], 10, 32)
	if err != nil {
		return CacheJob{}, fmt.Errorf("minAvailable=%q: %w", match[5], err)
	}

	return CacheJob{
		UID:          match[1],
		Namespace:    match[2],
		Queue:        match[3],
		Name:         match[4],
		MinAvailable: int32(minAvailable),
	}, nil
}

func ParseTaskCache(line string) (CacheTask, error) {
	match := cachePatterns.task.FindStringSubmatch(line)
	if len(match) < 7 {
		return CacheTask{}, fmt.Errorf("cache dump missing task fields")
	}

	resources, err := parseDumpResources(match[6])
	if err != nil {
		return CacheTask{}, fmt.Errorf("resreq: %w", err)
	}

	return CacheTask{
		UID:       strings.TrimSpace(match[1]),
		Namespace: strings.TrimSpace(match[2]),
		Name:      strings.TrimSpace(match[3]),
		Job:       strings.TrimSpace(match[4]),
		Status:    strings.TrimSpace(match[5]),
		Resreq:    resources,
	}, nil
}

func capture(re *regexp.Regexp, line, field string) (string, error) {
	match := re.FindStringSubmatch(line)
	if len(match) < 2 || match[1] == "" {
		return "", fmt.Errorf("cache dump missing %s", field)
	}

	return match[1], nil
}

func parseDumpResources(value string) (map[string]float64, error) {
	result := map[string]float64{}
	requestBacked := map[string]bool{}

	if strings.TrimSpace(value) == "" {
		return result, nil
	}

	for _, part := range strings.Split(value, ",") {
		fields := strings.Fields(strings.TrimSpace(part))

		if len(fields) != 2 {
			continue
		}

		value, err := strconv.ParseFloat(fields[1], 64)
		if err != nil {
			return nil, fmt.Errorf("%s=%q: %w", fields[0], fields[1], err)
		}

		originalName := fields[0]
		resourceName := canonicalQueueResourceName(originalName)
		fromRequest := strings.HasPrefix(strings.ToLower(originalName), "requests.")

		if requestBacked[resourceName] && !fromRequest {
			continue
		}

		switch originalName {
		case "cpu":
			result[resourceName] = value / 1000
		case "memory":
			result[resourceName] = value
		case "pods":
			// Volcano stores the pod-count scalar as whole units, unlike
			// extended resources and ephemeral-storage which use MilliValue.
			result[resourceName] = value
		default:
			result[resourceName] = value / 1000
		}

		requestBacked[resourceName] = fromRequest || requestBacked[resourceName]
	}

	return result, nil
}

// CaptureCacheDump owns the complete active capture sequence. It discovers the
// leader, opens the following log stream, starts the parser, rechecks the
// leader, sends SIGUSR2, and returns the parsed node/job/task cache dump.
func (m *Client) CaptureCacheDump(ctx context.Context) (CacheDump, error) {
	m.cacheMu.Lock()
	defer m.cacheMu.Unlock()

	scheduler, err := m.GetVolcanoScheduler(ctx)
	dump := CacheDump{
		Scheduler: scheduler,
		Nodes:     map[string]CacheNode{},
		Jobs:      map[string]CacheJob{},
	}

	if err != nil {
		return dump, err
	}

	log.Printf("capturing Volcano cache dump from scheduler %s/%s", scheduler.Namespace, scheduler.Name)

	streamCtx, cancel := context.WithTimeout(ctx, cacheDumpTimeout)
	defer cancel()

	stream, err := m.streamPodLogs(streamCtx, scheduler)
	if err != nil {
		return dump, err
	}

	nodes, jobs, err := captureCacheDumpFromStream(streamCtx, stream, func() error {
		current, leaderErr := m.GetVolcanoScheduler(streamCtx)
		if leaderErr != nil {
			return fmt.Errorf("recheck scheduler leader: %w", leaderErr)
		}

		if !sameScheduler(scheduler, current) {
			return fmt.Errorf(
				"scheduler leader changed from %s/%s to %s/%s before cache signal",
				scheduler.Namespace,
				scheduler.Name,
				current.Namespace,
				current.Name,
			)
		}

		log.Printf("sending SIGUSR2 to Volcano scheduler %s/%s", scheduler.Namespace, scheduler.Name)

		return m.signalCacheDump(streamCtx, scheduler)
	})
	dump.Nodes = nodes
	dump.Jobs = jobs

	log.Printf(
		"captured Volcano cache dump from %s/%s: nodes=%d jobs=%d err=%v",
		scheduler.Namespace,
		scheduler.Name,
		len(dump.Nodes),
		len(dump.Jobs),
		err,
	)

	return dump, err
}

func sameScheduler(left, right Scheduler) bool {
	if left.Namespace != right.Namespace || left.Name != right.Name {
		return false
	}

	return left.UID == "" || right.UID == "" || left.UID == right.UID
}

func captureCacheDumpFromStream(
	ctx context.Context,
	stream io.ReadCloser,
	signal func() error,
) (map[string]CacheNode, map[string]CacheJob, error) {
	resultChannel := make(chan cacheCaptureResult, 1)
	readStarted := make(chan struct{})
	reader := &readStartNotifier{
		reader:  stream,
		started: readStarted,
	}

	go func() {
		resultChannel <- scanCacheDump(reader)
	}()

	select {
	case <-readStarted:
	case <-ctx.Done():
		_ = stream.Close()

		return nil, nil, fmt.Errorf("start cache dump parser: %w", ctx.Err())
	}

	select {
	case result := <-resultChannel:
		_ = stream.Close()

		if result.err != nil {
			return result.nodes, result.jobs, fmt.Errorf("scheduler log stream ended before cache signal: %w", result.err)
		}

		return result.nodes, result.jobs, fmt.Errorf("scheduler cache dump completed before cache signal")
	default:
	}

	if err := signal(); err != nil {
		_ = stream.Close()

		return nil, nil, err
	}

	var result cacheCaptureResult

	select {
	case result = <-resultChannel:
	case <-ctx.Done():
		_ = stream.Close()

		timer := time.NewTimer(cacheStreamCloseWait)
		defer timer.Stop()

		select {
		case result = <-resultChannel:
		case <-timer.C:
		}

		return result.nodes, result.jobs, fmt.Errorf("cache dump capture: %w", ctx.Err())
	}

	_ = stream.Close()

	if result.err != nil {
		return result.nodes, result.jobs, result.err
	}

	if result.lastParseErr != nil {
		return result.nodes, result.jobs, fmt.Errorf(
			"cache dump completed with %d nodes and %d jobs; last schema error: %w",
			len(result.nodes),
			len(result.jobs),
			result.lastParseErr,
		)
	}

	return result.nodes, result.jobs, nil
}

func scanCacheDump(reader io.Reader) cacheCaptureResult {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)

	nodes := map[string]CacheNode{}
	jobs := map[string]CacheJob{}
	var lastParseErr error
	nodeStarted := false
	jobStarted := false
	var currentJobKey string

	for scanner.Scan() {
		line := scanner.Text()

		if strings.Contains(line, nodeDumpStart) {
			nodeStarted = true
			jobStarted = false
			nodes = map[string]CacheNode{}
			jobs = map[string]CacheJob{}
			lastParseErr = nil
			currentJobKey = ""

			continue
		}

		if nodeStarted && strings.Contains(line, jobDumpStart) {
			nodeStarted = false
			jobStarted = true

			continue
		}

		if jobStarted && strings.Contains(line, cacheDumpEnd) {
			return cacheCaptureResult{
				nodes:        nodes,
				jobs:         jobs,
				lastParseErr: lastParseErr,
			}
		}

		if nodeStarted && strings.Contains(line, NodeCacheDumpPrefix) {
			node, err := ParseNodeCache(line)
			if err != nil {
				lastParseErr = err

				continue
			}

			nodes[node.Name] = node

			continue
		}

		if !jobStarted {
			continue
		}

		if strings.Contains(line, "Job (") {
			job, err := ParseJobCache(line)
			if err != nil {
				lastParseErr = err
				currentJobKey = ""

				continue
			}

			currentJobKey = job.Namespace + "/" + job.Name
			jobs[currentJobKey] = job

			continue
		}

		if currentJobKey == "" || !strings.Contains(line, "Task (") {
			continue
		}

		task, err := ParseTaskCache(line)
		if err != nil {
			lastParseErr = err

			continue
		}

		job := jobs[currentJobKey]
		job.Tasks = append(job.Tasks, task)
		jobs[currentJobKey] = job
	}

	if err := scanner.Err(); err != nil {
		return cacheCaptureResult{
			nodes:        nodes,
			jobs:         jobs,
			lastParseErr: lastParseErr,
			err:          err,
		}
	}

	return cacheCaptureResult{
		nodes:        nodes,
		jobs:         jobs,
		lastParseErr: lastParseErr,
		err:          fmt.Errorf("scheduler log stream ended before cache dump completed"),
	}
}
