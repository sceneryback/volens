package cluster

import (
	"bufio"
	"context"
	"fmt"
	"io"
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

type CacheDump struct {
	Scheduler Scheduler            `json:"scheduler"`
	Nodes     map[string]CacheNode `json:"nodes"`
}

type cacheCaptureResult struct {
	nodes        map[string]CacheNode
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
}{
	node:        regexp.MustCompile(`Node \((.*?)\)`),
	allocatable: regexp.MustCompile(`allocatable<(.*?)>`),
	idle:        regexp.MustCompile(`idle <(.*?)>`),
	used:        regexp.MustCompile(`used <(.*?)>`),
	releasing:   regexp.MustCompile(`releasing <(.*?)>`),
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

func capture(re *regexp.Regexp, line, field string) (string, error) {
	match := re.FindStringSubmatch(line)
	if len(match) < 2 || match[1] == "" {
		return "", fmt.Errorf("cache dump missing %s", field)
	}

	return match[1], nil
}

func parseDumpResources(value string) (map[string]float64, error) {
	result := map[string]float64{}

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

		switch fields[0] {
		case "cpu":
			result[fields[0]] = value / 1000
		case "memory":
			result[fields[0]] = value
		case "pods":
			// Volcano stores the pod-count scalar as whole units, unlike
			// extended resources and ephemeral-storage which use MilliValue.
			result[fields[0]] = value
		default:
			result[fields[0]] = value / 1000
		}
	}

	return result, nil
}

// CaptureCacheDump owns the complete active capture sequence. It discovers the
// leader, opens the following log stream, starts the parser, rechecks the
// leader, sends SIGUSR2, and returns the parsed node cache dump.
func (m *Client) CaptureCacheDump(ctx context.Context) (CacheDump, error) {
	m.cacheMu.Lock()
	defer m.cacheMu.Unlock()

	scheduler, err := m.GetVolcanoScheduler(ctx)
	dump := CacheDump{
		Scheduler: scheduler,
		Nodes:     map[string]CacheNode{},
	}

	if err != nil {
		return dump, err
	}

	streamCtx, cancel := context.WithTimeout(ctx, cacheDumpTimeout)
	defer cancel()

	stream, err := m.streamPodLogs(streamCtx, scheduler)
	if err != nil {
		return dump, err
	}

	nodes, err := captureCacheDumpFromStream(streamCtx, stream, func() error {
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

		return m.signalCacheDump(streamCtx, scheduler)
	})
	dump.Nodes = nodes

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
) (map[string]CacheNode, error) {
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

		return nil, fmt.Errorf("start cache dump parser: %w", ctx.Err())
	}

	select {
	case result := <-resultChannel:
		_ = stream.Close()

		if result.err != nil {
			return result.nodes, fmt.Errorf("scheduler log stream ended before cache signal: %w", result.err)
		}

		return result.nodes, fmt.Errorf("scheduler cache dump completed before cache signal")
	default:
	}

	if err := signal(); err != nil {
		_ = stream.Close()

		return nil, err
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

		return result.nodes, fmt.Errorf("cache dump capture: %w", ctx.Err())
	}

	_ = stream.Close()

	if result.err != nil {
		return result.nodes, result.err
	}

	if result.lastParseErr != nil {
		return result.nodes, fmt.Errorf(
			"cache dump completed with %d nodes; last schema error: %w",
			len(result.nodes),
			result.lastParseErr,
		)
	}

	return result.nodes, nil
}

func scanCacheDump(reader io.Reader) cacheCaptureResult {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)

	nodes := map[string]CacheNode{}
	var lastParseErr error
	started := false

	for scanner.Scan() {
		line := scanner.Text()

		if strings.Contains(line, nodeDumpStart) {
			started = true
			nodes = map[string]CacheNode{}
			lastParseErr = nil

			continue
		}

		if started && strings.Contains(line, nodeDumpEnd) {
			return cacheCaptureResult{
				nodes:        nodes,
				lastParseErr: lastParseErr,
			}
		}

		if !started || !strings.Contains(line, NodeCacheDumpPrefix) {
			continue
		}

		node, err := ParseNodeCache(line)
		if err != nil {
			lastParseErr = err

			continue
		}

		nodes[node.Name] = node
	}

	if err := scanner.Err(); err != nil {
		return cacheCaptureResult{
			nodes:        nodes,
			lastParseErr: lastParseErr,
			err:          err,
		}
	}

	return cacheCaptureResult{
		nodes:        nodes,
		lastParseErr: lastParseErr,
		err:          fmt.Errorf("scheduler log stream ended before cache dump completed"),
	}
}
