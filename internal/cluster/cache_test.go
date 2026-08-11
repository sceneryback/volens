package cluster

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestParseNodeCacheFromSchedulerLog(t *testing.T) {
	line := `I0731 cache.go:1] Node (ccseagent-2adc1eb1ff): allocatable<cpu 192000.00, memory 1564859215464.00, ephemeral-storage 3039115531748000.00, huawei.com/Ascend910 8000.00, hugepages-2Mi 0.00> idle <cpu 20063.00, memory 917360094824.00, hugepages-2Mi 0.00, ephemeral-storage 3039113434596000.00, huawei.com/Ascend910 2000.00>, used <cpu 171937.00, memory 647499120640.00, huawei.com/Ascend910 6000.00, ephemeral-storage 2097152000.00>, releasing <cpu 0.00, memory 0.00>`

	node, err := ParseNodeCache(line)
	if err != nil {
		t.Fatal(err)
	}

	if node.Name != "ccseagent-2adc1eb1ff" {
		t.Fatalf("name=%q", node.Name)
	}

	assertNear(t, node.Allocatable["cpu"], 192)
	assertNear(t, node.Idle["cpu"], 20.063)
	assertNear(t, node.Idle["memory"], 917360094824)
	assertNear(t, node.Idle["huawei.com/Ascend910"], 2)
	assertNear(t, node.Idle["ephemeral-storage"], 3039113434596)
	assertNear(t, node.Used["huawei.com/Ascend910"], 6)
	assertNear(t, node.Releasing["cpu"], 0)
}

func TestParseNodeCacheRejectsMissingName(t *testing.T) {
	if _, err := ParseNodeCache("idle <cpu 1000.00>"); err == nil {
		t.Fatal("expected missing node error")
	}
}

func TestParseNodeCacheRejectsMissingRequiredResourceField(t *testing.T) {
	line := `Node (node-a): allocatable<cpu 1000.00> idle <cpu 500.00>, used <cpu 500.00>`

	if _, err := ParseNodeCache(line); err == nil {
		t.Fatal("expected missing releasing field error")
	}
}

func TestParseDumpResourcesRejectsInvalidNumber(t *testing.T) {
	if _, err := parseDumpResources("cpu invalid"); err == nil {
		t.Fatal("expected number error")
	}
}

func TestParseDumpResourcesKeepsPodCountAsWholeUnits(t *testing.T) {
	resources, err := parseDumpResources("pods 110.00, nvidia.com/gpu 8000.00")
	if err != nil {
		t.Fatal(err)
	}

	assertNear(t, resources["pods"], 110)
	assertNear(t, resources["nvidia.com/gpu"], 8)
}

func TestParseDumpResourcesCollapsesRequestAliases(t *testing.T) {
	resources, err := parseDumpResources(
		"pods 110.00, requests.pods 1000.00, nvidia.com/gpu 8000.00, requests.nvidia.com/gpu 4000.00",
	)
	if err != nil {
		t.Fatal(err)
	}

	if len(resources) != 2 {
		t.Fatalf("resources=%+v", resources)
	}

	assertNear(t, resources["pods"], 1)
	assertNear(t, resources["nvidia.com/gpu"], 4)
}

func TestCaptureCacheDumpFromStreamStartsScannerBeforeSignal(t *testing.T) {
	reader, writer := io.Pipe()
	stream := &observedReadCloser{
		ReadCloser:  reader,
		readStarted: make(chan struct{}),
	}

	t.Cleanup(func() {
		_ = writer.Close()
	})

	dump := strings.Join([]string{
		nodeDumpStart,
		cacheNodeLine("node-a", "500.00"),
		cacheNodeLine("node-b", "2500.00"),
		nodeDumpEnd,
		cacheDumpEnd,
	}, "\n") + "\n"

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	signalCalls := 0
	nodes, jobs, err := captureCacheDumpFromStream(ctx, stream, func() error {
		signalCalls++

		select {
		case <-stream.readStarted:
		case <-ctx.Done():
			return fmt.Errorf("scanner did not start before signal: %w", ctx.Err())
		}

		if _, writeErr := io.WriteString(writer, dump); writeErr != nil {
			return writeErr
		}

		return writer.Close()
	})
	if err != nil {
		t.Fatal(err)
	}

	if signalCalls != 1 {
		t.Fatalf("signal calls=%d, want 1", signalCalls)
	}

	if len(nodes) != 2 {
		t.Fatalf("nodes=%v", nodes)
	}

	if len(jobs) != 0 {
		t.Fatalf("jobs=%v", jobs)
	}

	assertNear(t, nodes["node-a"].Idle["cpu"], .5)
	assertNear(t, nodes["node-b"].Idle["cpu"], 2.5)
	assertNear(t, nodes["node-b"].Idle["nvidia.com/gpu"], 2)
}

func TestScanCacheDumpUsesLatestCompleteBoundaries(t *testing.T) {
	input := strings.Join([]string{
		cacheNodeLine("before-start", "9000.00"),
		nodeDumpStart,
		cacheNodeLine("stale", "8000.00"),
		nodeDumpStart,
		"unrelated scheduler log line",
		cacheNodeLine("node-a", "500.00"),
		cacheNodeLine("node-b", "2500.00"),
		nodeDumpEnd,
		cacheDumpEnd,
		cacheNodeLine("after-end", "7000.00"),
	}, "\n") + "\n"

	result := scanCacheDump(strings.NewReader(input))
	if result.err != nil {
		t.Fatal(result.err)
	}

	if result.lastParseErr != nil {
		t.Fatal(result.lastParseErr)
	}

	if len(result.nodes) != 2 {
		t.Fatalf("nodes=%v", result.nodes)
	}

	if _, found := result.nodes["stale"]; found {
		t.Fatal("node from an incomplete prior dump was retained")
	}

	if _, found := result.nodes["after-end"]; found {
		t.Fatal("node after the dump end boundary was retained")
	}

	assertNear(t, result.nodes["node-a"].Idle["cpu"], .5)
	assertNear(t, result.nodes["node-b"].Idle["cpu"], 2.5)
}

func TestScanCacheDumpParsesJobsAndTasks(t *testing.T) {
	input := strings.Join([]string{
		nodeDumpStart,
		cacheNodeLine("node-a", "500.00"),
		nodeDumpEnd,
		cacheJobLine("job-uid", "default", "batch", "pg-a", 2),
		cacheTaskLine("task-a", "default", "pod-a", "default/pg-a", "Running", "cpu 1000.00, memory 2048.00, pods 1.00, nvidia.com/gpu 4000.00"),
		cacheTaskLine("task-b", "default", "pod-b", "default/pg-a", "Pending", "cpu 2000.00, memory 4096.00, pods 1.00, nvidia.com/gpu 1000.00"),
		cacheDumpEnd,
	}, "\n") + "\n"

	result := scanCacheDump(strings.NewReader(input))
	if result.err != nil {
		t.Fatal(result.err)
	}

	job := result.jobs["default/pg-a"]
	if job.UID != "job-uid" || job.Queue != "batch" || job.MinAvailable != 2 {
		t.Fatalf("job=%+v", job)
	}

	if len(job.Tasks) != 2 {
		t.Fatalf("tasks=%+v", job.Tasks)
	}

	if job.Tasks[0].Status != "Running" || job.Tasks[0].Resreq["cpu"] != 1 ||
		job.Tasks[0].Resreq["nvidia.com/gpu"] != 4 {
		t.Fatalf("task[0]=%+v", job.Tasks[0])
	}

	if job.Tasks[1].Status != "Pending" || job.Tasks[1].Resreq["cpu"] != 2 ||
		job.Tasks[1].Resreq["nvidia.com/gpu"] != 1 {
		t.Fatalf("task[1]=%+v", job.Tasks[1])
	}
}

func TestScanCacheDumpReturnsPartialNodesWhenEndIsMissing(t *testing.T) {
	input := strings.Join([]string{
		nodeDumpStart,
		cacheNodeLine("node-a", "500.00"),
	}, "\n") + "\n"

	result := scanCacheDump(strings.NewReader(input))
	if result.err == nil || !strings.Contains(result.err.Error(), "ended before cache dump completed") {
		t.Fatalf("err=%v", result.err)
	}

	if len(result.nodes) != 1 {
		t.Fatalf("partial nodes=%v", result.nodes)
	}

	assertNear(t, result.nodes["node-a"].Idle["cpu"], .5)
}

func TestCaptureCacheDumpFromStreamReportsSchemaErrorWithValidNodes(t *testing.T) {
	dump := strings.Join([]string{
		nodeDumpStart,
		cacheNodeLine("node-a", "500.00"),
		`Node (broken): allocatable<cpu invalid> idle <cpu 1000.00>, used <cpu 0.00>, releasing <cpu 0.00>`,
		nodeDumpEnd,
		cacheDumpEnd,
	}, "\n") + "\n"

	nodes, err := captureDumpAfterSignal(t, dump)
	if err == nil || !strings.Contains(err.Error(), "last schema error") {
		t.Fatalf("err=%v", err)
	}

	if len(nodes) != 1 {
		t.Fatalf("nodes=%v", nodes)
	}

	assertNear(t, nodes["node-a"].Idle["cpu"], .5)
}

func TestCaptureCacheDumpFromStreamReturnsSignalError(t *testing.T) {
	reader, writer := io.Pipe()
	t.Cleanup(func() {
		_ = writer.Close()
	})

	wantErr := errors.New("signal failed")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	nodes, jobs, err := captureCacheDumpFromStream(ctx, reader, func() error {
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("err=%v, want %v", err, wantErr)
	}

	if nodes != nil {
		t.Fatalf("nodes=%v, want nil", nodes)
	}

	if jobs != nil {
		t.Fatalf("jobs=%v, want nil", jobs)
	}
}

func TestCaptureCacheDumpFromStreamReturnsPartialNodesOnCancellation(t *testing.T) {
	reader, writer := io.Pipe()
	t.Cleanup(func() {
		_ = writer.Close()
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	nodes, _, err := captureCacheDumpFromStream(ctx, reader, func() error {
		go func() {
			_, _ = io.WriteString(
				writer,
				nodeDumpStart+"\n"+cacheNodeLine("node-a", "500.00")+"\n",
			)
			cancel()
		}()

		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v, want context canceled", err)
	}

	if len(nodes) != 1 {
		t.Fatalf("partial nodes=%v", nodes)
	}

	assertNear(t, nodes["node-a"].Idle["cpu"], .5)
}

func TestCaptureCacheDumpWaitsForCaptureLock(t *testing.T) {
	manager := &Client{}
	manager.cacheMu.Lock()

	locked := true
	defer func() {
		if locked {
			manager.cacheMu.Unlock()
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	done := make(chan error, 1)

	go func() {
		close(started)

		_, err := manager.CaptureCacheDump(ctx)
		done <- err
	}()

	<-started
	cancel()

	select {
	case err := <-done:
		t.Fatalf("capture bypassed serialization lock: %v", err)
	case <-time.After(25 * time.Millisecond):
	}

	manager.cacheMu.Unlock()
	locked = false

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err=%v, want context canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("capture did not resume after serialization lock was released")
	}
}

func TestSameScheduler(t *testing.T) {
	tests := []struct {
		name  string
		left  Scheduler
		right Scheduler
		want  bool
	}{
		{
			name:  "same UID",
			left:  Scheduler{Namespace: "volcano-system", Name: "scheduler", UID: "uid-a"},
			right: Scheduler{Namespace: "volcano-system", Name: "scheduler", UID: "uid-a"},
			want:  true,
		},
		{
			name:  "missing UID remains compatible",
			left:  Scheduler{Namespace: "volcano-system", Name: "scheduler"},
			right: Scheduler{Namespace: "volcano-system", Name: "scheduler", UID: "uid-a"},
			want:  true,
		},
		{
			name:  "replacement Pod",
			left:  Scheduler{Namespace: "volcano-system", Name: "scheduler", UID: "uid-a"},
			right: Scheduler{Namespace: "volcano-system", Name: "scheduler", UID: "uid-b"},
			want:  false,
		},
		{
			name:  "different namespace",
			left:  Scheduler{Namespace: "volcano-system", Name: "scheduler"},
			right: Scheduler{Namespace: "other", Name: "scheduler"},
			want:  false,
		},
		{
			name:  "different name",
			left:  Scheduler{Namespace: "volcano-system", Name: "scheduler-a"},
			right: Scheduler{Namespace: "volcano-system", Name: "scheduler-b"},
			want:  false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := sameScheduler(test.left, test.right); got != test.want {
				t.Fatalf("sameScheduler(%+v, %+v)=%t, want %t", test.left, test.right, got, test.want)
			}
		})
	}
}

type observedReadCloser struct {
	io.ReadCloser
	readStarted chan struct{}
	startOnce   sync.Once
}

func (r *observedReadCloser) Read(buffer []byte) (int, error) {
	r.startOnce.Do(func() {
		close(r.readStarted)
	})

	return r.ReadCloser.Read(buffer)
}

func captureDumpAfterSignal(t *testing.T, dump string) (map[string]CacheNode, error) {
	t.Helper()

	reader, writer := io.Pipe()
	t.Cleanup(func() {
		_ = writer.Close()
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	nodes, _, err := captureCacheDumpFromStream(ctx, reader, func() error {
		if _, err := io.WriteString(writer, dump); err != nil {
			return err
		}

		return writer.Close()
	})

	return nodes, err
}

func cacheNodeLine(name, idleCPU string) string {
	return fmt.Sprintf(
		"Node (%s): allocatable<cpu 16000.00, memory 1000.00, pods 110.00, nvidia.com/gpu 8000.00> "+
			"idle <cpu %s, memory 900.00, pods 100.00, nvidia.com/gpu 2000.00>, "+
			"used <cpu 15500.00, memory 100.00>, releasing <cpu 0.00, memory 0.00>",
		name,
		idleCPU,
	)
}

func cacheJobLine(uid, namespace, queue, name string, minAvailable int32) string {
	return fmt.Sprintf(
		"Job (%s): namespace %s (%s), name %s, minAvailable %d, podGroup <nil>, preemptable false, revocableZone , minAvailable , maxAvailable ",
		uid,
		namespace,
		queue,
		name,
		minAvailable,
	)
}

func cacheTaskLine(uid, namespace, name, job, status, resreq string) string {
	return fmt.Sprintf(
		"    0: Task (%s:%s/%s): job %s, status %s, pri 1, resreq %s, preemptable false, revocableZone ",
		uid,
		namespace,
		name,
		job,
		status,
		resreq,
	)
}

func assertNear(t *testing.T, got, want float64) {
	t.Helper()

	if math.Abs(got-want) > math.Max(1e-9, math.Abs(want)*1e-12) {
		t.Fatalf("got %v, want %v", got, want)
	}
}
