# Volcano scheduler cache

Use this skill before source-level inference when live scheduler cache evidence is needed.

1. Obtain scheduler cache evidence during `/api/analyze` only through the public `clusterManager.CaptureCacheDump` method.
2. The method derives the leader Lease from the scheduler's parsed `--scheduler-name`, `--leader-elect`, and `--lock-object-namespace` flags, then matches its `holderIdentity` to a ready discovered scheduler Pod. It does not assume a fixed Lease name or namespace and does not watch cluster-wide Node Leases. It then opens a following log stream, starts the parser, rechecks the leader, sends `SIGUSR2`, and parses the dump. Do not invoke a standalone command, start a separate log command, or supply a scheduler Pod manually.
3. Preserve lines containing `Node (` and their `allocatable`, `idle`, `used`, and `releasing` fields as node evidence.
4. Preserve lines between `Dump of jobs info in scheduler cache` and the scheduler memory stats as job evidence. Parse `Job (...)` lines for namespace/name/queue and child `Task (...)` lines for status and `resreq`; use these to locally reconstruct proportion queue resources instead of relying on scheduler `/metrics`.
5. For proportion enqueue checks, compute `realCapability = min(totalNodeAllocatable - totalQueueGuarantee + queueGuarantee, queue.spec.capability)` and `need = candidateMinResources + allocated + inqueue - elastic`. A queue passes the capacity gate when `need <= realCapability`.
6. Display node `idle / allocatable`, but reproduce the scheduling resource gate conservatively. Volcano uses `FutureIdle = Idle + Releasing - Pipelined`, while the standard dump omits `Pipelined`: a request greater than `idle + releasing` is a proven failure, but a possible fit is still indeterminate. Kubernetes Node allocatable is only a display/fallback upper bound. CPU and extended/scalar dump values use Volcano's 1000x representation; memory uses bytes.
7. If the dump is partial, omits a requested device dimension, or is unavailable, return `passed: false` with `determinate: false`. Do not turn the Kubernetes allocatable fallback into a scheduling conclusion.
8. Do not send signals other than `USR2`.

Expected node evidence includes `allocatable<...>`, `idle <...>`, `used <...>`, and `releasing <...>`. Expected job evidence includes `Job (...)` and nested `Task (...) ... status ..., resreq ...` lines from `pkg/scheduler/cache/dumper.go`.
