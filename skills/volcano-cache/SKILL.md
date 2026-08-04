# Volcano scheduler cache

Use this skill before source-level inference when live scheduler cache evidence is needed.

1. Obtain scheduler cache evidence during `/api/analyze` only through the public `clusterManager.CaptureCacheDump` method.
2. The method derives the leader Lease from the scheduler's parsed `--scheduler-name`, `--leader-elect`, and `--lock-object-namespace` flags, then matches its `holderIdentity` to a ready discovered scheduler Pod. It does not assume a fixed Lease name or namespace and does not watch cluster-wide Node Leases. It then opens a following log stream, starts the parser, rechecks the leader, sends `SIGUSR2`, and parses the dump. Do not invoke a standalone command, start a separate log command, or supply a scheduler Pod manually.
3. Preserve lines containing `Node (` and their `allocatable`, `idle`, `used`, and `releasing` fields as evidence.
4. Compare requests with `idle`, not Kubernetes Node allocatable. CPU and extended/scalar dump values use Volcano's 1000x representation; memory uses bytes.
5. If the dump is partial or unavailable, return `passed: false` with `determinate: false`. Do not turn the allocatable fallback into a scheduling conclusion.
6. Do not send signals other than `USR2`.

Expected node evidence includes `allocatable<...>`, `idle <...>`, `used <...>`, and `releasing <...>`.
