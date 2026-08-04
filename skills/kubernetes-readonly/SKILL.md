# Kubernetes scheduling evidence

Use these read-only tools only during the final LLM source fallback, after deterministic Volcano checks cannot explain why the selected Pod remains Pending.

Available tools:

- `k8s_get_target_pod` returns a sanitized, scheduling-focused view of the exact Pod selected for this analysis.
- `k8s_list_target_pod_events` returns Events for that Pod name and current UID, with an optional bounded limit.
- `k8s_list_target_podgroup_events` derives the PodGroup from the selected Pod and returns its bounded Events.
- `k8s_get_node` returns a sanitized scheduling view of a node already present in this analysis report.
- `k8s_get_volcano_scheduler_logs` returns a bounded log tail from the current Volcano scheduler leader.
- `source_read_volcano_scheduler_file` reads one source file from the dynamically generated hook index for the exact selected branch/tag worktree.

Stay inside the target Pod, its PodGroup, report-node, scheduler-leader log, and selected-source index scope. Prefer existing report evidence, call a tool only when the missing fact can change the conclusion, and treat tool errors as unknown evidence rather than proof of a scheduling failure.

These tools never expose Secrets, ConfigMaps, environment values, arbitrary annotations, arbitrary Pod logs, exec, scheduler-cache signaling, or write operations. Cache capture is a deterministic service step and is not an LLM-callable tool. Source reads reject unindexed paths and worktree escapes.
