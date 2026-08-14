# Volens

[English](README.md) | [简体中文](README_zh.md)

Volens is an LLM-assisted diagnostic tool for Pods that remain Pending under the Volcano scheduler. It runs as a Pod in the target cluster and gives operators a Web UI for selecting a Pending Pod, replaying the relevant scheduler checks, and receiving a concise root cause with suggested actions.

Volens is read-only toward Kubernetes workloads and does not patch or replace `vc-scheduler`. It supports multiple Volcano releases by detecting the running scheduler version and using the matching source branch during analysis.

## Architecture

One Volens instance works across supported Volcano releases. It reads Kubernetes resources through in-cluster `client-go`, obtains version and cache evidence from the active `vc-scheduler`, runs registered diagnostic rules in order, and uses a commit-specific Volcano source worktree when deeper analysis is required.

![Volens architecture](docs/images/volens-architecture.svg)

## What you get

- A guided diagnosis in scheduler order: **enqueue → JobValid → node allocation**.
- Queue capacity and admission evidence, including the resource dimension that blocks enqueue.
- Per-node `free / total` resources and filter results for CPU, memory, Pods, GPU/NPU, taints, affinity, ports, volumes, and other enabled checks.
- Clear result symbols: `✓` passed, `×` failed, `?` needs more evidence, `—` disabled or not reached.
- A short final conclusion with practical suggestions instead of a long model transcript.
- Automatic Volcano version detection, with an optional branch override in the UI.
- Deterministic rules first; source- and log-grounded LLM analysis only when the normal checks cannot reach a conclusion.

## Screenshots

### Queue admission

See the selected queue's effective capacity, allocated/inqueue resources, current PodGroup demand, and the exact insufficient dimensions.

![Volens queue admission checks](docs/images/volens-enqueue.png)

### Job validity

Review Pod/PodGroup resources and the active `JobValid` checks before node allocation.

![Volens JobValid checks](docs/images/volens-job-valid.png)

### Node allocation

Compare the Pod request with each node's Volcano cache resources and enabled filters. Failed resource values are highlighted directly in the table.

![Volens node allocation checks](docs/images/volens-node-allocation.png)

### Final diagnosis

The report ends with one root cause and a small set of recommended actions.

![Volens final diagnosis](docs/images/volens-summary.png)

## Quick start

### Prerequisites

- A Kubernetes cluster with Volcano installed.
- Permission to create the ServiceAccount, ClusterRole, ClusterRoleBinding, Deployment, and NetworkPolicy in `deploy/volens.yaml`.
- A Volens image accessible from the cluster.
- Network access to the configured Volcano Git remote if branch updates are enabled.
- An OpenAI-compatible inference endpoint only if LLM fallback is required. Deterministic checks work without it.

### 1. Build and push the image

```bash
docker build -t <registry>/volens:<tag> .
docker push <registry>/volens:<tag>
```

Update the image in `deploy/volens.yaml`:

```yaml
containers:
  - name: volens
    image: <registry>/volens:<tag>
```

### 2. Configure the model endpoint

Edit these environment variables in `deploy/volens.yaml` when LLM fallback is needed:

```yaml
- name: LLM_BASE_URL
  value: "http://inference.default.svc:8000"
- name: LLM_MODEL
  value: "your-model"
```

For an endpoint that requires a key, create the referenced Secret:

```bash
kubectl -n volcano-system create secret generic volens-llm \
  --from-literal=api-key='<api-key>'
```

Do not place provider keys directly in the manifest or image.

### 3. Deploy Volens

```bash
kubectl apply -f deploy/volens.yaml
kubectl -n volcano-system rollout status deployment/volens
```

The supplied manifest intentionally does not expose a Service. Access is through Kubernetes RBAC-controlled port forwarding:

```bash
kubectl -n volcano-system port-forward deployment/volens 8080:8080
```

Open [http://localhost:8080](http://localhost:8080).

### 4. Run an analysis

1. Select a Pending Pod.
2. Keep the recommended Volcano branch or choose another remote branch.
3. Click **Analyze**.
4. Read the report from top to bottom: enqueue, JobValid, node allocation, then summary.

If enqueue is definitively rejected, later stages are shown as not reached because Volcano would not allocate that PodGroup yet.

## Common configuration

| Variable | Default | Purpose |
|---|---|---|
| `VOLCANO_REPO_URL` | `https://github.com/volcano-sh/volcano.git` | Volcano source repository used for version-aligned analysis |
| `VOLCANO_GIT_UPDATE` | `true` | Fetch updated branches and tags before source preparation |
| `LLM_BASE_URL` | empty | OpenAI-compatible hosted or in-cluster inference endpoint |
| `LLM_API_KEY` | empty | Optional bearer token, normally loaded from a Secret |
| `LLM_MODEL` | `gpt-4.1-mini` | Model name sent to the endpoint |
| `LLM_MAX_TOOL_ROUNDS` | `4` | Maximum source/log tool rounds for one LLM fallback |
| `LLM_TIMEOUT` | `10m` | Overall timeout for the LLM fallback |
| `VOLENS_ANALYZE_TIMEOUT` | `15m` | Overall timeout for one analysis |
| `VOLENS_ADDR` | `:8080` | HTTP listen address |

For a private Volcano repository, mount SSH credentials and `known_hosts`, or use an HTTPS credential helper. Keep credentials out of `VOLCANO_REPO_URL` in committed manifests.

## Run from source

For local development, provide a kubeconfig and an existing writable Volcano clone:

```bash
git clone https://github.com/volcano-sh/volcano.git ../volcano
KUBECONFIG="$HOME/.kube/config" \
VOLENS_SOURCE_DIR="$(pwd)/../volcano" \
go run ./cmd/volens
```

Then open [http://localhost:8080](http://localhost:8080).

## HTTP endpoints

- `GET /healthz`
- `GET /api/pods`
- `GET /api/branches`
- `POST /api/analyze`

Example:

```bash
curl -X POST http://localhost:8080/api/analyze \
  -H 'Content-Type: application/json' \
  -d '{"namespace":"default","pod":"pending-pod","branch":"release-1.12"}'
```

The `branch` field is optional. When omitted, Volens uses the detected scheduler version to select source automatically.
