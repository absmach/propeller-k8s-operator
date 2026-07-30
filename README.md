# Propeller K8s Operator

The Propeller K8s Operator is a Kubernetes operator designed to manage and schedule **WebAssembly (WASM) tasks** on a hybrid fleet of devices, which can be either Kubernetes pods or external devices using [propeller](https://github.com/absmach/propeller). It introduces:

- `Proplet` as the hybrid worker primitive (k8s or external), and
- `Task` as the canonical task API, plus supporting CRDs for scheduling and federated learning.

## Overview

The operator enables users to:

- Define a pool of worker nodes (`Proplets`).
- Schedule WASM workloads (`Task`) onto those workers.
- Orchestrate federated learning workflows via `FederatedJob` and `TrainingRound` layered on top of `Task`.

### Key Features

- **Hybrid Worker Management:** Manage both Kubernetes-native workers (as `Deployments`) and external devices (e.g., IoT devices, bare-metal servers) as a unified pool of resources.
- **Canonical WASM Task API:** `Task` describes workloads in terms of `imageUrl`/`file`, `cliArgs`, `env`, `inputs`, `daemon`, `mode`, and scheduling knobs (`propletId`), matching the Propeller proplet runtime contract.
- **Flexible Scheduling:** Use explicit `propletId` to target specific proplets, or let the scheduler auto-select.
- **MQTT Integration:** The operator communicates with proplets via MQTT for task dispatch and result collection.
- **Federated Learning Orchestration:** `FederatedJob` and `TrainingRound` manage FL rounds and k-of-n aggregation.

## Custom Resources

### 1. `Proplet` (fleet primitive)

Two types:

- **`k8s`**: Operator creates a `Deployment` running the proplet image. Tasks are dispatched via MQTT to the running proplet, same as an external proplet.
- **`external`**: External device (IoT, Docker container, etc.) connecting via MQTT. No K8s resources created.

### 2. `Task` (canonical task API)

Smallest unit of work, representing a single task to be executed by a proplet. Tasks always dispatch via MQTT to the proplet's own Wasmtime runtime, regardless of whether the target proplet is `k8s` or `external`. The WASM module comes from either `file` (inline base64 bytes) or `imageUrl` (an OCI registry reference, fetched by the proplet through the registry proxy) — `imageUrl` is a WASM module reference, not a container image; the operator never runs it as one.

### 3. `PropellerJob` (batch)

Groups multiple tasks into a managed batch. Supports `parallel`, `sequential`, or `configurable` execution modes.

### 4. `FederatedJob` and `TrainingRound` (federated learning)

`FederatedJob` defines a multi-round FL experiment. Each round creates a `TrainingRound` that dispatches `Task`s to participants, collects results, and aggregates once k-of-n updates are received.

## Architecture

| Controller                | Owns          | Responsibility                                                          |
| ------------------------- | ------------- | ----------------------------------------------------------------------- |
| `PropletReconciler`       | Proplet       | Creates/reconciles k8s Deployments; monitors external proplets via MQTT |
| `TaskReconciler`          | Task          | Schedules WASM on selected proplet; always dispatches via MQTT          |
| `PropellerJobReconciler`  | PropellerJob  | Creates child Tasks and aggregates outcomes                             |
| `FederatedJobReconciler`  | FederatedJob  | Creates TrainingRound resources sequentially                            |
| `TrainingRoundReconciler` | TrainingRound | Creates per-participant Tasks; aggregates once k-of-n complete          |

## Prerequisites

- A Kubernetes cluster (local: [k3d](https://k3d.io/stable/))
- `kubectl`, `make`, Docker
- An MQTT broker for external proplet communication

## Quick Start

Make sure you have started propeller and have an MQTT broker available. If not, follow the [propeller getting started instructions](https://www.absmach.eu/docs/propeller/getting-started/).

```bash
# 1. Create cluster
k3d cluster create propeller

# 2. Install CRDs
make install

# 3. Start operator with MQTT
make run ARGS="--mqtt-address='tcp://your-mqtt:1883' \
  --tenant-id='<tenant>' \
  --channel-id='<channel>' \
  --entity-id='<entity>' \
  --api-key='<api-key>'"

# 4. (another terminal) Create a proplet. Make sure you replace the placeholders in `config/samples/propeller_v1_proplet.yaml` with your credentials.
kubectl apply -f config/samples/propeller_v1_proplet.yaml

# 5. Run a WASM task
kubectl apply -f config/samples/propeller_v1_task.yaml
```

## Sample Configurations

All samples are in `config/samples/`. Edit them with your MQTT credentials before applying.

### Proplet Samples

| File                                 | Type     | Description                                                                          |
| ------------------------------------ | -------- | ------------------------------------------------------------------------------------ |
| `propeller_v1_proplet.yaml`          | k8s      | Proplet managed as a K8s Deployment. Requires `k8s.image`.                           |
| `propeller_v1_proplet_external.yaml` | external | External device proplet. No K8s resources.                                           |
| `propeller_v1_proplet_full.yaml`     | k8s      | Every optional `k8s.env.*` flag enabled — runs all feature-gated Task samples below. |
| `propeller_v1_proplet_wasi_nn.yaml`  | k8s      | Dedicated `proplet-wasi-nn` image for the WASI-NN example.                           |

### Registry Proxy Sample

`propeller_v1_task_with_image.yaml`, `propeller_v1_task_tee.yaml` (on real TEE hardware), and `FederatedJob`/`TrainingRound`'s `taskWasmImage` all dispatch via `spec.imageUrl`, which the proplet resolves by requesting chunks from the registry proxy over MQTT — a standalone service from the sibling [`propeller`](https://github.com/absmach/propeller) repo (`cmd/proxy`), not a CRD this operator manages. `propeller_proxy_deployment.yaml` is a plain Deployment + Service for it, mirroring `propeller/docker/compose.propeller.yaml`'s proxy service:

```bash
# edit connectionConfig to a *distinct* Atom entity (same tenant/channel as
# your operator and proplets, but its own entity ID — MQTT client IDs must
# be unique per connection) and PROXY_REGISTRY_URL/credentials, then:
kubectl apply -f config/samples/propeller_proxy_deployment.yaml
kubectl logs deploy/propeller-proxy -f
```

Confirmed: the image pulls and the container starts and correctly attempts to subscribe to `registry/proplet` on your tenant/channel (verify in the logs) — it needs real, distinct credentials to get past that point. Without this running, any `imageUrl`-based Task or FederatedJob round reaches "requesting binary from registry" and then never completes (the proplet's chunk-assembly wait times out).

### Testing Other WASM Examples

The samples above cover the examples in the sibling [`propeller`](https://github.com/absmach/propeller) repo's `examples/` directory with pre-built `.wasm` content already embedded. For anything else — a different example, or your own module — use `hack/apply-example-task.sh`, which base64-encodes a compiled `.wasm` file and applies a generated `Task` manifest, since a plain Kubernetes manifest has no way to load a local file's content on its own:

```bash
# (in the propeller repo) build the example first, e.g.:
#   make compute

PROPELLER_DIR=~/code/absmach/propeller \
  ./hack/apply-example-task.sh compute compute 5
kubectl get task compute-example -w
```

`spec.file` works fine at real sizes (confirmed with a 298KB module) as long as the MQTT connection between operator and proplet is healthy — if a `Task` stays `running` with no result, check for repeated reconnects in the operator's logs before assuming it's a size problem. For genuinely large modules, or to avoid embedding one in every manifest, publish it to an OCI registry and use `spec.imageUrl` instead, which the proplet fetches in chunks via the registry proxy (see `propeller_v1_task_with_image.yaml`).

## Development

### Building

```bash
make build
```

### Running Tests

```bash
make test          # Unit/integration tests (envtest)
make test-e2e      # End-to-end tests (Kind)
```

### Running Locally on k3d

```bash
# 1. Create cluster
k3d cluster create propeller

# 2. Install CRDs
make install

# 3. Run operator
make run ARGS="--mqtt-address='tcp://your-mqtt:1883' \
  --tenant-id='<tenant>' \
  --channel-id='<channel>' \
  --entity-id='<entity>' \
  --api-key='<api-key>'"

# 4. Test
kubectl apply -f config/samples/propeller_v1_proplet.yaml
kubectl apply -f config/samples/propeller_v1_task.yaml

# 5. Cleanup
k3d cluster delete propeller
```

## License

This project is licensed under the Apache 2.0 License - see the [LICENSE](LICENSE) file for details.
