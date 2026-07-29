# Propeller K8s Operator

The Propeller K8s Operator is a Kubernetes operator designed to manage and schedule **WebAssembly (WASM) tasks** on a hybrid fleet of devices, which can be either Kubernetes pods or external devices. It introduces:

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

- **`k8s`**: Operator creates a `Deployment` running the proplet image. Tasks with container images run as K8s Jobs; tasks with WASM files are dispatched via MQTT to the running proplet.
- **`external`**: External device (IoT, Docker container, etc.) connecting via MQTT. No K8s resources created.

### 2. `Task` (canonical task API)

Smallest unit of work, representing a single task to be executed by a proplet. Tasks always dispatch via MQTT to the proplet's own Wasmtime runtime, regardless of whether the target proplet is `k8s` or `external`. The WASM module comes from either `file` (inline base64 bytes) or `imageUrl` (an OCI registry reference, fetched by the proplet through the registry proxy) — `imageUrl` is a WASM module reference, not a container image; the operator never runs it as one.

### 3. `PropellerJob` (batch)

Groups multiple tasks into a managed batch. Supports `parallel`, `sequential`, or `configurable` execution modes.

### 4. `FederatedJob` and `TrainingRound` (federated learning)

`FederatedJob` defines a multi-round FL experiment. Each round creates a `TrainingRound` that dispatches `Task`s to participants, collects results, and aggregates once k-of-n updates are received.

## Architecture

| Controller                | Owns          | Responsibility                                                                        |
| ------------------------- | ------------- | ------------------------------------------------------------------------------------- |
| `PropletReconciler`       | Proplet       | Creates/reconciles k8s Deployments; monitors external proplets via MQTT               |
| `TaskReconciler`          | Task          | Schedules WASM on selected proplet; always dispatches via MQTT                        |
| `PropellerJobReconciler`  | PropellerJob  | Creates child Tasks and aggregates outcomes                                           |
| `FederatedJobReconciler`  | FederatedJob  | Creates TrainingRound resources sequentially                                          |
| `TrainingRoundReconciler` | TrainingRound | Creates per-participant Tasks; aggregates once k-of-n complete                        |

## Prerequisites

- A Kubernetes cluster (local: [k3d](https://k3d.io/stable/))
- `kubectl`, `make`, Docker
- An MQTT broker for external proplet communication

## Quick Start

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

# 4. (another terminal) Create a proplet
kubectl apply -f config/samples/propeller_v1_proplet.yaml

# 5. Run a WASM task
kubectl apply -f config/samples/propeller_v1_task.yaml
```

## Sample Configurations

All samples are in `config/samples/`. Edit them with your MQTT credentials before applying.

### Proplet Samples

| File                                 | Type     | Description                                                |
| ------------------------------------ | -------- | ---------------------------------------------------------- |
| `propeller_v1_proplet.yaml`          | k8s      | Proplet managed as a K8s Deployment. Requires `k8s.image`. |
| `propeller_v1_proplet_external.yaml` | external | External device proplet. No K8s resources.                 |

### Task Samples

| File                                | Execution | Description                                                      |
| ----------------------------------- | --------- | ---------------------------------------------------------------- |
| `propeller_v1_task.yaml`            | MQTT      | WASM file dispatched via MQTT to target proplet.                 |
| `propeller_v1_task_with_image.yaml` | MQTT      | WASM OCI image reference dispatched via MQTT.                    |
| `propeller_v1_task_broadcast.yaml`  | Broadcast | WASM file sent to all proplets simultaneously.                   |
| `propeller_v1_task_recurring.yaml`  | MQTT      | Cron-scheduled recurring task with `isRecurring` and `schedule`. |
| `propeller_v1_task_monitoring.yaml` | MQTT      | Task with inline monitoring profile for metrics collection.      |
| `propeller_v1_task_dag_a.yaml`      | MQTT      | DAG dependency target (no dependencies).                         |
| `propeller_v1_task_dag.yaml`        | MQTT      | DAG dependent task (`dependsOn` + `runIf`).                      |

### Workflow Samples

| File                             | CRD          | Description                                     |
| -------------------------------- | ------------ | ----------------------------------------------- |
| `propeller_v1_propellerjob.yaml` | PropellerJob | Batch of parallel tasks with inline task specs. |
| `propeller_v1_federatedjob.yaml` | FederatedJob | Multi-round federated learning experiment.      |

## Test Plan

### 1. WASM File Task (MQTT to k8s Proplet)

Dispatches a base64 WASM binary via MQTT to the running k8s proplet. The proplet's Wasmtime runtime executes it.

```bash
kubectl apply -f config/samples/propeller_v1_task.yaml
# Edit propletId to target your k8s proplet
kubectl get task wasm-addition -w
# Expected: pending → running → completed
# Result: 42 (10 + 32)
```

### 2. Broadcast Task

Sends a WASM file to all proplets simultaneously via MQTT.

```bash
kubectl apply -f config/samples/propeller_v1_task_broadcast.yaml
kubectl get task broadcast-task -w
```

### 3. Recurring Cron Task

Task that runs on a cron schedule and re-queues after completion.

```bash
kubectl apply -f config/samples/propeller_v1_task_recurring.yaml
kubectl get task recurring-task -o jsonpath='{.status.nextRun}'
```

### 4. Monitoring Profile Task

Task with metrics collection enabled during execution.

```bash
kubectl apply -f config/samples/propeller_v1_task_monitoring.yaml
kubectl get task monitored-task -w
```

### 5. DAG Dependency Tasks

Tasks with dependency gates. Task B only runs after Task A completes successfully.

```bash
kubectl apply -f config/samples/propeller_v1_task_dag_a.yaml
# After dag-task-a completes:
kubectl apply -f config/samples/propeller_v1_task_dag.yaml
kubectl get task dag-task-a,dag-task-b -w
```

### 6. PropellerJob (Parallel Batch)

Creates multiple child Tasks and tracks them as a batch.

```bash
kubectl apply -f config/samples/propeller_v1_propellerjob.yaml
kubectl get pjob sample-propeller-job -w
kubectl get tasks -l jobId
```

### 7. FederatedJob (Federated Learning)

Multi-round FL experiment with k-of-n aggregation.

```bash
kubectl apply -f config/samples/propeller_v1_federatedjob.yaml
kubectl get federatedjob sample-fl-job -w
kubectl get trainingrounds
kubectl get tasks
```

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
