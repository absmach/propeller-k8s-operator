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
- **Flexible Scheduling:** Use explicit `propletId` to target specific proplets.
- **MQTT Integration (adapter):** The operator communicates with external `Proplets` via MQTT `manager/start` and `results` messages, but exposes a Kubernetes-native `Task` CRD to users.
- **Federated Learning Orchestration:** `FederatedJob` and `TrainingRound` manage FL rounds and k-of-n aggregation by setting env vars such as `ROUND_ID`, `MODEL_URI`, and `HYPERPARAMS`; FL logic remains inside the proplet runtime and FL libraries.

## Custom Resources

The operator introduces several Custom Resource Definitions (CRDs):

### 1. `Proplet` (fleet primitive)

`Proplet` represents a worker node that is available to execute `Task`s. There are two types of `Proplets`:

- **`k8s`:** A `Proplet` that is managed by the operator and runs as a `Deployment` within the Kubernetes cluster.
- **`external`:** A `Proplet` that represents an external device. These devices are expected to communicate with the operator via MQTT.

**Example `Proplet` (k8s):**

```yaml
apiVersion: propeller.propeller.abstractmachines.fr/v1
kind: Proplet
metadata:
  name: my-k8s-proplet
spec:
  type: k8s
  k8s:
    image: my-worker-image:latest
    logLevel: info
    replicas: 1
  connectionConfig:
    # ... MQTT connection details
```

**Example `Proplet` (external):**

```yaml
apiVersion: propeller.propeller.abstractmachines.fr/v1
kind: Proplet
metadata:
  name: my-external-proplet
spec:
  type: external
  external:
    deviceType: raspberry-pi
    capabilities:
      - gpio
      - sensor
  connectionConfig:
    # ... MQTT connection details
```

### 2. `Task` (canonical task API)

`Task` (`propeller.propeller.abstractmachines.fr/v1`) represents a WASM workload to be executed on a `Proplet`.

Key fields include:

- `imageUrl`: OCI reference to the WASM image.
- `file`: base64-encoded WASM binary (if provided, takes precedence over `imageUrl`).
- `cliArgs`: command-line arguments.
- `inputs`: numeric inputs (mapped to runtime `args`).
- `env`: environment variables (used for FL activation via `ROUND_ID`, `MODEL_URI`, `HYPERPARAMS`, etc.).
- `daemon`: whether the task is long-running.
- `mode`: execution mode (e.g. `infer`, `train`).
- `propletId`: specific proplet to run the task on.

**Example `Task`:**

```yaml
apiVersion: propeller.propeller.abstractmachines.fr/v1
kind: Task
metadata:
  name: my-task
spec:
  name: my-task
  functionName: my-function
  imageUrl: oci://registry/my-wasm-image:latest
  cliArgs:
    - "--flag"
  inputs:
    - 1
    - 2
  env:
    FOO: bar
  mode: infer
  daemon: false
  propletSelector:
  propletId: my-k8s-proplet
```

When the selected `Proplet` is:

- **k8s-backed**: the operator creates a `Job` + `ConfigMap` and runs the image inside the cluster; results are extracted into `status.results`.
- **external**: the operator publishes an MQTT `manager/start` message derived from the `Task.spec`, and updates `status` from MQTT `results` messages.

### 3. `FederatedJob` and `TrainingRound` (federated learning)

`FederatedJob` (`propeller.propeller.abstractmachines.fr/v1alpha1`) defines a federated learning experiment:

- `experimentId`, `modelRef`, and `taskWasmImage`.
- Participants (proplet IDs).
- `kOfN` and `timeoutSeconds`.
- Aggregation algorithm/config via `aggregator`.

`TrainingRound` (`propeller.propeller.abstractmachines.fr/v1alpha1`) represents a single FL round:

- Creates one `Task` per participant with FL env vars:
  - `ROUND_ID`, `MODEL_URI`, `HYPERPARAMS`, `PROPLET_ID`, and, when available, aggregated global update env vars.
- Tracks per-participant status and updates received.
- Aggregates updates once `kOfN` is met and records an `AggregatedModelRef` for the next round.

The FL logic itself (training, update computation, and FL-specific behavior) lives in the WASM client and proplet runtime; the operator only orchestrates CRDs and env vars.

## Architecture

The operator consists of several controllers:

- **`PropletReconciler`**:
  - Manages the lifecycle of `Proplet` resources.
  - For `k8s` `Proplets`, creates and reconciles a `Deployment`.
  - For `external` `Proplets`, monitors health via MQTT liveness and updates status.

- **`TaskReconciler`**:
  - Canonical task controller that:
    - Validates the spec and resolves the target proplet (direct `propletId`).
    - For k8s-backed proplets: creates `Job` + `ConfigMap`, tracks Job completion/failure, and extracts results into `status.results`.
    - For external proplets: publishes MQTT `manager/start` messages derived from `Task.spec` and expects results via MQTT `results` which update `status`.

- **`FederatedJobReconciler`**:
  - Validates `FederatedJob` specs.
  - Creates and sequences `TrainingRound` CRs across multiple rounds.
  - Tracks job phase, current round, and `aggregatedModelRef`.

- **`TrainingRoundReconciler`**:
  - For each round, creates participant `Task`s targeting each proplet with appropriate FL env vars.
  - Tracks participant task completion, counts updates, enforces `kOfN` and `timeoutSeconds`.
  - Aggregates collected FL updates (via FL packages) and stores an aggregated model reference and update envelope for subsequent rounds or finalization.

### MQTT Communication

The operator uses MQTT to communicate with external `Proplets`. The communication is structured around a base topic, and the operator:

- Subscribes to liveness topics to maintain `Proplet` status.
- Subscribes to `results` topics and maps results back to `Task.status`.
- Publishes `manager/start` messages for external `Task` executions, using fields derived solely from the `Task` spec (no hidden runtime hacks).

## Prerequisites

To test the operator end-to-end you need:

- A Kubernetes cluster and a kubeconfig with access to it.
- `kubectl` configured to talk to that cluster.
- `make` and a container runtime (for example Docker or containerd).
- Access to a container registry where you can push images (for operator and WASM workloads).
- An MQTT broker that your cluster can reach, if you want to test external `Proplet`s.

For installation and basic usage of these tools, use the official upstream documentation or any project-specific docs you already have in your environment.

## Choose your setup values

Before running anything, pick your own names and connection details. You can export them once and reuse them in all commands:

```bash
export KUBE_CONTEXT="<your-kube-context-name>"
export OPERATOR_NAMESPACE="<your-operator-namespace>"
export WORKLOAD_NAMESPACE="<your-workload-namespace>"           # where Proplets and Tasks live

export OPERATOR_IMAGE="<your-registry>/propeller-operator:<tag>"
export PROPLET_IMAGE="<your-registry>/propeller-proplet:<tag>"
export WASM_IMAGE="<your-registry>/your-wasm-client:<tag>"

export MQTT_BROKER_ADDRESS="tcp://<your-mqtt-host>:<port>"
export MQTT_DOMAIN_ID="<your-propeller-domain-id>"
export MQTT_CHANNEL_ID="<your-propeller-channel-id>"
export MQTT_CLIENT_ID="<your-propeller-client-id>"
export MQTT_CLIENT_KEY="<your-propeller-client-key>"
```

- **`KUBE_CONTEXT`**: the Kubernetes context you want to use for testing.
- **`OPERATOR_NAMESPACE`**: namespace where the operator manager will run.
- **`WORKLOAD_NAMESPACE`**: namespace for `Proplet` and `Task` resources.
- **`OPERATOR_IMAGE`**: container image for the operator manager.
- **`PROPLET_IMAGE`**: container image for k8s-backed proplets.
- **`WASM_IMAGE`**: container image (or WASM runner) used by `Task` Jobs.
- **`MQTT_*` values**: connection information for your MQTT/Propeller backend, used by external proplets and by the operator when talking to external devices.

Always replace the placeholder values with your own. Do not reuse example names from this README in production.

## Install CRDs

1. Point `kubectl` at your chosen cluster:

   ```bash
   kubectl config use-context "${KUBE_CONTEXT}"
   ```

2. Install or update the CRDs into that cluster:

   ```bash
   make install
   ```

   This applies the CRDs defined under `config/crd` into the cluster referenced by your current context.

3. Create the namespaces you plan to use:

   ```bash
   kubectl create namespace "${OPERATOR_NAMESPACE}"
   kubectl create namespace "${WORKLOAD_NAMESPACE}"
   ```

   If the namespaces already exist, `kubectl` will report that; you can keep using them.

## Deploy the operator

You can deploy the operator into the cluster using the Kustomize manifests in `config/default`. Make sure the namespace in those manifests matches `OPERATOR_NAMESPACE`. If it does not, update the `namespace` field in `config/default/kustomization.yaml` and any related manifests before applying.

Build and deploy:

```bash
kubectl config use-context "${KUBE_CONTEXT}"

cd path/to/propeller-k8s-operator
make docker-build IMG="${OPERATOR_IMAGE}"

cd config/manager
kustomize edit set image controller="${OPERATOR_IMAGE}"
cd -

kustomize build config/default | kubectl apply -f -
```

Alternatively, you can run the operator from your workstation against the cluster (helpful for local development):

```bash
kubectl config use-context "${KUBE_CONTEXT}"
cd path/to/propeller-k8s-operator

make run -- \
  --mqtt-address="${MQTT_BROKER_ADDRESS}" \
  --domain-id="${MQTT_DOMAIN_ID}" \
  --channel-id="${MQTT_CHANNEL_ID}" \
  --client-id="${MQTT_CLIENT_ID}" \
  --client-key="${MQTT_CLIENT_KEY}"
```

When running locally, the operator connects to the cluster referenced by your current kubeconfig context.

## Test plan

This section walks through a minimal but complete test flow for the operator.

### A. Confirm the operator is running

1. Check the deployment and pods in your operator namespace:

   ```bash
   kubectl get deployments -n "${OPERATOR_NAMESPACE}"
   kubectl get pods -n "${OPERATOR_NAMESPACE}"
   ```

2. Watch the operator logs (replace the pod name with your own):

   ```bash
   kubectl logs -n "${OPERATOR_NAMESPACE}" "<your-operator-pod-name>"
   ```

The operator is ready when the manager pod is in `Running` state and the logs show controllers starting without errors.

### B. Create Proplets

You can use either k8s-backed proplets (managed as Deployments) or external proplets (MQTT-connected devices).

#### B.1. Kubernetes-backed Proplet

1. Copy the sample into your own file:

   ```bash
   cp config/samples/propeller_v1_proplet.yaml my-k8s-proplet.yaml
   ```

2. Edit `my-k8s-proplet.yaml`:

   - Set `metadata.name` to your chosen proplet name, for example `<your-k8s-proplet-name>`.
   - Add `metadata.namespace` and set it to `WORKLOAD_NAMESPACE`.
   - Under `spec.k8s.image`, set `PROPLET_IMAGE`.
   - Under `spec.connectionConfig`, set:
     - `mqttAddress` to `MQTT_BROKER_ADDRESS`.
     - `domainId`, `channelId`, `clientId`, `clientKey` to your `MQTT_*` values.
   - Adjust `spec.resources` as needed for your cluster.

3. Apply the manifest:

   ```bash
   kubectl apply -n "${WORKLOAD_NAMESPACE}" -f my-k8s-proplet.yaml
   ```

4. Confirm the proplet and its backing Deployment:

   ```bash
   kubectl get proplets -n "${WORKLOAD_NAMESPACE}"
   kubectl describe proplet -n "${WORKLOAD_NAMESPACE}" "<your-k8s-proplet-name>"

   kubectl get deployments -n "${WORKLOAD_NAMESPACE}"
   kubectl get pods -n "${WORKLOAD_NAMESPACE}"
   ```

#### B.2. External Proplet (MQTT device)

1. Copy the external proplet sample:

   ```bash
   cp config/samples/propeller_v1_proplet_external.yaml my-external-proplet.yaml
   ```

2. Edit `my-external-proplet.yaml`:

   - Set `metadata.name` and `metadata.namespace` (use `WORKLOAD_NAMESPACE` or another namespace you created).
   - Under `spec.external`, set `deviceType` and any `capabilities` that describe your device.
   - Under `spec.connectionConfig`, set all `MQTT_*` values as in the k8s-backed case.

3. Apply and check status:

   ```bash
   kubectl apply -n "${WORKLOAD_NAMESPACE}" -f my-external-proplet.yaml
   kubectl get proplets -n "${WORKLOAD_NAMESPACE}"
   kubectl describe proplet -n "${WORKLOAD_NAMESPACE}" "<your-external-proplet-name>"
   ```

The external proplet will only reach a healthy state once the MQTT-connected device is actually online and talking to the broker.

### C. Run a simple Task (k8s-backed execution)

The goal here is to run a small workload on a k8s-backed proplet and observe how the operator creates a Job and tracks its status.

1. Create a Task manifest, for example `my-task.yaml`:

   ```yaml
   apiVersion: propeller.propeller.abstractmachines.fr/v1
   kind: Task
   metadata:
     name: <your-task-name>
     namespace: <your-workload-namespace>
   spec:
     name: <your-task-name>
     functionName: my-function
     imageUrl: "<your-wasm-image>"
     propletSelector:
     propletId: "<your-k8s-proplet-name>"
     mode: "infer"
     daemon: false
     env:
       EXAMPLE_ENV: "example-value"
     # cliArgs:
     #   - "--flag"
     #   - "value"
   ```

   Replace all placeholders with your own values and keep the namespace aligned with `WORKLOAD_NAMESPACE`.

2. Apply the task and watch it:

   ```bash
   kubectl apply -n "${WORKLOAD_NAMESPACE}" -f my-task.yaml

   kubectl get tasks -n "${WORKLOAD_NAMESPACE}"
   kubectl describe task -n "${WORKLOAD_NAMESPACE}" "<your-task-name>"
   ```

3. Inspect the Job and pod created by the operator:

   ```bash
   kubectl get jobs -n "${WORKLOAD_NAMESPACE}"
   kubectl describe job -n "${WORKLOAD_NAMESPACE}" "<your-task-name>-job"

   kubectl get pods -n "${WORKLOAD_NAMESPACE}" -l "job-name=<your-task-name>-job"
   kubectl logs -n "${WORKLOAD_NAMESPACE}" "<your-job-pod-name>"
   ```

You should see the `Task` move through phases such as `Pending`, `Running`, and (if your workload completes successfully) `Completed`. The Job and its pod will reflect the container image and arguments you configured in the `Task` spec.

## Development

To contribute to the development of the Propeller K8s Operator, you will need:

- Go (version 1.24 or higher)
- Docker
- `make`

### Building from Source

```bash
make build
```

### Running Tests

```bash
make test
```

## License

This project is licensed under the Apache 2.0 License - see the [LICENSE](LICENSE) file for details.
