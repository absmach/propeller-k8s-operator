# Propeller K8s Operator

The Propeller K8s Operator is a Kubernetes operator designed to manage and schedule tasks on a fleet of devices, which can be either Kubernetes pods or external devices. It introduces two custom resources: `Proplet` and `Task`.

## Overview

The operator enables users to define a pool of worker nodes (`Proplets`) and schedule tasks (`Tasks`) to be executed on them. This is particularly useful for IoT and edge computing scenarios where tasks need to be offloaded to a distributed network of devices.

### Key Features

- **Hybrid Worker Management:** Manage both Kubernetes-native workers (as `Deployments`) and external devices (e.g., IoT devices, bare-metal servers) as a unified pool of resources.
- **Task Scheduling:** A sophisticated scheduling mechanism that allows `Tasks` to specify requirements for the `Proplets` they can run on. This includes resource requirements, device capabilities, and label-based selection.
- **MQTT Integration:** The operator communicates with external `Proplets` via the MQTT protocol, allowing for real-time status updates and task management.
- **Extensible Scheduling:** The scheduling logic is pluggable, with a round-robin scheduler provided by default.

## Custom Resources

The operator introduces two Custom Resource Definitions (CRDs):

### 1. `Proplet`

A `Proplet` represents a worker node that is available to execute `Tasks`. There are two types of `Proplets`:

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

### 2. `Task`

A `Task` represents a unit of work to be executed on a `Proplet`. It specifies the function to be executed, any required inputs, and constraints for selecting a suitable `Proplet`.

**Example `Task`:**

```yaml
apiVersion: propeller.propeller.abstractmachines.fr/v1
kind: Task
metadata:
  name: my-task
spec:
  functionName: "process-data"
  propletSelector:
    matchCapabilities:
      - sensor
  resourceRequirements:
    cpu: "500m"
    memory: "256Mi"
```

## Architecture

The operator consists of two main controllers:

- **`PropletReconciler`:** This controller is responsible for managing the lifecycle of `Proplet` resources. For `k8s` `Proplets`, it creates and manages a corresponding `Deployment`. For `external` `Proplets`, it monitors their health via MQTT liveness messages.
- **`TaskReconciler`:** This controller manages the lifecycle of `Task` resources. It finds a suitable `Proplet` based on the `Task`'s requirements, schedules the `Task` to a `Proplet`, and then monitors the `Task`'s execution.

### MQTT Communication

The operator uses MQTT to communicate with external `Proplets`. The communication is structured around a base topic, and the operator subscribes to topics for liveness and task results, and publishes messages to start tasks.

## Getting Started

To get started with the Propeller K8s Operator, you will need:

1. A Kubernetes cluster.
2. `kubectl` installed and configured to communicate with your cluster.
3. An MQTT broker accessible from your cluster.

### Installation

1. **Clone the repository:**

   ```bash
   git clone https://github.com/absmach/propeller-k8s-operator.git
   cd propeller-k8s-operator
   ```

2. **Install the CRDs:**

   ```bash
   make install
   ```

3. **Deploy the operator:**

   ```bash
   make deploy
   ```

### Creating a `Proplet` and a `Task`

1. **Create a `Proplet`:**

   Create a file named `my-proplet.yaml` with the contents similar to [example-proplet](./config/samples/propeller_v1_proplet.yaml)

   Apply the manifest:

   ```bash
   kubectl apply -f my-proplet.yaml
   ```

2. **Create a `Task`:**

   Create a file named `my-task.yaml` with the contents similar to [example-task](./config/samples/propeller_v1_task.yaml)

   Apply the manifest:

   ```bash
   kubectl apply -f my-task.yaml
   ```

The operator will now schedule the `Task` to the `Proplet`.

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
