# LLM-D Flow Control Helm Chart

Helm chart for deploying LLM inference services with optional flow control capabilities using KServe and OpenShift AI.

## Overview

This chart deploys LLM inference services with support for two deployment methods:
- **vllm**: Standard vLLM-based inference service
- **llmd**: Enhanced deployment with optional flow control and priority-based scheduling

## Prerequisites

The helm chart was developed in the following environment.

- Single Node OpenShift 4.20.0
- Red Hat Connectivity Link 1.3.4
- Red Hat OpenShift AI 3.4.0

The SNO instance was on an AWS g6.12xlarge instance, with 4x NVIDIA L4s available. 

## Installation

```bash
helm install <release-name> .
```

### Install with custom values

```bash
helm install <release-name> . -f custom-values.yaml
```

## Configuration

### Basic Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `deploymentMethod` | Deployment method: `vllm` or `llmd` | `llmd` |
| `modelDeployment.modelName` | Name of the model to deploy | `qwen` |
| `modelDeployment.modelUri` | Model URI (e.g., HuggingFace model) | `hf://Qwen/Qwen3.5-4b` |
| `modelDeployment.replicas` | Number of model replicas | `1` |

### vLLM Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `vllm.namespace` | Namespace for vLLM deployment | `demo-vllm` |

### LLM-D Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `llmd.namespace` | Namespace for LLM-D deployment | `demo-llmd` |
| `llmd.flowControl` | Enable flow control features | `true` |

### Service Account Configuration (Flow Control)

When `llmd.flowControl` is enabled, you can configure multiple service accounts with different priority levels:

| Parameter | Description |
|-----------|-------------|
| `serviceAccounts[].name` | Service account name |
| `serviceAccounts[].namespace` | Service account namespace |
| `serviceAccounts[].priority` | Priority level (higher = more priority) |

**Default service accounts:**
```yaml
serviceAccounts:
  - name: llm-inferencer
    namespace: sa-high-prio
    priority: 100
  - name: llm-inferencer
    namespace: sa-low-prio
    priority: 10
```

## Deployment Methods

### 1. vLLM Deployment

Standard vLLM-based deployment without flow control.

```yaml
deploymentMethod: vllm
modelDeployment:
  modelName: my-model
  modelUri: hf://path/to/model
  replicas: 4
vllm:
  namespace: my-vllm-namespace
```

**Features:**
- Simple LLM inference service
- No authentication required
- Gateway and route configuration included
- GPU resource allocation

### 2. LLM-D Deployment with Flow Control

Enhanced deployment with priority-based request scheduling and flow control.

```yaml
deploymentMethod: llmd
modelDeployment:
  modelName: my-model
  modelUri: hf://path/to/model
  replicas: 4
llmd:
  namespace: my-llmd-namespace
  flowControl: true
```

**Features:**
- Priority-based request scheduling.
- Multiple priority bands with fairness policies.
- Authentication enabled (when flowControl is `true`)
- Advanced endpoint picker with multiple scoring plugins

## Resources Created

### Common Resources
- `LLMInferenceService`: Main inference service (vllm or llmd)
- `Gateway`: OpenShift AI inference gateway
- `GatewayClass`: Gateway class configuration
- `Namespace`: Model deployment namespace

### Flow Control Resources (LLM-D only)
When `llmd.flowControl` is enabled:
- `InferenceObjective`: Per-service-account objectives with priority settings
- `ServiceAccount`: Service accounts for different priority levels
- `Role`: RBAC role for inference access
- `ClusterRoleBinding`: Cluster-level role binding
- Service account namespaces

## Resource Requirements

Each model replica requests:
- **CPU:** 4 cores
- **Memory:** 8 GB
- **GPU:** 1 NVIDIA GPU

Ensure your cluster has sufficient GPU resources for the requested number of replicas.

This helm chart was developed on a SNO cluster w/4x NVIDIA-L4 GPUs.

## Examples

### Example 1: Simple vLLM Deployment

```yaml
deploymentMethod: vllm
modelDeployment:
  modelName: llama2
  modelUri: hf://meta-llama/Llama-2-7b-hf
  replicas: 2
vllm:
  namespace: llm-production
```

### Example 2: LLM-D with Flow Control

```yaml
deploymentMethod: llmd
modelDeployment:
  modelName: qwen-large
  modelUri: hf://Qwen/Qwen3-7B
  replicas: 4
llmd:
  namespace: llm-inference
  flowControl: true
serviceAccounts:
  - name: llm-inferencer
    namespace: critical-workloads
    priority: 200
  - name: llm-inferencer
    namespace: standard-workloads
    priority: 100
  - name: llm-inferencer
    namespace: batch-jobs
    priority: 50
```

### Example 3: LLM-D without Flow Control

```yaml
deploymentMethod: llmd
modelDeployment:
  modelName: small-model
  modelUri: hf://Qwen/Qwen3-0.6b
  replicas: 2
llmd:
  namespace: llm-dev
  flowControl: false
```

## Accessing the Service

The inference service is exposed through:
- **Gateway:** `openshift-ai-inference` in the `openshift-ingress` namespace
- **Route:** Automatically created by the LLMInferenceService

Access the model via the generated route endpoint.

## Uninstallation

```bash
helm uninstall <release-name>
```

Note: Depending on your configuration, you may need to manually clean up namespaces and other cluster-scoped resources.

## Troubleshooting

### Pods not scheduling
- Verify GPU nodes are available and properly labeled
- Check GPU tolerations match your node taints
- Ensure sufficient GPU resources are available

### Flow control not working
- Verify `llmd.flowControl` is set to `true`
- Check that service accounts and InferenceObjectives are created
- Review RHCL resources, such as the Kuadrant.

### Model fails to load
- Check model URI is correct and accessible
- Verify sufficient memory for the model
- Review pod logs for tokenizer cache issues

## Version

Chart version: 0.1.0
