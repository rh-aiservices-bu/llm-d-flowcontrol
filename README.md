# llm-d-flowcontrol
Repository to demonstrate a deployment of LLM-D with flow control capabilities.

Flow Control within llm-d, is a feature that enables intelligent request queuing. Each request is tagged with 2 headers to identify it's source (or tenant)  and it's priority. The EPP will leverage these headers to create a tuple called a `flowKey`, which is assigned a priority band. Each request is assigned a `flowKey`, and each `flowKey` represented a separate in-memory queue in the EPP. When it comes to scheduling, the EPP will traverse the queues based on 3 tiers:

- Priority - i.e. the flowKey(s) with the highest priority band is chosen first.
- Fairness - i.e. within the highest priorityBand, which flowKey is dispatched next.
- Ordering - i.e. within the flowKey itself, which request should be served.

Further reading on the topic can be found in the llm-d github repository.

## Repository Components

### `helm-chart`

Deploys LLM-D with optional flow control capabilities on OpenShift. Creates either a standard vLLM backend or an LLM-D deployment with priority-based request queuing. When flow control is enabled, configures service accounts with different priority levels (high/low) and corresponding InferenceObjective resources to demonstrate prioritized inference request handling. See [helm-chart/README.md](helm-chart/README.md) for detailed documentation.

### `flow-control-testing`

Demonstrates flow control effectiveness by saturating a model with concurrent requests split between high-priority and low-priority workers. See [flow-control-testing/README.md](flow-control-testing/README.md) for detailed documentation.

### `send-inference-request.sh`

Convenience script to test LLMInferenceService deployments. Takes two arguments: service account namespace and LLMInferenceService namespace. Automatically discovers the deployed service, generates a short-lived token, and sends a test chat completion request to verify the deployment is working. If flow control is enabled on the deployment, you can ensure it is working in the `*router-scheduler*` pod in the namespace the model is deployed in, and looking for a flowPriority value other than '0'. 

i.e.,

```
$ oc get pods -n demo-llmd 

NAME                                            READY   STATUS                     RESTARTS   AGE
qwen-kserve-79c7cb4bf-cwjgc                     1/1     Running                    0          144m
qwen-kserve-router-scheduler-55fbfff56b-28hr2   2/2     Running                    2          21h

$ oc logs -n demo-llmd qwen-kserve-router-scheduler-55fbfff56b-28hr2 | grep flowPriority | tail -n 1

{"level":"Level(-4)","ts":"2026-06-24T11:05:03Z","logger":"flow-controller.sweepFinalizedItems","caller":"internal/processor.go:435","msg":"Swept finalized items and released capacity.","shardID":"shard-0000","flowKey":"https://kubernetes.default.svc:100","flowID":"https://kubernetes.default.svc","flowPriority":100,"count":0}
```

## Quickstart

A brief deployment guide to quickly demonstrate the flow control capabilities of llm-d in RHOAI 3.4. This assumes you don't want to stray from the default configuration.

### Prerequisites

- `>=4.20.0` OpenShift Cluster with at least 1 GPU available.
- `oc` CLI authenticated to the cluster, with `cluster-admin` access.
- The following operators installed and configured:
    - Red Hat OpenShift AI `>=3.4.0`
    - Red Hat Connectivity Link `>=1.3.3`

### Step 1: Deploy model

This is just a case of running the helm-chart with the default values.

```bash
cd helm-chart
helm install llmd-fc .
```

This will start running an instance of the Qwen3.5-4B in the `demo-llmd` namespace. It will also create a couple of projects, service accounts and clusterrolebindings, so we can send requests from different namespaces.

### Step 2: Run Test

The test will, by default, send requests to the `llmInferenceService` object in the `demo-llmd` namespace. Assuming the Gateway is setup, and the helm chart installed correctly, the test will pick up the model endpoint, generate 1 token per service account, and start sending (concurrent) requests. 

```bash
cd flow-control-testing
pip install aiohttp pyyaml
python3 flow_control_test.py
```

The script will create a directory called "trace_results", in which, information about each of the runs will be stored. You can retrieve the prompts that were used in the requests, the requests and outputs themselves, and the summary information of the run. This summary information will also be presented at the end, looking similiar to [this](https://github.com/rh-aiservices-bu/llm-d-flowcontrol/tree/main/flow-control-testing#example-output).

### Clean up:

- The test does not create any artifacts on the cluster itself. If you want to clean up your run results, you can just remove the `trace_results` folder.

- To clear up the model deployment, you just need to uninstall the helm chart:

```bash
helm uninstall llmd-fc
```

### Troubleshooting

- Each component has it's own troubleshooting guide in their respective repositories.

