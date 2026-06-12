# llm-d-flowcontrol
Repository to demostrate a deployment of LLM-D with flow control capabilities.

## Helm Chart

Deploys LLM-D with optional flow control capabilities on OpenShift. Creates either a standard vLLM backend or an LLM-D deployment with priority-based request queuing. When flow control is enabled, configures service accounts with different priority levels (high/low) and corresponding InferenceObjective resources to demonstrate prioritized inference request handling.

## send-inference-request.sh

Convenience script to test LLMInferenceService deployments. Takes two arguments: service account namespace and LLMInferenceService namespace. Automatically discovers the deployed service, generates a short-lived token, and sends a test chat completion request to verify the deployment is working.
