#!/bin/bash

# Check arguments
if [[ -z $1 ]] || [[ -z $2 ]]; then
	echo "Usage: $0 <serviceaccount-namespace> <llminferenceservice-namespace>"
	exit 1
fi

SA_NS=$1
LLMIS_NS=$2

# Check if oc is logged in
if ! oc whoami &>/dev/null; then
	echo "Error: Not logged into OpenShift. Please run 'oc login' first."
	exit 1
fi

# Check number of LLMInferenceServices in the namespace
LLMIS_COUNT=$(oc get LLMinferenceservice -n ${LLMIS_NS} --no-headers 2>/dev/null | wc -l)
if [[ $LLMIS_COUNT -eq 0 ]]; then
	echo "Error: No LLMInferenceService found in namespace '${LLMIS_NS}'"
	exit 1
elif [[ $LLMIS_COUNT -gt 1 ]]; then
	echo "Error: Found ${LLMIS_COUNT} LLMInferenceServices in namespace '${LLMIS_NS}', expected exactly 1"
	exit 1
fi

# Get the LLMInferenceService name
LLMIS_NAME=$(oc get LLMinferenceservice -n ${LLMIS_NS} --no-headers -o custom-columns=":metadata.name")

# Collect info
URL=$(oc get LLMinferenceservice/${LLMIS_NAME} -n ${LLMIS_NS} -ojsonpath='{ .status.addresses[0].url }')
TOKEN=$(oc create token -n ${SA_NS} llm-inferencer --duration=10m)
MODEL_NAME=$(oc get LLMinferenceservice/${LLMIS_NAME} -n ${LLMIS_NS} -ojsonpath='{ .spec.model.name }')

# Verify we got the required values
if [[ -z $URL ]]; then
	echo "Error: Failed to retrieve URL from LLMInferenceService '${LLMIS_NAME}'"
	exit 1
fi

if [[ -z $TOKEN ]]; then
	echo "Error: Failed to create token in namespace '${SA_NS}'"
	exit 1
fi

if [[ -z $MODEL_NAME ]]; then
	echo "Error: Failed to retrieve model name from LLMInferenceService '${LLMIS_NAME}'"
	exit 1
fi

echo "Using LLMInferenceService: ${LLMIS_NAME}"
echo "Model: ${MODEL_NAME}"
echo "URL: ${URL}"
echo ""

curl -X POST "${URL}/v1/chat/completions" \
	-H "Content-Type: application/json" \
	-H "Authorization: Bearer ${TOKEN}" \
	--data "{
		\"model\": \"${MODEL_NAME}\",
		\"messages\": [
			{
				\"role\": \"user\",
				\"content\": \"/no_think Give me a 1 sentence summary about sardines.\"
			}
		]
	}" | jq .
