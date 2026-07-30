#!/usr/bin/env bash
# Applies a Task CR built from a compiled WASM example in the sibling
# propeller repo's build/ directory (see `make <example>` there), so
# examples beyond the checked-in addition sample can be exercised against
# this operator without hand-encoding base64 into a YAML file.
#
# Usage:
#   hack/apply-example-task.sh <example> <function> [input ...]
#
# Examples:
#   hack/apply-example-task.sh addition add 10 32
#   hack/apply-example-task.sh compute compute 5
#   hack/apply-example-task.sh hello-world main
#
# Env overrides:
#   PROPELLER_DIR   path to the propeller repo (default: ../propeller)
#   PROPLET_ID      propletSelector.propletId to target (default: k8s-proplet)
#   NAMESPACE       kubectl namespace (default: default)
#   DAEMON          "true" to set spec.daemon (default: false)
set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "usage: $0 <example> <function> [input ...]" >&2
  echo "  built wasm files live in \${PROPELLER_DIR:-../propeller}/build/<example>.wasm" >&2
  exit 1
fi

example="$1"
function_name="$2"
shift 2

propeller_dir="${PROPELLER_DIR:-../propeller}"
proplet_id="${PROPLET_ID:-k8s-proplet}"
namespace="${NAMESPACE:-default}"
daemon="${DAEMON:-false}"

wasm_file="${propeller_dir}/build/${example}.wasm"
if [[ ! -f "$wasm_file" ]]; then
  echo "wasm not found: $wasm_file" >&2
  echo "build it first, e.g.: (cd \"$propeller_dir\" && make ${example})" >&2
  exit 1
fi

wasm_b64=$(base64 -w0 "$wasm_file")
task_name="${example}-example"

inputs_yaml=""
for v in "$@"; do
  inputs_yaml+=$'\n    - "'"${v//\"/\\\"}"'"'
done

manifest=$(cat <<EOF
apiVersion: propeller.propeller.absmach.eu/v1
kind: Task
metadata:
  name: ${task_name}
  labels:
    app.kubernetes.io/name: propeller-k8s-operator
    app.kubernetes.io/managed-by: hack-apply-example-task
spec:
  name: ${task_name}
  functionName: ${function_name}
  kind: standard
  file: "${wasm_b64}"
  daemon: ${daemon}
  propletSelector:
    propletId: "${proplet_id}"
EOF
)
if [[ -n "$inputs_yaml" ]]; then
  manifest="${manifest}
  inputs:${inputs_yaml}"
fi

echo "$manifest" | kubectl apply -n "$namespace" -f -
echo "Applied Task/${task_name}. Watch with: kubectl get task ${task_name} -n ${namespace} -w"
