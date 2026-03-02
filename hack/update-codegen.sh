#!/usr/bin/env bash
set -o errexit
set -o nounset
set -o pipefail

SCRIPT_ROOT=$(dirname "${BASH_SOURCE[0]}")/..
MODULE_NAME="go.miloapis.net/search"

# Find code-generator
CODEGEN_PKG=$(go list -m -f '{{.Dir}}' k8s.io/code-generator 2>/dev/null)

if [ -z "${CODEGEN_PKG}" ]; then
    echo "ERROR: k8s.io/code-generator not found in go.mod"
    echo "Run: go get k8s.io/code-generator"
    exit 1
fi

echo "Using code-generator from: ${CODEGEN_PKG}"

# Source the code generation helper
source "${CODEGEN_PKG}/kube_codegen.sh"

# Generate deepcopy
echo "Generating deepcopy..."
kube::codegen::gen_helpers \
  --boilerplate "${SCRIPT_ROOT}/hack/boilerplate.go.txt" \
  "${SCRIPT_ROOT}/pkg/apis"

# Generate OpenAPI definitions
echo "Generating OpenAPI definitions..."
go run k8s.io/kube-openapi/cmd/openapi-gen \
  --go-header-file "${SCRIPT_ROOT}/hack/boilerplate.go.txt" \
  --output-dir "${SCRIPT_ROOT}/pkg/generated/openapi" \
  --output-pkg "${MODULE_NAME}/pkg/generated/openapi" \
  --output-file zz_generated.openapi.go \
  --report-filename /dev/null \
  "${MODULE_NAME}/pkg/apis/search/v1alpha1" \
  "k8s.io/apimachinery/pkg/apis/meta/v1" \
  "k8s.io/apimachinery/pkg/api/resource" \
  "k8s.io/apimachinery/pkg/runtime" \
  "k8s.io/apimachinery/pkg/version"

echo ""
echo "Code generation complete!"
echo ""
echo "Generated:"
echo "  - Deepcopy functions: pkg/apis/search/v1alpha1/zz_generated.deepcopy.go"
echo "  - OpenAPI: pkg/generated/openapi/"
