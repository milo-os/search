#!/bin/sh
# Runs a ResourceSearchQuery in a user's own Milo context and checks the exact
# set of result names.
#
# Usage: sh ./query-as.sh <job-name> <user> <kind>[,<kind>...] [expected-name ...]
#
# The query targets the given kinds of e2e-user-ctx.miloapis.com/v1alpha1 with
# an empty query string, so every document the tenant filter lets through is
# returned. kubectl can't set user extras, so the request is sent from a Job
# whose ServiceAccount impersonates <user> with the extras Milo's
# UserContextAuthorizationDecorator sets in a user context:
#   iam.miloapis.com/parent-type: User
#   iam.miloapis.com/parent-name: <user>
# The script fails unless the response is HTTP 200 or 201 (the apiserver
# answers a create with 201 Created), has no denied targets, and its result
# names are exactly the expected names (same count, same members). A Job that
# fails or doesn't finish, or a status other than 200 or 201, is retried up to
# five times; a wrong result set or denied targets never are.
set -eu

JOB=$1
AS_USER=$2
KINDS=$3
shift 3
NS=search-system

TARGETS=$(printf '%s' "$KINDS" | jq -Rc \
  'split(",") | map({group: "e2e-user-ctx.miloapis.com", version: "v1alpha1", kind: .})')
QUERY_BODY=$(jq -nc --argjson targets "$TARGETS" '{
  apiVersion: "search.miloapis.com/v1alpha1",
  kind: "ResourceSearchQuery",
  metadata: {generateName: "e2e-uc-"},
  spec: {targetResources: $targets, query: "", limit: 50}
}')

# Runs inside the Job container; $TOKEN, $AS_USER and $QUERY_BODY expand there.
REQUEST=$(cat <<'EOF'
set -e
TOKEN=$(cat /var/run/secrets/kubernetes.io/serviceaccount/token)
curl -sS -X POST https://kubernetes.default.svc/apis/search.miloapis.com/v1alpha1/resourcesearchqueries \
  --cacert /var/run/secrets/kubernetes.io/serviceaccount/ca.crt \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -H "Accept: application/json" \
  -H "Impersonate-User: $AS_USER" \
  -H "Impersonate-Extra-iam.miloapis.com%2Fparent-type: User" \
  -H "Impersonate-Extra-iam.miloapis.com%2Fparent-name: $AS_USER" \
  -d "$QUERY_BODY" \
  -w "\nHTTP_CODE:%{http_code}\n"
EOF
)

# Runs the query once. Returns 0 with $RESPONSE set when the Job completes and
# the apiserver answers HTTP 200 or 201; returns 1 otherwise (Job failed or
# didn't finish in time, or another HTTP status), so the caller can retry.
run_query() {
  kubectl delete job "$JOB" -n "$NS" --ignore-not-found=true --wait=true >/dev/null
  jq -n --arg job "$JOB" --arg ns "$NS" --arg user "$AS_USER" --arg body "$QUERY_BODY" --arg request "$REQUEST" '{
    apiVersion: "batch/v1",
    kind: "Job",
    metadata: {name: $job, namespace: $ns, labels: {"e2e.search.test/suite": "user-context"}},
    spec: {
      backoffLimit: 0,
      ttlSecondsAfterFinished: 300,
      template: {
        spec: {
          restartPolicy: "Never",
          serviceAccountName: "search-user-ctx-tester",
          containers: [{
            name: "tester",
            image: "curlimages/curl:latest",
            env: [{name: "AS_USER", value: $user}, {name: "QUERY_BODY", value: $body}],
            command: ["/bin/sh", "-c", $request]
          }]
        }
      }
    }
  }' | kubectl apply -f - >/dev/null

  # Poll for up to 45s rather than `kubectl wait`, so a failed Job ends the
  # wait at once. Five attempts then fit in the 300s step timeout.
  STATE=""
  DEADLINE=$(($(date +%s) + 45))
  while [ "$(date +%s)" -lt "$DEADLINE" ]; do
    STATE=$(kubectl get job "$JOB" -n "$NS" -o jsonpath='{.status.succeeded}/{.status.failed}' 2>/dev/null || true)
    case "$STATE" in
      1/*) break ;;
      */[1-9]*) break ;;
    esac
    sleep 1
  done
  case "$STATE" in
    1/*) ;;
    *)
      echo "--- Job $JOB did not complete (succeeded/failed: $STATE); diagnostics ---"
      kubectl get job "$JOB" -n "$NS" -o yaml || true
      kubectl logs -n "$NS" "job/$JOB" --all-containers=true --tail=200 || true
      return 1
      ;;
  esac

  LOG=$(kubectl logs -n "$NS" "job/$JOB" --all-containers=true --tail=-1)
  echo "$LOG"
  RESPONSE=$(printf '%s\n' "$LOG" | sed '/^HTTP_CODE:/,$d')
  HTTP_CODE=$(printf '%s\n' "$LOG" | sed -n 's/^HTTP_CODE://p')
  case "$HTTP_CODE" in
    200|201) return 0 ;;
  esac
  echo "--- expected HTTP 200 or 201 for $AS_USER querying $KINDS, got $HTTP_CODE ---"
  return 1
}

# Retry only transport-level failures. A wrong result set or denied targets
# are never retried: the indexes are already complete (T0 checked), so a
# mismatch is a real leak or a real miss.
ATTEMPTS=5
RESPONSE=""
attempt=1
while ! run_query; do
  if [ "$attempt" -ge "$ATTEMPTS" ]; then
    echo "FAIL: no HTTP 200 or 201 response for $AS_USER querying $KINDS after $ATTEMPTS attempts"
    exit 1
  fi
  attempt=$((attempt + 1))
  echo "Retrying ($attempt/$ATTEMPTS) in 3s..."
  sleep 3
done

DENIED=$(printf '%s' "$RESPONSE" | jq -c '.status.deniedTargetResources // []')
if [ "$DENIED" != "[]" ]; then
  echo "FAIL: expected no denied targets for $AS_USER querying $KINDS, got $DENIED"
  exit 1
fi

GOT=$(printf '%s' "$RESPONSE" | jq -c '[.status.results[]?.resource.metadata.name] | sort')
WANT=$(jq -nc '$ARGS.positional | sort' --args "$@")
if [ "$GOT" != "$WANT" ]; then
  echo "FAIL: $AS_USER querying $KINDS in their user context"
  echo "  want: $WANT"
  echo "  got:  $GOT"
  exit 1
fi
echo "OK: $AS_USER querying $KINDS got exactly $GOT"
