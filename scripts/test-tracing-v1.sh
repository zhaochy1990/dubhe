#!/usr/bin/env bash
set -euo pipefail

cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."

go_cli=${GO:-go}
run_dir=$(mktemp -d)
service_pid=
cleanup() {
    local status=$?
    trap - EXIT
    if [[ $status -ne 0 && -f "$run_dir/service.log" ]]; then
        cat "$run_dir/service.log" >&2
    fi
    if [[ -n $service_pid ]]; then
        kill "$service_pid" 2>/dev/null || true
        wait "$service_pid" 2>/dev/null || true
    fi
    rm -rf -- "$run_dir"
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

"$go_cli" test ./...
"$go_cli" build -o "$run_dir/dubhe-tracing" ./cmd/dubhe-tracing

database_driver=sqlite
if [[ -n ${DUBHE_TEST_MYSQL_DSN:-} ]]; then
    database_driver=mysql
    export DUBHE_MYSQL_DSN="$DUBHE_TEST_MYSQL_DSN"
    unset DUBHE_TEST_SQLITE_PATH
else
    export DUBHE_TEST_SQLITE_PATH="$run_dir/tracing.db"
    export DUBHE_SQLITE_DSN="$DUBHE_TEST_SQLITE_PATH"
fi
cat > "$run_dir/tracing.yaml" <<'YAML'
auth:
  workloads:
    - api_key: "workload-token"
      scope: &scope {tenant_id: "tenant-alpha", environment_id: "staging", project_id: "support"}
      producer_id: "agent-worker"
  registrars:
    - api_key: "register-token"
      scope: *scope
      producer_id: "agent-worker"
  gateways:
    - api_key: "audit-token"
      scope: *scope
      producer_id: "tool-gateway"
  readers:
    - api_key: "content-reader"
      scope: *scope
      agent_ids: ["support-agent", "orders-agent", "operations-agent"]
      model_input: true
      tool_parameters: true
    - api_key: "model-reader"
      scope: *scope
      agent_ids: ["support-agent", "orders-agent"]
      model_input: true
    - api_key: "tools-reader"
      scope: *scope
      agent_ids: ["support-agent", "orders-agent"]
      tool_parameters: true
    - api_key: "reader-token"
      scope: *scope
      agent_ids: ["support-agent"]
    - api_key: "other-tenant-reader"
      scope: {tenant_id: "tenant-beta", environment_id: "staging", project_id: "support"}
      agent_ids: ["support-agent"]
    - api_key: "other-environment-reader"
      scope: {tenant_id: "tenant-alpha", environment_id: "Staging", project_id: "support"}
      agent_ids: ["support-agent"]
    - api_key: "other-project-reader"
      scope: {tenant_id: "tenant-alpha", environment_id: "staging", project_id: "billing"}
      agent_ids: ["support-agent"]
    - api_key: "other-agent-reader"
      scope: *scope
      agent_ids: ["billing-agent"]
tracing:
  scopes:
    - scope: *scope
      content_days: 1
      agents:
        - {id: "support-agent", name: "Customer Support"}
        - {id: "orders-agent", name: "Orders"}
        - {id: "operations-agent", name: "International Customer Service Operations and Escalation Agent"}
      models:
        - {id: "fixture-v1", name: "provider-fixture"}
      tool_ids: ["orders.lookup", "orders.update", "local.echo"]
YAML

env -u DUBHE_TRACING_AUTH -u DUBHE_SQLITE_READ_DSN -u DUBHE_MYSQL_READ_DSN \
    DUBHE_LISTEN=127.0.0.1:0 \
    "$run_dir/dubhe-tracing" --config "$run_dir/tracing.yaml" --database-driver "$database_driver" \
    >"$run_dir/service.log" 2>&1 &
service_pid=$!
deadline=$((SECONDS + 15))
while true; do
    service_address=$(sed -nE 's/.*listening on (127\.0\.0\.1:[0-9]+).*/\1/p' "$run_dir/service.log")
    if [[ -n $service_address ]]; then
        break
    fi
    if ! kill -0 "$service_pid" 2>/dev/null || (( SECONDS >= deadline )); then
        printf '%s\n' 'The Go Tracing service did not start' >&2
        exit 1
    fi
    sleep 0.1
done
export DUBHE_TEST_BASE_URL="http://$service_address"

npm --prefix packages/dubhe ci --no-audit --no-fund
npm --prefix packages/dubhe run build
npm --prefix packages/dubhe test
