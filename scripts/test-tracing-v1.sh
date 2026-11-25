#!/usr/bin/env bash
set -euo pipefail

cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."

go_cli=${GO:-go}
run_dir=$(mktemp -d)
mysql_container=
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
    if [[ -n $mysql_container ]]; then
        docker stop "$mysql_container" >/dev/null 2>&1 || true
    fi
    rm -rf -- "$run_dir"
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

if [[ -n ${DUBHE_TEST_MYSQL_DSN:-} || -n ${DUBHE_TEST_MYSQL_URL:-} ]]; then
    : "${DUBHE_TEST_MYSQL_DSN:?Set both DUBHE_TEST_MYSQL_DSN and DUBHE_TEST_MYSQL_URL for an existing test database}"
    : "${DUBHE_TEST_MYSQL_URL:?Set both DUBHE_TEST_MYSQL_DSN and DUBHE_TEST_MYSQL_URL for an existing test database}"
else
    mysql_container=$(docker run -d --rm -p 127.0.0.1::3306 \
        -e MYSQL_ROOT_PASSWORD=dubhe-local-root -e MYSQL_DATABASE=dubhe \
        -e MYSQL_USER=dubhe -e MYSQL_PASSWORD=dubhe-local mysql:8.4)
    mysql_address=$(docker port "$mysql_container" 3306/tcp)
    deadline=$((SECONDS + 90))
    until docker exec -e MYSQL_PWD=dubhe-local "$mysql_container" \
        mysql -h127.0.0.1 -udubhe -D dubhe -Nse 'SELECT 1' >/dev/null 2>&1; do
        if (( SECONDS >= deadline )); then
            docker logs "$mysql_container" >&2
            exit 1
        fi
        sleep 1
    done
    DUBHE_TEST_MYSQL_DSN="dubhe:dubhe-local@tcp($mysql_address)/dubhe"
    DUBHE_TEST_MYSQL_URL="mysql://dubhe:dubhe-local@$mysql_address/dubhe"
fi
export DUBHE_TEST_MYSQL_DSN DUBHE_TEST_MYSQL_URL

"$go_cli" test ./...
"$go_cli" build -o "$run_dir/dubhe-tracing" ./cmd/dubhe-tracing

export DUBHE_MYSQL_DSN="$DUBHE_TEST_MYSQL_DSN"
export DUBHE_LISTEN=127.0.0.1:0
DUBHE_TRACING_AUTH=$(cat <<'JSON'
{
  "workloads": {
    "workload-token": {
      "scope": {"tenant_id": "tenant-alpha", "environment_id": "staging", "project_id": "support"},
      "producer_id": "agent-worker"
    }
  },
  "registrars": {
    "register-token": {
      "scope": {"tenant_id": "tenant-alpha", "environment_id": "staging", "project_id": "support"},
      "producer_id": "agent-worker"
    }
  },
  "readers": {
    "reader-token": {
      "scope": {"tenant_id": "tenant-alpha", "environment_id": "staging", "project_id": "support"},
      "agent_ids": ["support-agent"]
    },
    "other-tenant-reader": {
      "scope": {"tenant_id": "tenant-beta", "environment_id": "staging", "project_id": "support"},
      "agent_ids": ["support-agent"]
    },
    "other-environment-reader": {
      "scope": {"tenant_id": "tenant-alpha", "environment_id": "Staging", "project_id": "support"},
      "agent_ids": ["support-agent"]
    },
    "other-project-reader": {
      "scope": {"tenant_id": "tenant-alpha", "environment_id": "staging", "project_id": "billing"},
      "agent_ids": ["support-agent"]
    },
    "other-agent-reader": {
      "scope": {"tenant_id": "tenant-alpha", "environment_id": "staging", "project_id": "support"},
      "agent_ids": ["billing-agent"]
    }
  }
}
JSON
)
export DUBHE_TRACING_AUTH

"$run_dir/dubhe-tracing" >"$run_dir/service.log" 2>&1 &
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
