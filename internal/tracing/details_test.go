package tracing

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	common "go.opentelemetry.io/proto/otlp/common/v1"
)

func TestModelDetailsAndContentPermission(t *testing.T) {
	for _, scenario := range []struct {
		name    string
		enabled bool
		age     time.Duration
	}{
		{name: "enabled", enabled: true},
		{name: "disabled"},
		{name: "expired", enabled: true, age: 48 * time.Hour},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			testModelDetails(t, scenario.enabled, scenario.age)
		})
	}
}

func testModelDetails(t *testing.T, enabled bool, age time.Duration) {
	policy := ScopePolicy{Scope: testScope, ModelCatalog: []ModelDisplay{{ID: "model-v1", Name: "test-model"}}}
	if enabled {
		policy.ContentDays = 1
	}
	server, _ := sqliteAPI(t, Options{Scopes: []ScopePolicy{policy}})
	traceID, rootID, modelID := bytes.Repeat([]byte{1}, 16), bytes.Repeat([]byte{2}, 8), bytes.Repeat([]byte{3}, 8)
	var binding registrationRequest
	if err := json.Unmarshal(registrationBody(t, traceID, rootID), &binding); err != nil {
		t.Fatal(err)
	}
	binding.RegisteredAt = binding.RegisteredAt.Add(-age)
	binding.SessionStartedAt = binding.SessionStartedAt.Add(-age)
	body, err := json.Marshal(binding)
	if err != nil {
		t.Fatal(err)
	}
	request(t, server, http.MethodPost, "/internal/v1/observability/runs", "register", "application/json", "",
		body, http.StatusNoContent)
	_, model := testSpans(traceID, rootID, modelID)
	model.Attributes = append(model.Attributes,
		&common.KeyValue{Key: "gen_ai.request.model", Value: &common.AnyValue{Value: &common.AnyValue_StringValue{StringValue: "test-model"}}},
		&common.KeyValue{Key: "gen_ai.usage.output_tokens", Value: &common.AnyValue{Value: &common.AnyValue_IntValue{IntValue: 0}}},
		&common.KeyValue{Key: "gen_ai.input.messages", Value: &common.AnyValue{Value: &common.AnyValue_StringValue{
			StringValue: `[{"role":"user","parts":[{"type":"text","content":"private prompt"}]}]`}}},
	)
	exportSpans(t, server, false, http.StatusOK, model)
	model.Attributes[len(model.Attributes)-1].Value = &common.AnyValue{Value: &common.AnyValue_StringValue{
		StringValue: `[{"role":"user","parts":[{"type":"text","content":"replayed prompt"}]}]`,
	}}
	if replay := exportSpans(t, server, false, http.StatusOK, model); len(replay) != 0 {
		t.Fatalf("equivalent metadata replay rejected: %x", replay)
	}
	path := "/v1/observability/nodes/span:" + hex.EncodeToString(modelID) + "?" + scopeQuery
	var view nodeView
	raw := request(t, server, http.MethodGet, path, "query", "", "", nil, http.StatusOK)
	if err := json.Unmarshal(raw, &view); err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(view.Metadata, []byte(`"output_tokens":0`)) ||
		!bytes.Contains(view.Metadata, []byte(`"model_name":"test-model"`)) ||
		bytes.Contains(raw, []byte("private prompt")) || bytes.Contains(raw, []byte("input_tokens")) {
		t.Fatalf("unexpected model summary: %s", raw)
	}
	raw = request(t, server, http.MethodGet, path, "query-content", "", "", nil, http.StatusOK)
	if bytes.Contains(raw, []byte("private prompt")) != (enabled && age == 0) ||
		bytes.Contains(raw, []byte("replayed prompt")) || !bytes.Contains(raw, []byte(`"output_tokens":0`)) {
		t.Fatalf("unexpected content after replay: %s", raw)
	}
}

func TestSessionQueriesShareTraceAuthorization(t *testing.T) {
	server, _ := sqliteAPI(t)
	traceID, rootID := bytes.Repeat([]byte{1}, 16), bytes.Repeat([]byte{2}, 8)
	request(t, server, http.MethodPost, "/internal/v1/observability/runs", "register", "application/json", "",
		registrationBody(t, traceID, rootID), http.StatusNoContent)
	const sessions = "/v1/observability/sessions"
	raw := request(t, server, http.MethodGet, sessions+"?"+scopeQuery+"&q=SESSION&agent_id=agent", "query", "", "", nil, http.StatusOK)
	var result struct {
		Sessions []struct {
			SessionID string `json:"session_id"`
		} `json:"sessions"`
		Total        int
		Distribution []struct{ Count int }
	}
	if err := json.Unmarshal(raw, &result); err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, bucket := range result.Distribution {
		count += bucket.Count
	}
	if result.Total != 1 || count != 1 || len(result.Sessions) != 1 || result.Sessions[0].SessionID != "session" {
		t.Fatalf("unexpected session result: %s", raw)
	}
	raw = request(t, server, http.MethodGet, sessions+"?"+scopeQuery, "other-agent", "", "", nil, http.StatusOK)
	if !bytes.Contains(raw, []byte(`"total":0`)) || bytes.Contains(raw, []byte(`"session_id"`)) {
		t.Fatalf("unexpected restricted session result: %s", raw)
	}
	path := sessions + "/session/traces?" + scopeQuery
	raw = request(t, server, http.MethodGet, path, "query", "", "", nil, http.StatusOK)
	if !bytes.Contains(raw, []byte(hex.EncodeToString(traceID))) {
		t.Fatalf("registered trace missing from session: %s", raw)
	}
	request(t, server, http.MethodGet, path, "other-agent", "", "", nil, http.StatusNotFound)
	request(t, server, http.MethodGet, path, "other-project", "", "", nil, http.StatusNotFound)
}

func TestSessionPaginationWithIdenticalIDsAcrossProjects(t *testing.T) {
	server, _ := sqliteAPI(t)
	traceID, rootID := bytes.Repeat([]byte{1}, 16), bytes.Repeat([]byte{2}, 8)
	firstScope := registrationBody(t, traceID, rootID)
	var binding registrationRequest
	if err := json.Unmarshal(firstScope, &binding); err != nil {
		t.Fatal(err)
	}
	binding.Scope.ProjectID = "other-project"
	secondScope, err := json.Marshal(binding)
	if err != nil {
		t.Fatal(err)
	}
	const runs = "/internal/v1/observability/runs"
	request(t, server, http.MethodPost, runs, "register", "application/json", "", firstScope, http.StatusNoContent)
	request(t, server, http.MethodPost, runs, "register-other", "application/json", "", secondScope, http.StatusNoContent)
	const path = "/v1/observability/sessions?limit=1"
	getPage := func(path, token string) sessionList {
		t.Helper()
		var page sessionList
		body := request(t, server, http.MethodGet, path, token, "", "", nil, http.StatusOK)
		if err := json.Unmarshal(body, &page); err != nil {
			t.Fatal(err)
		}
		return page
	}
	first := getPage(path, "two-projects")
	if first.Total != 2 || len(first.Sessions) != 1 || first.Sessions[0].ProjectID != testScope.ProjectID || first.NextCursor == "" {
		t.Fatalf("unexpected first page: %+v", first)
	}
	second := getPage(path+"&cursor="+first.NextCursor, "two-projects")
	if second.Total != 2 || len(second.Sessions) != 1 || second.Sessions[0].ProjectID != binding.Scope.ProjectID || second.NextCursor != "" {
		t.Fatalf("unexpected second page: %+v", second)
	}
	request(t, server, http.MethodGet, path+"&cursor="+first.NextCursor, "query", "", "", nil, http.StatusBadRequest)
}
