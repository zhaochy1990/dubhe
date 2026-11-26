package tracing

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	common "go.opentelemetry.io/proto/otlp/common/v1"
	"google.golang.org/protobuf/proto"
)

func TestModelDetailsAndContentPermission(t *testing.T) {
	for _, scenario := range []struct {
		name    string
		enabled bool
		expires bool
	}{
		{name: "enabled", enabled: true},
		{name: "disabled"},
		{name: "expired", enabled: true, expires: true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			testModelDetails(t, scenario.enabled, scenario.expires)
		})
	}
}

func testModelDetails(t *testing.T, enabled, expires bool) {
	policy := ScopePolicy{Scope: testScope, ModelCatalog: []ModelDisplay{{ID: "model-v1", Name: "test-model"}}}
	if enabled {
		policy.ContentDays = 1
	}
	options := Options{Scopes: []ScopePolicy{policy}}
	server, db := sqliteAPI(t, options)
	traceID, rootID, modelID := bytes.Repeat([]byte{1}, 16), bytes.Repeat([]byte{2}, 8), bytes.Repeat([]byte{3}, 8)
	request(t, server, http.MethodPost, "/internal/v1/observability/runs", "register", "application/json", "",
		registrationBody(t, traceID, rootID), http.StatusNoContent)
	_, model := testSpans(traceID, rootID, modelID)
	model.Attributes = append(model.Attributes,
		&common.KeyValue{Key: "gen_ai.request.model", Value: &common.AnyValue{Value: &common.AnyValue_StringValue{StringValue: "test-model"}}},
		&common.KeyValue{Key: "gen_ai.usage.output_tokens", Value: &common.AnyValue{Value: &common.AnyValue_IntValue{IntValue: 0}}},
		&common.KeyValue{Key: "gen_ai.input.messages", Value: &common.AnyValue{Value: &common.AnyValue_StringValue{
			StringValue: `[{"role":"user","parts":[{"type":"text","content":"private prompt"}]}]`}}},
	)
	withoutInput := proto.CloneOf(model)
	withoutInput.SpanId = bytes.Repeat([]byte{4}, 8)
	withoutInput.Attributes = withoutInput.Attributes[:len(withoutInput.Attributes)-1]
	exportSpans(t, server, false, http.StatusOK, model, withoutInput)
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
		!bytes.Contains(raw, []byte(`"content_state":"forbidden"`)) ||
		bytes.Contains(raw, []byte("private prompt")) || bytes.Contains(raw, []byte("input_tokens")) {
		t.Fatalf("unexpected model summary: %s", raw)
	}
	if expires {
		// Advance the capture deadline without waiting for the retention period.
		deadline, _ := unixNano(time.Now().Add(-time.Hour))
		if err := db.Exec("UPDATE tracing_trace_content SET expires_at_ns = ?", (store{db: db}).timestamp(deadline)).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := PurgeContent(t.Context(), db, options); err != nil {
		t.Fatal(err)
	}
	if expires {
		var payload string
		if err := db.Raw("SELECT payload FROM tracing_content WHERE object_id = ?", "span:"+hex.EncodeToString(modelID)).Row().Scan(&payload); err != nil {
			t.Fatal(err)
		}
		if payload != "{}" {
			t.Fatal("purged content still has a payload")
		}
	}
	state := "not_captured"
	if enabled && !expires {
		state = "available"
	} else if enabled {
		state = "expired"
	}
	raw = request(t, server, http.MethodGet, path, "query-content", "", "", nil, http.StatusOK)
	if bytes.Contains(raw, []byte("private prompt")) != (enabled && !expires) ||
		!bytes.Contains(raw, []byte(`"content_state":"`+state+`"`)) ||
		bytes.Contains(raw, []byte("replayed prompt")) || !bytes.Contains(raw, []byte(`"output_tokens":0`)) {
		t.Fatalf("unexpected content after replay: %s", raw)
	}
	path = "/v1/observability/nodes/span:" + hex.EncodeToString(withoutInput.SpanId) + "?" + scopeQuery
	raw = request(t, server, http.MethodGet, path, "query-content", "", "", nil, http.StatusOK)
	if !bytes.Contains(raw, []byte(`"content_state":"not_captured"`)) {
		t.Fatalf("unexpected status for a Span that never captured inputs: %s", raw)
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
