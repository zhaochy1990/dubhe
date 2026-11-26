package tracing

import (
	"bytes"
	"compress/gzip"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"testing"
	"time"

	collector "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	common "go.opentelemetry.io/proto/otlp/common/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/genproto/googleapis/rpc/code"
	"google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/protobuf/proto"
	"gorm.io/gorm"
)

var testScope = Scope{TenantID: "tenant", EnvironmentID: "staging", ProjectID: "project"}

func sqliteAPI(t *testing.T, options ...Options) (*httptest.Server, *gorm.DB) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "tracing.db")
	open := func(dsn string) *gorm.DB {
		db, err := OpenDatabase(t.Context(), "sqlite", dsn)
		if err != nil {
			t.Fatal(err)
		}
		pool, err := db.DB()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { pool.Close() })
		return db
	}
	writer := open(path)
	if err := InitializeDatabase(t.Context(), writer); err != nil {
		t.Fatal(err)
	}
	reader := open((&url.URL{Scheme: "file", Path: path, RawQuery: "mode=ro"}).String())
	otherProject := testScope
	otherProject.ProjectID = "other-project"
	handler, err := NewHandler(writer, reader, AuthConfig{
		Workloads: []WorkloadCredential{{APIKey: "ingest", Scope: testScope, ProducerID: "worker"}},
		Registrars: []WorkloadCredential{
			{APIKey: "register", Scope: testScope, ProducerID: "worker"},
			{APIKey: "register-other", Scope: otherProject, ProducerID: "worker"},
		},
		Readers: []ReaderCredential{
			{APIKey: "query", Scope: testScope, AgentIDs: []string{"agent"}},
			{APIKey: "query-content", Scope: testScope, AgentIDs: []string{"agent"}, ModelInput: true, ToolParameters: true},
			{APIKey: "other-agent", Scope: testScope, AgentIDs: []string{"other-agent"}},
			{APIKey: "other-project", Scope: otherProject, AgentIDs: []string{"agent"}},
			{APIKey: "two-projects", Grants: []ReaderGrant{
				{Scope: testScope, AgentIDs: []string{"agent"}},
				{Scope: otherProject, AgentIDs: []string{"agent"}},
			}},
		},
	}, options...)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return server, writer
}

func request(t *testing.T, server *httptest.Server, method, path, token, contentType, encoding string, body []byte, want int) []byte {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), method, server.URL+path, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("X-API-Key", token)
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if encoding != "" {
		req.Header.Set("Content-Encoding", encoding)
	}
	res, err := server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	raw, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	if res.StatusCode != want {
		t.Fatalf("%s %s: got HTTP %d, want %d. Body: %q", method, path, res.StatusCode, want, raw)
	}
	return raw
}

func registrationBody(t *testing.T, traceID, spanID []byte) []byte {
	t.Helper()
	now := time.Now().UTC()
	input := registrationRequest{
		SessionID: "session", TurnID: "turn", RootAgentID: "agent", RootProducerID: "worker",
		TraceID: hex.EncodeToString(traceID), RootSpanID: hex.EncodeToString(spanID),
		SessionStartedAt: now.Add(-time.Second), RegisteredAt: now, AcceptUntil: now.Add(time.Hour),
	}
	input.Scope.TenantID, input.Scope.EnvironmentID, input.Scope.ProjectID = testScope.TenantID, testScope.EnvironmentID, testScope.ProjectID
	body, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func testSpans(traceID, rootID, modelID []byte) (*tracepb.Span, *tracepb.Span) {
	attribute := func(key, value string) *common.KeyValue {
		return &common.KeyValue{Key: key, Value: &common.AnyValue{Value: &common.AnyValue_StringValue{StringValue: value}}}
	}
	root := &tracepb.Span{
		TraceId: traceID, SpanId: rootID, StartTimeUnixNano: 1<<63 + 1, EndTimeUnixNano: 1<<63 + 4,
		Attributes: []*common.KeyValue{attribute("dubhe.agent.id", "agent"), attribute("dubhe.span.kind", "run"), attribute("dubhe.run.outcome", "completed")},
	}
	model := &tracepb.Span{
		TraceId: traceID, SpanId: modelID, ParentSpanId: rootID, StartTimeUnixNano: 1<<63 + 2, EndTimeUnixNano: 1<<63 + 3,
		Attributes: []*common.KeyValue{attribute("dubhe.agent.id", "agent"), attribute("dubhe.span.kind", "llm")},
	}
	return root, model
}

func exportSpans(t *testing.T, server *httptest.Server, compress bool, want int, spans ...*tracepb.Span) []byte {
	t.Helper()
	batch := &collector.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{{ScopeSpans: []*tracepb.ScopeSpans{{Spans: spans}}}},
	}
	body, err := proto.Marshal(batch)
	if err != nil {
		t.Fatal(err)
	}
	encoding := ""
	if compress {
		var compressed bytes.Buffer
		writer := gzip.NewWriter(&compressed)
		if _, err := writer.Write(body); err != nil {
			t.Fatal(err)
		}
		if err := writer.Close(); err != nil {
			t.Fatal(err)
		}
		body = compressed.Bytes()
		encoding = "gzip"
	}
	return request(t, server, http.MethodPost, "/v1/traces", "ingest", protobufType, encoding, body, want)
}

const scopeQuery = "tenant_id=tenant&environment_id=staging&project_id=project"

func TestSQLiteTraceLifecycle(t *testing.T) {
	t.Parallel()
	server, _ := sqliteAPI(t)
	traceID, rootID, modelID := bytes.Repeat([]byte{1}, 16), bytes.Repeat([]byte{2}, 8), bytes.Repeat([]byte{3}, 8)
	registration := registrationBody(t, traceID, rootID)
	const registrationPath = "/internal/v1/observability/runs"
	request(t, server, http.MethodPost, registrationPath, "ingest", "application/json", "", registration, http.StatusUnauthorized)
	request(t, server, http.MethodPost, registrationPath, "register", "application/json", "", registration, http.StatusNoContent)
	request(t, server, http.MethodPost, registrationPath, "register", "application/json", "", registration, http.StatusNoContent)
	conflict := bytes.Replace(registration, []byte(hex.EncodeToString(traceID)), []byte(hex.EncodeToString(bytes.Repeat([]byte{4}, 16))), 1)
	request(t, server, http.MethodPost, registrationPath, "register", "application/json", "", conflict, http.StatusConflict)

	root, model := testSpans(traceID, rootID, modelID)
	exportSpans(t, server, true, http.StatusOK, model)
	exportSpans(t, server, false, http.StatusOK, root, model)
	rawTraceID := hex.EncodeToString(traceID)
	tracePath := "/v1/observability/traces/" + rawTraceID + "?" + scopeQuery
	raw := request(t, server, http.MethodGet, tracePath, "query", "", "", nil, http.StatusOK)
	var view traceView
	if err := json.Unmarshal(raw, &view); err != nil {
		t.Fatal(err)
	}
	if view.SessionID != "session" || view.RootAgent != "agent" || view.RunState == nil || *view.RunState != "completed" ||
		len(view.Nodes) != 2 || view.Nodes[0].SpanID != hex.EncodeToString(rootID) || view.Nodes[0].ParentSpanID != nil ||
		view.Nodes[1].ParentSpanID == nil || *view.Nodes[1].ParentSpanID != view.Nodes[0].SpanID ||
		view.Nodes[1].EndNS != model.EndTimeUnixNano {
		t.Fatalf("unexpected trace after idempotent replay: %+v", view)
	}

	nodePath := "/v1/observability/nodes/span:" + hex.EncodeToString(modelID) + "?" + scopeQuery
	request(t, server, http.MethodGet, nodePath+"&expected_trace_id="+rawTraceID, "query", "", "", nil, http.StatusOK)
	request(t, server, http.MethodGet, nodePath+"&expected_trace_id="+hex.EncodeToString(bytes.Repeat([]byte{4}, 16)), "query", "", "", nil, http.StatusNotFound)
	for _, path := range []string{tracePath, nodePath} {
		request(t, server, http.MethodGet, path, "other-agent", "", "", nil, http.StatusNotFound)
		request(t, server, http.MethodGet, path, "other-project", "", "", nil, http.StatusNotFound)
		request(t, server, http.MethodGet, path, "ingest", "", "", nil, http.StatusNotFound)
		request(t, server, http.MethodGet, path+"&project_id=project", "query", "", "", nil, http.StatusNotFound)
	}

	paged := request(t, server, http.MethodGet, tracePath+"&limit=1", "query", "", "", nil, http.StatusOK)
	var first, second traceView
	if err := json.Unmarshal(paged, &first); err != nil {
		t.Fatal(err)
	}
	if len(first.Nodes) != 1 || first.Nodes[0].Kind != "run" || first.NextCursor == "" {
		t.Fatalf("unexpected first Trace page: %s", paged)
	}
	paged = request(t, server, http.MethodGet, tracePath+"&limit=1&cursor="+first.NextCursor, "query", "", "", nil, http.StatusOK)
	if err := json.Unmarshal(paged, &second); err != nil {
		t.Fatal(err)
	}
	if len(second.Nodes) != 1 || second.Nodes[0].Kind != "llm" || !second.Nodes[0].UnloadedParent || second.NextCursor != "" {
		t.Fatalf("unexpected second Trace page: %s", paged)
	}

	changed := proto.CloneOf(model)
	changed.EndTimeUnixNano++
	var ack collector.ExportTraceServiceResponse
	if err := proto.Unmarshal(exportSpans(t, server, false, http.StatusOK, changed), &ack); err != nil {
		t.Fatal(err)
	}
	if ack.GetPartialSuccess().GetRejectedSpans() != 1 {
		t.Fatalf("unexpected acknowledgement for conflicting Span: %v", &ack)
	}
	if fresh := request(t, server, http.MethodGet, tracePath, "query", "", "", nil, http.StatusOK); !bytes.Equal(fresh, raw) {
		t.Fatalf("a conflicting Span changed the stored trace: %s", fresh)
	}
}

func TestSQLiteOTLPRollback(t *testing.T) {
	t.Parallel()
	server, writer := sqliteAPI(t)
	traceID, rootID, modelID := bytes.Repeat([]byte{1}, 16), bytes.Repeat([]byte{2}, 8), bytes.Repeat([]byte{3}, 8)
	request(t, server, http.MethodPost, "/internal/v1/observability/runs", "register", "application/json", "", registrationBody(t, traceID, rootID), http.StatusNoContent)
	if err := writer.Exec(`CREATE TRIGGER reject_model BEFORE INSERT ON tracing_spans
		WHEN NEW.kind = 'llm' BEGIN SELECT RAISE(ABORT, 'test storage failure'); END`).Error; err != nil {
		t.Fatal(err)
	}
	root, model := testSpans(traceID, rootID, modelID)
	var failure status.Status
	if err := proto.Unmarshal(exportSpans(t, server, false, http.StatusServiceUnavailable, root, model), &failure); err != nil {
		t.Fatal(err)
	}
	if failure.Code != int32(code.Code_UNAVAILABLE) {
		t.Fatalf("expected retryable OTLP error, got %v", &failure)
	}
	var view traceView
	path := "/v1/observability/traces/" + hex.EncodeToString(traceID) + "?" + scopeQuery
	if err := json.Unmarshal(request(t, server, http.MethodGet, path, "query", "", "", nil, http.StatusOK), &view); err != nil {
		t.Fatal(err)
	}
	if len(view.Nodes) != 0 || view.RunState != nil {
		t.Fatalf("failed transaction was partially committed: %+v", view)
	}
}
