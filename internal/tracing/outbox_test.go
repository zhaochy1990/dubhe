package tracing

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestGatewayOutboxAcknowledgesOnlyCommittedProjection(t *testing.T) {
	api, db := sqliteAPI(t)
	const traceID = "01020304050607080102030405060708"
	const callID = "gateway-only"
	const nodePath = "/v1/observability/nodes/tool:" + callID + "?" + scopeQuery
	now := time.Now().UTC().Add(-time.Second)
	event := auditEvent{
		EventID: "decision-1", CallID: callID, TraceID: traceID, AgentID: "agent", ToolID: "test",
		Sequence: 1, Type: "decision", OccurredAt: now, Decision: "block", ExecutionState: "not_executed",
		PolicyVersion: "policy", Initiator: actorIdentity{Kind: "user", ID: "verified"},
		Binding: &gatewayBinding{RegisteredAt: now},
	}
	var leased, acked atomic.Int32
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-API-Key") != "outbox-key" || r.Method != http.MethodPost {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		switch r.URL.Path {
		case "/events/lease":
			attempt := leased.Add(1)
			item := leasedAuditEvent{Token: "lease", Version: 1, Event: event}
			if attempt == 1 {
				item.Event.Binding = nil
			}
			writeJSON(w, http.StatusOK, map[string]any{"deliveries": []leasedAuditEvent{item}})
		case "/events/ack":
			req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, api.URL+nodePath, nil)
			if err != nil {
				t.Error(err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			req.Header.Set("X-API-Key", "query")
			res, err := api.Client().Do(req)
			if err != nil {
				t.Error(err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			defer res.Body.Close()
			if res.StatusCode != http.StatusOK {
				t.Errorf("Gateway received ACK before projection was visible: HTTP %d", res.StatusCode)
			}
			if acked.Add(1) == 1 {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(gateway.Close)
	consumer, err := NewGatewayOutbox(db, Options{}, GatewayOutboxConfig{
		URL: gateway.URL + "/events", APIKey: "outbox-key", Scope: testScope, SourceID: "gateway",
	})
	if err != nil {
		t.Fatal(err)
	}
	var failures []error
	for range 3 {
		if err := consumer.consume(t.Context(), func(err error) { failures = append(failures, err) }); err != nil {
			t.Fatal(err)
		}
	}
	if len(failures) != 2 || !errors.Is(failures[0], errAuditBinding) || acked.Load() != 2 {
		t.Fatalf("unexpected lease results: failures=%v acknowledgements=%d", failures, acked.Load())
	}
	body := request(t, api, http.MethodGet, nodePath, "query", "", "", nil, http.StatusOK)
	var view nodeDetail
	if err := json.Unmarshal(body, &view); err != nil {
		t.Fatal(err)
	}
	if view.Source != "gateway" || view.SpanID != "" || len(view.Audit) != 1 || view.Audit[0].ExecutionState != "not_executed" {
		t.Fatalf("unexpected Gateway-only detail: %s", body)
	}
	body = request(t, api, http.MethodGet, "/v1/observability/traces/"+traceID+"?"+scopeQuery, "query", "", "", nil, http.StatusOK)
	var trace traceView
	if err := json.Unmarshal(body, &trace); err != nil {
		t.Fatal(err)
	}
	if !trace.Observed || trace.RunState != nil || trace.Governance.Result != "blocked" {
		t.Fatalf("unexpected rootless Trace: %s", body)
	}
}
