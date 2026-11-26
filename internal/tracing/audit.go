package tracing

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"gorm.io/gorm"
)

// gatewayBinding carries the linkage already verified by the Gateway. When
// the Gateway has no shared business IDs it can leave them absent to create
// an independent Session and turn for this trusted Trace.
type gatewayBinding struct {
	SessionID        string    `json:"session_id,omitempty"`
	TurnID           string    `json:"turn_id,omitempty"`
	RootAgentID      string    `json:"root_agent_id,omitempty"`
	SessionStartedAt time.Time `json:"session_started_at,omitzero"`
	RegisteredAt     time.Time `json:"registered_at"`
}

func (b gatewayBinding) registration(scope Scope, traceID []byte, event auditEvent) (registration, bool) {
	startedAt := b.SessionStartedAt
	if startedAt.IsZero() {
		startedAt = b.RegisteredAt
	}
	sessionNS, sessionOK := unixNano(startedAt)
	registeredNS, registerOK := unixNano(b.RegisteredAt)
	if b.SessionID == "" {
		if b.TurnID != "" {
			return registration{}, false
		}
		b.SessionID = "gateway:" + event.TraceID
	}
	if b.TurnID == "" {
		b.TurnID = "gateway:" + event.TraceID
	}
	if b.RootAgentID == "" {
		b.RootAgentID = event.AgentID
	}
	if !sessionOK || !registerOK || sessionNS > registeredNS || b.RegisteredAt.After(event.OccurredAt) ||
		!opaqueID(b.SessionID) || !opaqueID(b.TurnID) || !opaqueID(b.RootAgentID) {
		return registration{}, false
	}
	binding := registration{
		Scope: scope, TraceID: traceID, SessionStartedAt: sessionNS,
		runBinding: runBinding{
			SessionID: b.SessionID, TurnID: b.TurnID, RootAgentID: b.RootAgentID,
			// Gateway-only bindings have no claim on any SDK producer or Span.
			RootSpanID: string(make([]byte, 8)), RegisteredAt: registeredNS, AcceptUntil: registeredNS,
		},
	}
	return binding, true
}

// auditEvent is a snapshot from the Gateway's committed Outbox. Sequence is
// monotonic per call. The first decision contains the evaluated parameters.
type auditEvent struct {
	EventID        string          `json:"event_id"`
	CallID         string          `json:"call_id"`
	TraceID        string          `json:"trace_id"`
	SpanID         string          `json:"span_id,omitempty"`
	AgentID        string          `json:"agent_id"`
	ToolID         string          `json:"tool_id"`
	Sequence       int64           `json:"sequence"`
	Type           string          `json:"type"`
	OccurredAt     time.Time       `json:"occurred_at"`
	Decision       string          `json:"decision"`
	ExecutionState string          `json:"execution_state"`
	PolicyVersion  string          `json:"policy_version"`
	Initiator      actorIdentity   `json:"initiator"`
	Parameters     json.RawMessage `json:"parameters,omitempty"`
	ReasonCode     string          `json:"reason_code,omitempty"`
	Confirmation   string          `json:"confirmation_state,omitempty"`
	Binding        *gatewayBinding `json:"gateway_binding,omitempty"`
}

func (e auditEvent) valid() bool {
	if !opaqueID(e.EventID) || !opaqueID(e.CallID) || !opaqueID(e.AgentID) || !opaqueID(e.ToolID) ||
		!opaqueID(e.PolicyVersion) || !e.Initiator.valid() || e.Sequence < 1 || e.Sequence > 1<<53-1 ||
		(e.ReasonCode != "" && !opaqueID(e.ReasonCode)) ||
		(e.Parameters != nil && (e.Sequence != 1 || !jsonObject(e.Parameters))) {
		return false
	}
	if _, ok := unixNano(e.OccurredAt); !ok {
		return false
	}
	switch e.Confirmation {
	case "", "pending", "approved", "rejected", "expired":
	default:
		return false
	}
	switch e.Decision {
	case "allow", "deny", "block", "require_confirmation":
	default:
		return false
	}
	if e.ExecutionState != "not_executed" && (e.Decision != "allow" || e.Confirmation == "pending" ||
		e.Confirmation == "rejected" || e.Confirmation == "expired") {
		return false
	}
	switch e.Type {
	case "decision":
		return e.Sequence == 1 && e.ExecutionState == "not_executed"
	case "confirmation", "revalidation":
		return e.Sequence > 1 && e.ExecutionState == "not_executed"
	case "forward_intent":
		return e.Sequence > 1 && e.ExecutionState == "executing"
	case "execution_result", "reconciliation":
		return e.Sequence > 1 && (e.ExecutionState == "succeeded" || e.ExecutionState == "failed" || e.ExecutionState == "unknown")
	default:
		return false
	}
}

func (e *auditEvent) normalize(scope Scope) ([]byte, []byte, error) {
	if !e.valid() {
		return nil, nil, errors.New("invalid Gateway audit event")
	}
	traceID, ok := parseID(e.TraceID, 16)
	if !ok {
		return nil, nil, errors.New("invalid Gateway audit Trace")
	}
	e.TraceID = hex.EncodeToString(traceID)
	if e.Binding != nil {
		if _, valid := e.Binding.registration(scope, traceID, *e); !valid {
			return nil, nil, errors.New("invalid Gateway registration")
		}
	}
	var spanID []byte
	if e.SpanID != "" {
		spanID, ok = parseID(e.SpanID, 8)
		if !ok {
			return nil, nil, errors.New("invalid Gateway audit Span")
		}
		e.SpanID = hex.EncodeToString(spanID)
	}
	return traceID, spanID, nil
}

var (
	errAuditConflict = errors.New("audit event conflicts with stored facts")
	errAuditBinding  = errors.New("audit Trace binding is unavailable")
	errAuditOrder    = errors.New("an earlier Gateway audit event has not arrived")
)

func (s service) gatewayAudit(w http.ResponseWriter, r *http.Request) {
	identity, authorized, err := s.authenticate(r, gatewayRole)
	if err != nil {
		writeDetail(w, http.StatusServiceUnavailable, "Authentication service unavailable")
		return
	}
	if !authorized {
		writeDetail(w, http.StatusUnauthorized, "Gateway authentication required")
		return
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxOTLPBytes))
	decoder.DisallowUnknownFields()
	var event auditEvent
	if decoder.Decode(&event) != nil || decoder.Decode(new(any)) != io.EOF {
		writeDetail(w, http.StatusBadRequest, "Invalid Gateway audit event")
		return
	}
	traceID, spanID, err := event.normalize(identity.Scope)
	if err != nil {
		writeDetail(w, http.StatusBadRequest, err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 8*time.Second)
	defer cancel()
	switch err := s.writeStore.ingestAudit(ctx, identity, traceID, spanID, event); {
	case errors.Is(err, errAuditConflict):
		writeDetail(w, http.StatusConflict, err.Error())
	case errors.Is(err, errAuditBinding), errors.Is(err, errAuditOrder):
		writeDetail(w, http.StatusServiceUnavailable, err.Error())
	case err != nil:
		writeDetail(w, http.StatusServiceUnavailable, "Tracing storage unavailable")
	default:
		w.WriteHeader(http.StatusNoContent)
	}
}

func (s store) ingestAudit(ctx context.Context, source authenticatedIdentity, traceID, spanID []byte, event auditEvent) error {
	current := time.Now()
	now, _ := unixNano(current)
	occurred, _ := unixNano(event.OccurredAt)
	event.OccurredAt = event.OccurredAt.UTC()
	parameters, binding := event.Parameters, event.Binding
	event.Parameters, event.Binding = nil, nil
	facts, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		trace := append(source.Scope.args(), traceID)
		var rootAgent, rootProducer string
		var registeredAt uint64
		err := lockedRow(tx, "SELECT root_agent_id, root_producer_id, registered_at_ns FROM tracing_traces WHERE "+scopeWhere+
			" AND trace_id = ?", trace...).Scan(&rootAgent, &rootProducer, &registeredAt)
		if errors.Is(err, sql.ErrNoRows) {
			if binding == nil || event.Sequence != 1 {
				return errAuditBinding
			}
			run, _ := binding.registration(source.Scope, traceID, event)
			if err := s.registerTx(tx, run); err != nil {
				if errors.Is(err, errRegistrationConflict) {
					return errAuditConflict
				}
				return err
			}
			rootAgent, registeredAt = run.RootAgentID, run.RegisteredAt
		} else if err != nil {
			return err
		}
		cutoff, _ := unixNano(current.Add(-s.policy.retention))
		if registeredAt <= cutoff {
			return nil
		}
		if rootProducer != "" && rootAgent != event.AgentID {
			var grant int
			err := tx.Raw("SELECT 1 FROM tracing_run_grants WHERE "+scopeWhere+
				" AND trace_id = ? AND agent_id = ? LIMIT 1", append(trace, event.AgentID)...).Row().Scan(&grant)
			if errors.Is(err, sql.ErrNoRows) {
				return errAuditConflict
			}
			if err != nil {
				return err
			}
		}

		eventKey := append(source.Scope.args(), source.ProducerID, event.EventID)
		var replay []byte
		err = lockedRow(tx, "SELECT facts FROM tracing_audit_events WHERE "+scopeWhere+
			" AND source_id = ? AND event_id = ?", eventKey...).Scan(&replay)
		if err == nil {
			if equivalentJSON(replay, facts) {
				return nil
			}
			return errAuditConflict
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}

		var previous auditEvent
		var callTrace, callSpan, previousFacts []byte
		var callSource string
		var lastUpdate uint64
		callKey := append(source.Scope.args(), event.CallID)
		err = lockedRow(tx, "SELECT trace_id, span_id, source_id, facts, updated_ns FROM tracing_tool_calls WHERE "+
			scopeWhere+" AND call_id = ?", callKey...).Scan(&callTrace, &callSpan, &callSource, &previousFacts, &lastUpdate)
		fresh := errors.Is(err, sql.ErrNoRows)
		if err != nil && !fresh {
			return err
		}
		if fresh {
			if event.Sequence != 1 {
				return errAuditOrder
			}
			if err := insertIfMissing(tx, source.Scope, "tracing_tool_calls", "call_id", map[string]any{
				"call_id": event.CallID, "trace_id": traceID, "span_id": spanID, "source_id": source.ProducerID,
				"agent_id": event.AgentID, "started_ns": s.timestamp(occurred), "updated_ns": s.timestamp(occurred), "facts": string(facts),
				"decision": event.Decision, "confirmation_state": event.Confirmation, "execution_state": event.ExecutionState,
			}); err != nil {
				return err
			}
		} else {
			if string(callTrace) != string(traceID) || string(callSpan) != string(spanID) || callSource != source.ProducerID ||
				json.Unmarshal(previousFacts, &previous) != nil || previous.AgentID != event.AgentID ||
				previous.ToolID != event.ToolID || previous.Initiator != event.Initiator {
				return errAuditConflict
			}
			if event.Sequence <= previous.Sequence {
				return errAuditConflict
			}
			if event.Sequence > previous.Sequence+1 {
				return errAuditOrder
			}
			if !validAuditTransition(previous, event) {
				return errAuditConflict
			}
			if err := tx.Table("tracing_tool_calls").Where(scopeWhere+" AND call_id = ?", callKey...).
				Updates(map[string]any{"updated_ns": s.timestamp(max(occurred, lastUpdate)), "facts": string(facts),
					"decision": event.Decision, "confirmation_state": event.Confirmation, "execution_state": event.ExecutionState}).Error; err != nil {
				return err
			}
		}
		if err := tx.Table("tracing_audit_events").Create(map[string]any{
			"tenant_id": source.Scope.TenantID, "environment_id": source.Scope.EnvironmentID, "project_id": source.Scope.ProjectID,
			"source_id": source.ProducerID, "event_id": event.EventID, "call_id": event.CallID,
			"sequence_no": event.Sequence, "facts": string(facts),
		}).Error; err != nil {
			return err
		}
		if !fresh || len(parameters) == 0 {
			return nil
		}
		deadline, err := s.contentDeadline(tx, source.Scope, traceID)
		if err != nil || now >= deadline {
			return err
		}
		return s.putContent(tx, source.Scope, traceID, "tool:"+event.CallID, fields{"parameters": parameters})
	})
}

func validAuditTransition(previous, event auditEvent) bool {
	switch event.Type {
	case "confirmation":
		return previous.Decision == "require_confirmation" && previous.Confirmation == "pending" &&
			event.Decision == previous.Decision && (event.Confirmation == "approved" ||
			event.Confirmation == "rejected" || event.Confirmation == "expired")
	case "revalidation":
		return previous.Decision == "require_confirmation" && previous.Confirmation == "approved" &&
			event.Confirmation == "approved" && event.Decision != "require_confirmation"
	case "forward_intent":
		return previous.Decision == "allow" && previous.ExecutionState == "not_executed" &&
			event.Confirmation == previous.Confirmation
	case "execution_result":
		return previous.ExecutionState == "executing" && event.Confirmation == previous.Confirmation &&
			event.PolicyVersion == previous.PolicyVersion
	case "reconciliation":
		return previous.ExecutionState == "unknown" && event.ExecutionState != "unknown" &&
			event.Confirmation == previous.Confirmation && event.PolicyVersion == previous.PolicyVersion
	default:
		return false
	}
}
