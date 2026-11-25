package tracing

import (
	"encoding/hex"
	"errors"
	"strings"
	"time"
	"unicode/utf8"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

type Scope struct {
	TenantID      string `json:"tenant_id"`
	EnvironmentID string `json:"environment_id"`
	ProjectID     string `json:"project_id"`
}

func (s Scope) valid() bool {
	return opaqueID(s.TenantID) && opaqueID(s.EnvironmentID) && opaqueID(s.ProjectID)
}

func (s Scope) args() []any {
	return []any{s.TenantID, s.EnvironmentID, s.ProjectID}
}

func opaqueID(id string) bool {
	return id != "" && len(id) <= 128 && !strings.ContainsRune(id, 0) && utf8.ValidString(id)
}

func otelID(id []byte, size int) bool {
	return len(id) == size && strings.Trim(string(id), "\x00") != ""
}

func parseID(raw string, size int) ([]byte, bool) {
	id, err := hex.DecodeString(raw)
	return id, err == nil && otelID(id, size)
}

type registrationRequest struct {
	Scope struct {
		TenantID      string `json:"tenantId"`
		EnvironmentID string `json:"environmentId"`
		ProjectID     string `json:"projectId"`
	} `json:"scope"`
	SessionID        string    `json:"sessionId"`
	TurnID           string    `json:"turnId"`
	RootAgentID      string    `json:"rootAgentId"`
	RootProducerID   string    `json:"rootProducerId"`
	TraceID          string    `json:"traceId"`
	RootSpanID       string    `json:"rootSpanId"`
	SessionStartedAt time.Time `json:"sessionStartedAt"`
	RegisteredAt     time.Time `json:"registeredAt"`
	AcceptUntil      time.Time `json:"acceptUntil"`
}

type runBinding struct {
	SessionID      string
	TurnID         string
	RootSpanID     string
	RootProducerID string
	RootAgentID    string
	RegisteredAt   uint64
	AcceptUntil    uint64
}

type registration struct {
	Scope            Scope
	TraceID          []byte
	SessionStartedAt uint64
	runBinding
}

func (r registrationRequest) normalize() (registration, bool) {
	scope := Scope{r.Scope.TenantID, r.Scope.EnvironmentID, r.Scope.ProjectID}
	startedAt, startOK := unixNano(r.SessionStartedAt)
	registeredAt, registerOK := unixNano(r.RegisteredAt)
	acceptUntil, untilOK := unixNano(r.AcceptUntil)
	traceID, traceOK := parseID(r.TraceID, 16)
	spanID, spanOK := parseID(r.RootSpanID, 8)
	if !scope.valid() || !opaqueID(r.SessionID) || !opaqueID(r.TurnID) ||
		!opaqueID(r.RootAgentID) || !opaqueID(r.RootProducerID) ||
		!traceOK || !spanOK || !startOK || !registerOK || !untilOK ||
		startedAt > registeredAt || registeredAt >= acceptUntil {
		return registration{}, false
	}
	return registration{
		Scope:            scope,
		TraceID:          traceID,
		SessionStartedAt: startedAt,
		runBinding: runBinding{
			SessionID:      r.SessionID,
			TurnID:         r.TurnID,
			RootSpanID:     string(spanID),
			RootProducerID: r.RootProducerID,
			RootAgentID:    r.RootAgentID,
			RegisteredAt:   registeredAt,
			AcceptUntil:    acceptUntil,
		},
	}, true
}

func unixNano(t time.Time) (uint64, bool) {
	const maxSeconds = ^uint64(0) / uint64(time.Second)
	seconds := t.Unix()
	if seconds < 0 || uint64(seconds) > maxSeconds {
		return 0, false
	}
	nanos := uint64(seconds)*uint64(time.Second) + uint64(t.Nanosecond())
	return nanos, nanos >= uint64(seconds)*uint64(time.Second)
}

type spanRecord struct {
	TraceID  []byte
	SpanID   []byte
	ParentID []byte
	AgentID  string
	Kind     string
	StartNS  uint64
	EndNS    uint64
	Status   tracepb.Status_StatusCode
	Outcome  string
}

var errInvalidSpan = errors.New("invalid OTLP span")

func normalizeSpan(span *tracepb.Span) (spanRecord, error) {
	status := span.GetStatus().GetCode()
	if !otelID(span.GetTraceId(), 16) || !otelID(span.GetSpanId(), 8) ||
		(len(span.GetParentSpanId()) != 0 && !otelID(span.GetParentSpanId(), 8)) ||
		span.GetStartTimeUnixNano() == 0 || span.GetEndTimeUnixNano() < span.GetStartTimeUnixNano() ||
		status < tracepb.Status_STATUS_CODE_UNSET || status > tracepb.Status_STATUS_CODE_ERROR {
		return spanRecord{}, errInvalidSpan
	}
	attributes := make(map[string]string, 3)
	for _, item := range span.GetAttributes() {
		key := item.GetKey()
		switch key {
		case "dubhe.span.kind", "dubhe.agent.id", "dubhe.run.outcome":
			_, duplicate := attributes[key]
			value, isString := item.GetValue().GetValue().(*commonpb.AnyValue_StringValue)
			if duplicate || !isString {
				return spanRecord{}, errInvalidSpan
			}
			attributes[key] = value.StringValue
		}
	}
	agent := attributes["dubhe.agent.id"]
	kind := attributes["dubhe.span.kind"]
	outcome, hasOutcome := attributes["dubhe.run.outcome"]
	if !opaqueID(agent) || (kind != "run" && kind != "llm") ||
		(hasOutcome && (kind != "run" || (outcome != "completed" && outcome != "failed" && outcome != "cancelled"))) {
		return spanRecord{}, errInvalidSpan
	}
	return spanRecord{
		TraceID:  span.GetTraceId(),
		SpanID:   span.GetSpanId(),
		ParentID: span.GetParentSpanId(),
		AgentID:  agent,
		Kind:     kind,
		StartNS:  span.GetStartTimeUnixNano(),
		EndNS:    span.GetEndTimeUnixNano(),
		Status:   status,
		Outcome:  outcome,
	}, nil
}
