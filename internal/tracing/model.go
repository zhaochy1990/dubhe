package tracing

import (
	"bytes"
	"encoding/hex"
	"errors"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

type Scope struct {
	TenantID      string `json:"tenant_id" mapstructure:"tenant_id"`
	EnvironmentID string `json:"environment_id" mapstructure:"environment_id"`
	ProjectID     string `json:"project_id" mapstructure:"project_id"`
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

type actorIdentity struct {
	Kind string `json:"kind"`
	ID   string `json:"id"`
}

func (a actorIdentity) valid() bool {
	return (a.Kind == "user" || a.Kind == "workload") && opaqueID(a.ID)
}

type agentGrant struct {
	ProducerID string `json:"producerId"`
	AgentID    string `json:"agentId"`
}

type registrationRequest struct {
	Scope struct {
		TenantID      string `json:"tenantId"`
		EnvironmentID string `json:"environmentId"`
		ProjectID     string `json:"projectId"`
	} `json:"scope"`
	SessionID        string         `json:"sessionId"`
	TurnID           string         `json:"turnId"`
	RootAgentID      string         `json:"rootAgentId"`
	RootProducerID   string         `json:"rootProducerId"`
	TraceID          string         `json:"traceId"`
	RootSpanID       string         `json:"rootSpanId"`
	SessionStartedAt time.Time      `json:"sessionStartedAt"`
	RegisteredAt     time.Time      `json:"registeredAt"`
	AcceptUntil      time.Time      `json:"acceptUntil"`
	AgentGrants      []agentGrant   `json:"agentGrants,omitempty"`
	SessionInitiator *actorIdentity `json:"sessionInitiator,omitempty"`
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
	AgentGrants      []agentGrant
	SessionInitiator *actorIdentity
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
		(r.SessionInitiator != nil && !r.SessionInitiator.valid()) ||
		startedAt > registeredAt || registeredAt >= acceptUntil {
		return registration{}, false
	}
	slices.SortFunc(r.AgentGrants, func(a, b agentGrant) int {
		if a.ProducerID == b.ProducerID {
			return strings.Compare(a.AgentID, b.AgentID)
		}
		return strings.Compare(a.ProducerID, b.ProducerID)
	})
	for i, grant := range r.AgentGrants {
		if !opaqueID(grant.ProducerID) || !opaqueID(grant.AgentID) || (i > 0 && grant == r.AgentGrants[i-1]) {
			return registration{}, false
		}
	}
	return registration{
		AgentGrants:      r.AgentGrants,
		SessionInitiator: r.SessionInitiator,
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
	Metadata fields
	Content  fields
}

var errInvalidSpan = errors.New("invalid OTLP span")

func normalizeSpan(span *tracepb.Span) (spanRecord, error) {
	status := span.GetStatus().GetCode()
	if !otelID(span.GetTraceId(), 16) || !otelID(span.GetSpanId(), 8) ||
		(len(span.GetParentSpanId()) != 0 && !otelID(span.GetParentSpanId(), 8)) ||
		bytes.Equal(span.GetSpanId(), span.GetParentSpanId()) ||
		span.GetStartTimeUnixNano() == 0 || span.GetEndTimeUnixNano() < span.GetStartTimeUnixNano() ||
		status < tracepb.Status_STATUS_CODE_UNSET || status > tracepb.Status_STATUS_CODE_ERROR {
		return spanRecord{}, errInvalidSpan
	}
	agent, kind, metadata, content, ok := normalizeAttributes(span.GetAttributes())
	if !ok {
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
		Metadata: metadata,
		Content:  content,
	}, nil
}
