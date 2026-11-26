package tracing

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"gorm.io/gorm"
)

const day = 24 * time.Hour

// ScopePolicy is controlled by the Tracing operator. Map model names to their
// trusted catalog IDs and Agent IDs to the names shown in authorized views.
type ScopePolicy struct {
	Scope               Scope          `mapstructure:"scope"`
	ContentDays         int            `mapstructure:"content_days"`
	ContentEnabledSince string         `mapstructure:"content_enabled_since"`
	AgentCatalog        []AgentDisplay `mapstructure:"agents"`
	ModelCatalog        []ModelDisplay `mapstructure:"models"`
	ToolIDs             []string       `mapstructure:"tool_ids"`
}

type AgentDisplay struct {
	ID   string `json:"id" mapstructure:"id"`
	Name string `json:"name" mapstructure:"name"`
}

type ModelDisplay struct {
	ID   string `mapstructure:"id"`
	Name string `mapstructure:"name"`
}

type Options struct {
	TraceDays        int                   `mapstructure:"trace_days"`
	Scopes           []ScopePolicy         `mapstructure:"scopes"`
	Outboxes         []GatewayOutboxConfig `mapstructure:"gateway_outboxes"`
	CursorSigningKey string                `mapstructure:"cursor_signing_key"`
}

type scopePolicy struct {
	contentDays     int
	contentStartsNS uint64
	agents          map[string]string
	models          map[string]string
	modelIDs        map[string]bool
	toolIDs         []string
}

type policies struct {
	retention time.Duration
	scopes    map[Scope]scopePolicy
}

func compilePolicies(options Options) (*policies, error) {
	days := options.TraceDays
	if days == 0 {
		days = 30
	}
	if days < 1 || days > 365 {
		return nil, errors.New("trace_days must be between 1 and 365")
	}
	result := &policies{retention: time.Duration(days) * day, scopes: make(map[Scope]scopePolicy)}
	for _, policy := range options.Scopes {
		if _, exists := result.scopes[policy.Scope]; exists || !policy.Scope.valid() ||
			policy.ContentDays < 0 || policy.ContentDays > days {
			return nil, errors.New("invalid or duplicate Tracing scope policy")
		}
		scope := scopePolicy{
			contentDays: policy.ContentDays, agents: make(map[string]string),
			models: make(map[string]string), modelIDs: make(map[string]bool), toolIDs: append([]string(nil), policy.ToolIDs...),
		}
		if policy.ContentEnabledSince != "" {
			starts, err := time.Parse(time.RFC3339Nano, policy.ContentEnabledSince)
			ns, ok := unixNano(starts)
			if err != nil || !ok {
				return nil, errors.New("content_enabled_since must be an RFC3339 timestamp")
			}
			scope.contentStartsNS = ns
		}
		for _, agent := range policy.AgentCatalog {
			if _, exists := scope.agents[agent.ID]; exists || !opaqueID(agent.ID) || !opaqueID(agent.Name) {
				return nil, errors.New("invalid or duplicate Agent catalog entry")
			}
			scope.agents[agent.ID] = agent.Name
		}
		for _, model := range policy.ModelCatalog {
			if _, exists := scope.models[model.Name]; exists || !validModelName(model.Name) || !opaqueID(model.ID) {
				return nil, errors.New("invalid or duplicate model catalog entry")
			}
			scope.models[model.Name] = model.ID
			scope.modelIDs[model.ID] = true
		}
		for _, toolID := range policy.ToolIDs {
			if !opaqueID(toolID) {
				return nil, errors.New("invalid tool catalog entry")
			}
		}
		result.scopes[policy.Scope] = scope
	}
	return result, nil
}

func (s store) cutoff(now time.Time) any {
	ns, _ := unixNano(now.Add(-s.policy.retention))
	return s.timestamp(ns)
}

func (s store) enableContent(tx *gorm.DB, run registration) error {
	policy := s.policy.scopes[run.Scope]
	days := policy.contentDays
	if days == 0 || run.RegisteredAt < policy.contentStartsNS {
		return nil
	}
	expires := run.RegisteredAt + uint64(time.Duration(days)*day)
	if expires < run.RegisteredAt {
		return errors.New("content expiry exceeds supported timestamp range")
	}
	return insertIfMissing(tx, run.Scope, "tracing_trace_content", "trace_id", map[string]any{
		"trace_id": run.TraceID, "expires_at_ns": s.timestamp(expires),
	})
}

func (s store) contentDeadline(tx *gorm.DB, scope Scope, traceID []byte) (uint64, error) {
	if s.policy.scopes[scope].contentDays == 0 {
		return 0, nil
	}
	var deadline uint64
	err := lockedRow(tx, `SELECT expires_at_ns FROM tracing_trace_content
		WHERE `+scopeWhere+` AND trace_id = ?`, append(scope.args(), traceID)...).Scan(&deadline)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return deadline, err
}

func (s store) putContent(tx *gorm.DB, scope Scope, traceID []byte, id string, content fields) error {
	payload, err := json.Marshal(content)
	if err != nil {
		return err
	}
	return insertIfMissing(tx, scope, "tracing_content", "object_id", map[string]any{
		"trace_id": traceID, "object_id": id, "payload": string(payload),
	})
}

func (s store) readContent(tx *gorm.DB, reader authenticatedIdentity, traceID []byte, node nodeView) (json.RawMessage, string, error) {
	var allowed bool
	switch node.Kind {
	case "llm":
		allowed = reader.ModelInput
	case "tool":
		allowed = reader.ToolParameters
	default:
		return node.Metadata, "", nil
	}
	if !allowed {
		return node.Metadata, "forbidden", nil
	}
	if s.policy.scopes[reader.Scope].contentDays == 0 {
		return node.Metadata, "not_captured", nil
	}
	now, _ := unixNano(time.Now())
	args := append([]any{s.timestamp(now), node.NodeID}, reader.Scope.args()...)
	args = append(args, traceID)
	var deadline uint64
	var captured bool
	var payload []byte
	err := tx.Raw(`SELECT p.expires_at_ns, c.object_id IS NOT NULL,
		CASE WHEN p.expires_at_ns > ? THEN c.payload END
		FROM tracing_trace_content p LEFT JOIN tracing_content c ON `+
		scopeJoin("c", "p")+` AND c.trace_id = p.trace_id
		AND c.object_id = ?
		WHERE p.tenant_id = ? AND p.environment_id = ? AND p.project_id = ? AND p.trace_id = ?`, args...).Row().Scan(&deadline, &captured, &payload)
	if errors.Is(err, sql.ErrNoRows) {
		return node.Metadata, "not_captured", nil
	}
	if err != nil {
		return nil, "", err
	}
	if !captured {
		return node.Metadata, "not_captured", nil
	}
	if deadline <= now {
		return node.Metadata, "expired", nil
	}
	var metadata, content map[string]json.RawMessage
	if err := json.Unmarshal(node.Metadata, &metadata); err != nil {
		return nil, "", err
	}
	if err := json.Unmarshal(payload, &content); err != nil {
		return nil, "", err
	}
	if len(content) == 0 {
		return node.Metadata, "expired", nil
	}
	for _, key := range contentKeys(node.Kind) {
		if value, exists := content[key]; exists {
			metadata[key] = value
		}
	}
	data, err := json.Marshal(metadata)
	return data, "available", err
}

func contentKeys(kind string) []string {
	switch kind {
	case "llm":
		return []string{"messages", "system_instructions", "tool_definitions"}
	case "tool":
		return []string{"parameters"}
	default:
		return nil
	}
}

// PurgeContent clears expired payloads, keeping their object IDs and deadline
// until the Trace expires so readers can distinguish expired and absent content.
// Removing a disabled or no longer readable policy cascades to its payloads.
// Retries cannot reopen a capture window.
func PurgeContent(ctx context.Context, db *gorm.DB, options Options) error {
	policy, err := compilePolicies(options)
	if err != nil {
		return err
	}
	s := store{db: db, policy: policy}
	now := time.Now()
	ns, _ := unixNano(now)
	query := `DELETE FROM tracing_trace_content WHERE EXISTS (
		SELECT 1 FROM tracing_traces t WHERE ` + scopeJoin("t", "tracing_trace_content") + `
		AND t.trace_id = tracing_trace_content.trace_id AND t.registered_at_ns <= ?)`
	args := []any{s.cutoff(now)}
	var enabled []string
	for scope, entry := range policy.scopes {
		if entry.contentDays != 0 {
			enabled = append(enabled, "("+scopeWhere+")")
			args = append(args, scope.args()...)
		}
	}
	if len(enabled) == 0 {
		query = "DELETE FROM tracing_trace_content"
		args = nil
	} else {
		query += " OR NOT (" + strings.Join(enabled, " OR ") + ")"
	}
	err = db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec(query, args...).Error; err != nil {
			return err
		}
		return tx.Exec(`UPDATE tracing_content SET payload = '{}'
			WHERE CAST(payload AS CHAR) <> '{}' AND EXISTS (
			SELECT 1 FROM tracing_trace_content p WHERE `+scopeJoin("p", "tracing_content")+`
			AND p.trace_id = tracing_content.trace_id AND p.expires_at_ns <= ?)`, s.timestamp(ns)).Error
	})
	if err != nil {
		return fmt.Errorf("purge Tracing content: %w", err)
	}
	return nil
}

func scopeJoin(left, right string) string {
	return left + ".tenant_id = " + right + ".tenant_id AND " +
		left + ".environment_id = " + right + ".environment_id AND " +
		left + ".project_id = " + right + ".project_id"
}
