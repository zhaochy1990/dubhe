package tracing

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const scopeWhere = "tenant_id = ? AND environment_id = ? AND project_id = ?"

type store struct {
	db     *gorm.DB
	policy *policies
}

var errRegistrationConflict = errors.New("trusted run registration conflicts with an existing binding")

func insertIfMissing(tx *gorm.DB, scope Scope, table, key string, values map[string]any) error {
	values["tenant_id"] = scope.TenantID
	values["environment_id"] = scope.EnvironmentID
	values["project_id"] = scope.ProjectID
	column := clause.Column{Name: key}
	return tx.Table(table).Clauses(clause.OnConflict{
		DoNothing: true,
		// MySQL implements the no-op with an assignment to the existing row.
		DoUpdates: clause.Set{{Column: column, Value: column}},
	}).Create(values).Error
}

func (s store) register(ctx context.Context, run registration) error {
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		return s.registerTx(tx, run)
	})
}

func (s store) registerTx(tx *gorm.DB, run registration) error {
	if err := insertIfMissing(tx, run.Scope, "tracing_sessions", "session_id", map[string]any{
		"session_id": run.SessionID, "root_agent_id": run.RootAgentID, "started_at_ns": s.timestamp(run.SessionStartedAt),
	}); err != nil {
		return err
	}

	var sessionAgent string
	var startedAt uint64
	err := lockedRow(tx, `SELECT root_agent_id, started_at_ns FROM tracing_sessions
		WHERE `+scopeWhere+` AND session_id = ?`, append(run.Scope.args(), run.SessionID)...).
		Scan(&sessionAgent, &startedAt)
	if err != nil {
		return err
	}
	if sessionAgent != run.RootAgentID || startedAt != run.SessionStartedAt {
		return errRegistrationConflict
	}

	var exists int
	err = tx.Raw("SELECT 1 FROM tracing_traces WHERE "+scopeWhere+" AND trace_id = ?",
		append(run.Scope.args(), run.TraceID)...).Row().Scan(&exists)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if err := insertIfMissing(tx, run.Scope, "tracing_traces", "trace_id", map[string]any{
		"trace_id": run.TraceID, "session_id": run.SessionID, "turn_id": run.TurnID,
		"root_span_id": []byte(run.RootSpanID), "root_producer_id": run.RootProducerID,
		"root_agent_id": run.RootAgentID, "registered_at_ns": s.timestamp(run.RegisteredAt),
		"accept_until_ns": s.timestamp(run.AcceptUntil),
	}); err != nil {
		return err
	}

	var existing runBinding
	err = lockedRow(tx, `SELECT session_id, turn_id, root_span_id, root_producer_id, root_agent_id,
		registered_at_ns, accept_until_ns FROM tracing_traces
		WHERE `+scopeWhere+` AND trace_id = ?`, append(run.Scope.args(), run.TraceID)...).
		Scan(&existing.SessionID, &existing.TurnID, &existing.RootSpanID, &existing.RootProducerID,
			&existing.RootAgentID, &existing.RegisteredAt, &existing.AcceptUntil)
	// A conflict on (session_id, turn_id) can leave no row for this trace_id.
	if errors.Is(err, sql.ErrNoRows) {
		return errRegistrationConflict
	}
	if err != nil {
		return err
	}
	if existing != run.runBinding {
		return errRegistrationConflict
	}
	if err := syncRunGrants(tx, run, exists == 0); err != nil {
		return err
	}
	if run.SessionInitiator != nil {
		if err := bindInitiator(tx, run); err != nil {
			return err
		}
	}
	if exists == 0 {
		return s.enableContent(tx, run)
	}
	return nil
}

func bindInitiator(tx *gorm.DB, run registration) error {
	if err := insertIfMissing(tx, run.Scope, "tracing_session_initiators", "session_id", map[string]any{
		"session_id": run.SessionID, "actor_kind": run.SessionInitiator.Kind, "actor_id": run.SessionInitiator.ID,
	}); err != nil {
		return err
	}
	var actor actorIdentity
	err := lockedRow(tx, "SELECT actor_kind, actor_id FROM tracing_session_initiators WHERE "+scopeWhere+" AND session_id = ?",
		append(run.Scope.args(), run.SessionID)...).Scan(&actor.Kind, &actor.ID)
	if err != nil {
		return err
	}
	if actor != *run.SessionInitiator {
		return errRegistrationConflict
	}
	return nil
}

func syncRunGrants(tx *gorm.DB, run registration, fresh bool) error {
	if fresh {
		for _, grant := range run.AgentGrants {
			if err := insertIfMissing(tx, run.Scope, "tracing_run_grants", "agent_id", map[string]any{
				"trace_id": run.TraceID, "producer_id": grant.ProducerID, "agent_id": grant.AgentID,
			}); err != nil {
				return err
			}
		}
		return nil
	}
	rows, err := tx.Raw(`SELECT producer_id, agent_id FROM tracing_run_grants WHERE `+scopeWhere+
		` AND trace_id = ? ORDER BY producer_id, agent_id`, append(run.Scope.args(), run.TraceID)...).Rows()
	if err != nil {
		return err
	}
	defer rows.Close()
	var stored []agentGrant
	for rows.Next() {
		var grant agentGrant
		if err := rows.Scan(&grant.ProducerID, &grant.AgentID); err != nil {
			return err
		}
		stored = append(stored, grant)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if !slices.Equal(stored, run.AgentGrants) {
		return errRegistrationConflict
	}
	return nil
}

type spanGrant struct {
	rootSpanID string
	producerID string
	agentID    string
	acceptBy   uint64
	contentBy  uint64
	agents     map[string]bool
}

func (g spanGrant) permits(span spanRecord, producer string, now uint64) bool {
	if now >= g.acceptBy {
		return false
	}
	if span.Kind == "run" {
		return g.producerID == producer && g.agentID == span.AgentID &&
			string(span.SpanID) == g.rootSpanID && len(span.ParentID) == 0
	}
	return (g.agents[span.AgentID] || (g.producerID == producer && g.agentID == span.AgentID)) &&
		string(span.SpanID) != g.rootSpanID && len(span.ParentID) != 0
}

func (s store) loadSpanGrant(tx *gorm.DB, workload authenticatedIdentity, traceID []byte, cutoff any) (spanGrant, error) {
	args := append(workload.Scope.args(), traceID)
	grant := spanGrant{agents: make(map[string]bool)}
	err := lockedRow(tx, `SELECT root_span_id, root_producer_id, root_agent_id, accept_until_ns
		FROM tracing_traces WHERE `+scopeWhere+` AND trace_id = ? AND registered_at_ns > ?`,
		append(args, cutoff)...).Scan(&grant.rootSpanID, &grant.producerID, &grant.agentID, &grant.acceptBy)
	if errors.Is(err, sql.ErrNoRows) {
		return grant, nil
	}
	if err != nil {
		return grant, err
	}
	grant.contentBy, err = s.contentDeadline(tx, workload.Scope, traceID)
	if err != nil {
		return grant, err
	}
	query := "SELECT agent_id FROM tracing_run_grants WHERE " + scopeWhere + " AND trace_id = ? AND producer_id = ?"
	if tx.Dialector.Name() == "mysql" {
		query += " FOR UPDATE"
	}
	rows, err := tx.Raw(query, append(args, workload.ProducerID)...).Rows()
	if err != nil {
		return grant, err
	}
	defer rows.Close()
	for rows.Next() {
		var agent string
		if err := rows.Scan(&agent); err != nil {
			return grant, err
		}
		grant.agents[agent] = true
	}
	return grant, rows.Err()
}

func (s store) ingest(ctx context.Context, workload authenticatedIdentity, spans []spanRecord) (int64, error) {
	current := time.Now()
	now, _ := unixNano(current)
	grants := make(map[string]spanGrant)
	policy := s.policy.scopes[workload.Scope]
	var rejected int64
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		for _, span := range spans {
			traceKey := string(span.TraceID)
			grant, loaded := grants[traceKey]
			if !loaded {
				var err error
				grant, err = s.loadSpanGrant(tx, workload, span.TraceID, s.cutoff(current))
				if err != nil {
					return err
				}
				grants[traceKey] = grant
			}
			if !grant.permits(span, workload.ProducerID, now) || !policy.normalizeCatalog(&span) {
				rejected++
				continue
			}
			equivalent, err := s.persistSpan(tx, workload, span, now < grant.contentBy)
			if err != nil {
				return err
			}
			if !equivalent {
				rejected++
			}
		}
		return nil
	})
	return rejected, err
}

func (policy scopePolicy) normalizeCatalog(span *spanRecord) bool {
	if span.Kind == "tool_client" {
		return slices.Contains(policy.toolIDs, span.Metadata["tool_id"].(string))
	}
	if span.Kind != "llm" {
		return true
	}
	name, _ := span.Metadata["model_name"].(string)
	id, _ := span.Metadata["model_id"].(string)
	catalogID, known := policy.models[name]
	switch {
	case name != "" && known:
		if id != "" && id != catalogID {
			return false
		}
		span.Metadata["model_id"] = catalogID
	default:
		delete(span.Metadata, "model_name")
		if !policy.modelIDs[id] {
			delete(span.Metadata, "model_id")
		}
	}
	return true
}

func (s store) persistSpan(tx *gorm.DB, workload authenticatedIdentity, span spanRecord, capture bool) (bool, error) {
	metadata, err := json.Marshal(span.Metadata)
	if err != nil {
		return false, err
	}
	var existingID []byte
	err = lockedRow(tx, "SELECT trace_id FROM tracing_spans WHERE "+scopeWhere+" AND span_id = ?",
		append(workload.Scope.args(), span.SpanID)...).Scan(&existingID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return false, err
	}
	if err := checkSpanParent(tx, workload.Scope, span); err != nil {
		if errors.Is(err, errInvalidSpan) {
			return false, nil
		}
		return false, err
	}
	var parent any
	if len(span.ParentID) != 0 {
		parent = span.ParentID
	}
	start := s.timestamp(span.StartNS)
	end := s.timestamp(span.EndNS)
	if err := insertIfMissing(tx, workload.Scope, "tracing_spans", "span_id", map[string]any{
		"span_id": span.SpanID, "trace_id": span.TraceID, "parent_span_id": parent, "producer_id": workload.ProducerID,
		"agent_id": span.AgentID, "kind": span.Kind, "start_ns": start, "end_ns": end,
		"status": span.Status, "metadata": string(metadata),
	}); err != nil {
		return false, err
	}

	var existing spanRecord
	var producer string
	var storedMetadata []byte
	err = lockedRow(tx, `SELECT trace_id, parent_span_id, producer_id, agent_id, kind, start_ns, end_ns, status, metadata
		FROM tracing_spans WHERE `+scopeWhere+` AND span_id = ?`, append(workload.Scope.args(), span.SpanID)...).
		Scan(&existing.TraceID, &existing.ParentID, &producer, &existing.AgentID, &existing.Kind,
			&existing.StartNS, &existing.EndNS, &existing.Status, &storedMetadata)
	if err != nil {
		return false, err
	}

	if string(existing.TraceID) != string(span.TraceID) ||
		string(existing.ParentID) != string(span.ParentID) ||
		producer != workload.ProducerID || existing.AgentID != span.AgentID ||
		existing.Kind != span.Kind || existing.StartNS != span.StartNS ||
		existing.EndNS != span.EndNS || existing.Status != span.Status ||
		!equivalentJSON(storedMetadata, metadata) {
		return false, nil
	}
	if existingID == nil && capture && len(span.Content) != 0 {
		if err := s.putContent(tx, workload.Scope, span.TraceID, "span:"+hex.EncodeToString(span.SpanID), span.Content); err != nil {
			return false, err
		}
	}
	if span.Kind == "run" {
		err := tx.Table("tracing_traces").
			Where(scopeWhere+" AND trace_id = ?", append(workload.Scope.args(), span.TraceID)...).
			Updates(map[string]any{"start_ns": start, "end_ns": end, "run_state": span.Metadata["run_outcome"]}).Error
		if err != nil {
			return false, err
		}
	}
	return true, nil
}

func equivalentJSON(left, right []byte) bool {
	var a, b any
	return json.Unmarshal(left, &a) == nil && json.Unmarshal(right, &b) == nil && reflect.DeepEqual(a, b)
}

// Known parents must belong to the same Trace. Walking the known chain also
// catches a cycle when an out-of-order ancestor eventually arrives.
func checkSpanParent(tx *gorm.DB, scope Scope, span spanRecord) error {
	for parent := span.ParentID; len(parent) != 0; {
		if string(parent) == string(span.SpanID) {
			return errInvalidSpan
		}
		var traceID, next []byte
		err := lockedRow(tx, "SELECT trace_id, parent_span_id FROM tracing_spans WHERE "+scopeWhere+" AND span_id = ?",
			append(scope.args(), parent)...).Scan(&traceID, &next)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		if string(traceID) != string(span.TraceID) {
			return errInvalidSpan
		}
		parent = next
	}
	return nil
}
