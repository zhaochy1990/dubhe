package tracing

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const scopeWhere = "tenant_id = ? AND environment_id = ? AND project_id = ?"

type store struct {
	db *gorm.DB
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
		return nil
	})
}

type spanGrant struct {
	rootSpanID string
	producerID string
	agentID    string
	acceptBy   uint64
}

func (g spanGrant) permits(span spanRecord, producer string, now uint64) bool {
	if g.producerID != producer || g.agentID != span.AgentID || g.acceptBy < now {
		return false
	}
	if span.Kind == "run" {
		return string(span.SpanID) == g.rootSpanID && len(span.ParentID) == 0
	}
	return string(span.SpanID) != g.rootSpanID && string(span.ParentID) == g.rootSpanID
}

func (s store) ingest(ctx context.Context, workload workload, spans []spanRecord) (int64, error) {
	now, _ := unixNano(time.Now())
	grants := make(map[string]spanGrant)
	var rejected int64
	err := s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		for _, span := range spans {
			traceKey := string(span.TraceID)
			grant, loaded := grants[traceKey]
			if !loaded {
				err := lockedRow(tx, `SELECT root_span_id, root_producer_id, root_agent_id, accept_until_ns
					FROM tracing_traces WHERE `+scopeWhere+` AND trace_id = ?`,
					append(workload.Scope.args(), span.TraceID)...).
					Scan(&grant.rootSpanID, &grant.producerID, &grant.agentID, &grant.acceptBy)
				if err != nil && !errors.Is(err, sql.ErrNoRows) {
					return err
				}
				grants[traceKey] = grant
			}
			if !grant.permits(span, workload.ProducerID, now) {
				rejected++
				continue
			}
			equivalent, err := s.persistSpan(tx, workload, span)
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

func (s store) persistSpan(tx *gorm.DB, workload workload, span spanRecord) (bool, error) {
	metadata := "{}"
	var outcome any
	if span.Outcome != "" {
		outcome = span.Outcome
		// Run outcomes have already been validated against a fixed set of values.
		metadata = `{"run_outcome":"` + span.Outcome + `"}`
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
		"status": span.Status, "metadata": metadata,
	}); err != nil {
		return false, err
	}

	var existing spanRecord
	var producer string
	var storedMetadata []byte
	err := lockedRow(tx, `SELECT trace_id, parent_span_id, producer_id, agent_id, kind, start_ns, end_ns, status, metadata
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
		!equivalentMetadata(storedMetadata, span.Outcome) {
		return false, nil
	}
	if span.Kind == "run" {
		err := tx.Table("tracing_traces").
			Where(scopeWhere+" AND trace_id = ?", append(workload.Scope.args(), span.TraceID)...).
			Updates(map[string]any{"start_ns": start, "end_ns": end, "run_state": outcome}).Error
		if err != nil {
			return false, err
		}
	}
	return true, nil
}

func equivalentMetadata(raw []byte, outcome string) bool {
	var metadata map[string]string
	if err := json.Unmarshal(raw, &metadata); err != nil || metadata == nil {
		return false
	}
	if outcome == "" {
		return len(metadata) == 0
	}
	return len(metadata) == 1 && metadata["run_outcome"] == outcome
}
