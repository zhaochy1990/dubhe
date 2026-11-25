package tracing

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"
)

const scopeWhere = "tenant_id = ? AND environment_id = ? AND project_id = ?"

type store struct {
	db *sql.DB
}

var errRegistrationConflict = errors.New("trusted run registration conflicts with an existing binding")

func (s store) register(ctx context.Context, run registration) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx, `
		INSERT INTO tracing_sessions
			(tenant_id, environment_id, project_id, session_id, root_agent_id, started_at_ns)
		VALUES (?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE session_id = session_id`,
		append(run.Scope.args(), run.SessionID, run.RootAgentID, run.SessionStartedAt)...)
	if err != nil {
		return err
	}
	var sessionAgent string
	var startedAt uint64
	err = tx.QueryRowContext(ctx, `
		SELECT root_agent_id, started_at_ns FROM tracing_sessions
		WHERE `+scopeWhere+` AND session_id = ? FOR UPDATE`,
		append(run.Scope.args(), run.SessionID)...).Scan(&sessionAgent, &startedAt)
	if err != nil {
		return err
	}
	if sessionAgent != run.RootAgentID || startedAt != run.SessionStartedAt {
		return errRegistrationConflict
	}

	_, err = tx.ExecContext(ctx, `
		INSERT INTO tracing_traces
			(tenant_id, environment_id, project_id, trace_id, session_id, turn_id, root_span_id,
			 root_producer_id, root_agent_id, registered_at_ns, accept_until_ns)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE trace_id = trace_id`,
		append(run.Scope.args(), run.TraceID, run.SessionID, run.TurnID, []byte(run.RootSpanID),
			run.RootProducerID, run.RootAgentID, run.RegisteredAt, run.AcceptUntil)...)
	if err != nil {
		return err
	}

	var existing runBinding
	err = tx.QueryRowContext(ctx, `
		SELECT session_id, turn_id, root_span_id, root_producer_id, root_agent_id,
			registered_at_ns, accept_until_ns FROM tracing_traces
		WHERE `+scopeWhere+` AND trace_id = ? FOR UPDATE`, append(run.Scope.args(), run.TraceID)...).
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
	return tx.Commit()
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
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	now, _ := unixNano(time.Now())
	grants := make(map[string]spanGrant)
	var rejected int64
	for _, span := range spans {
		traceKey := string(span.TraceID)
		grant, loaded := grants[traceKey]
		if !loaded {
			err := tx.QueryRowContext(ctx, `
				SELECT root_span_id, root_producer_id, root_agent_id, accept_until_ns
				FROM tracing_traces WHERE `+scopeWhere+` AND trace_id = ? FOR UPDATE`,
				append(workload.Scope.args(), span.TraceID)...).
				Scan(&grant.rootSpanID, &grant.producerID, &grant.agentID, &grant.acceptBy)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return 0, err
			}
			grants[traceKey] = grant
		}
		if !grant.permits(span, workload.ProducerID, now) {
			rejected++
			continue
		}
		equivalent, err := persistSpan(ctx, tx, workload, span)
		if err != nil {
			return 0, err
		}
		if !equivalent {
			rejected++
		}
	}
	return rejected, tx.Commit()
}

func persistSpan(ctx context.Context, tx *sql.Tx, workload workload, span spanRecord) (bool, error) {
	metadata := "{}"
	var outcome sql.NullString
	if span.Outcome != "" {
		outcome = sql.NullString{String: span.Outcome, Valid: true}
		// Run outcomes have already been validated against a fixed set of values.
		metadata = `{"run_outcome":"` + span.Outcome + `"}`
	}
	var parent any
	if len(span.ParentID) != 0 {
		parent = span.ParentID
	}
	_, err := tx.ExecContext(ctx, `
		INSERT INTO tracing_spans
			(tenant_id, environment_id, project_id, span_id, trace_id, parent_span_id, producer_id,
			 agent_id, kind, start_ns, end_ns, status, metadata)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE span_id = span_id`,
		append(workload.Scope.args(), span.SpanID, span.TraceID, parent, workload.ProducerID,
			span.AgentID, span.Kind, span.StartNS, span.EndNS, span.Status, metadata)...)
	if err != nil {
		return false, err
	}
	var existing spanRecord
	var producer string
	var storedMetadata []byte
	err = tx.QueryRowContext(ctx, `
		SELECT trace_id, parent_span_id, producer_id, agent_id, kind, start_ns, end_ns, status, metadata
		FROM tracing_spans WHERE `+scopeWhere+` AND span_id = ? FOR UPDATE`,
		append(workload.Scope.args(), span.SpanID)...).
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
		_, err := tx.ExecContext(ctx, `
			UPDATE tracing_traces SET start_ns = ?, end_ns = ?, run_state = ?
			WHERE `+scopeWhere+` AND trace_id = ?`,
			append([]any{span.StartNS, span.EndNS, outcome},
				append(workload.Scope.args(), span.TraceID)...)...)
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
