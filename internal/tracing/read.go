package tracing

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"

	"gorm.io/gorm"
)

type traceView struct {
	Scope
	TraceID   string     `json:"trace_id"`
	SessionID string     `json:"session_id"`
	TurnID    string     `json:"turn_id"`
	RootAgent string     `json:"root_agent_id"`
	StartNS   *uint64    `json:"start_ns"`
	EndNS     *uint64    `json:"end_ns"`
	RunState  *string    `json:"run_state"`
	Nodes     []nodeView `json:"nodes"`
}

type nodeView struct {
	NodeID       string          `json:"node_id"`
	TraceID      string          `json:"trace_id"`
	SpanID       string          `json:"span_id"`
	ParentSpanID *string         `json:"parent_span_id"`
	ProducerID   string          `json:"producer_id"`
	AgentID      string          `json:"agent_id"`
	Kind         string          `json:"kind"`
	StartNS      uint64          `json:"start_ns"`
	EndNS        uint64          `json:"end_ns"`
	Status       int             `json:"status"`
	Metadata     json.RawMessage `json:"metadata"`
}

type nodeDetail struct {
	Scope
	nodeView
}

func scanNode(row interface{ Scan(...any) error }) (nodeView, error) {
	var view nodeView
	var spanID, traceID, parent, metadata []byte
	if err := row.Scan(&spanID, &traceID, &parent, &view.ProducerID, &view.AgentID, &view.Kind,
		&view.StartNS, &view.EndNS, &view.Status, &metadata); err != nil {
		return nodeView{}, err
	}
	view.Metadata = metadata
	view.TraceID = hex.EncodeToString(traceID)
	view.SpanID = hex.EncodeToString(spanID)
	view.NodeID = "span:" + view.SpanID
	if len(parent) != 0 {
		value := hex.EncodeToString(parent)
		view.ParentSpanID = &value
	}
	return view, nil
}

func (s store) trace(ctx context.Context, reader authenticatedIdentity, traceID []byte) (*traceView, error) {
	if len(reader.AgentIDs) == 0 {
		return nil, nil
	}
	var result *traceView
	err := s.readTransaction(ctx, func(tx *gorm.DB) error {
		args := appendAgents(append(reader.Scope.args(), traceID), reader.AgentIDs)
		agents := agentPlaceholders(len(reader.AgentIDs))
		view := traceView{Scope: reader.Scope, TraceID: hex.EncodeToString(traceID), Nodes: []nodeView{}}
		err := tx.Raw(`SELECT session_id, turn_id, root_agent_id, start_ns, end_ns, run_state FROM tracing_traces
			WHERE `+scopeWhere+` AND trace_id = ? AND root_agent_id IN (`+agents+`)`, args...).Row().
			Scan(&view.SessionID, &view.TurnID, &view.RootAgent, &view.StartNS, &view.EndNS, &view.RunState)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}

		rows, err := tx.Raw(`SELECT span_id, trace_id, parent_span_id, producer_id, agent_id, kind,
			start_ns, end_ns, status, metadata FROM tracing_spans
			WHERE `+scopeWhere+` AND trace_id = ? AND agent_id IN (`+agents+`)
			ORDER BY start_ns, span_id`, args...).Rows()
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			node, err := scanNode(rows)
			if err != nil {
				return err
			}
			view.Nodes = append(view.Nodes, node)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		result = &view
		return nil
	})
	return result, err
}

func (s store) node(ctx context.Context, reader authenticatedIdentity, spanID, expectedTraceID []byte) (*nodeDetail, error) {
	if len(reader.AgentIDs) == 0 {
		return nil, nil
	}
	args := append(reader.Scope.args(), spanID)
	args = appendAgents(args, reader.AgentIDs)
	args = appendAgents(args, reader.AgentIDs)
	agents := agentPlaceholders(len(reader.AgentIDs))
	query := `
		SELECT s.span_id, s.trace_id, s.parent_span_id, s.producer_id, s.agent_id, s.kind,
			s.start_ns, s.end_ns, s.status, s.metadata
		FROM tracing_spans AS s JOIN tracing_traces AS t
			ON s.tenant_id = t.tenant_id AND s.environment_id = t.environment_id
			   AND s.project_id = t.project_id AND s.trace_id = t.trace_id
		WHERE s.tenant_id = ? AND s.environment_id = ? AND s.project_id = ? AND s.span_id = ?
			AND t.root_agent_id IN (` + agents + `) AND s.agent_id IN (` + agents + `)`
	if expectedTraceID != nil {
		query += " AND s.trace_id = ?"
		args = append(args, expectedTraceID)
	}
	view, err := scanNode(s.db.WithContext(ctx).Raw(query, args...).Row())
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &nodeDetail{reader.Scope, view}, nil
}

func agentPlaceholders(count int) string {
	return "?" + strings.Repeat(", ?", count-1)
}

func appendAgents(args []any, agents []string) []any {
	for _, agent := range agents {
		args = append(args, agent)
	}
	return args
}
