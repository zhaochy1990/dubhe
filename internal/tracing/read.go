package tracing

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"time"

	"gorm.io/gorm"
)

type traceSummary struct {
	Scope
	TraceID   string  `json:"trace_id"`
	SessionID string  `json:"session_id"`
	TurnID    string  `json:"turn_id"`
	RootAgent string  `json:"root_agent_id"`
	AgentName string  `json:"agent_name,omitempty"`
	StartNS   *uint64 `json:"start_ns"`
	EndNS     *uint64 `json:"end_ns"`
	RunState  *string `json:"run_state"`
	Observed  bool    `json:"observed_range,omitempty"`
}

type traceView struct {
	traceSummary
	Nodes      []nodeView     `json:"nodes"`
	Governance governanceView `json:"governance"`
	NextCursor string         `json:"next_cursor,omitempty"`
}

type governanceView struct {
	ToolCalls int    `json:"tool_calls"`
	Result    string `json:"result,omitempty"`
}

const (
	noGovernance = iota
	pendingConfirmation
	denied
	blocked
)

func governingResult(priority int) string {
	switch priority {
	case blocked:
		return "blocked"
	case denied:
		return "denied"
	case pendingConfirmation:
		return "pending_confirmation"
	default:
		return ""
	}
}

func auditPriority(event auditEvent) int {
	switch {
	case event.Decision == "block":
		return blocked
	case event.Decision == "deny" || event.Confirmation == "rejected" || event.Confirmation == "expired":
		return denied
	case event.Confirmation == "pending":
		return pendingConfirmation
	default:
		return noGovernance
	}
}

type nodeView struct {
	NodeID         string          `json:"node_id"`
	TraceID        string          `json:"trace_id"`
	SpanID         string          `json:"span_id,omitempty"`
	ParentSpanID   *string         `json:"parent_span_id"`
	UnloadedParent bool            `json:"unloaded_parent,omitempty"`
	ProducerID     string          `json:"producer_id"`
	AgentID        string          `json:"agent_id"`
	AgentName      string          `json:"agent_name,omitempty"`
	Kind           string          `json:"kind"`
	Source         string          `json:"source"`
	StartNS        uint64          `json:"start_ns"`
	EndNS          uint64          `json:"end_ns"`
	Status         *int            `json:"status,omitempty"`
	Metadata       json.RawMessage `json:"metadata"`
	auditFacts     *auditEvent
}

type nodeDetail struct {
	Scope
	nodeView
	Audit []auditEvent `json:"audit,omitempty"`
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
	view.Source = "sdk"
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
		var err error
		result, err = s.traceWithNodes(tx, reader, traceID, time.Now())
		return err
	})
	return result, err
}

func (s store) traceWithNodes(tx *gorm.DB, reader authenticatedIdentity, traceID []byte, now time.Time) (*traceView, error) {
	args := appendAgents(append(reader.Scope.args(), traceID, s.cutoff(now)), reader.AgentIDs)
	agents := agentPlaceholders(len(reader.AgentIDs))
	view := traceView{traceSummary: traceSummary{Scope: reader.Scope, TraceID: hex.EncodeToString(traceID)}}
	err := tx.Raw(`SELECT session_id, turn_id, root_agent_id, start_ns, end_ns, run_state FROM tracing_traces
		WHERE `+scopeWhere+` AND trace_id = ? AND registered_at_ns > ? AND root_agent_id IN (`+agents+`)`, args...).Row().
		Scan(&view.SessionID, &view.TurnID, &view.RootAgent, &view.StartNS, &view.EndNS, &view.RunState)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	view.Nodes, err = s.traceNodes(tx, reader, traceID, nil)
	if err != nil {
		return nil, err
	}
	view.AgentName = s.policy.scopes[reader.Scope].agents[view.RootAgent]
	if view.StartNS == nil && len(view.Nodes) > 0 {
		start, end := view.Nodes[0].StartNS, view.Nodes[0].EndNS
		for _, node := range view.Nodes {
			start, end = min(start, node.StartNS), max(end, node.EndNS)
		}
		view.StartNS, view.EndNS, view.Observed = &start, &end, true
	}
	var priority int
	var last *auditEvent
	for _, node := range view.Nodes {
		if event := node.auditFacts; event != nil {
			view.Governance.ToolCalls++
			priority = max(priority, auditPriority(*event))
			if last == nil || event.OccurredAt.After(last.OccurredAt) {
				last = event
			}
		}
	}
	view.Governance.Result = governingResult(priority)
	if view.Governance.Result == "" && view.RunState == nil && last != nil {
		view.Governance.Result = last.ExecutionState
	}
	return &view, nil
}

type nodeSelection struct {
	spanID []byte
	callID string
}

func (s store) traceNodes(tx *gorm.DB, reader authenticatedIdentity, traceID []byte, selection *nodeSelection) ([]nodeView, error) {
	args := appendAgents(append(reader.Scope.args(), traceID), reader.AgentIDs)
	filter := " WHERE " + scopeWhere + " AND trace_id = ? AND agent_id IN (" + agentPlaceholders(len(reader.AgentIDs)) + ")"
	spanQuery := `SELECT span_id, trace_id, parent_span_id, producer_id, agent_id, kind,
		start_ns, end_ns, status, metadata FROM tracing_spans` + filter
	toolQuery := "SELECT span_id, source_id, agent_id, started_ns, updated_ns, facts FROM tracing_tool_calls" + filter
	spanArgs, toolArgs := slices.Clone(args), slices.Clone(args)
	if selection != nil {
		spanQuery += " AND span_id = ?"
		spanArgs = append(spanArgs, selection.spanID)
		if selection.callID != "" {
			toolQuery += " AND call_id = ?"
			toolArgs = append(toolArgs, selection.callID)
		} else {
			toolQuery += " AND span_id = ?"
			toolArgs = append(toolArgs, selection.spanID)
		}
	}
	rows, err := tx.Raw(spanQuery, spanArgs...).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	nodes := []nodeView{}
	toolClients := make(map[string]int)
	for rows.Next() {
		node, err := scanNode(rows)
		if err != nil {
			return nil, err
		}
		if node.Kind == "tool_client" {
			toolClients[node.SpanID] = len(nodes)
		}
		nodes = append(nodes, node)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows.Close()
	rows, err = tx.Raw(toolQuery, toolArgs...).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var facts, spanID []byte
		node := nodeView{TraceID: hex.EncodeToString(traceID), Kind: "tool", Source: "gateway"}
		if err := rows.Scan(&spanID, &node.ProducerID, &node.AgentID, &node.StartNS, &node.EndNS, &facts); err != nil {
			return nil, err
		}
		event, metadata, err := gatewayMetadata(facts)
		if err != nil {
			return nil, err
		}
		node.NodeID = "tool:" + event.CallID
		node.auditFacts = &event
		index, found := toolClients[hex.EncodeToString(spanID)]
		if found {
			client := nodes[index]
			var observed fields
			if err := json.Unmarshal(client.Metadata, &observed); err != nil {
				return nil, err
			}
			if observed["tool_route"] == "gateway" && observed["client_call_id"] == event.CallID &&
				observed["tool_id"] == event.ToolID && client.AgentID == node.AgentID {
				node.SpanID, node.ParentSpanID = client.SpanID, client.ParentSpanID
				node.StartNS, node.EndNS, node.Status = client.StartNS, client.EndNS, client.Status
				metadata["client"] = observed
				metadata["client_producer_id"] = client.ProducerID
				node.Metadata, err = json.Marshal(metadata)
				if err != nil {
					return nil, err
				}
				nodes[index] = node
				continue
			}
		}
		node.Metadata, err = json.Marshal(metadata)
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, node)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows.Close()
	visible := make(map[string]bool, len(nodes))
	for _, node := range nodes {
		visible[node.SpanID] = node.SpanID != ""
	}
	for i := range nodes {
		node := &nodes[i]
		node.AgentName = s.policy.scopes[reader.Scope].agents[node.AgentID]
		if node.ParentSpanID != nil && !visible[*node.ParentSpanID] {
			if selection != nil {
				parentID, _ := parseID(*node.ParentSpanID, 8)
				var exists int
				err := tx.Raw("SELECT 1 FROM tracing_spans"+filter+" AND span_id = ?",
					append(args, parentID)...).Row().Scan(&exists)
				if err == nil {
					continue
				}
				if !errors.Is(err, sql.ErrNoRows) {
					return nil, err
				}
			}
			node.ParentSpanID = nil
		}
	}
	slices.SortFunc(nodes, func(a, b nodeView) int {
		if a.StartNS < b.StartNS {
			return -1
		}
		if a.StartNS > b.StartNS {
			return 1
		}
		return strings.Compare(a.NodeID, b.NodeID)
	})
	return nodes, nil
}

func gatewayMetadata(facts []byte) (auditEvent, fields, error) {
	var event auditEvent
	if err := json.Unmarshal(facts, &event); err != nil {
		return auditEvent{}, nil, err
	}
	metadata := fields{
		"call_id": event.CallID, "tool_id": event.ToolID, "decision": event.Decision, "execution_state": event.ExecutionState,
		"policy_version": event.PolicyVersion, "initiator": event.Initiator,
	}
	if event.ReasonCode != "" {
		metadata["reason_code"] = event.ReasonCode
	}
	if event.Confirmation != "" {
		metadata["confirmation_state"] = event.Confirmation
	}
	return event, metadata, nil
}

func (s store) node(ctx context.Context, reader authenticatedIdentity, nodeID string, lookupID any, expectedTraceID []byte) (*nodeDetail, error) {
	if len(reader.AgentIDs) == 0 {
		return nil, nil
	}
	table, key := "tracing_spans", "span_id"
	if strings.HasPrefix(nodeID, "tool:") {
		table, key = "tracing_tool_calls", "call_id"
	}
	var result *nodeDetail
	err := s.readTransaction(ctx, func(tx *gorm.DB) error {
		args := appendAgents(append(reader.Scope.args(), lookupID), reader.AgentIDs)
		query := "SELECT trace_id, span_id FROM " + table + " WHERE " + scopeWhere + " AND " + key +
			" = ? AND agent_id IN (" + agentPlaceholders(len(reader.AgentIDs)) + ")"
		if expectedTraceID != nil {
			query += " AND trace_id = ?"
			args = append(args, expectedTraceID)
		}
		var traceID []byte
		var selection nodeSelection
		if err := tx.Raw(query, args...).Row().Scan(&traceID, &selection.spanID); errors.Is(err, sql.ErrNoRows) {
			return nil
		} else if err != nil {
			return err
		}
		if table == "tracing_tool_calls" {
			selection.callID = strings.TrimPrefix(nodeID, "tool:")
		}
		args = appendAgents(append(reader.Scope.args(), traceID, s.cutoff(time.Now())), reader.AgentIDs)
		var granted int
		err := tx.Raw("SELECT 1 FROM tracing_traces WHERE "+scopeWhere+" AND trace_id = ? AND registered_at_ns > ?"+
			" AND root_agent_id IN ("+agentPlaceholders(len(reader.AgentIDs))+")", args...).Row().Scan(&granted)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		nodes, err := s.traceNodes(tx, reader, traceID, &selection)
		if err != nil {
			return err
		}
		for _, node := range nodes {
			// Preserve Span deep links after the Gateway evidence arrives.
			if node.NodeID != nodeID && (node.SpanID == "" || "span:"+node.SpanID != nodeID) {
				continue
			}
			node.Metadata, err = s.readContent(tx, reader, traceID, node.NodeID, node.Metadata)
			if err != nil {
				return err
			}
			result = &nodeDetail{Scope: reader.Scope, nodeView: node}
			if node.Kind == "tool" {
				result.Audit, err = readAudit(tx, reader.Scope, strings.TrimPrefix(node.NodeID, "tool:"))
			}
			return err
		}
		return nil
	})
	return result, err
}

func readAudit(tx *gorm.DB, scope Scope, callID string) ([]auditEvent, error) {
	rows, err := tx.Raw(`SELECT facts FROM tracing_audit_events WHERE `+scopeWhere+
		` AND call_id = ? ORDER BY sequence_no`, append(scope.args(), callID)...).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var events []auditEvent
	for rows.Next() {
		var facts []byte
		var event auditEvent
		if err := rows.Scan(&facts); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(facts, &event); err != nil {
			return nil, err
		}
		// Expose the SDK Span identifier only via the authorized node association.
		event.SpanID = ""
		events = append(events, event)
	}
	return events, rows.Err()
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
