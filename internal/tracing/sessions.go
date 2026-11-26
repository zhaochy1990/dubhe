package tracing

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"
	_ "time/tzdata"

	"gorm.io/gorm"
)

type sessionView struct {
	Scope
	SessionID  string         `json:"session_id"`
	RootAgent  string         `json:"root_agent_id"`
	AgentName  string         `json:"agent_name,omitempty"`
	StartedNS  uint64         `json:"started_at_ns"`
	ActivityNS uint64         `json:"last_activity_ns"`
	DurationNS *uint64        `json:"duration_ns"`
	Observed   bool           `json:"observed_range,omitempty"`
	Result     *string        `json:"result"`
	ToolCalls  int            `json:"tool_calls"`
	Initiator  *actorIdentity `json:"initiator,omitempty"`
}

type timeBucket struct {
	From  time.Time `json:"from"`
	To    time.Time `json:"to"`
	Count int       `json:"count"`
}

type sessionAgent struct {
	Scope
	AgentDisplay
}

type sessionList struct {
	Sessions     []sessionView  `json:"sessions"`
	Total        int            `json:"total"`
	Distribution []timeBucket   `json:"distribution"`
	Agents       []sessionAgent `json:"agents"`
	NextCursor   string         `json:"next_cursor,omitempty"`
}

type sessionTraces struct {
	Traces     []traceSummary `json:"traces"`
	NextCursor string         `json:"next_cursor,omitempty"`
}

type sessionFilters struct {
	From, To       time.Time
	Agents, Result []string
	Search         string
	Timezone       string
	location       *time.Location
	limit          int
}

type pageCursor struct {
	Stamp       uint64    `json:"stamp"`
	ID          string    `json:"id"`
	Scope       Scope     `json:"scope"`
	Fingerprint string    `json:"fingerprint"`
	From        time.Time `json:"from,omitzero"`
	To          time.Time `json:"to,omitzero"`
}

func (s service) signCursor(cursor pageCursor) string {
	raw, _ := json.Marshal(cursor)
	mac := hmac.New(sha256.New, s.cursorKey)
	_, _ = mac.Write(raw)
	return base64.RawURLEncoding.EncodeToString(append(raw, mac.Sum(nil)...))
}

func (s service) parseCursor(query url.Values) (pageCursor, bool) {
	if !query.Has("cursor") {
		return pageCursor{}, true
	}
	cursorValues := query["cursor"]
	if len(cursorValues) != 1 || len(cursorValues[0]) > 2048 {
		return pageCursor{}, false
	}
	raw, err := base64.RawURLEncoding.DecodeString(cursorValues[0])
	if err != nil || len(raw) <= sha256.Size {
		return pageCursor{}, false
	}
	data, sig := raw[:len(raw)-sha256.Size], raw[len(raw)-sha256.Size:]
	mac := hmac.New(sha256.New, s.cursorKey)
	_, _ = mac.Write(data)
	var cursor pageCursor
	ok := hmac.Equal(mac.Sum(nil), sig) && json.Unmarshal(data, &cursor) == nil &&
		cursor.Fingerprint != "" && validCursorID(cursor.ID)
	return cursor, ok
}

func validCursorID(id string) bool {
	if opaqueID(id) {
		return true
	}
	return strings.HasPrefix(id, "tool:") && opaqueID(strings.TrimPrefix(id, "tool:"))
}

func cursorFingerprint(reader authenticatedIdentity, scopeKey string, filters sessionFilters) string {
	grants := slices.Clone(reader.readerScopes())
	slices.SortFunc(grants, func(a, b ReaderGrant) int { return compareScopes(a.Scope, b.Scope) })
	for i := range grants {
		ids := slices.Clone(grants[i].AgentIDs)
		slices.Sort(ids)
		grants[i].AgentIDs = slices.Compact(ids)
	}
	raw, _ := json.Marshal([]any{grants, scopeKey, filters})
	hash := sha256.Sum256(raw)
	return hex.EncodeToString(hash[:])
}

func compareScopes(a, b Scope) int {
	if tenant := strings.Compare(a.TenantID, b.TenantID); tenant != 0 {
		return tenant
	}
	if environment := strings.Compare(a.EnvironmentID, b.EnvironmentID); environment != 0 {
		return environment
	}
	return strings.Compare(a.ProjectID, b.ProjectID)
}

func compareSessionKeys(a, b pageCursor) int {
	switch {
	case a.Stamp < b.Stamp:
		return -1
	case a.Stamp > b.Stamp:
		return 1
	}
	if session := strings.Compare(a.ID, b.ID); session != 0 {
		return session
	}
	return compareScopes(a.Scope, b.Scope)
}

func sessionKey(view sessionView) pageCursor {
	return pageCursor{Stamp: view.StartedNS, ID: view.SessionID, Scope: view.Scope}
}

func pageSize(q url.Values) (int, bool) {
	if !q.Has("limit") {
		return 50, true
	}
	n, err := strconv.Atoi(q.Get("limit"))
	return n, err == nil && len(q["limit"]) == 1 && n >= 1 && n <= 200
}

func normalizeFilters(q url.Values, cursor pageCursor, now time.Time) (sessionFilters, bool) {
	f := sessionFilters{To: now.UTC(), Timezone: "UTC", Search: strings.ToLower(strings.TrimSpace(q.Get("q")))}
	var ok bool
	f.limit, ok = pageSize(q)
	if !ok || len(f.Search) > 256 || len(q["agent_id"]) > 100 {
		return f, false
	}
	for _, key := range []string{"q", "from", "to", "tz"} {
		if len(q[key]) > 1 {
			return f, false
		}
	}
	if !cursor.To.IsZero() {
		f.To = cursor.To
	}
	var err error
	if q.Has("to") {
		f.To, err = time.Parse(time.RFC3339Nano, q.Get("to"))
		if err != nil {
			return f, false
		}
	}
	f.From = f.To.Add(-7 * day)
	if !cursor.From.IsZero() {
		f.From = cursor.From
	}
	if q.Has("from") {
		f.From, err = time.Parse(time.RFC3339Nano, q.Get("from"))
		if err != nil {
			return f, false
		}
	}
	f.From, f.To = f.From.UTC(), f.To.UTC()
	if _, ok := unixNano(f.From); !ok {
		return f, false
	}
	if _, ok := unixNano(f.To); !ok || !f.From.Before(f.To) || f.To.Sub(f.From) > 30*day {
		return f, false
	}
	if q.Has("tz") {
		f.Timezone = q.Get("tz")
		if f.Timezone == "" || f.Timezone == "Local" || len(f.Timezone) > 64 {
			return f, false
		}
	}
	f.location, err = time.LoadLocation(f.Timezone)
	if err != nil {
		return f, false
	}
	f.Agents, f.Result = q["agent_id"], q["result"]
	slices.Sort(f.Agents)
	slices.Sort(f.Result)
	f.Agents, f.Result = slices.Compact(f.Agents), slices.Compact(f.Result)
	for _, agent := range f.Agents {
		if !opaqueID(agent) {
			return f, false
		}
	}
	for _, result := range f.Result {
		switch result {
		case "blocked", "denied", "pending_confirmation", "completed", "failed", "cancelled",
			"not_executed", "executing", "succeeded", "unknown":
		default:
			return f, false
		}
	}
	return f, true
}

func (s service) listSessions(w http.ResponseWriter, r *http.Request) {
	reader, query, authorized, authErr := s.authorizedReader(r)
	if authErr != nil {
		writeDetail(w, http.StatusServiceUnavailable, "Authentication service unavailable")
		return
	}
	if !authorized {
		writeDetail(w, http.StatusNotFound, "Not available")
		return
	}
	now := time.Now()
	cursor, validCursor := s.parseCursor(query)
	filters, validFilters := normalizeFilters(query, cursor, now)
	fingerprint := cursorFingerprint(reader, "sessions", filters)
	if !validCursor || !validFilters || (cursor.ID != "" && cursor.Fingerprint != fingerprint) {
		writeDetail(w, http.StatusBadRequest, "Invalid Session filters or cursor")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 8*time.Second)
	defer cancel()
	result, next, err := s.readStore.sessions(ctx, reader, filters, cursor, now)
	if err != nil {
		writeDetail(w, http.StatusServiceUnavailable, "Tracing storage unavailable")
		return
	}
	if next != nil {
		next.From, next.To, next.Fingerprint = filters.From, filters.To, fingerprint
		result.NextCursor = s.signCursor(*next)
	}
	writeJSON(w, http.StatusOK, result)
}

func (s store) sessions(ctx context.Context, reader authenticatedIdentity, filters sessionFilters, cursor pageCursor, now time.Time) (sessionList, *pageCursor, error) {
	result := sessionList{Sessions: []sessionView{}, Agents: []sessionAgent{}, Distribution: makeBuckets(filters)}
	err := s.readTransaction(ctx, func(tx *gorm.DB) error {
		for _, grant := range reader.readerScopes() {
			if len(grant.AgentIDs) > 0 {
				if err := s.sessionsInScope(tx, &result, grant, filters, cursor, now); err != nil {
					return err
				}
			}
		}
		return nil
	})
	var next *pageCursor
	slices.SortFunc(result.Sessions, func(a, b sessionView) int { return compareSessionKeys(sessionKey(b), sessionKey(a)) })
	slices.SortFunc(result.Agents, func(a, b sessionAgent) int {
		if id := strings.Compare(a.ID, b.ID); id != 0 {
			return id
		}
		return compareScopes(a.Scope, b.Scope)
	})
	if len(result.Sessions) > filters.limit {
		result.Sessions = result.Sessions[:filters.limit]
		key := sessionKey(result.Sessions[filters.limit-1])
		next = &key
	}
	return result, next, err
}

func (s store) sessionsInScope(tx *gorm.DB, result *sessionList, reader ReaderGrant, filters sessionFilters, cursor pageCursor, now time.Time) error {
	from, _ := unixNano(filters.From)
	to, _ := unixNano(filters.To)
	agents := agentPlaceholders(len(reader.AgentIDs))
	args := appendAgents(append(reader.Scope.args(), s.timestamp(from), s.timestamp(to)), reader.AgentIDs)
	args = append(args, s.cutoff(now))
	args = appendAgents(args, reader.AgentIDs)
	args = append(args, blocked, denied, pendingConfirmation)
	for range 3 {
		args = appendAgents(args, reader.AgentIDs)
	}
	catalog := s.policy.scopes[reader.Scope].agents
	options := make(map[string]bool)
	rows, err := tx.Raw(`
		WITH session_window AS (
			SELECT * FROM tracing_sessions WHERE `+scopeWhere+`
				AND started_at_ns >= ? AND started_at_ns < ? AND root_agent_id IN (`+agents+`)
		), visible_traces AS (
			SELECT t.*, MAX(COALESCE(t.end_ns, t.start_ns, t.registered_at_ns)) OVER (
				PARTITION BY t.session_id
			) AS activity_ns, ROW_NUMBER() OVER (
				PARTITION BY t.session_id ORDER BY COALESCE(t.start_ns, t.registered_at_ns) DESC, t.trace_id DESC
			) AS row_num
			FROM tracing_traces t JOIN session_window s ON `+scopeJoin("t", "s")+` AND t.session_id = s.session_id
			WHERE t.registered_at_ns > ? AND t.root_agent_id IN (`+agents+`)
		), tool_outcomes AS (
			SELECT t.session_id, c.trace_id, c.updated_ns, c.execution_state,
				CASE WHEN c.decision = 'block' THEN ?
					WHEN c.decision = 'deny' OR c.confirmation_state IN ('rejected','expired') THEN ?
					WHEN c.confirmation_state = 'pending' THEN ? ELSE 0 END AS priority,
				ROW_NUMBER() OVER (PARTITION BY c.trace_id ORDER BY c.updated_ns DESC, c.call_id DESC) AS call_num
			FROM tracing_tool_calls c JOIN visible_traces t ON `+scopeJoin("c", "t")+` AND c.trace_id = t.trace_id
			WHERE c.agent_id IN (`+agents+`)
		), governance AS (
			SELECT session_id, COUNT(*) AS calls, MAX(updated_ns) AS activity, MAX(priority) AS priority
				FROM tool_outcomes GROUP BY session_id
		), `+observedRangeCTE(agents)+`
		SELECT s.session_id, s.root_agent_id, s.started_at_ns, t.activity_ns, t.start_ns, t.end_ns, t.run_state,
			COALESCE(g.calls, 0), COALESCE(g.priority, 0), g.activity, i.actor_kind, i.actor_id,
			o.observed_start, o.observed_end, e.execution_state
		FROM session_window s JOIN visible_traces t ON `+scopeJoin("s", "t")+` AND s.session_id = t.session_id
		LEFT JOIN governance g ON s.session_id = g.session_id
		LEFT JOIN call_ranges o ON t.trace_id = o.trace_id
		LEFT JOIN tool_outcomes e ON t.trace_id = e.trace_id AND e.call_num = 1
		LEFT JOIN tracing_session_initiators i ON `+scopeJoin("s", "i")+` AND s.session_id = i.session_id
		WHERE t.row_num = 1 ORDER BY s.started_at_ns DESC, s.session_id DESC`, args...).Rows()
	if err != nil {
		return err
	}
	defer rows.Close()
	selected := 0
	for rows.Next() {
		view := sessionView{Scope: reader.Scope}
		var start, end, activity, observedStart, observedEnd *uint64
		var priority int
		var actorKind, actorID, execution *string
		if err := rows.Scan(&view.SessionID, &view.RootAgent, &view.StartedNS, &view.ActivityNS,
			&start, &end, &view.Result, &view.ToolCalls, &priority, &activity, &actorKind, &actorID,
			&observedStart, &observedEnd, &execution); err != nil {
			return err
		}
		if actorKind != nil && actorID != nil {
			view.Initiator = &actorIdentity{Kind: *actorKind, ID: *actorID}
		}
		view.AgentName = catalog[view.RootAgent]
		options[view.RootAgent] = true
		if start == nil && observedStart != nil {
			start, end, view.Observed = observedStart, observedEnd, true
		}
		for _, value := range []*uint64{start, end, activity} {
			if value != nil {
				view.ActivityNS = max(view.ActivityNS, *value)
			}
		}
		if start != nil && end != nil && *end >= *start {
			duration := *end - *start
			view.DurationNS = &duration
		}
		if outcome := governingResult(priority); outcome != "" {
			view.Result = &outcome
		} else if view.Result == nil {
			view.Result = execution
		}
		if !filters.matches(view) {
			continue
		}
		result.Total++
		stamp := time.Unix(int64(view.StartedNS/uint64(time.Second)), int64(view.StartedNS%uint64(time.Second)))
		index, found := slices.BinarySearchFunc(result.Distribution, stamp, func(bucket timeBucket, t time.Time) int {
			if !t.Before(bucket.To) {
				return -1
			}
			if t.Before(bucket.From) {
				return 1
			}
			return 0
		})
		if found {
			result.Distribution[index].Count++
		}
		if cursor.ID != "" && compareSessionKeys(sessionKey(view), cursor) >= 0 {
			continue
		}
		if selected <= filters.limit {
			result.Sessions = append(result.Sessions, view)
			selected++
		}
	}
	for id := range options {
		result.Agents = append(result.Agents, sessionAgent{Scope: reader.Scope, AgentDisplay: AgentDisplay{ID: id, Name: catalog[id]}})
	}
	return rows.Err()
}

// Each visible_traces CTE is confined to a single credential scope. Every
// matching span or call must also be visible to that scope's Agent grant.
func observedRangeCTE(agents string) string {
	return `observations AS (
		SELECT t.trace_id, n.start_ns AS started, n.end_ns AS ended
		FROM tracing_spans n JOIN visible_traces t ON ` + scopeJoin("n", "t") + ` AND n.trace_id = t.trace_id
		WHERE t.start_ns IS NULL AND n.agent_id IN (` + agents + `)
		UNION ALL
		SELECT t.trace_id, c.started_ns, c.updated_ns
		FROM tracing_tool_calls c JOIN visible_traces t ON ` + scopeJoin("c", "t") + ` AND c.trace_id = t.trace_id
		WHERE t.start_ns IS NULL AND c.agent_id IN (` + agents + `)
	), call_ranges AS (
		SELECT trace_id, MIN(started) AS observed_start, MAX(ended) AS observed_end FROM observations GROUP BY trace_id
	)`
}

func (f sessionFilters) matches(view sessionView) bool {
	name := view.AgentName
	if name == "" {
		name = view.RootAgent
	}
	return (len(f.Agents) == 0 || slices.Contains(f.Agents, view.RootAgent)) &&
		(len(f.Result) == 0 || (view.Result != nil && slices.Contains(f.Result, *view.Result))) &&
		(f.Search == "" || strings.Contains(strings.ToLower(view.SessionID), f.Search) ||
			strings.Contains(strings.ToLower(name), f.Search))
}

func makeBuckets(f sessionFilters) []timeBucket {
	local := f.From.In(f.location)
	hourly := f.To.Sub(f.From) <= day
	at := time.Date(local.Year(), local.Month(), local.Day(), 0, 0, 0, 0, f.location)
	if hourly {
		at = local.Add(-time.Duration(local.Minute())*time.Minute - time.Duration(local.Second())*time.Second -
			time.Duration(local.Nanosecond()))
	}
	var buckets []timeBucket
	for at.Before(f.To) {
		next := at.AddDate(0, 0, 1)
		if hourly {
			next = at.Add(time.Hour)
		}
		if !next.After(at) {
			next = at.Add(day)
		}
		buckets = append(buckets, timeBucket{From: at, To: next})
		at = next
	}
	return buckets
}

func (s service) listSessionTraces(w http.ResponseWriter, r *http.Request) {
	reader, query, authorized, authErr := s.authorizedReader(r)
	if authErr != nil {
		writeDetail(w, http.StatusServiceUnavailable, "Authentication service unavailable")
		return
	}
	sessionID := r.PathValue("session_id")
	if sessionID == "" && len(query["session_id"]) == 1 {
		sessionID = query.Get("session_id")
	}
	if !authorized || !opaqueID(sessionID) {
		writeDetail(w, http.StatusNotFound, "Not available")
		return
	}
	limit, validLimit := pageSize(query)
	cursor, validCursor := s.parseCursor(query)
	fingerprint := cursorFingerprint(reader, "session:"+sessionID, sessionFilters{})
	if !validLimit || !validCursor || (cursor.ID != "" && cursor.Fingerprint != fingerprint) {
		writeDetail(w, http.StatusBadRequest, "Invalid Trace page size or cursor")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 8*time.Second)
	defer cancel()
	result, next, err := s.readStore.sessionTraces(ctx, reader, sessionID, cursor, limit)
	switch {
	case err != nil:
		writeDetail(w, http.StatusServiceUnavailable, "Tracing storage unavailable")
	case result == nil:
		writeDetail(w, http.StatusNotFound, "Not available")
	default:
		if next != nil {
			next.Fingerprint = fingerprint
			result.NextCursor = s.signCursor(*next)
		}
		writeJSON(w, http.StatusOK, result)
	}
}

func (s store) sessionTraces(ctx context.Context, reader authenticatedIdentity, sessionID string, cursor pageCursor, limit int) (*sessionTraces, *pageCursor, error) {
	if len(reader.AgentIDs) == 0 {
		return nil, nil, nil
	}
	var result *sessionTraces
	var next *pageCursor
	err := s.readTransaction(ctx, func(tx *gorm.DB) error {
		args := appendAgents(append(reader.Scope.args(), sessionID, s.cutoff(time.Now())), reader.AgentIDs)
		base := " FROM tracing_traces WHERE " + scopeWhere + " AND session_id = ? AND registered_at_ns > ? AND root_agent_id IN (" +
			agentPlaceholders(len(reader.AgentIDs)) + ")"
		var exists int
		err := tx.Raw("SELECT 1"+base+" LIMIT 1", args...).Row().Scan(&exists)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		result = &sessionTraces{Traces: []traceSummary{}}
		const sortTime = "COALESCE(start_ns, registered_at_ns)"
		if cursor.ID != "" {
			id, valid := parseID(cursor.ID, 16)
			if !valid {
				return nil
			}
			base += " AND (" + sortTime + " < ? OR (" + sortTime + " = ? AND trace_id < ?))"
			args = append(args, s.timestamp(cursor.Stamp), s.timestamp(cursor.Stamp), id)
		}
		agents := agentPlaceholders(len(reader.AgentIDs))
		args = appendAgents(appendAgents(append(args, limit+1), reader.AgentIDs), reader.AgentIDs)
		rows, err := tx.Raw(`WITH visible_traces AS (
			SELECT *, `+sortTime+` AS sort_time`+base+`
			ORDER BY sort_time DESC, trace_id DESC LIMIT ?
		), `+observedRangeCTE(agents)+`
		SELECT t.trace_id, t.turn_id, t.root_agent_id, t.start_ns, t.end_ns, t.run_state, t.sort_time,
			o.observed_start, o.observed_end
		FROM visible_traces t LEFT JOIN call_ranges o ON t.trace_id = o.trace_id
		ORDER BY t.sort_time DESC, t.trace_id DESC`, args...).Rows()
		if err != nil {
			return err
		}
		defer rows.Close()
		var last pageCursor
		for rows.Next() {
			if len(result.Traces) == limit {
				next = &last
				break
			}
			var traceID []byte
			var observedStart, observedEnd *uint64
			view := traceSummary{Scope: reader.Scope, SessionID: sessionID}
			if err := rows.Scan(&traceID, &view.TurnID, &view.RootAgent, &view.StartNS, &view.EndNS, &view.RunState, &last.Stamp, &observedStart, &observedEnd); err != nil {
				return err
			}
			if view.StartNS == nil && observedStart != nil {
				view.StartNS, view.EndNS, view.Observed = observedStart, observedEnd, true
			}
			view.TraceID = hex.EncodeToString(traceID)
			view.AgentName = s.policy.scopes[reader.Scope].agents[view.RootAgent]
			last.ID = view.TraceID
			result.Traces = append(result.Traces, view)
		}
		return rows.Err()
	})
	return result, next, err
}
