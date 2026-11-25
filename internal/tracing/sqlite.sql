CREATE TABLE IF NOT EXISTS tracing_sessions (
    tenant_id TEXT NOT NULL,
    environment_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    root_agent_id TEXT NOT NULL,
    started_at_ns TEXT NOT NULL,
    PRIMARY KEY (tenant_id, environment_id, project_id, session_id)
);

CREATE TABLE IF NOT EXISTS tracing_traces (
    tenant_id TEXT NOT NULL,
    environment_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    trace_id BLOB NOT NULL,
    session_id TEXT NOT NULL,
    turn_id TEXT NOT NULL,
    root_span_id BLOB NOT NULL,
    root_producer_id TEXT NOT NULL,
    root_agent_id TEXT NOT NULL,
    registered_at_ns TEXT NOT NULL,
    accept_until_ns TEXT NOT NULL,
    start_ns TEXT,
    end_ns TEXT,
    run_state TEXT,
    PRIMARY KEY (tenant_id, environment_id, project_id, trace_id),
    UNIQUE (tenant_id, environment_id, project_id, session_id, turn_id),
    FOREIGN KEY (tenant_id, environment_id, project_id, session_id)
        REFERENCES tracing_sessions (tenant_id, environment_id, project_id, session_id)
);

CREATE TABLE IF NOT EXISTS tracing_spans (
    tenant_id TEXT NOT NULL,
    environment_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    span_id BLOB NOT NULL,
    trace_id BLOB NOT NULL,
    parent_span_id BLOB,
    producer_id TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    start_ns TEXT NOT NULL,
    end_ns TEXT NOT NULL,
    status INTEGER NOT NULL,
    metadata TEXT NOT NULL,
    PRIMARY KEY (tenant_id, environment_id, project_id, span_id),
    FOREIGN KEY (tenant_id, environment_id, project_id, trace_id)
        REFERENCES tracing_traces (tenant_id, environment_id, project_id, trace_id)
);

CREATE INDEX IF NOT EXISTS trace_nodes
    ON tracing_spans (tenant_id, environment_id, project_id, trace_id, start_ns, span_id);
