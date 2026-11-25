CREATE TABLE IF NOT EXISTS tracing_sessions (
    tenant_id VARBINARY(128) NOT NULL,
    environment_id VARBINARY(128) NOT NULL,
    project_id VARBINARY(128) NOT NULL,
    session_id VARBINARY(128) NOT NULL,
    root_agent_id VARBINARY(128) NOT NULL,
    started_at_ns BIGINT UNSIGNED NOT NULL,
    PRIMARY KEY (tenant_id, environment_id, project_id, session_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS tracing_traces (
    tenant_id VARBINARY(128) NOT NULL,
    environment_id VARBINARY(128) NOT NULL,
    project_id VARBINARY(128) NOT NULL,
    trace_id BINARY(16) NOT NULL,
    session_id VARBINARY(128) NOT NULL,
    turn_id VARBINARY(128) NOT NULL,
    root_span_id BINARY(8) NOT NULL,
    root_producer_id VARBINARY(128) NOT NULL,
    root_agent_id VARBINARY(128) NOT NULL,
    registered_at_ns BIGINT UNSIGNED NOT NULL,
    accept_until_ns BIGINT UNSIGNED NOT NULL,
    start_ns BIGINT UNSIGNED,
    end_ns BIGINT UNSIGNED,
    run_state VARCHAR(16),
    PRIMARY KEY (tenant_id, environment_id, project_id, trace_id),
    UNIQUE KEY turn_trace (tenant_id, environment_id, project_id, session_id, turn_id),
    FOREIGN KEY (tenant_id, environment_id, project_id, session_id)
        REFERENCES tracing_sessions (tenant_id, environment_id, project_id, session_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS tracing_spans (
    tenant_id VARBINARY(128) NOT NULL,
    environment_id VARBINARY(128) NOT NULL,
    project_id VARBINARY(128) NOT NULL,
    span_id BINARY(8) NOT NULL,
    trace_id BINARY(16) NOT NULL,
    parent_span_id BINARY(8),
    producer_id VARBINARY(128) NOT NULL,
    agent_id VARBINARY(128) NOT NULL,
    kind VARCHAR(16) NOT NULL,
    start_ns BIGINT UNSIGNED NOT NULL,
    end_ns BIGINT UNSIGNED NOT NULL,
    status TINYINT UNSIGNED NOT NULL,
    metadata JSON NOT NULL,
    PRIMARY KEY (tenant_id, environment_id, project_id, span_id),
    KEY trace_nodes (tenant_id, environment_id, project_id, trace_id, start_ns, span_id),
    FOREIGN KEY (tenant_id, environment_id, project_id, trace_id)
        REFERENCES tracing_traces (tenant_id, environment_id, project_id, trace_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS tracing_trace_content (
    tenant_id VARBINARY(128) NOT NULL,
    environment_id VARBINARY(128) NOT NULL,
    project_id VARBINARY(128) NOT NULL,
    trace_id BINARY(16) NOT NULL,
    expires_at_ns BIGINT UNSIGNED NOT NULL,
    PRIMARY KEY (tenant_id, environment_id, project_id, trace_id),
    KEY content_expiry (expires_at_ns),
    FOREIGN KEY (tenant_id, environment_id, project_id, trace_id)
        REFERENCES tracing_traces (tenant_id, environment_id, project_id, trace_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS tracing_content (
    tenant_id VARBINARY(128) NOT NULL,
    environment_id VARBINARY(128) NOT NULL,
    project_id VARBINARY(128) NOT NULL,
    trace_id BINARY(16) NOT NULL,
    object_id VARBINARY(134) NOT NULL,
    payload JSON NOT NULL,
    PRIMARY KEY (tenant_id, environment_id, project_id, trace_id, object_id),
    FOREIGN KEY (tenant_id, environment_id, project_id, trace_id)
        REFERENCES tracing_trace_content (tenant_id, environment_id, project_id, trace_id) ON DELETE CASCADE
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS tracing_run_grants (
    tenant_id VARBINARY(128) NOT NULL,
    environment_id VARBINARY(128) NOT NULL,
    project_id VARBINARY(128) NOT NULL,
    trace_id BINARY(16) NOT NULL,
    producer_id VARBINARY(128) NOT NULL,
    agent_id VARBINARY(128) NOT NULL,
    PRIMARY KEY (tenant_id, environment_id, project_id, trace_id, producer_id, agent_id),
    FOREIGN KEY (tenant_id, environment_id, project_id, trace_id)
        REFERENCES tracing_traces (tenant_id, environment_id, project_id, trace_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS tracing_tool_calls (
    tenant_id VARBINARY(128) NOT NULL,
    environment_id VARBINARY(128) NOT NULL,
    project_id VARBINARY(128) NOT NULL,
    call_id VARBINARY(128) NOT NULL,
    trace_id BINARY(16) NOT NULL,
    span_id BINARY(8),
    source_id VARBINARY(128) NOT NULL,
    agent_id VARBINARY(128) NOT NULL,
    started_ns BIGINT UNSIGNED NOT NULL,
    updated_ns BIGINT UNSIGNED NOT NULL,
    decision VARCHAR(24) NOT NULL,
    confirmation_state VARCHAR(16) NOT NULL,
    execution_state VARCHAR(16) NOT NULL,
    facts JSON NOT NULL,
    PRIMARY KEY (tenant_id, environment_id, project_id, call_id),
    KEY tool_trace (tenant_id, environment_id, project_id, trace_id, agent_id),
    FOREIGN KEY (tenant_id, environment_id, project_id, trace_id)
        REFERENCES tracing_traces (tenant_id, environment_id, project_id, trace_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS tracing_audit_events (
    tenant_id VARBINARY(128) NOT NULL,
    environment_id VARBINARY(128) NOT NULL,
    project_id VARBINARY(128) NOT NULL,
    source_id VARBINARY(128) NOT NULL,
    event_id VARBINARY(128) NOT NULL,
    call_id VARBINARY(128) NOT NULL,
    sequence_no BIGINT UNSIGNED NOT NULL,
    facts JSON NOT NULL,
    PRIMARY KEY (tenant_id, environment_id, project_id, source_id, event_id),
    UNIQUE KEY call_sequence (tenant_id, environment_id, project_id, call_id, sequence_no),
    FOREIGN KEY (tenant_id, environment_id, project_id, call_id)
        REFERENCES tracing_tool_calls (tenant_id, environment_id, project_id, call_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS tracing_session_initiators (
    tenant_id VARBINARY(128) NOT NULL,
    environment_id VARBINARY(128) NOT NULL,
    project_id VARBINARY(128) NOT NULL,
    session_id VARBINARY(128) NOT NULL,
    actor_kind VARCHAR(16) NOT NULL,
    actor_id VARBINARY(128) NOT NULL,
    PRIMARY KEY (tenant_id, environment_id, project_id, session_id),
    FOREIGN KEY (tenant_id, environment_id, project_id, session_id)
        REFERENCES tracing_sessions (tenant_id, environment_id, project_id, session_id)
) ENGINE=InnoDB;
