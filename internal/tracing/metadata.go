package tracing

import (
	"bytes"
	"encoding/json"
	"math"
	"strings"
	"unicode/utf8"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
)

const maxContentBytes = 256 * 1024

type fields map[string]any

type attributeSpec struct {
	field string
	kind  string
	read  func(*commonpb.AnyValue) (any, bool)
}

var spanAttributes = map[string]attributeSpec{
	"dubhe.span.kind":                 {"", "", readID},
	"dubhe.agent.id":                  {"", "", readID},
	"dubhe.run.outcome":               {"run_outcome", "run", enum("completed", "failed", "cancelled")},
	"dubhe.model.id":                  {"model_id", "llm", readID},
	"gen_ai.request.model":            {"model_name", "llm", readModelName},
	"gen_ai.usage.input_tokens":       {"input_tokens", "llm", readCount},
	"gen_ai.usage.output_tokens":      {"output_tokens", "llm", readCount},
	"gen_ai.request.max_tokens":       {"max_output_tokens", "llm", readCount},
	"gen_ai.request.temperature":      {"temperature", "llm", readTemperature},
	"gen_ai.request.reasoning.effort": {"reasoning_effort", "llm", enum("none", "minimal", "low", "medium", "high", "xhigh", "max")},
	"gen_ai.system_instructions":      {"system_instructions", "llm", readParts},
	"gen_ai.tool.definitions":         {"tool_definitions", "llm", readToolDefinitions},
	"gen_ai.input.messages":           {"messages", "llm", readMessages},
	"dubhe.tool.id":                   {"tool_id", "tool_client", readID},
	"dubhe.tool.route":                {"tool_route", "tool_client", enum("gateway", "local", "direct")},
	"dubhe.tool.call_id":              {"client_call_id", "tool_client", readID},
	"dubhe.tool.client_result":        {"client_result", "tool_client", enum("response", "timeout", "error", "cancelled")},
	"http.response.status_code":       {"response_code", "tool_client", readResponseCode},
	"dubhe.error.code":                {"error_code", "", enum("timeout", "error", "cancelled")},
}

func readID(v *commonpb.AnyValue) (any, bool) {
	value := v.GetStringValue()
	return value, opaqueID(value)
}

func validModelName(name string) bool {
	return name != "" && len(name) <= 512 && utf8.ValidString(name) && !strings.ContainsRune(name, 0)
}

func readModelName(v *commonpb.AnyValue) (any, bool) {
	name := v.GetStringValue()
	return name, validModelName(name)
}

func enum(values ...string) func(*commonpb.AnyValue) (any, bool) {
	return func(v *commonpb.AnyValue) (any, bool) {
		value := v.GetStringValue()
		for _, allowed := range values {
			if value == allowed {
				return value, true
			}
		}
		return nil, false
	}
}

func readCount(v *commonpb.AnyValue) (any, bool) {
	n, ok := v.GetValue().(*commonpb.AnyValue_IntValue)
	if !ok || n.IntValue < 0 || n.IntValue > 1<<53-1 {
		return nil, false
	}
	return n.IntValue, true
}

func readResponseCode(v *commonpb.AnyValue) (any, bool) {
	n, ok := readCount(v)
	return n, ok && n.(int64) >= 100 && n.(int64) <= 599
}

func readTemperature(v *commonpb.AnyValue) (any, bool) {
	var n float64
	switch value := v.GetValue().(type) {
	case *commonpb.AnyValue_DoubleValue:
		n = value.DoubleValue
	case *commonpb.AnyValue_IntValue:
		n = float64(value.IntValue)
	default:
		return nil, false
	}
	return n, !math.IsInf(n, 0) && !math.IsNaN(n) && n >= 0
}

type inputPart struct {
	Type      string          `json:"type"`
	Content   *string         `json:"content,omitempty"`
	ID        string          `json:"id,omitempty"`
	Name      string          `json:"name,omitempty"`
	Arguments json.RawMessage `json:"arguments,omitempty"`
	Response  *string         `json:"response,omitempty"`
}

func (p inputPart) valid(role string) bool {
	if p.ID != "" && !opaqueID(p.ID) {
		return false
	}
	switch p.Type {
	case "text":
		return p.Content != nil && p.ID == "" && p.Name == "" && p.Arguments == nil && p.Response == nil
	case "tool_call":
		return role == "assistant" && p.Content == nil && p.Response == nil && opaqueID(p.Name) && jsonObject(p.Arguments)
	case "tool_call_response":
		return role == "tool" && p.Response != nil && p.Content == nil && p.Name == "" && p.Arguments == nil
	default:
		return false
	}
}

func jsonObject(raw json.RawMessage) bool {
	raw = bytes.TrimSpace(raw)
	return len(raw) >= 2 && len(raw) <= maxContentBytes && raw[0] == '{' && json.Valid(raw)
}

func decodeContent(v *commonpb.AnyValue, into any) bool {
	data := []byte(v.GetStringValue())
	if data == nil || len(data) > maxContentBytes {
		return false
	}
	// The SDK uses JSON strings. Accept native OTLP arrays and objects too.
	if len(data) == 0 && v.GetArrayValue() != nil {
		value, ok := otlpJSON(v, 0)
		if !ok {
			return false
		}
		data, _ = json.Marshal(value)
	}
	if len(data) > maxContentBytes || !json.Valid(data) {
		return false
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	return decoder.Decode(into) == nil
}

func otlpJSON(v *commonpb.AnyValue, depth int) (any, bool) {
	if depth > 32 {
		return nil, false
	}
	switch value := v.GetValue().(type) {
	case *commonpb.AnyValue_StringValue:
		return value.StringValue, true
	case *commonpb.AnyValue_IntValue:
		return value.IntValue, true
	case *commonpb.AnyValue_DoubleValue:
		return value.DoubleValue, !math.IsInf(value.DoubleValue, 0) && !math.IsNaN(value.DoubleValue)
	case *commonpb.AnyValue_BoolValue:
		return value.BoolValue, true
	case *commonpb.AnyValue_ArrayValue:
		items := make([]any, 0, len(value.ArrayValue.GetValues()))
		for _, item := range value.ArrayValue.GetValues() {
			result, ok := otlpJSON(item, depth+1)
			if !ok {
				return nil, false
			}
			items = append(items, result)
		}
		return items, true
	case *commonpb.AnyValue_KvlistValue:
		items := make(fields)
		for _, item := range value.KvlistValue.GetValues() {
			result, ok := otlpJSON(item.GetValue(), depth+1)
			if _, exists := items[item.GetKey()]; exists || !ok {
				return nil, false
			}
			items[item.GetKey()] = result
		}
		return items, true
	default:
		return nil, false
	}
}

func readParts(v *commonpb.AnyValue) (any, bool) {
	var parts []inputPart
	if !decodeContent(v, &parts) || parts == nil {
		return nil, false
	}
	for _, part := range parts {
		if !part.valid("system") {
			return nil, false
		}
	}
	return parts, true
}

type inputMessage struct {
	Role  string      `json:"role"`
	Parts []inputPart `json:"parts"`
}

func readMessages(v *commonpb.AnyValue) (any, bool) {
	var messages []inputMessage
	if !decodeContent(v, &messages) || messages == nil {
		return nil, false
	}
	for _, message := range messages {
		switch message.Role {
		case "system", "developer", "user", "assistant", "tool":
		default:
			return nil, false
		}
		if message.Parts == nil {
			return nil, false
		}
		for _, part := range message.Parts {
			if !part.valid(message.Role) {
				return nil, false
			}
		}
	}
	return messages, true
}

type toolDefinition struct {
	Type        string          `json:"type"`
	Name        string          `json:"name"`
	Description *string         `json:"description,omitempty"`
	Parameters  json.RawMessage `json:"parameters,omitempty"`
}

func readToolDefinitions(v *commonpb.AnyValue) (any, bool) {
	var definitions []toolDefinition
	if !decodeContent(v, &definitions) || definitions == nil {
		return nil, false
	}
	for _, tool := range definitions {
		if tool.Type != "function" || !opaqueID(tool.Name) || (tool.Parameters != nil && !jsonObject(tool.Parameters)) {
			return nil, false
		}
	}
	return definitions, true
}

func normalizeAttributes(attrs []*commonpb.KeyValue) (string, string, fields, fields, bool) {
	known := make(map[string]*commonpb.AnyValue)
	for _, attribute := range attrs {
		key := attribute.GetKey()
		if _, ok := spanAttributes[key]; !ok {
			continue
		}
		if _, duplicate := known[key]; duplicate {
			return "", "", nil, nil, false
		}
		known[key] = attribute.GetValue()
	}
	agent, kind := known["dubhe.agent.id"].GetStringValue(), known["dubhe.span.kind"].GetStringValue()
	if !opaqueID(agent) || (kind != "run" && kind != "llm" && kind != "agent" && kind != "tool_client") {
		return "", "", nil, nil, false
	}
	metadata, content := make(fields), make(fields)
	for key, value := range known {
		spec := spanAttributes[key]
		if spec.field == "" {
			continue
		}
		decoded, ok := spec.read(value)
		if !ok || (spec.kind != "" && spec.kind != kind) {
			return "", "", nil, nil, false
		}
		switch spec.field {
		case "system_instructions", "messages", "tool_definitions":
			content[spec.field] = decoded
		default:
			metadata[spec.field] = decoded
		}
	}
	if kind == "tool_client" && (metadata["tool_id"] == nil || metadata["tool_route"] == nil ||
		(metadata["client_call_id"] != nil && metadata["tool_route"] != "gateway")) {
		return "", "", nil, nil, false
	}
	return agent, kind, metadata, content, true
}
