import type { BaseMessage } from '@langchain/core/messages';
import type { LLMResult } from '@langchain/core/outputs';
import type { Attributes, Span } from '@opentelemetry/api';

const reasoningEfforts = new Set(['none', 'minimal', 'low', 'medium', 'high', 'xhigh', 'max']);
const messageRoles: Readonly<Record<string, string>> = {
  human: 'user', ai: 'assistant', system: 'system', tool: 'tool', function: 'tool',
};
type Part =
  | { type: 'text'; content: string }
  | { type: 'tool_call'; id?: string; name: string; arguments: Record<string, unknown> }
  | { type: 'tool_call_response'; id?: string; response: string };

function record(value: unknown): Record<string, unknown> {
  return value != null && typeof value === 'object' && !Array.isArray(value)
    ? value as Record<string, unknown>
    : {};
}

function text(value: unknown): string | undefined {
  return typeof value === 'string' && value.isWellFormed() ? value : undefined;
}

function boundedText(value: unknown, maxBytes: number): string | undefined {
  const valueText = text(value);
  return valueText && !valueText.includes('\0') && Buffer.byteLength(valueText) <= maxBytes ? valueText : undefined;
}

function count(value: unknown): number | undefined {
  return typeof value === 'number' && Number.isSafeInteger(value) && value >= 0 ? value : undefined;
}

function partsFor(content: unknown): Part[] {
  const plain = text(content);
  if (plain !== undefined) return [{ type: 'text', content: plain }];
  if (!Array.isArray(content)) return [];
  return content.flatMap((part): Part[] => {
    const { type, text: value } = record(part);
    const content = text(value);
    return type === 'text' && content !== undefined ? [{ type: 'text', content }] : [];
  });
}

function chatMessages(messages: BaseMessage[]) {
  return messages.flatMap((message) => {
    const fields = record(message);
    const type = message.type;
    const role = type === 'generic' ? text(fields.role) : messageRoles[type];
    if (!role || !['system', 'developer', 'user', 'assistant', 'tool'].includes(role)) return [];
    let parts = partsFor(message.content);
    if (role === 'tool') {
      parts = parts.flatMap((part): Part[] => part.type === 'text'
        ? [{ type: 'tool_call_response', id: boundedText(fields.tool_call_id, 128), response: part.content }]
        : []);
    }
    if (role === 'assistant' && Array.isArray(fields.tool_calls)) {
      for (const call of fields.tool_calls) {
        const { id, name, args } = record(call);
        const toolName = boundedText(name, 128);
        if (toolName && args && typeof args === 'object' && !Array.isArray(args)) {
          parts.push({ type: 'tool_call', id: boundedText(id, 128), name: toolName, arguments: record(args) });
        }
      }
    }
    return [{ role, parts }];
  });
}

function toolDefinitions(tools: unknown) {
  if (!Array.isArray(tools)) return [];
  return tools.flatMap((tool) => {
    const wrapper = record(tool);
    const definition = record(wrapper.function ?? tool);
    const name = boundedText(definition.name, 128);
    const parameters = definition.parameters ?? definition.input_schema ?? definition.parametersJsonSchema;
    if (!name) return [];
    return [{
      type: 'function', name, description: text(definition.description),
      parameters: parameters && typeof parameters === 'object' && !Array.isArray(parameters) ? parameters : undefined,
    }];
  });
}

function jsonContent(value: unknown): string | undefined {
  try {
    const json = JSON.stringify(value);
    return Buffer.byteLength(json) <= 256 * 1024 ? json : undefined;
  } catch {
    return undefined;
  }
}

export function modelRequest(extra: unknown, capture: boolean, input: BaseMessage[] | string[]): Attributes {
  const invocation = record(record(extra).invocation_params);
  const model = boundedText(
    invocation.model ?? invocation.model_name ?? invocation.modelName ?? invocation.model_id ?? invocation.modelId, 512,
  );
  const temperature = invocation.temperature;
  const effort = text(invocation.reasoning_effort ?? invocation.reasoningEffort ?? record(invocation.reasoning).effort);
  const attributes: Attributes = {
    'gen_ai.request.model': model || undefined,
    'gen_ai.request.temperature': typeof temperature === 'number' && Number.isFinite(temperature) && temperature >= 0
      ? temperature : undefined,
    'gen_ai.request.max_tokens': count(
      invocation.max_completion_tokens ?? invocation.max_output_tokens ?? invocation.max_tokens ??
      invocation.maxOutputTokens ?? invocation.maxTokens,
    ),
    'gen_ai.request.reasoning.effort': effort && reasoningEfforts.has(effort) ? effort : undefined,
  };
  if (capture) {
    const messages = typeof input[0] === 'string'
      ? (input as string[]).map((content) => ({ role: 'user', parts: partsFor(content) }))
      : chatMessages(input as BaseMessage[]);
    if (messages.length) attributes['gen_ai.input.messages'] = jsonContent(messages);
    const system = partsFor(invocation.instructions ?? invocation.system_instruction ?? invocation.systemInstruction ?? invocation.system);
    if (system.length) attributes['gen_ai.system_instructions'] = jsonContent(system);
    const definitions = toolDefinitions(invocation.tools);
    if (definitions.length) attributes['gen_ai.tool.definitions'] = jsonContent(definitions);
  }
  return attributes;
}

export function recordUsage(span: Span, result: LLMResult, extra: unknown): void {
  if (record(extra).cached === true || !Array.isArray(result?.generations)) return;
  const output = record(result.llmOutput);
  const providerUsage = record(output.tokenUsage ?? output.usage);
  const generations = result.generations.flat();
  const message = generations.length === 1 ? record(record(generations[0]).message) : {};
  const usage = record(message.usage_metadata);
  const input = count(usage.input_tokens) ?? count(providerUsage.promptTokens ?? providerUsage.input_tokens ?? providerUsage.inputTokens);
  const outputTokens = count(usage.output_tokens) ?? count(providerUsage.completionTokens ?? providerUsage.output_tokens ?? providerUsage.outputTokens);
  if (input !== undefined) span.setAttribute('gen_ai.usage.input_tokens', input);
  if (outputTokens !== undefined) span.setAttribute('gen_ai.usage.output_tokens', outputTokens);
}
