import { hrTime } from '@opentelemetry/core';
import { SpanStatusCode, type Span } from '@opentelemetry/api';
import { validId } from './sdk.js';

export interface ToolOptions {
  toolId: string;
  route: 'local' | 'direct' | 'gateway';
}

export interface ToolObservation {
  gatewayHeaders(destination: string): Promise<Readonly<Record<string, string>>>;
  recordResponse(response: { statusCode: number; callId?: string }): void;
}

export function toolObservation(
  span: Span | undefined,
  options: ToolOptions,
  getBinding?: (destination: string) => Promise<string | undefined>,
): ToolObservation {
  span?.setAttributes({ 'dubhe.tool.id': options.toolId, 'dubhe.tool.route': options.route });
  let hasResponse = false;
  return {
    async gatewayHeaders(destination): Promise<Readonly<Record<string, string>>> {
      if (options.route !== 'gateway') throw new TypeError('Gateway headers require a Gateway tool route');
      if (!span || !getBinding || !span.isRecording()) return {};
      const binding = await getBinding(destination);
      if (!binding) return {};
      const { traceId, spanId, traceFlags } = span.spanContext();
      return {
        traceparent: `00-${traceId}-${spanId}-${traceFlags.toString(16).padStart(2, '0')}`,
        'x-dubhe-trace-binding': binding,
      };
    },
    recordResponse({ statusCode, callId }) {
      if (!span || hasResponse || !Number.isInteger(statusCode) || statusCode < 100 || statusCode > 599) return;
      hasResponse = true;
      responseSpans.add(span);
      span.setAttributes({ 'http.response.status_code': statusCode, 'dubhe.tool.client_result': 'response' });
      if (options.route === 'gateway' && validId(callId)) span.setAttribute('dubhe.tool.call_id', callId);
      span.setStatus({ code: statusCode >= 400 ? SpanStatusCode.ERROR : SpanStatusCode.OK });
    },
  };
}

export function endTool(span: Span, outcome: 'completed' | 'failed' | 'cancelled', error?: unknown): void {
  if (outcome === 'completed') {
    if (!responseSpans.has(span)) span.setStatus({ code: SpanStatusCode.OK });
  } else {
    const code = outcome === 'cancelled' ? 'cancelled'
      : error instanceof Error && error.name === 'TimeoutError' ? 'timeout' : 'error';
    // Preserve actual HTTP observations when the application maps a response to an exception.
    if (!responseSpans.has(span)) {
      span.setAttribute('dubhe.tool.client_result', code);
      if (outcome === 'failed') span.setStatus({ code: SpanStatusCode.ERROR });
    }
    span.setAttribute('dubhe.error.code', code);
  }
  span.end(hrTime());
}

// Keep response state outside the exported attributes.
const responseSpans = new WeakSet<Span>();
