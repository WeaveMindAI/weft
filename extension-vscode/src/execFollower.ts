// Subscribes to the dispatcher's execution SSE stream and forwards
// node lifecycle events into the graph webview.
//
// The dispatcher exposes /events/execution/{color} as SSE. Each
// event arrives with kind + payload, tagged to match
// DispatcherEvent in weft-dispatcher/src/events.rs. We translate a
// subset into the wire-level messages the webview already expects:
//   NodeStarted   → execEvent { state: 'running', inputs }
//   NodeCompleted → execEvent { state: 'completed', outputs }
//                 + liveData for inspector
//                 + edgeActive pulses for the outgoing edges
//   NodeFailed    → execEvent { state: 'failed', error }
//   NodeSkipped   → execEvent { state: 'skipped' }
//
// A single follower tracks a single color at a time. Switching
// follows (the user picks a different past execution in the
// sidebar) disposes the current EventSource and spins a new one.

import * as vscode from 'vscode';

import type { DispatcherClient } from './dispatcher';
import type { HostMessage, NodeExecEvent, LiveDataItem, EdgeActiveEvent } from './shared/protocol';

export type DispatcherEvent =
  | { kind: 'execution_started'; color: string; entry_node: string; project_id: string }
  | { kind: 'node_started'; color: string; node: string; lane: string; input: unknown; project_id: string }
  | { kind: 'node_completed'; color: string; node: string; lane: string; output: unknown; project_id: string }
  | { kind: 'node_failed'; color: string; node: string; lane: string; error: string; project_id: string }
  | { kind: 'node_skipped'; color: string; node: string; lane: string; project_id: string }
  | { kind: 'execution_completed'; color: string; project_id: string; outputs: unknown }
  | { kind: 'execution_failed'; color: string; project_id: string; error: string };

export type PostFn = (msg: HostMessage) => void;
export type InspectorUpdateFn = (nodeId: string, patch: { lastInputs?: Record<string, unknown>; lastOutputs?: Record<string, unknown>; lastStatus?: string }) => void;

export class ExecutionFollower implements vscode.Disposable {
  private eventSource: { close: () => void } | undefined;
  private currentColor: string | undefined;

  constructor(
    private readonly client: DispatcherClient,
    private readonly post: PostFn,
    private readonly updateInspector: InspectorUpdateFn,
  ) {}

  /** Stop the current follow (if any) and start following a new
   *  execution. The webview gets an execReset up front so any old
   *  colors / pulses drop. */
  follow(color: string): void {
    this.stop();
    this.currentColor = color;
    this.post({ kind: 'execReset' });

    // The protocol SSE messages arrive as JSON. `subscribe` opens
    // an EventSource through the client; each `message` event has
    // data = JSON-encoded DispatcherEvent.
    this.eventSource = this.client.subscribe(`/events/execution/${color}`, (ev) => {
      try {
        const raw = JSON.parse(ev.data) as DispatcherEvent;
        this.apply(raw);
      } catch (err) {
        console.warn('[weft/execFollower] bad SSE payload', err);
      }
    });
  }

  /** Hydrate a past execution by replaying every journaled event
   *  up front. Called when the user clicks an execution in the
   *  sidebar. Follows newly-arriving events too so a still-running
   *  execution stays live. */
  async replay(color: string): Promise<void> {
    this.stop();
    this.currentColor = color;
    this.post({ kind: 'execReset' });

    try {
      const events = await this.client.get<DispatcherEvent[]>(`/executions/${color}/replay`);
      for (const e of events) this.apply(e);
    } catch (err) {
      console.warn('[weft/execFollower] replay failed', err);
    }

    // Continue live-following in case the execution is still in-flight.
    this.eventSource = this.client.subscribe(`/events/execution/${color}`, (ev) => {
      try {
        this.apply(JSON.parse(ev.data) as DispatcherEvent);
      } catch (err) {
        console.warn('[weft/execFollower] bad SSE payload', err);
      }
    });
  }

  stop(): void {
    this.eventSource?.close();
    this.eventSource = undefined;
    this.currentColor = undefined;
  }

  dispose(): void {
    this.stop();
  }

  private apply(e: DispatcherEvent): void {
    switch (e.kind) {
      case 'node_started': {
        const execEvent: NodeExecEvent = {
          nodeId: e.node,
          state: 'running',
          input: e.input,
        };
        this.post({ kind: 'execEvent', event: execEvent });
        const inputs = toRecord(e.input);
        if (inputs) {
          this.updateInspector(e.node, { lastInputs: inputs, lastStatus: 'running' });
          const items: LiveDataItem[] = Object.entries(inputs).map(([port, value]) => ({
            type: 'text',
            label: `in.${port}`,
            data: formatLive(value),
          }));
          this.post({ kind: 'liveData', nodeId: e.node, items });
        }
        break;
      }
      case 'node_completed': {
        const execEvent: NodeExecEvent = {
          nodeId: e.node,
          state: 'completed',
          output: e.output,
        };
        this.post({ kind: 'execEvent', event: execEvent });
        const outputs = toRecord(e.output);
        if (outputs) {
          this.updateInspector(e.node, { lastOutputs: outputs, lastStatus: 'completed' });
          const items: LiveDataItem[] = Object.entries(outputs).map(([port, value]) => ({
            type: 'text',
            label: `out.${port}`,
            data: formatLive(value),
          }));
          this.post({ kind: 'liveData', nodeId: e.node, items });
        }
        break;
      }
      case 'node_failed': {
        const execEvent: NodeExecEvent = {
          nodeId: e.node,
          state: 'failed',
          error: e.error,
        };
        this.post({ kind: 'execEvent', event: execEvent });
        this.updateInspector(e.node, { lastStatus: 'failed' });
        break;
      }
      case 'node_skipped': {
        const execEvent: NodeExecEvent = { nodeId: e.node, state: 'skipped' };
        this.post({ kind: 'execEvent', event: execEvent });
        break;
      }
      case 'execution_completed':
      case 'execution_failed':
        // Top-level terminal events. Flip the ActionBar's running
        // flag explicitly so the Stop button hides even if one of
        // the per-node events was dropped (SSE overruns, closed
        // connection, etc.). The sidebar picks them up via its
        // own /executions refresh.
        this.post({
          kind: 'execTerminal',
          color: e.color,
          state: e.kind === 'execution_completed' ? 'completed' : 'failed',
        });
        break;
    }
  }
}

function toRecord(v: unknown): Record<string, unknown> | undefined {
  if (v && typeof v === 'object' && !Array.isArray(v)) return v as Record<string, unknown>;
  return undefined;
}

function formatLive(v: unknown): string {
  if (typeof v === 'string') return v.length > 120 ? v.slice(0, 117) + '…' : v;
  try {
    const s = JSON.stringify(v);
    return s.length > 200 ? s.slice(0, 197) + '…' : s;
  } catch {
    return String(v);
  }
}

// eslint-disable-next-line @typescript-eslint/no-unused-vars
type _UnusedEdgeActive = EdgeActiveEvent;
