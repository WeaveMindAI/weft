// Subscribes to the dispatcher's execution SSE stream and forwards
// node lifecycle events into the graph webview.
//
// The dispatcher exposes /events/execution/{color} as SSE. Each
// event arrives with kind + payload, tagged to match
// DispatcherEvent in weft-dispatcher/src/events.rs. We translate
// each event into one `execEvent` (graph state: running /
// completed / failed / ...).
//
// The follower does NOT post node-body panel content. That panel is
// fed by graphView's `/live` (infra) and `/display` (trigger)
// pollers, which run independently of execution.
//
// A single follower tracks a single color at a time. Switching
// follows (the user picks a different past execution in the
// sidebar) disposes the current EventSource and spins a new one.

import * as vscode from 'vscode';

import type { DispatcherClient } from './dispatcher';
import type { HostMessage, NodeExecEvent } from './shared/protocol';

export type DispatcherEvent =
  | { kind: 'execution_started'; color: string; entry_node: string; project_id: string }
  | { kind: 'node_started'; color: string; node: string; lane: string; input: unknown; project_id: string }
  | { kind: 'node_suspended'; color: string; node: string; lane: string; token: string; project_id: string }
  | { kind: 'node_resumed'; color: string; node: string; lane: string; token: string; value: unknown; project_id: string }
  | { kind: 'node_cancelled'; color: string; node: string; lane: string; reason: string; project_id: string }
  | { kind: 'node_completed'; color: string; node: string; lane: string; output: unknown; project_id: string }
  | { kind: 'node_failed'; color: string; node: string; lane: string; error: string; project_id: string }
  | { kind: 'node_skipped'; color: string; node: string; lane: string; project_id: string }
  | { kind: 'execution_completed'; color: string; project_id: string; outputs: unknown }
  | { kind: 'execution_failed'; color: string; project_id: string; error: string }
  | { kind: 'execution_cancelled'; color: string; project_id: string; reason: string }
  // Infra lifecycle. Emitted by the dispatcher's infra_event_bridge
  // from supervisor-written rows; drive action-bar refresh so
  // transient `stopping` / `terminating` states show up in the UI.
  | { kind: 'infra_status_changed'; project_id: string; node_id: string; status: string }
  | { kind: 'infra_flaky'; project_id: string; node_id: string; reason: string }
  | { kind: 'infra_recovered'; project_id: string; node_id: string }
  | { kind: 'infra_terminated'; project_id: string; node_id: string }
  // Project lifecycle. Emitted by the dispatcher when a project's
  // overall lifecycle state changes (independent of any single
  // execution). Drive action-bar refresh so the UI sees the new
  // verb set without polling.
  | { kind: 'project_registered'; project_id: string; name: string }
  | { kind: 'project_activated'; project_id: string }
  | { kind: 'project_deactivated'; project_id: string }
  // External-URL invalidation. Trigger nodes that mint a tenant-
  // public URL emit this when the URL changes (re-activate, new
  // mount path, etc.). UI invalidates any cached chip.
  | { kind: 'trigger_url_changed'; project_id: string; node_id: string; url: string }
  // Cost report. Workers emit one per `report_cost` call.
  | { kind: 'cost_reported'; color: string; project_id: string; service: string; amount_usd: number }
  // Operator-visible banner: the supervisor couldn't parse the
  // project's `health_protocols_json`. Surfaces as an action-bar
  // banner; the user fixes the config and the next tick recovers.
  | { kind: 'infra_config_error'; project_id: string; error: string };

export type PostFn = (msg: HostMessage) => void;

export class ExecutionFollower implements vscode.Disposable {
  private eventSource: { close: () => void } | undefined;
  private currentColor: string | undefined;

  constructor(
    private readonly client: DispatcherClient,
    private readonly post: PostFn,
  ) {}

  /** Stop the current follow (if any) and start following a new
   *  execution. The webview gets an execReset up front so any old
   *  colors / pulses drop. */
  follow(color: string): void {
    this.stop();
    this.currentColor = color;
    this.post({ kind: 'execReset' });

    this.eventSource = this.client.subscribe(`/events/execution/${color}`, (ev) => {
      try {
        this.apply(JSON.parse(ev.data) as DispatcherEvent);
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
          lane: e.lane,
          input: e.input,
        };
        this.post({ kind: 'execEvent', event: execEvent });
        break;
      }
      case 'node_suspended': {
        const execEvent: NodeExecEvent = {
          nodeId: e.node,
          state: 'suspended',
          lane: e.lane,
          token: e.token,
        };
        this.post({ kind: 'execEvent', event: execEvent });
        break;
      }
      case 'node_resumed': {
        const execEvent: NodeExecEvent = {
          nodeId: e.node,
          state: 'running',
          lane: e.lane,
          token: e.token,
          resumeValue: e.value,
        };
        this.post({ kind: 'execEvent', event: execEvent });
        break;
      }
      case 'node_cancelled': {
        const execEvent: NodeExecEvent = {
          nodeId: e.node,
          state: 'cancelled',
          lane: e.lane,
          error: e.reason,
        };
        this.post({ kind: 'execEvent', event: execEvent });
        break;
      }
      case 'node_completed': {
        const execEvent: NodeExecEvent = {
          nodeId: e.node,
          state: 'completed',
          lane: e.lane,
          output: e.output,
        };
        this.post({ kind: 'execEvent', event: execEvent });
        break;
      }
      case 'node_failed': {
        const execEvent: NodeExecEvent = {
          nodeId: e.node,
          state: 'failed',
          lane: e.lane,
          error: e.error,
        };
        this.post({ kind: 'execEvent', event: execEvent });
        break;
      }
      case 'node_skipped': {
        const execEvent: NodeExecEvent = { nodeId: e.node, state: 'skipped', lane: e.lane };
        this.post({ kind: 'execEvent', event: execEvent });
        break;
      }
      case 'execution_completed':
      case 'execution_failed':
      case 'execution_cancelled':
        this.post({
          kind: 'execTerminal',
          color: e.color,
          state:
            e.kind === 'execution_completed'
              ? 'completed'
              : e.kind === 'execution_cancelled'
                ? 'cancelled'
                : 'failed',
        });
        break;
      // Events handled by `autoFollow` (action-bar refresh) or
      // user-facing banners; this execution-scoped follower
      // intentionally no-ops on them. Listing them keeps the
      // switch exhaustive so a new variant fails to compile here
      // until a reviewer routes it explicitly.
      case 'execution_started':
      case 'infra_status_changed':
      case 'infra_flaky':
      case 'infra_recovered':
      case 'infra_terminated':
      case 'project_registered':
      case 'project_activated':
      case 'project_deactivated':
      case 'trigger_url_changed':
      case 'cost_reported':
      case 'infra_config_error':
        break;
      default: {
        // Exhaustiveness: if a new DispatcherEvent variant is
        // added to the union without a route here, TypeScript
        // will narrow `e` to the new shape (not `never`) and
        // this assignment will fail to compile.
        const _exhaustive: never = e;
        return _exhaustive;
      }
    }
  }
}
