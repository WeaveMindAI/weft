// Subscribes to the dispatcher's execution SSE stream and forwards
// node lifecycle events into the graph webview.
//
// The dispatcher exposes /events/execution/{color} as SSE. Each
// event arrives with kind + payload, tagged to match the Rust
// enum. We translate
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
import type {
  BusPayload,
  CorruptionSite,
  HostMessage,
  LoopIteration,
  LoopTerminationReason,
  NodeExecEvent,
} from './shared/protocol';

// SYNC: DispatcherEvent <-> crates/weft-dispatcher/src/events.rs DispatcherEvent
export type DispatcherEvent =
  | { kind: 'execution_started'; color: string; entry_node: string; project_id: string }
  | { kind: 'node_started'; color: string; node: string; frames: LoopIteration[]; input: unknown; closed_ports: string[]; project_id: string }
  | { kind: 'node_suspended'; color: string; node: string; frames: LoopIteration[]; token: string; project_id: string }
  | { kind: 'node_resumed'; color: string; node: string; frames: LoopIteration[]; token: string | null; value: unknown; project_id: string }
  | { kind: 'node_cancelled'; color: string; node: string; frames: LoopIteration[]; reason: string; project_id: string }
  | { kind: 'node_completed'; color: string; node: string; frames: LoopIteration[]; output: unknown; project_id: string }
  | { kind: 'node_failed'; color: string; node: string; frames: LoopIteration[]; error: string; project_id: string }
  | { kind: 'node_skipped'; color: string; node: string; frames: LoopIteration[]; closed_ports: string[]; project_id: string }
  | { kind: 'port_type_mismatch'; color: string; node: string; frames: LoopIteration[]; port: string; expected: string; actual: string; project_id: string }
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
  | { kind: 'infra_config_error'; project_id: string; error: string }
  // Bus events: live + replay. The inspector renders one IRC-style
  // log per node per bus. `bus_id` is the channel's uuid (matches
  // the uuid embedded in the bus marker JSON that flows on pulses);
  // `from` on a message is the sender's registered name (stamped by
  // the bus on the producer side, never spoofed). For ephemeral buses
  // `payload` is null and the size + 8-byte SHA-256 prefix describe
  // what was sent.
  | { kind: 'bus_joined'; color: string; project_id: string; bus_id: string; offset: number; name: string; at_unix: number }
  | { kind: 'bus_left'; color: string; project_id: string; bus_id: string; offset: number; name: string; at_unix: number }
  | { kind: 'bus_message'; color: string; project_id: string; bus_id: string; offset: number; from: string; msg_kind: string; payload: BusPayload; payload_byte_size: number; payload_sha256_prefix: string; at_unix: number }
  | { kind: 'bus_closed'; color: string; project_id: string; bus_id: string; offset: number; at_unix: number }
  // Loop events. Carry the inspector groupId + parent_frames so
  // nested loops and parallel sibling iterations route to distinct
  // inspector cards.
  | { kind: 'loop_instantiated'; color: string; project_id: string; group_id: string; parent_frames: LoopIteration[]; iter_count: number; parallel: boolean }
  | { kind: 'loop_iteration_launched'; color: string; project_id: string; group_id: string; parent_frames: LoopIteration[]; index: number }
  | { kind: 'loop_out_fired'; color: string; project_id: string; group_id: string; parent_frames: LoopIteration[]; index: number; done_vote?: boolean | null }
  | { kind: 'loop_terminated'; color: string; project_id: string; group_id: string; parent_frames: LoopIteration[]; reason: LoopTerminationReason }
  // Graph-level participation: a node is wired to a bus. Derived
  // dispatcher-side from PulseEmitted events carrying a bus marker,
  // so source AND target nodes get one BusParticipant edge each.
  // `ephemeral` is sniffed from the marker JSON, so the webview learns
  // mode the same time it learns about the bus and renders the panel
  // header badge without a separate event.
  | { kind: 'bus_participant'; color: string; project_id: string; bus_id: string; node_id: string; ephemeral: boolean }
  // One journal row the dispatcher could not apply during fold.
  // Emitted one-shot at replay time per affected row. The webview
  // groups by color and renders a muted "N journal rows corrupted"
  // collapsed disclosure in the inspector; not a banner, not red.
  | { kind: 'journal_corruption'; color: string; project_id: string; site: CorruptionSite; reason: string };

export type PostFn = (msg: HostMessage) => void;

export class ExecutionFollower implements vscode.Disposable {
  private eventSource: { close: () => void } | undefined;
  private currentColor: string | undefined;

  constructor(
    private readonly client: DispatcherClient,
    private readonly post: PostFn,
  ) {}

  /** Start following a fresh execution (live from the first event).
   *  The webview gets an execReset up front so any old colors / pulses
   *  drop. */
  follow(color: string): void {
    this.start(color, false);
  }

  /** Hydrate a past execution by replaying every journaled event up
   *  front, then keep following so a still-running execution stays
   *  live. Called when the user clicks an execution in the sidebar. */
  async replay(color: string): Promise<void> {
    await this.start(color, true);
  }

  /** Single follow path for both live and replay. Subscribe-FIRST,
   *  buffering live events, THEN run the replay GET, THEN drain the
   *  buffer. This closes the gap where an event that fired between
   *  "replay GET returned" and "subscribe attached" was dropped
   *  forever. Re-applying an event that appears in BOTH the replay and
   *  the buffer is harmless: node executions are keyed by
   *  (nodeId, framesKey) and updated in place (idempotent), and bus +
   *  loop logs are deduped at append time in App.svelte (bus by
   *  (busId, offset); loop by (groupId, kind, parentFrames, index)). */
  private async start(color: string, doReplay: boolean): Promise<void> {
    this.stop();
    this.currentColor = color;
    this.post({ kind: 'execReset' });

    // Buffer live events until the replay (if any) has been applied,
    // so live events never overtake their historical context.
    let buffering = doReplay;
    const buffer: DispatcherEvent[] = [];
    const onData = (data: string) => {
      let event: DispatcherEvent;
      try {
        event = JSON.parse(data) as DispatcherEvent;
      } catch (err) {
        console.warn('[weft/execFollower] bad SSE payload', err);
        return;
      }
      if (buffering) buffer.push(event);
      else this.apply(event);
    };
    this.eventSource = this.client.subscribe(
      `/events/execution/${color}`,
      (ev) => onData(ev.data),
      {
        // The dispatcher's per-execution stream stays open (keep-alive)
        // for the life of the project channel, so a clean close or an
        // error both mean the live link is GONE, not "execution done"
        // (that arrives as an execution_completed event on the open
        // stream). Surface it so the UI stops presenting the run as
        // live instead of leaving it stuck "running" forever.
        onClosed: () => {
          if (this.currentColor === color) this.post({ kind: 'followLost', color, reason: 'closed' });
        },
        onError: (err) => {
          console.warn('[weft/execFollower] live follow lost', err);
          if (this.currentColor === color) this.post({ kind: 'followLost', color, reason: 'error' });
        },
      },
    );

    if (doReplay) {
      try {
        const events = await this.client.get<DispatcherEvent[]>(`/executions/${color}/replay`);
        // A follow switch may have landed while the GET was in flight.
        if (this.currentColor !== color) return;
        for (const e of events) this.apply(e);
      } catch (err) {
        if (this.currentColor !== color) return;
        // The history failed to load. `followLost` tells the webview the
        // follow is dead (Stop button hidden, "re-open to reconnect"
        // toast), so the follow MUST actually be dead: tear down the SSE
        // and drop the buffer before posting, instead of leaving the
        // stream live underneath a UI that asserts it's gone. Re-opening
        // the execution retries the replay; a follow with no history is
        // not worth keeping half-alive.
        console.warn('[weft/execFollower] replay failed', err);
        this.stop();
        this.post({
          kind: 'followLost',
          color,
          reason: 'error',
        });
        return;
      }
      // Drain anything that arrived during the replay, then go live.
      buffering = false;
      for (const e of buffer) this.apply(e);
      buffer.length = 0;
    }
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
          frames: e.frames,
          input: e.input,
          closedPorts: e.closed_ports,
        };
        this.post({ kind: 'execEvent', event: execEvent });
        break;
      }
      case 'node_suspended': {
        // SSE 'node_suspended' = engine 'WaitingForInput'. Use the
        // Rust-side label directly so the inspector renders one
        // canonical state instead of two names (suspended-via-SSE
        // vs waiting_for_input-via-fold).
        const execEvent: NodeExecEvent = {
          nodeId: e.node,
          state: 'waiting_for_input',
          frames: e.frames,
        };
        this.post({ kind: 'execEvent', event: execEvent });
        break;
      }
      case 'node_resumed': {
        // A resume is rendered as a plain re-dispatch (the row returns
        // to `running`). The delivered value shows up in the execution
        // replay itself, so the event carries no resume payload.
        const execEvent: NodeExecEvent = {
          nodeId: e.node,
          state: 'running',
          frames: e.frames,
        };
        this.post({ kind: 'execEvent', event: execEvent });
        break;
      }
      case 'node_cancelled': {
        const execEvent: NodeExecEvent = {
          nodeId: e.node,
          state: 'cancelled',
          frames: e.frames,
          error: e.reason,
        };
        this.post({ kind: 'execEvent', event: execEvent });
        break;
      }
      case 'node_completed': {
        const execEvent: NodeExecEvent = {
          nodeId: e.node,
          state: 'completed',
          frames: e.frames,
          output: e.output,
        };
        this.post({ kind: 'execEvent', event: execEvent });
        break;
      }
      case 'node_failed': {
        const execEvent: NodeExecEvent = {
          nodeId: e.node,
          state: 'failed',
          frames: e.frames,
          error: e.error,
        };
        this.post({ kind: 'execEvent', event: execEvent });
        break;
      }
      case 'node_skipped': {
        const execEvent: NodeExecEvent = {
          nodeId: e.node,
          state: 'skipped',
          frames: e.frames,
          closedPorts: e.closed_ports,
        };
        this.post({ kind: 'execEvent', event: execEvent });
        break;
      }
      case 'port_type_mismatch': {
        // Non-terminal: attach a warning to the firing's row without a
        // state change. The node keeps running; one port's value was
        // dropped and the port closed.
        this.post({
          kind: 'execPortWarning',
          nodeId: e.node,
          frames: e.frames,
          port: e.port,
          expected: e.expected,
          actual: e.actual,
        });
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
      case 'bus_joined':
        // Forward `offset` on every bus event so the inspector can
        // dedupe replay-vs-live, render a stable row id for
        // debugging, and (in future) request gaps after SSE
        // reconnect.
        this.post({
          kind: 'busEvent',
          event: { kind: 'joined', busId: e.bus_id, offset: e.offset, name: e.name, atUnix: e.at_unix },
        });
        break;
      case 'bus_left':
        this.post({
          kind: 'busEvent',
          event: { kind: 'left', busId: e.bus_id, offset: e.offset, name: e.name, atUnix: e.at_unix },
        });
        break;
      case 'bus_message':
        this.post({
          kind: 'busEvent',
          event: {
            kind: 'message',
            busId: e.bus_id,
            offset: e.offset,
            from: e.from,
            msgKind: e.msg_kind,
            payload: e.payload,
            payloadByteSize: e.payload_byte_size,
            payloadSha256Prefix: e.payload_sha256_prefix,
            atUnix: e.at_unix,
          },
        });
        break;
      case 'bus_closed':
        this.post({
          kind: 'busEvent',
          event: { kind: 'closed', busId: e.bus_id, offset: e.offset, atUnix: e.at_unix },
        });
        break;
      case 'bus_participant':
        this.post({
          kind: 'busParticipant',
          busId: e.bus_id,
          nodeId: e.node_id,
          meta: { ephemeral: e.ephemeral },
        });
        break;
      case 'loop_instantiated':
        this.post({
          kind: 'loopEvent',
          event: {
            kind: 'instantiated',
            groupId: e.group_id,
            parentFrames: e.parent_frames,
            iterCount: e.iter_count,
            parallel: e.parallel,
          },
        });
        break;
      case 'loop_iteration_launched':
        this.post({
          kind: 'loopEvent',
          event: {
            kind: 'iteration_launched',
            groupId: e.group_id,
            parentFrames: e.parent_frames,
            index: e.index,
          },
        });
        break;
      case 'loop_out_fired':
        this.post({
          kind: 'loopEvent',
          event: {
            kind: 'out_fired',
            groupId: e.group_id,
            parentFrames: e.parent_frames,
            index: e.index,
            doneVote: e.done_vote ?? null,
          },
        });
        break;
      case 'loop_terminated':
        this.post({
          kind: 'loopEvent',
          event: {
            kind: 'terminated',
            groupId: e.group_id,
            parentFrames: e.parent_frames,
            reason: e.reason,
          },
        });
        break;
      case 'journal_corruption':
        this.post({
          kind: 'journalCorruption',
          site: e.site,
          reason: e.reason,
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
