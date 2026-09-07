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

import type * as vscode from 'vscode';

import type { DispatcherClient } from './dispatcher';
import type {
  WirePayload,
  CancelCause,
  CorruptionSite,
  HostMessage,
  LoopIteration,
  LoopTerminationReason,
  NodeExecEvent,
  SkipReason,
} from '../../packages/weft-graph/src/protocol';

// A replay may stall while live updates keep arriving. Bound the waiting
// data by bytes, since a single node result can be much larger than a status.
export const MAX_REPLAY_BUFFER_BYTES = 8 * 1024 * 1024;

// SYNC: DispatcherEvent <-> crates/weft-dispatcher/src/events.rs DispatcherEvent, weavemind/website/src/lib/graph/dispatcher-host.ts translateDispatcherEvent
// SYNC: event_id <-> crates/weft-dispatcher/src/events.rs IdentifiedEvent
export type DispatcherEvent = { event_id: string } & (
  | { kind: 'execution_started'; color: string; entry_node: string; project_id: string; at_unix: number }
  | { kind: 'node_started'; color: string; node: string; frames: LoopIteration[]; input: unknown; closed_ports: string[]; project_id: string; at_unix: number }
  | { kind: 'node_suspended'; color: string; node: string; frames: LoopIteration[]; token: string; project_id: string; at_unix: number }
  | { kind: 'node_resumed'; color: string; node: string; frames: LoopIteration[]; token: string | null; value: unknown; project_id: string; at_unix: number }
  | { kind: 'node_cancelled'; color: string; node: string; frames: LoopIteration[]; reason: string; project_id: string; at_unix: number }
  | { kind: 'node_completed'; color: string; node: string; frames: LoopIteration[]; output: unknown; project_id: string; at_unix: number }
  | { kind: 'node_failed'; color: string; node: string; frames: LoopIteration[]; error: string; project_id: string; at_unix: number }
  | { kind: 'node_skipped'; color: string; node: string; frames: LoopIteration[]; closed_ports: string[]; reason: SkipReason; project_id: string; at_unix: number }
  | { kind: 'port_type_mismatch'; color: string; node: string; frames: LoopIteration[]; port: string; expected: string; actual: string; project_id: string; at_unix: number }
  | { kind: 'execution_completed'; color: string; project_id: string; outputs: unknown; at_unix: number }
  | { kind: 'execution_failed'; color: string; project_id: string; error: string; at_unix: number }
  | { kind: 'execution_cancelled'; color: string; project_id: string; reason: string; cause?: CancelCause; at_unix: number }
  | { kind: 'execution_tagged'; color: string; project_id: string; tags: string[]; at_unix: number }
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
  // A lifecycle axis flipped (entering/leaving activating /
  // deactivating / building / cancelling_build, or landing at rest).
  // Carries both axes; the host treats it as a status-refresh signal
  // like every other project_* event.
  | { kind: 'project_transition_changed'; project_id: string; status: string; transition: string }
  // External-URL invalidation. Trigger nodes that mint a tenant-
  // public URL emit this when the URL changes (re-activate, new
  // mount path, etc.). UI invalidates any cached chip.
  | { kind: 'trigger_url_changed'; project_id: string; node_id: string; url: string }
  // One metered call's cost record (a provider meter's figure), attributed
  // to the exact firing. amount_usd null = the meter could not resolve the
  // figure. cost_id is the record's stable identity (the webview dedups on
  // it: the same journal row can arrive via both replay and live streams).
  | { kind: 'cost_reported'; color: string; project_id: string; node_id: string; frames: LoopIteration[]; cost_id: string; service: string; amount_usd: number | null; origin: 'their-own' | 'ours'; at_unix: number }
  // Operator-visible banner: the supervisor couldn't parse the
  // project's `health_protocols_json`. Surfaces as an action-bar
  // banner; the user fixes the config and the next tick recovers.
  | { kind: 'infra_config_error'; project_id: string; error: string }
  // Bus events: live + replay. The inspector renders one IRC-style
  // log per node per bus. `bus_id` is the channel's uuid (matches
  // the uuid embedded in the bus marker JSON that flows on pulses);
  // `from` on a message is the sender's registered name (stamped by
  // the bus on the producer side, never spoofed). A journaled bus's
  // window carries its `messages`; an ephemeral bus's window carries
  // only the `totals` rollup.
  // SYNC: DispatcherEvent 'bus_window' messages <-> crates/weft-core/src/bus.rs WindowedBusMessage, packages/weft-graph/src/protocol.ts BusInspectorEvent 'message'
  // SYNC: DispatcherEvent 'bus_window' totals <-> crates/weft-core/src/bus.rs BusWindowTotal, packages/weft-graph/src/protocol.ts BusInspectorEvent 'window' totals
  | { kind: 'bus_joined'; color: string; project_id: string; bus_id: string; offset: number; name: string; at_unix: number }
  | { kind: 'bus_left'; color: string; project_id: string; bus_id: string; offset: number; name: string; at_unix: number }
  | { kind: 'bus_window'; color: string; project_id: string; bus_id: string; first_offset: number; last_offset: number; messages: Array<{ offset: number; from: string; msg_kind: string; payload: WirePayload; payload_byte_size: number; at_unix: number }>; totals: Array<{ from: string; msg_kind: string; count: number; bytes: number }>; at_unix: number }
  | { kind: 'bus_closed'; color: string; project_id: string; bus_id: string; offset: number; at_unix: number }
  // Live caller connection events. One caller per execution (keyed by
  // color, no bus_id). The webview replays the caller exchange the same
  // way it replays a bus; `payload` is the same tagged WirePayload a
  // bus window's messages carry.
  // SYNC: DispatcherEvent 'caller_inbound'/'caller_outbound' <-> crates/weft-journal/src/events.rs CallerInbound/CallerOutbound, crates/weft-dispatcher/src/events.rs CallerInbound/CallerOutbound, packages/weft-graph/src/protocol.ts CallerInspectorEvent 'inbound'/'outbound'
  | { kind: 'caller_connected'; color: string; project_id: string; offset: number; protocol: string; at_unix: number }
  | { kind: 'caller_inbound'; color: string; project_id: string; offset: number; payload: WirePayload; payload_byte_size: number; at_unix: number }
  | { kind: 'caller_outbound'; color: string; project_id: string; offset: number; payload: WirePayload; payload_byte_size: number; terminal: boolean; at_unix: number }
  | { kind: 'caller_errored'; color: string; project_id: string; offset: number; message: string; at_unix: number }
  | { kind: 'caller_disconnected'; color: string; project_id: string; offset: number; reason: string; at_unix: number }
  // Loop events. Carry the inspector groupId + parent_frames so
  // nested loops and parallel sibling iterations route to distinct
  // inspector cards.
  // SYNC: loop_instantiated <-> crates/weft-dispatcher/src/events.rs LoopInstantiated, packages/weft-graph/src/protocol.ts LoopInspectorEvent 'instantiated'
  | { kind: 'loop_instantiated'; color: string; project_id: string; group_id: string; parent_frames: LoopIteration[]; iter_cap: number | null; parallel: boolean; at_unix: number }
  | { kind: 'loop_iteration_launched'; color: string; project_id: string; group_id: string; parent_frames: LoopIteration[]; index: number; at_unix: number }
  | { kind: 'loop_out_fired'; color: string; project_id: string; group_id: string; parent_frames: LoopIteration[]; index: number; done_vote?: boolean | null; at_unix: number }
  | { kind: 'loop_terminated'; color: string; project_id: string; group_id: string; parent_frames: LoopIteration[]; reason: LoopTerminationReason; at_unix: number }
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
  | { kind: 'journal_corruption'; color: string; project_id: string; site: CorruptionSite; reason: string });

function identifiedEvent(value: unknown): DispatcherEvent {
  if (!value || typeof value !== 'object' || !('event_id' in value)
      || typeof value.event_id !== 'string' || value.event_id.length === 0) {
    throw new Error('Execution event has no delivery identity');
  }
  return value as DispatcherEvent;
}

export type PostFn = (msg: HostMessage) => void;

export class ExecutionFollower implements vscode.Disposable {
  private eventSource: { close: () => void } | undefined;
  private generation = 0;
  private cancelStart: (() => void) | undefined;
  private historyAbort: AbortController | undefined;

  constructor(
    private readonly client: DispatcherClient,
    private readonly post: PostFn,
  ) {}

  /** Hydrate a past execution by replaying every journaled event up
   *  front, then keep following so a still-running execution stays
   *  live. Called when the user clicks an execution in the sidebar. */
  async replay(color: string): Promise<void> {
    await this.start(color);
  }

  /** The one follow path. Subscribe-FIRST,
   *  buffering live events, THEN run the replay GET, THEN drain the
   *  buffer. This closes the gap where an event that fired between
   *  "replay GET returned" and "subscribe attached" was dropped
   *  forever. Stable event identities remove the overlap without
   *  comparing payloads or assuming that repeated updates are harmless. */
  private async start(color: string): Promise<void> {
    this.stop();
    const generation = this.generation;
    const historyAbort = new AbortController();
    this.historyAbort = historyAbort;
    const isCurrent = () => this.generation === generation;
    this.post({ kind: 'execReset' });

    let opened!: (ready: boolean) => void;
    const ready = new Promise<boolean>((resolve) => { opened = resolve; });
    this.cancelStart = () => opened(false);
    const lost = (reason: 'closed' | 'error') => {
      if (!isCurrent()) return;
      this.stop();
      this.post({ kind: 'followLost', color, reason });
    };

    // Buffer live events until the replay has been applied,
    // so live events never overtake their historical context.
    let buffering = true;
    const buffer: DispatcherEvent[] = [];
    let bufferedBytes = 0;
    // Track only the initial history. This cannot grow with a long-running
    // live stream, and also covers history the bridge has not delivered yet.
    const historyIds = new Set<string>();
    const applyLive = (event: DispatcherEvent) => {
      if (!historyIds.has(event.event_id)) this.apply(event);
    };
    const onData = (data: string) => {
      if (!isCurrent()) return;
      if (buffering) {
        bufferedBytes += Buffer.byteLength(data, 'utf8');
        if (bufferedBytes > MAX_REPLAY_BUFFER_BYTES) {
          console.warn('[weft/execFollower] live updates exceeded the replay buffer; reopen the run to reload its history');
          lost('error');
          return;
        }
      }
      let event: DispatcherEvent;
      try {
        event = identifiedEvent(JSON.parse(data));
      } catch (err) {
        console.warn('[weft/execFollower] bad SSE payload', err);
        lost('error');
        return;
      }
      if (buffering) {
        buffer.push(event);
      } else applyLive(event);
    };
    this.eventSource = this.client.subscribe(
      `/events/execution/${color}`,
      (ev) => onData(ev.data),
      {
        // Starting fetch is not enough: history must be read only once
        // the server has attached the live subscription.
        onOpen: () => opened(true),
        // The dispatcher's per-execution stream stays open (keep-alive)
        // for the life of the project channel, so a clean close or an
        // error both mean the live link is GONE, not "execution done"
        // (that arrives as an execution_completed event on the open
        // stream). Surface it so the UI stops presenting the run as
        // live instead of leaving it stuck "running" forever.
        onClosed: () => {
          lost('closed');
        },
        onError: (err) => {
          console.warn('[weft/execFollower] live follow lost', err);
          lost('error');
        },
      },
    );

    if (!await ready || !isCurrent()) return;
    this.cancelStart = undefined;

    {
      try {
        const events = await this.client.get<DispatcherEvent[]>(`/executions/${color}/replay`, historyAbort.signal);
        // A follow switch may have landed while the GET was in flight.
        if (!isCurrent()) return;
        for (const raw of events) {
          const event = identifiedEvent(raw);
          if (!historyIds.has(event.event_id)) {
            historyIds.add(event.event_id);
            this.apply(event);
          }
        }
      } catch (err) {
        if (!isCurrent()) return;
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
      for (const e of buffer) applyLive(e);
      buffer.length = 0;
    }
  }

  stop(): void {
    this.generation++;
    this.cancelStart?.();
    this.cancelStart = undefined;
    this.historyAbort?.abort();
    this.historyAbort = undefined;
    this.eventSource?.close();
    this.eventSource = undefined;
  }

  dispose(): void {
    this.stop();
  }

  private apply(e: DispatcherEvent): void {
    switch (e.kind) {
      case 'node_started': {
        const execEvent: NodeExecEvent = {
          nodeId: e.node,
          atUnix: e.at_unix,
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
          atUnix: e.at_unix,
          state: 'waiting_for_input',
          frames: e.frames,
        };
        this.post({ kind: 'execEvent', event: execEvent });
        break;
      }
      case 'node_resumed': {
        // A resume is rendered as a re-dispatch (the row returns to
        // `running`), flagged `resumed` so the reducer keeps the
        // firing's accumulated per-attempt state (port warnings)
        // instead of resetting it like a fresh start. The delivered
        // value shows up in the execution replay itself, so the event
        // carries no resume payload.
        const execEvent: NodeExecEvent = {
          nodeId: e.node,
          atUnix: e.at_unix,
          state: 'running',
          resumed: true,
          frames: e.frames,
        };
        this.post({ kind: 'execEvent', event: execEvent });
        break;
      }
      case 'node_cancelled': {
        const execEvent: NodeExecEvent = {
          nodeId: e.node,
          atUnix: e.at_unix,
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
          atUnix: e.at_unix,
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
          atUnix: e.at_unix,
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
          atUnix: e.at_unix,
          state: 'skipped',
          frames: e.frames,
          closedPorts: e.closed_ports,
          skipReason: e.reason,
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
        this.post({
          kind: 'execTerminal',
          color: e.color,
          state: e.kind === 'execution_completed' ? 'completed' : 'failed',
          atUnix: e.at_unix,
        });
        break;
      case 'execution_cancelled':
        // A cancel carries why: the text and, on every row written since
        // the cause existed, the structured value (a sibling run's stop
        // names the run and the tag).
        this.post({
          kind: 'execTerminal',
          color: e.color,
          state: 'cancelled',
          reason: e.reason,
          cause: e.cause,
          atUnix: e.at_unix,
        });
        break;
      case 'execution_tagged':
        this.post({ kind: 'execTags', color: e.color, tags: e.tags });
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
      case 'bus_window':
        // One journal row per aggregation window. A journaled bus's
        // window unpacks into per-message panel lines (boundaries and
        // senders kept); an ephemeral window carries only the rollup
        // and renders as one summary line.
        for (const m of e.messages) {
          this.post({
            kind: 'busEvent',
            event: {
              kind: 'message',
              busId: e.bus_id,
              offset: m.offset,
              from: m.from,
              msgKind: m.msg_kind,
              payload: m.payload,
              payloadByteSize: m.payload_byte_size,
              atUnix: m.at_unix,
            },
          });
        }
        // An empty `messages` list IS the ephemeral discriminator: the
        // backend never emits an empty window row, so a window without
        // messages always carries the rollup.
        if (e.messages.length === 0) {
          this.post({
            kind: 'busEvent',
            event: {
              kind: 'window',
              busId: e.bus_id,
              offset: e.first_offset,
              lastOffset: e.last_offset,
              totals: e.totals.map((t) => ({
                from: t.from,
                msgKind: t.msg_kind,
                count: t.count,
                bytes: t.bytes,
              })),
              atUnix: e.at_unix,
            },
          });
        }
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
      // Live caller connection: forwarded as one `callerEvent` stream
      // (one caller per execution, keyed by color, so no busId). The
      // webview replays it like a bus panel.
      case 'caller_connected':
        this.post({
          kind: 'callerEvent',
          event: { kind: 'connected', offset: e.offset, protocol: e.protocol, atUnix: e.at_unix },
        });
        break;
      case 'caller_inbound':
        this.post({
          kind: 'callerEvent',
          event: {
            kind: 'inbound',
            offset: e.offset,
            payload: e.payload,
            payloadByteSize: e.payload_byte_size,
            atUnix: e.at_unix,
          },
        });
        break;
      case 'caller_outbound':
        this.post({
          kind: 'callerEvent',
          event: {
            kind: 'outbound',
            offset: e.offset,
            payload: e.payload,
            payloadByteSize: e.payload_byte_size,
            terminal: e.terminal,
            atUnix: e.at_unix,
          },
        });
        break;
      case 'caller_errored':
        this.post({
          kind: 'callerEvent',
          event: { kind: 'errored', offset: e.offset, message: e.message, atUnix: e.at_unix },
        });
        break;
      case 'caller_disconnected':
        this.post({
          kind: 'callerEvent',
          event: { kind: 'disconnected', offset: e.offset, reason: e.reason, atUnix: e.at_unix },
        });
        break;
      case 'loop_instantiated':
        this.post({
          kind: 'loopEvent',
          event: {
            kind: 'instantiated',
            groupId: e.group_id,
            parentFrames: e.parent_frames,
            iterCap: e.iter_cap,
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
      case 'cost_reported':
        // One metered call's cost record: fold it onto the firing's row
        // (the webview dedups by costId across replay/live overlap).
        this.post({
          kind: 'execCost',
          nodeId: e.node_id,
          frames: e.frames,
          costId: e.cost_id,
          amountUsd: e.amount_usd,
          origin: e.origin,
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
      case 'project_transition_changed':
      case 'trigger_url_changed':
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
