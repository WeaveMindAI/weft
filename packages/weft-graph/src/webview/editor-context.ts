// The full editor context handed to panel SLOTS (`leftPanel` / `rightPanel`).
//
// The slot model: the shared editor owns the canvas + toolbar and exposes two
// side regions a consumer fills. Rather than curate a per-slot parameter list
// (which the editor would have to keep widening as panels want more), each slot
// receives the COMPLETE editor context: all live editor state PLUS the
// communication channel to the host (`send` / `onMessage`). A panel can then do
// anything a first-class part of the editor could: read any state, send any
// `WebviewMessage` to the host, subscribe to any `HostMessage` from it. A web
// host can fill these (e.g. a code view on the left + a replay bar on the right);
// the VS Code extension fills neither (it uses its own file tab + right sidebar).
//
// Every field is a GETTER so the object is a stable reference whose reads are
// reactive: a slot consumer reads `ctx.activeSource` and re-renders when it
// changes, without the editor re-creating the context object.

import type { HostMessage, WebviewMessage, EditOp, Diagnostic } from '../protocol';
import type { ProjectDefinition, ExecutionState } from './lib/types';
import type { EditRpcResult } from './lib/projection/types';

export interface EditorContext {
  /// The translated project currently rendered on the canvas (the active
  /// file's graph), or null before the first parse.
  readonly project: ProjectDefinition | null;
  /// The raw `.weft` source of the ACTIVE file: the entry file at depth 0, or
  /// the included sub-file after the user navigates into an `@include`. Tracks
  /// navigation, so a code view bound to it always shows the active graph's
  /// source.
  readonly activeSource: string;
  /// The active file's display name (the navigation breadcrumb leaf). Empty at
  /// the entry file.
  readonly activeFileName: string;
  /// Include-navigation depth: 0 at the entry file, >0 inside an included file.
  readonly navDepth: number;
  /// The dotted alias chain of the current navigation (used to scope execution
  /// values to the active subgraph). Empty at depth 0.
  readonly execPrefix: string;
  /// Live + replayed execution state (per-node statuses + outputs + inspector
  /// logs). A replay panel writes a past run's end-state here via the host; the
  /// canvas already colors nodes from it.
  readonly executionState: ExecutionState;
  /// Current inline parse/translation error, or null.
  readonly error: string | null;
  /// Structured parse/enrich diagnostics for the ACTIVE file (with line +
  /// column), from the latest parse. A code panel paints these as inline
  /// squiggles; empty when the source is clean. Distinct from `error` (a single
  /// human string for the banner): these carry positions for the editor gutter.
  readonly diagnostics: readonly Diagnostic[];
  /// Whether the SOURCE view is open. The editor's toolbar Source button toggles
  /// it (via the host); a left-slot code panel reads this to show/hide itself, so
  /// the toolbar button and the panel share one open/closed state.
  readonly sourceOpen: boolean;

  /// Toggle the source view (the same intent as the toolbar Source button).
  /// Lets an injected panel (a code view's own collapse button) drive the shared
  /// open/closed state without reaching past the context.
  toggleSource(): void;

  /// Apply a DIRECT edit to the active file's `.weft` source (an editable code
  /// panel). The host re-parses and the editor adopts the result as external
  /// truth (canvas updates, pending ops re-apply), the inverse direction of a
  /// graph gesture. Debounce at the call site; the host coalesces re-parses.
  editActiveSource(source: string): void;

  /// Apply structural EDIT OPS (setConfig / removeConfig / ...) the way the
  /// canvas does: through the projection RPC that optimistically tracks the op,
  /// awaits the host's reply, and adopts the post-edit truth into `project` (so
  /// the canvas AND a panel reading `ctx.project` both refresh). Use this from a
  /// host-drawn config panel instead of a raw `send({applyEdits})`, whose reply
  /// would be dropped and leave the on-screen graph stale.
  applyEdits(ops: EditOp[]): Promise<EditRpcResult>;

  /// Post a message to the host (run, navigate, refresh status, anything a
  /// `WebviewMessage` expresses). The same channel the editor itself uses.
  send(msg: WebviewMessage): void;
  /// Subscribe to host messages (parse results, exec events, nav state, ...).
  /// Returns an unsubscribe fn. A panel uses this to react to host pushes it
  /// cares about beyond what the context state already surfaces.
  onMessage(handler: (msg: HostMessage) => void): () => void;
}
