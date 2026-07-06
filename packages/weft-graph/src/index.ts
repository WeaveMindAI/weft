// Public surface of the shared weft graph package.
//
// Any host renders the graph from here, injecting its own host transport
// (`setHostTransport`): the VS Code extension uses webview messaging (the
// default); a web host injects a transport backed by the dispatcher HTTP API.
// Everything below the transport (the editor, the node/group rendering, the
// projection engine, layout, validation) is shared.

export { default as App } from './webview/App.svelte';
export { default as ProjectEditor } from './webview/lib/components/project/ProjectEditor.svelte';

// Slot furniture a consumer reuses to wrap its injected panels: `SlotPanel`
// gives a resizable + collapsible container whose RULES (side, min/max width,
// collapse mode full|rail) the consumer declares. `CopyButton` is the shared
// copy-to-clipboard control (used by the replay panel's copy-state).
export { default as SlotPanel } from './webview/lib/components/SlotPanel.svelte';
export { default as CopyButton } from './webview/lib/components/ui/CopyButton.svelte';
// Chip editor for a node's tags (`config._tags`). Host-agnostic: it takes the
// current tags + an onChange, so a host panel (e.g. a signal-token minter) can
// reuse the exact canvas tag-editing UI + charset validation.
export { default as NodeTagsEditor } from './webview/lib/components/project/NodeTagsEditor.svelte';
// Read/normalize a node's tags from its config. One place owns the `_tags`
// config-key shape so hosts don't re-derive it.
export { nodeTags, TAGS_CONFIG_KEY } from './webview/lib/node-tags';
// The typed field renderer + the config-diff producer, so a host-drawn config
// panel edits a node's fields THROUGH the same machinery the canvas uses:
// FieldStrip draws the typed controls (with the debounced field editor), and
// `diffConfigOps` turns a new config map into the correct setConfig/removeConfig
// EditOps (source tokens via `formatConfigValue`), instead of a parallel
// hand-rolled renderer + token formatter that would drift.
export { default as FieldStrip } from './webview/lib/components/project/FieldStrip.svelte';
export { diffConfigOps } from './webview/lib/projection/config-diff';

// The one snake_case -> camelCase remap for the dispatcher status payload lives
// in `@weft/graph/status` (its own subpath so node-side consumers never pull
// the Svelte component graph in). Both hosts build their snapshot + the action
// bar's backend projection through it.

// The host seam: a consumer outside VS Code injects its transport before mount.
export { setHostTransport, teardownTransport, send, onMessage, resolveStoredFileUrl } from './webview/vscode';
export type { HostTransport } from './webview/vscode';

// The panel-slot seam: `App` takes `leftPanel` / `rightPanel` snippets, each
// handed the full `EditorContext` (all live editor state + the host channel), so
// a host injects whatever side panels it wants. A web host can fill these (e.g. a
// code view + replay bar); the VS Code extension fills neither (native chrome).
export type { EditorContext } from './webview/editor-context';

// The wire protocol (host <-> editor messages). A consumer's transport produces
// `HostMessage`s and consumes `WebviewMessage`s.
export type {
  HostMessage,
  WebviewMessage,
  CatalogEntry,
} from './protocol';

// The editor's view of a project + execution state, for consumers that build
// `HostMessage`s from their own backend data.
export type {
  ProjectDefinition,
  NodeInstance,
  Edge,
  NodeExecution,
  ExecutionState,
} from './webview/lib/types';

// Convert a compiled (Rust-shape) ProjectDefinition into the editor's view.
export { translateProject } from './webview/host-bridge';

// Shared node-feed transforms/guards (`infraLive` / `signalDisplay`), so every
// host maps the backend's live payloads to `LiveDataItem[]` identically.
export { isLiveDataItem, signalDisplayToLiveItems } from './live-data';

// Node catalog registration (the editor needs node metadata to render ports +
// config). A consumer feeds it the metadata catalog (the same metadata-only
// catalog the WASM parser uses). `NODE_TYPE_CONFIG` + `getAllNodes` expose the
// live registry so a host-drawn config panel can render a node's TYPED fields
// (the same field metadata the canvas uses) instead of blind text inputs.
export {
  registerCatalog,
  setCatalog,
  NODE_TYPE_CONFIG,
  getAllNodes,
} from './webview/lib/nodes';

// Field + node-template metadata types, for a host rendering config editors from
// the catalog (`NODE_TYPE_CONFIG[nodeType].fields`).
export type {
  FieldDefinition,
  FieldType,
  NodeTemplate,
} from './webview/lib/types';
