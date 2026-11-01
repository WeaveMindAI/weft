// Bridge between the host parse and the graph editor.
//
// Responsibility: translate the Rust compiler's ProjectDefinition (nodes,
// end-to-end wires naming the group ports they pass, a separate groups
// array, scoped ids) into the editor's ProjectDefinition shape (groups
// folded back into `nodes` as nodeType:"Group" NodeInstances, each wire
// split back into the legs a person wrote, with __inner handle routing).
// The Rust parse is the single source of the graph; this only reshapes it
// for xyflow.

import type {
  ProjectDefinition as HostProject,
  NodeDefinition as HostNode,
  Edge as HostEdge,
  GroupDefinition as HostGroup,
  PortDefinition as HostPort,
  InputDefinition as HostInput,
  FileRef,
} from '../protocol';
import type {
  ProjectDefinition as V1Project,
  NodeInstance,
  Edge as V1Edge,
  PortDefinition as V1Port,
  MetadataInput,
  MetadataRef,
} from './lib/types';
import { instanceFilled, instanceFilledValue } from '../protocol';

// THE TypeScript spelling of a loop's boundary-node ids (the Rust
// compiler mints a `LoopIn` and a `LoopOut` per loop, the only boundary
// nodes it mints). The two suffix constants are the one definition; both
// directions (the builders here and `parseLoopBoundary` below) derive
// from them.
// SYNC: LOOP_IN, LOOP_OUT <-> crates/weft-core/src/project.rs loop_in_id, loop_out_id
const LOOP_IN = '__in';
const LOOP_OUT = '__out';
export function loopInId(loopId: string): string {
  return `${loopId}${LOOP_IN}`;
}
export function loopOutId(loopId: string): string {
  return `${loopId}${LOOP_OUT}`;
}

/// A FRESH top-level copy of a wire port. The render shape IS the
/// wire's `InputDefinition` (an output/group port rides it with the
/// input-only members absent), so a generic spread carries every field
/// and adding one needs no edit here; a field-by-field copy here once
/// silently dropped several. The copy keeps the mutated-in-place render
/// graph from aliasing the parse message; it is shallow, which holds
/// because the projection only ever replaces whole port fields, never
/// mutates inside one.
function copyPort(p: HostPort | HostInput): V1Port {
  return { ...p };
}

/// One end of a drawn wire: a node's port, or a container's port on one
/// of its two faces. The editor spells the face a container's members see
/// with the `__inner` suffix on the handle (`x__inner` is `self.x`), the
/// face its surroundings see with the bare port name.
type LegEnd = { node: string; handle: string | null };

function innerHandle(port: string | null): string | null {
  return port ? `${port}__inner` : port;
}

/// A node end of a wire. A loop's `LoopIn`/`LoopOut` are nodes in the
/// compiled program but draw as the loop's own two faces, so an end on
/// one lands on the loop box.
function nodeEnd(id: string, handle: string | null, side: 'source' | 'target', loopIds: Set<string>): LegEnd {
  const bound = parseLoopBoundary(id, loopIds);
  if (!bound) return { node: id, handle };
  // What leaves a LoopIn and what lands on a LoopOut is the inside face;
  // what lands on a LoopIn and leaves a LoopOut is the outside one.
  const inside = (side === 'source') === (bound.role === 'In');
  return { node: bound.loopId, handle: inside ? innerHandle(handle) : handle };
}

/// The inverse of `loopInId`/`loopOutId`, derived from the same suffix
/// constants so the two directions cannot drift.
function parseLoopBoundary(id: string, loopIds: Set<string>): { loopId: string; role: 'In' | 'Out' } | null {
  if (id.endsWith(LOOP_IN)) {
    const loopId = id.slice(0, -LOOP_IN.length);
    if (loopIds.has(loopId)) return { loopId, role: 'In' };
  }
  if (id.endsWith(LOOP_OUT)) {
    const loopId = id.slice(0, -LOOP_OUT.length);
    if (loopIds.has(loopId)) return { loopId, role: 'Out' };
  }
  return null;
}

/// The lines a person wrote for one compiled wire, as the editor draws
/// them. The compiled wire runs end to end, from the node that makes the
/// value to the node that reads it, naming every group port it passes
/// (`via`); the editor draws one line per leg between two of those stops,
/// because that is what the source says (`g.x = a.out` outside the group,
/// `m.in = self.x` inside it) and what an edit rewrites.
///
/// Arriving at an input the leg lands on the group's outside face, and
/// the next leg leaves from its inside face; an output is the other way
/// round. A wire that starts at its source group's own port (a literal or
/// nothing, sent when the group opens) has no leg before that port, and a
/// wire that stops at its target group's own port (a port nobody on the
/// far side reads) has none after it. Each leg keeps its own keys: a
/// crossing holds the keys of the leg that ends there, and the last leg
/// has what is left of the wire's path.
export function legsOf(e: HostEdge, loopIds: Set<string>): V1Edge[] {
  const via = e.via ?? [];
  const legs: V1Edge[] = [];
  const push = (from: LegEnd, to: LegEnd, path: readonly string[]) => {
    legs.push({
      // The keys are part of the id, as in the compiler's `wire_id`: two
      // legs between the same ports reading different keys are two lines.
      id: `e-${from.node}-${from.handle ?? ''}-${to.node}-${to.handle ?? ''}${path.length > 0 ? `.${path.join('.')}` : ''}`,
      source: from.node,
      target: to.node,
      sourceHandle: from.handle,
      targetHandle: to.handle,
      ...(path.length > 0 ? { path: [...path] } : {}),
    });
  };
  let from: LegEnd | null = via[0]?.group === e.source ? null : nodeEnd(e.source, e.sourceHandle ?? null, 'source', loopIds);
  let consumed = 0;
  for (const c of via) {
    const path = c.path ?? [];
    if (from) push(from, { node: c.group, handle: c.side === 'in' ? c.port : innerHandle(c.port) }, path);
    consumed += path.length;
    from = { node: c.group, handle: c.side === 'in' ? innerHandle(c.port) : c.port };
  }
  const endsAtGroupPort = via.length > 0 && via[via.length - 1].group === e.target;
  if (from && !endsAtGroupPort) {
    push(from, nodeEnd(e.target, e.targetHandle ?? null, 'target', loopIds), (e.path ?? []).slice(consumed));
  }
  return legs;
}

/// Every leg of every wire, each once: wires that fan out through one
/// group port share the leg up to it (`g.x = a.out` feeds every member
/// reading `self.x`), and the source has that line once.
export function legsOfAll(edges: readonly HostEdge[], loopIds: Set<string>): V1Edge[] {
  const byId = new Map<string, V1Edge>();
  for (const e of edges) {
    for (const leg of legsOf(e, loopIds)) {
      if (!byId.has(leg.id)) byId.set(leg.id, leg);
    }
  }
  return [...byId.values()];
}

/// Which inputs a metadata wire fills, and from which metadata port, keyed
/// by the drawn node id the value lands on (a loop's boundary node is its
/// loop box). The compiler resolves a metadata wire when the program
/// compiles: it writes the value into the input as a port literal and the
/// wire never runs, so the editor draws no wire. It shows the input as
/// filled from that port instead, read-only, with the value the compiler
/// wrote, which is why the literal leaves `portLiterals` and moves here.
///
/// The wire stops where the value lands (a node's input, or a group's or a
/// loop's input seen from outside), which is the end of its last leg: a
/// loop's `LoopIn` draws as the loop box, so the fill is keyed on the
/// loop, which draws its `LoopIn`'s `portLiterals`. Its source
/// is the port it leaves: a group's own port when the wire starts there
/// (`via[0]` is that port's crossing), a loop's port when it leaves the
/// loop's `LoopOut`, otherwise the node's.
export function metadataFills(
  wires: readonly HostEdge[],
  loopIds: Set<string>,
): Map<string, Record<string, MetadataRef>> {
  const fills = new Map<string, Record<string, MetadataRef>>();
  for (const w of wires) {
    const legs = legsOf(w, loopIds);
    const last = legs[legs.length - 1];
    if (!last?.targetHandle) continue;
    const via0 = w.via?.[0];
    const from = via0?.group === w.source
      ? { node: via0.group, handle: via0.port }
      : nodeEnd(w.source, w.sourceHandle ?? null, 'source', loopIds);
    if (!from.handle) continue;
    let ports = fills.get(last.target);
    if (!ports) fills.set(last.target, (ports = {}));
    // One driver per input: the compiler refuses a second
    // (`double-driven-port`), so two here is a broken parse.
    if (ports[last.targetHandle]) {
      throw new Error(`host-bridge: two metadata wires fill '${last.target}.${last.targetHandle}'`);
    }
    ports[last.targetHandle] = {
      kind: 'port',
      source: from.node,
      port: from.handle,
      ...(w.path && w.path.length > 0 ? { path: [...w.path] } : {}),
    };
  }
  return fills;
}

/// A drawn node with the inputs a metadata wire fills (`metadataFills`)
/// moved out of its written values: each keeps the value the compiler
/// wrote, read-only, and the port it comes from.
export function withMetadataInputs(node: NodeInstance, filled: Record<string, MetadataRef> | undefined): NodeInstance {
  if (!filled) return node;
  const ports = new Set(Object.keys(filled));
  const drop = <T>(m: Record<string, T> | undefined): Record<string, T> | undefined =>
    m && Object.fromEntries(Object.entries(m).filter(([port]) => !ports.has(port)));
  const metadataInputs: Record<string, MetadataInput> = {};
  for (const [port, from] of Object.entries(filled)) {
    metadataInputs[port] = { from, value: node.portLiterals?.[port] };
  }
  return {
    ...node,
    portLiterals: drop(node.portLiterals),
    portLiteralSpans: drop(node.portLiteralSpans),
    metadataInputs,
  };
}

function resolveParentGroup(node: HostNode, groupIds: Set<string>): string | undefined {
  for (let i = node.scope.length - 1; i >= 0; i--) {
    const s = node.scope[i];
    if (groupIds.has(s)) return s;
  }
  return undefined;
}

/** How a group id reads to a PERSON, which is never the id itself for an
 *  included file's body. A body id is the file's path (`@src:lib:clean`),
 *  unspellable on purpose, so it shows as the file's own name (`clean`);
 *  a nested group's id carries its parents, so it shows as its own
 *  segment; anything else is already a name somebody wrote. */
// SYNC: displayName <-> crates/weft-compiler/src/source_name.rs display_name
function displayName(id: string, parentId: string | undefined): string {
  const prefix = parentId ? parentId + '.' : '';
  const local = prefix && id.startsWith(prefix) ? id.slice(prefix.length) : id;
  if (!local.startsWith('@')) return local;
  const segments = local.slice(1).split(':');
  return segments[segments.length - 1] || local;
}

/** A node's or a group's port literals as the editor holds them. The Rust
 *  parse resolves `@file` to the file's content, and enrich homes every
 *  port's constant in `portLiterals`. The editor's invariant is that a
 *  file-backed port literal holds the structural `@file` MARKER
 *  ({path, type}), never resolved content, so it serializes safely and
 *  renders file-backed. This puts the structural ref back from `fileRefs`;
 *  the resolved content is shown separately via the host's fileContents
 *  map. An `@instance_filled(@file(...))` field gets its ref back inside
 *  the marker, as its fallback. Undefined when neither map is present. */
function withFileMarkers(
  literals: Record<string, unknown> | undefined,
  fileRefs: Record<string, FileRef> | undefined,
): Record<string, unknown> | undefined {
  if (!literals && !fileRefs) return undefined;
  const out = { ...(literals ?? {}) };
  for (const [key, ref] of Object.entries(fileRefs ?? {})) {
    const marker = { __weftFileRef: { path: ref.path, type: ref.type, marker: ref.marker } };
    out[key] = instanceFilled(out[key]) ? instanceFilledValue(marker) : marker;
  }
  return out;
}

/// `loopIn` is the loop's `LoopIn` for a loop (its written values live
/// there alone, so the loop box draws them from it), absent for a group.
function groupToNodeInstance(g: HostGroup, loopIn: HostNode | undefined): NodeInstance {
  // `kind` is required on the wire; if it lands as anything other
  // than the two the editor draws, the host/webview view of the project
  // has drifted (version skew, corruption, or a full-mode parse: the
  // editor's parse keeps an `@include` as one opaque node, so a `call`
  // site or a `body` never reaches it). Silently treating an unknown
  // kind as Group would render a Loop as a regular group and hide the
  // divergence; surface it instead.
  if (g.kind !== 'group' && g.kind !== 'loop') {
    throw new Error(`host-bridge: unknown container kind '${String(g.kind)}' for id '${g.id}'`);
  }
  const isLoop = g.kind === 'loop';
  // The compiler mints a `LoopIn` for every loop it compiles; a loop
  // without one is a view of the project that has drifted.
  if (isLoop && !loopIn) {
    throw new Error(`host-bridge: loop '${g.id}' has no '${loopInId(g.id)}'`);
  }
  const written = loopIn ?? g;
  const parentId = g.parentGroupId ?? undefined;
  return {
    id: g.id,
    // Distinct nodeType per container so the renderer can pick a
    // loop-flavored xyflow type vs a group-flavored one. The lowering
    // (LoopIn/LoopOut boundary nodes) is unaffected by this.
    nodeType: isLoop ? 'Loop' : 'Group',
    // A user-written label wins when the language grows one (today the
    // wire always says null); the display name is what a person reads
    // the id as, so a nested group renders "inner" and an included
    // file's body renders its file name, never `@src:lib:clean`.
    label: g.label ?? displayName(g.id, parentId),
    // The webview reads the parent group from `config.parentId` (buildNodes,
    // getLayoutKey, edge scoping, the ancestor-collapse walk all read it there),
    // so the structural parent MUST be mirrored into config on every parse. Before
    // this it lived only on the top-level `parentId` field, so a re-parse left
    // `config.parentId` undefined: the node's scoped `id` said it was nested but
    // the graph thought it was top-level, and the next move op read the wrong
    // scope (the "every other move is a no-op" desync). A loop carries its loop
    // config too; both a group and a loop need parentId mirrored for nesting.
    config: {
      ...(isLoop && g.loopConfig ? (g.loopConfig as Record<string, unknown>) : {}),
      ...(parentId ? { parentId } : {}),
      // GroupNode renders the description from `config.description`.
      ...(g.description ? { description: g.description } : {}),
    },
    position: { x: 0, y: 0 },
    parentId,
    // The line the container was declared on: what autoOrganize keeps
    // siblings in the order of (the render list sorts groups first).
    sourceLine: g.headerSpan?.startLine ?? g.span?.startLine,
    // A container's interface-port literals reach the view the same way a
    // node's do, so everything downstream reads one place for both.
    portLiterals: withFileMarkers(written.portLiterals, written.fileRefs),
    portLiteralSpans: written.portLiteralSpans,
    inputs: g.inPorts.map(copyPort),
    outputs: g.outPorts.map(copyPort),
    ...(g.metadataPorts ? { metadataPorts: g.metadataPorts.map((m) => ({ ...m })) } : {}),
    features: {},
    ...(g.typeParams ? { typeParams: g.typeParams } : {}),
    scope: parentId ? [parentId] : [],
    // The included file's own group, which the editor opens by default.
    ...(g.anonymous ? { anonymous: true } : {}),
  };
}

function toV1Node(n: HostNode, groupIds: Set<string>): NodeInstance {
  // `label` is a first-class field on NodeDefinition; the compiler
  // extracts `label: "..."` lines from a node's config block and
  // promotes them. Defensively strip any leftover 'label' entry
  // from config so it doesn't show up as a synthetic text field in
  // the inline config form.
  const rawConfig = (n.config ?? {}) as Record<string, unknown>;
  const { label: _stripped, ...cleanConfig } = rawConfig;
  const portLiterals = withFileMarkers(n.portLiterals, n.fileRefs) ?? {};
  const parentId = resolveParentGroup(n, groupIds);
  // Mirror the structural parent into config.parentId, which is where the webview
  // reads it (see groupToNodeInstance for why). Without this a re-parse resets the
  // node's perceived scope to top-level while its scoped id stays nested.
  if (parentId) cleanConfig.parentId = parentId; else delete cleanConfig.parentId;
  return {
    id: n.id,
    nodeType: n.nodeType,
    label: n.label,
    config: cleanConfig,
    // Every port's written value + its written form. Dropping these here
    // was the bug where a port field's value vanished from the graph the
    // moment a parse round-trip replaced the truth (the source still had
    // it; only this translation lost it).
    portLiterals,
    portLiteralSpans: n.portLiteralSpans,
    position: n.position,
    parentId,
    // The declaration line, for autoOrganize's left-to-right order.
    sourceLine: n.headerSpan?.startLine ?? n.span?.startLine,
    inputs: n.inputs.map(copyPort),
    outputs: n.outputs.map(copyPort),
    ...(n.metadataPorts ? { metadataPorts: n.metadataPorts.map((m) => ({ ...m })) } : {}),
    features: n.features,
    ...(n.typeParams ? { typeParams: n.typeParams } : {}),
    requiresInfra: n.requiresInfra,
    perInstance: n.perInstance,
    scope: n.scope,
    groupBoundary: n.groupBoundary ?? undefined,
    includePath: n.includePath,
  };
}

export function translateProject(
  host: HostProject,
  weftCode: string,
  layoutCode: string,
): V1Project {
  const groupIds = new Set(host.groups.map((g) => g.id));
  const loopIds = new Set(host.groups.filter((g) => g.kind === 'loop').map((g) => g.id));
  const filled = metadataFills(host.metadataWires ?? [], loopIds);
  const loopIns = new Map(host.nodes.filter((n) => n.groupBoundary?.role === 'In').map((n) => [n.groupBoundary!.groupId, n]));
  const structuralNodes: NodeInstance[] = [];
  // Groups first (v1 requires parent-before-child order).
  for (const g of host.groups) {
    structuralNodes.push(withMetadataInputs(groupToNodeInstance(g, loopIns.get(g.id)), filled.get(g.id)));
  }
  for (const n of host.nodes) {
    // A loop's LoopIn / LoopOut draw as the loop box's two faces.
    if (n.groupBoundary) continue;
    structuralNodes.push(withMetadataInputs(toV1Node(n, groupIds), filled.get(n.id)));
  }
  const edges = legsOfAll(host.edges, loopIds);

  // Position/size/expanded are NOT applied here. The Rust ProjectDefinition emits
  // position=(0,0) and carries no view-state; the companion `.layout` file owns it.
  // The merge of (structural nodes, layout) is done in ONE place, `buildNodes` in
  // ProjectEditorInner, so the initial render and every post-edit re-render share
  // the same pure merge and cannot drift.
  return {
    id: host.id,
    weftCode,
    layoutCode,
    nodes: structuralNodes,
    edges,
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
  };
}
