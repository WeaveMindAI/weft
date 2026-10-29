// Apply EditOps to the editor's in-memory project: the optimistic mirror of
// the Rust edit-server. `foldOps` layers the pending-op queue onto the last
// confirmed truth to produce the visible projection, and doubles as the
// validator: an op that cannot apply (missing ref, duplicate id, kind
// mismatch) throws a user-readable error, and the fold partitions it into
// `dropped` with that error as the reason. One mechanism owns "what does this
// op mean", so projection, revalidation-after-truth-advance, and
// dependency-dropping (an op consuming a rejected op's product simply fails
// to apply) cannot drift apart.
//
// SYNC: op semantics <-> crates/weft-compiler/src/edit/ops.rs apply_op
// (resolution by exact-scoped-id or unique-local-id, add_edge replaces an
// existing driver, remove_group/remove_loop UNGROUP children into the parent
// scope, moves reject when the moved decl has connections).

import type { ProjectDefinition, NodeInstance, Edge, PortDefinition, NodeFeatures } from '../types';
import { isContainerNodeType, isLoopNodeType, isScopeBoxNodeType, containerKindOf, portValueFirstForm } from '../types';
import type { EditOp, EditPortSig, MetadataPort, RevertedPortSig } from '../../../protocol';
import { isGatePort } from '../../../protocol';
import { parseConfigToken } from '../value-format';
import type { FoldResult, PendingOp } from './types';

/** The slice of the node catalog the projection needs: default ports +
 *  features for a freshly-added node. `NODE_TYPE_CONFIG`'s NodeTemplate is
 *  structurally compatible. */
export interface ProjectionCatalogEntry {
  defaultInputs: PortDefinition[];
  defaultOutputs: PortDefinition[];
  metadataPorts?: MetadataPort[];
  features?: NodeFeatures;
}
export type ProjectionCatalog = Record<string, ProjectionCatalogEntry>;

function localIdOf(id: string): string {
  const i = id.lastIndexOf('.');
  return i < 0 ? id : id.slice(i + 1);
}

function scopedId(parentId: string | undefined, local: string): string {
  return parentId ? `${parentId}.${local}` : local;
}

/** Resolve a decl ref the way the server does: exact scoped id first, then a
 *  unique local-id match anywhere in the file. Zero matches and ambiguous
 *  matches fail with the server's wording so the toast reads the same
 *  whichever side caught it. */
function resolveDecl(project: ProjectDefinition, ref: string): NodeInstance {
  const exact = project.nodes.find((n) => n.id === ref);
  if (exact) return exact;
  const byLocal = project.nodes.filter((n) => localIdOf(n.id) === ref);
  if (byLocal.length === 1) return byLocal[0];
  if (byLocal.length > 1) throw new Error(`id is ambiguous (matches multiple): ${ref}`);
  throw new Error(`node not found: ${ref}`);
}

function resolveContainer(project: ProjectDefinition, ref: string): NodeInstance {
  const decl = resolveDecl(project, ref);
  if (!isContainerNodeType(decl.nodeType)) {
    throw new Error(`'${ref}' is a ${decl.nodeType} decl, not a container`);
  }
  return decl;
}

/** Ancestor chain (outermost first) for a node's `scope` field, recomputed
 *  whenever a node changes parent. */
function ancestorChain(project: ProjectDefinition, parentId: string | undefined): string[] {
  const chain: string[] = [];
  let pid = parentId;
  while (pid) {
    chain.unshift(pid);
    pid = project.nodes.find((n) => n.id === pid)?.parentId;
  }
  return chain;
}

function assertNoDuplicate(project: ProjectDefinition, id: string): void {
  if (project.nodes.some((n) => n.id === id)) {
    throw new Error(`id already exists in scope: ${id}`);
  }
}

function setParent(project: ProjectDefinition, node: NodeInstance, parentId: string | undefined): void {
  node.parentId = parentId;
  if (parentId) node.config.parentId = parentId;
  else delete node.config.parentId;
  node.scope = ancestorChain(project, parentId);
}

/** Rewrite every scoped-id under `oldPrefix` to live under `newPrefix`:
 *  node ids, parent pointers, scope arrays, and edge endpoints. The textual
 *  half of a rename/move/ungroup; the caller already validated the change. */
function rewriteSubtreePrefix(project: ProjectDefinition, oldPrefix: string, newPrefix: string): void {
  const reKey = (id: string): string =>
    id === oldPrefix ? newPrefix : id.startsWith(oldPrefix + '.') ? newPrefix + id.slice(oldPrefix.length) : id;
  for (const n of project.nodes) {
    n.id = reKey(n.id);
    if (n.parentId) {
      const p = reKey(n.parentId);
      n.parentId = p;
      n.config.parentId = p;
    }
    if (n.scope) n.scope = n.scope.map(reKey);
  }
  for (const e of project.edges) {
    e.source = reKey(e.source);
    e.target = reKey(e.target);
  }
}

/** Resolve an edge endpoint ref (`self`, an immediate child, or a top-level id)
 *  container to the xyflow form: scoped node id + handle (`__inner` suffix
 *  for the container's own interface side). */
/** Resolve an add_edge endpoint the way the server does: `self` is the scope
 *  group's own body side; otherwise probe the scope's immediate child
 *  `{scope}.{ref}`, then the bare top-level `ref` (the lowering's outer-ref
 *  fallthrough). Null when nothing matches; the scope rule then decides
 *  whether a top-level hit may be wired from inside the group. */
// SYNC: resolveEndpointId <-> crates/weft-compiler/src/edit/ops.rs require_endpoint,
// crates/weft-compiler/src/cst/nodes.rs endpoint_resolves_to,
// crates/weft-compiler/src/weft_compiler.rs rescope_endpoint
export function resolveEndpointId(
  project: ProjectDefinition,
  ref: string,
  scope: NodeInstance | undefined,
): { id: string; inner: boolean } | null {
  if (ref === 'self') return scope ? { id: scope.id, inner: true } : null;
  const local = scopedId(scope?.id, ref);
  if (project.nodes.some((n) => n.id === local)) return { id: local, inner: false };
  if (project.nodes.some((n) => n.id === ref)) return { id: ref, inner: false };
  return null;
}

function resolveEndpoint(
  project: ProjectDefinition,
  ref: string,
  port: string,
  scope: NodeInstance | undefined,
): { id: string; handle: string } {
  if (ref === 'self' && !scope) throw new Error(`'self' endpoint without a scope group`);
  const ep = resolveEndpointId(project, ref, scope);
  if (!ep) throw new Error(`node not found: ${ref}`);
  return { id: ep.id, handle: ep.inner ? `${port}__inner` : port };
}

/** The scope group of an edge op, with the server's wording when the ref is
 *  unknown or not a container. */
export function resolveScopeGroup(project: ProjectDefinition, scopeGroup: string | null): NodeInstance | undefined {
  return scopeGroup == null ? undefined : resolveContainer(project, scopeGroup);
}

/** Edges that touch a decl from outside its own subtree. The container's own
 *  `__inner`-handled edges are INSIDE the subtree (its body wiring); a plain
 *  handle on the container id is an external leg in the parent scope. */
function externalEdgesTouching(project: ProjectDefinition, declId: string, isContainer: boolean): Edge[] {
  const inSubtree = (id: string, handle: string | null): boolean => {
    if (id.startsWith(declId + '.')) return true;
    if (id === declId) return isContainer ? (handle?.endsWith('__inner') ?? false) : false;
    return false;
  };
  return project.edges.filter((e) => {
    const srcIn = inSubtree(e.source, e.sourceHandle);
    const tgtIn = inSubtree(e.target, e.targetHandle);
    const touches = e.source === declId || e.target === declId
      || e.source.startsWith(declId + '.') || e.target.startsWith(declId + '.');
    return touches && !(srcIn && tgtIn);
  });
}

function moveScope(project: ProjectDefinition, ref: string, targetGroup: string | null, expectKind: 'Node' | 'Group' | 'Loop'): void {
  const decl = resolveDecl(project, ref);
  const isContainer = isContainerNodeType(decl.nodeType);
  const actualKind = isContainer ? containerKindOf(decl.nodeType) ?? 'Group' : 'Node';
  if (actualKind !== expectKind) {
    throw new Error(`a ${expectKind} move called on '${ref}' which is a ${actualKind} decl`);
  }
  const target = targetGroup == null ? undefined : resolveContainer(project, targetGroup);
  if (target && (target.id === decl.id || target.id.startsWith(decl.id + '.'))) {
    throw new Error(`cannot move '${decl.id}' into its own subtree`);
  }
  const newId = scopedId(target?.id, localIdOf(decl.id));
  if (newId === decl.id) return; // no-op move
  const external = externalEdgesTouching(project, decl.id, isContainer);
  if (external.length > 0) {
    throw new Error(`cannot move '${decl.id}': it has connections in its current scope`);
  }
  assertNoDuplicate(project, newId);
  const oldId = decl.id;
  setParent(project, decl, target?.id);
  rewriteSubtreePrefix(project, oldId, newId);
  // Children's scope arrays now point at re-keyed ancestors; recompute from
  // the updated parent chain so nesting depth stays accurate.
  for (const n of project.nodes) {
    if (n.id.startsWith(newId + '.')) n.scope = ancestorChain(project, n.parentId);
  }
}

function renameContainer(project: ProjectDefinition, ref: string, newLabel: string, expectKind: 'Group' | 'Loop'): void {
  if (!newLabel) throw new Error('invalid edit argument: rename to empty label');
  const decl = resolveContainer(project, ref);
  const actualKind = containerKindOf(decl.nodeType) ?? 'Group';
  if (actualKind !== expectKind) {
    throw new Error(`a ${expectKind} rename called on '${ref}' which is a ${actualKind} decl`);
  }
  const newId = scopedId(decl.parentId, newLabel);
  if (newId === decl.id) {
    decl.label = newLabel;
    return;
  }
  assertNoDuplicate(project, newId);
  const oldId = decl.id;
  decl.label = newLabel;
  rewriteSubtreePrefix(project, oldId, newId);
}

function mergedSigPort(
  sig: EditPortSig,
  prior: PortDefinition | undefined,
  // NODE sig ports are header-declared, so they mirror the reparse's
  // `declaredType` stamp for the next header rewrite; container ports
  // never carry one (Rust guarantees it absent there).
  stampDeclared: boolean,
): PortDefinition {
  // The sig's `required` is its SPELLING (`x:` or `x?:`), and a written
  // `x?:` is what `optional` means, on a node and a container alike: it
  // runs without the value even when wired (the compiler stamps a
  // container's ports the same way).
  const { optional: _priorOptional, ...rest } = prior ?? {};
  return {
    ...rest,
    ...(!sig.required ? { optional: true } : {}),
    name: sig.name,
    required: sig.required,
    // The RENDERED type is what the port shows (a reparse re-runs
    // inference, so a required-only toggle must not repaint the port
    // with the header's generic spelling); `portType` alone carries the
    // header spelling when no rendered value rides along (containers).
    portType: sig.rendered ?? sig.portType,
    ...(stampDeclared ? { declaredType: sig.portType } : {}),
    // A SIGNATURE port is never a carry ghost. If a name that used to be a
    // carry ghost now arrives as a real signature input, the spread of
    // `prior` would inherit synthesizedFromCarry:true, and a later carry
    // clear would then wrongly sweep this genuine input. Force it off.
    synthesizedFromCarry: false,
  };
}

/** A CONTAINER's signature is its whole surface: the sig list replaces
 *  the rendered list outright. Its outputs carry the metadata ports too
 *  (`metadata: true`), which keep the value the body writes: a header
 *  rewrite never touches the body line. */
function updatePorts(node: NodeInstance, inputs: EditPortSig[], outputs: EditPortSig[]): void {
  // The server's refusals, in its words: a metadata port sits on the
  // output side, and it always names its type.
  const inInputs = inputs.find((s) => s.metadata);
  if (inInputs) throw new Error(`metadata port '${inInputs.name}' among the inputs: a metadata port is an output`);
  const untyped = outputs.find((s) => s.metadata && s.portType === '');
  if (untyped) throw new Error(`metadata port '${untyped.name}' has no type`);
  const merge = (sigs: EditPortSig[], existing: PortDefinition[]): PortDefinition[] =>
    sigs.map((sig) => mergedSigPort(sig, existing.find((p) => p.name === sig.name), false));
  node.inputs = merge(inputs, node.inputs);
  node.outputs = merge(outputs.filter((s) => !s.metadata), node.outputs);
  const metadata = outputs.filter((s) => s.metadata).map((sig): MetadataPort => {
    const { type: _priorType, declaredType: _priorDeclared, ...prior } = node.metadataPorts?.find((m) => m.name === sig.name) ?? {};
    return { ...prior, name: sig.name, type: sig.rendered ?? sig.portType, declaredType: sig.portType };
  });
  if (metadata.length > 0) node.metadataPorts = metadata;
  else delete node.metadataPorts;
}

/** A NODE's header is NOT its whole surface (the catalog provides the
 *  node type's own ports at enrich, which a re-parse restores). Mimic
 *  that here: keep every rendered port the op does not name, overlay
 *  the sig ports, append new ones, and drop ONLY the explicitly
 *  removed names, so catalog ports (and their edges) survive the
 *  optimistic rebuild exactly as they survive the real one. */
function updateNodePortSurface(
  node: NodeInstance,
  inputs: EditPortSig[],
  outputs: EditPortSig[],
  removedInputs: string[],
  removedOutputs: string[],
  revertedInputs: RevertedPortSig[],
  revertedOutputs: RevertedPortSig[],
): void {
  // A node's metadata ports are its type's, written in its metadata.json:
  // no header declares one.
  const metadata = [...inputs, ...outputs].find((s) => s.metadata);
  if (metadata) throw new Error(`metadata port '${metadata.name}' on a node: only a group or a loop declares one`);
  const merge = (
    sigs: EditPortSig[],
    existing: PortDefinition[],
    removed: string[],
    reverted: RevertedPortSig[],
  ): PortDefinition[] => {
    const gone = new Set(removed);
    const byName = new Map(sigs.map((s) => [s.name, s]));
    // The three lists partition the touched names: a name in two of
    // them is a producer sending contradictory intent, and quietly
    // picking one side would diverge from the server. Refuse at the
    // door instead (foldOps' clone-then-adopt keeps the batch atomic).
    const claimed = new Map<string, string>();
    const claim = (name: string, list: string) => {
      const other = claimed.get(name);
      if (other) throw new Error(`port '${name}' is both ${other} and ${list}`);
      claimed.set(name, list);
    };
    for (const s of sigs) claim(s.name, 'updated');
    for (const n of removed) claim(n, 'removed');
    for (const r of reverted) {
      claim(r.name, 'reverted');
      if (!existing.some((p) => p.name === r.name)) {
        throw new Error(`reverted port '${r.name}' is not on the node`);
      }
    }
    // A REVERTED port went back to its provided shape: its header line
    // is gone (the server's rewrite omits it), so it is no longer
    // declared, and requiredness returns to the provided value (never
    // `optional`, which only a written `x?:` sets). The
    // rendered TYPE stays as-is unless the revert carries one (a gesture
    // revert does; a deletion revert deliberately omits it, since the
    // provided spelling may be an uninstantiated generic and the
    // current rendered type is the best answer until the reparse).
    // Without this the projection would keep showing the pre-revert
    // state until the reparse lands, and the NEXT gesture (reading this
    // stale state) would write the reverted value back into source.
    const revByName = new Map(reverted.map((s) => [s.name, s]));
    const kept = existing
      .filter((p) => !gone.has(p.name))
      .map((p) => {
        const sig = byName.get(p.name);
        if (sig) return mergedSigPort(sig, p, true);
        const rev = revByName.get(p.name);
        if (rev) {
          const { declaredType: _undeclared, optional: _unwritten, ...rest } = p;
          return { ...rest, required: rev.required, portType: rev.portType ?? p.portType };
        }
        return p;
      });
    const appended = sigs
      .filter((s) => !existing.some((p) => p.name === s.name))
      .map((s) => mergedSigPort(s, undefined, true));
    return [...kept, ...appended];
  };
  node.inputs = merge(inputs, node.inputs, removedInputs, revertedInputs);
  node.outputs = merge(outputs, node.outputs, removedOutputs, revertedOutputs);
}

/** Mirror the lowering's carry-input synthesis on a Loop: each name in the
 *  carry list that has a matching output gets a derived (ghost) input; a
 *  ghost whose carry entry or paired output vanished is removed. Carry
 *  inputs are NEVER written into the source signature (the output side is
 *  the source of truth), so the projection derives them exactly like a
 *  re-parse would.
 *  SYNC: syncLoopCarryInputs <-> crates/weft-compiler/src/weft_compiler.rs
 *  (carry input synthesis in the loop-lowering pass). */
function syncLoopCarryInputs(loop: NodeInstance): void {
  const carry: string[] = Array.isArray(loop.config.carry)
    ? (loop.config.carry as unknown[]).filter((v): v is string => typeof v === 'string')
    : [];
  loop.inputs = loop.inputs.filter(
    (p: PortDefinition) => !p.synthesizedFromCarry || (carry.includes(p.name) && loop.outputs.some((o: PortDefinition) => o.name === p.name)),
  );
  for (const name of carry) {
    const out = loop.outputs.find((o: PortDefinition) => o.name === name);
    if (out && !loop.inputs.some((p: PortDefinition) => p.name === name)) {
      loop.inputs.push({
        name,
        portType: out.portType,
        required: out.required,
        accepts: ['wire'],
        synthesizedFromCarry: true,
      });
    }
  }
}

/** Mirror the compiler's `passesThrough` stamp on a container's inputs:
 *  every input of a group, and a loop's except the ones it iterates
 *  (`over`) or carries (`carry`), which launch its iterations. Re-run
 *  after anything that changes the inputs or those two lists, so a loop
 *  port moved into `over` regains its requiredness at once.
 *  SYNC: syncPassThrough <-> crates/weft-compiler/src/weft_compiler.rs ParsedGroup::passes_through */
function syncPassThrough(container: NodeInstance): void {
  const listed = (key: string): string[] => Array.isArray(container.config[key])
    ? (container.config[key] as unknown[]).filter((v): v is string => typeof v === 'string')
    : [];
  const launches = containerKindOf(container.nodeType) === 'Loop'
    ? new Set([...listed('over'), ...listed('carry')])
    : new Set<string>();
  container.inputs = container.inputs.map((p: PortDefinition) => {
    const { passesThrough: _was, ...rest } = p;
    return launches.has(p.name) ? rest : { ...rest, passesThrough: true };
  });
}

/** Drop edges bound to a port that no longer exists on `node` (mirrors the
 *  server dropping the dangling connection line). On a container, an
 *  `__inner` handle is the INSIDE leg of the opposite-direction interface
 *  port: an inner target receives from an OUT port, an inner source feeds an
 *  IN port. Runs AFTER carry-ghost re-synthesis so a surviving carry's seed
 *  wire isn't swept. */
function dropDanglingPortEdges(node: NodeInstance, project: ProjectDefinition): void {
  const inNames = new Set(node.inputs.map((p) => p.name));
  const outNames = new Set(node.outputs.map((p) => p.name));
  if (isLoopNodeType(node.nodeType)) {
    // Implicit loop ports (`self.index` read, `self.done` write) are valid
    // wiring surfaces outside the signature.
    inNames.add('index');
    outNames.add('done');
  }
  const portOf = (handle: string | null): string => (handle ?? '').replace(/__inner$/, '');
  project.edges = project.edges.filter((e) => {
    if (e.target === node.id) {
      const names = e.targetHandle?.endsWith('__inner') ? outNames : inNames;
      return names.has(portOf(e.targetHandle));
    }
    if (e.source === node.id) {
      const names = e.sourceHandle?.endsWith('__inner') ? inNames : outNames;
      return names.has(portOf(e.sourceHandle));
    }
    return true;
  });
}

function ungroup(project: ProjectDefinition, ref: string, expectKind: 'Group' | 'Loop'): void {
  const decl = resolveContainer(project, ref);
  const actualKind = containerKindOf(decl.nodeType) ?? 'Group';
  if (actualKind !== expectKind) {
    throw new Error(`a ${expectKind} removal called on '${ref}' which is a ${actualKind} decl`);
  }
  const gid = decl.id;
  const grandparent = decl.parentId;
  // Boundary wiring dies with the container: its inner legs (__inner handles
  // on gid) and its external legs (plain handles on gid in the parent scope).
  project.edges = project.edges.filter((e) => e.source !== gid && e.target !== gid);
  // Children climb one scope. Their subtree ids re-key from `gid.` to the
  // grandparent prefix; collisions with an existing sibling are a real
  // duplicate (the server's reparse would refuse the same way).
  const directChildren = project.nodes.filter((n) => n.parentId === gid);
  for (const child of directChildren) {
    const newId = scopedId(grandparent, localIdOf(child.id));
    assertNoDuplicate(project, newId);
    const oldId = child.id;
    setParent(project, child, grandparent);
    rewriteSubtreePrefix(project, oldId, newId);
  }
  project.nodes = project.nodes.filter((n) => n.id !== gid);
  for (const n of project.nodes) {
    if (n.scope?.includes(gid)) n.scope = ancestorChain(project, n.parentId);
  }
}

/** Apply one EditOp in place. Throws a user-readable error when the op cannot
 *  apply; the caller (foldOps) owns batch atomicity via clone-then-adopt. */
function applyOp(project: ProjectDefinition, op: EditOp, catalog: ProjectionCatalog): void {
  switch (op.op) {
    case 'set_config':
    case 'remove_config': {
      const node = resolveDecl(project, op.node);
      // Mirror the compiler's enrich normalization so the optimistic
      // projection matches the host's next parse: a value written for an
      // INPUT PORT homes in `portLiterals`, whichever spelling wrote it;
      // a key that names no port stays in `config`. A container or an
      // include has only the port home: a container's `config` is loop
      // knobs, which ride their own ops, and an include has nothing else.
      const input = node.inputs?.find((p) => p.name === op.key);
      const portsOnly = isScopeBoxNodeType(node.nodeType);
      if (portsOnly && input === undefined && !isGatePort(op.key)) {
        throw new Error(`'${op.node}' has no input port '${op.key}'`);
      }
      const portHomed = portsOnly || input !== undefined || isGatePort(op.key);
      if (portHomed) {
        const literals = (node.portLiterals ??= {});
        const spans = (node.portLiteralSpans ??= {});
        if (op.op === 'set_config') {
          literals[op.key] = parseConfigToken(op.value);
          if (!spans[op.key]) {
            // A fresh literal: record the written form so the field's
            // marker renders before the host round-trip (span stays
            // zeroed; only `origin` matters to the view). An op with no
            // form is written the way the host writes it for this node
            // kind (the node kind's first form).
            spans[op.key] = {
              span: { startLine: 0, startColumn: 0, endLine: 0, endColumn: 0 },
              origin: op.form ?? portValueFirstForm(node.nodeType),
            };
          }
        } else {
          delete literals[op.key];
          delete spans[op.key];
        }
        return;
      }
      if (op.op === 'set_config') node.config[op.key] = parseConfigToken(op.value);
      else delete node.config[op.key];
      return;
    }
    case 'set_value_form': {
      const node = resolveDecl(project, op.node);
      const spans = (node.portLiteralSpans ??= {});
      const existing = spans[op.key];
      if (existing) spans[op.key] = { ...existing, origin: op.form };
      return;
    }
    case 'set_loop_config':
    case 'remove_loop_config': {
      const decl = resolveContainer(project, op.loopId);
      if (containerKindOf(decl.nodeType) !== 'Loop') {
        throw new Error(`a Loop config op called on '${op.loopId}' which is not a Loop decl`);
      }
      if (op.op === 'set_loop_config') decl.config[op.key] = parseConfigToken(op.value);
      else delete decl.config[op.key];
      // The carry list derives the loop's ghost inputs: re-synthesize, then
      // sweep wires bound to a dissolved carry's input side.
      if (op.key === 'carry') {
        syncLoopCarryInputs(decl);
        dropDanglingPortEdges(decl, project);
      }
      if (op.key === 'over' || op.key === 'carry') syncPassThrough(decl);
      return;
    }
    case 'set_per_instance': {
      const node = resolveDecl(project, op.node);
      if (isContainerNodeType(node.nodeType)) {
        throw new Error(`SetPerInstance called on '${op.node}' which is a container; only a node exists once per instance`);
      }
      // What the source says flips at once; which nodes it reaches
      // (`derived`) is the compiler's to say, on the host's next parse.
      if (op.perInstance) node.perInstance = 'marked';
      else delete node.perInstance;
      return;
    }
    case 'set_label': {
      const node = resolveDecl(project, op.node);
      if (isContainerNodeType(node.nodeType)) {
        throw new Error(`SetLabel called on '${op.node}' which is a container; containers rename via RenameGroup/RenameLoop`);
      }
      node.label = op.label;
      return;
    }
    case 'add_node': {
      const parent = op.parentGroup == null ? undefined : resolveContainer(project, op.parentGroup);
      const id = scopedId(parent?.id, op.id);
      assertNoDuplicate(project, id);
      const entry = catalog[op.nodeType];
      if (!entry) throw new Error(`unknown node type: ${op.nodeType}`);
      const node: NodeInstance = {
        id,
        nodeType: op.nodeType,
        label: null,
        config: parent ? { parentId: parent.id } : {},
        position: { x: 0, y: 0 },
        parentId: parent?.id,
        inputs: entry.defaultInputs.map((p) => ({ ...p })),
        outputs: entry.defaultOutputs.map((p) => ({ ...p })),
        ...(entry.metadataPorts ? { metadataPorts: entry.metadataPorts.map((m) => ({ ...m })) } : {}),
        features: entry.features ?? {},
        scope: ancestorChain(project, parent?.id),
      };
      project.nodes.push(node);
      return;
    }
    case 'add_group':
    case 'add_loop': {
      const parent = op.parentGroup == null ? undefined : resolveContainer(project, op.parentGroup);
      const id = scopedId(parent?.id, op.label);
      assertNoDuplicate(project, id);
      const node: NodeInstance = {
        id,
        nodeType: op.op === 'add_loop' ? 'Loop' : 'Group',
        label: op.label,
        config: parent ? { parentId: parent.id } : {},
        position: { x: 0, y: 0 },
        parentId: parent?.id,
        inputs: [],
        outputs: [],
        features: {},
        scope: ancestorChain(project, parent?.id),
      };
      project.nodes.push(node);
      return;
    }
    case 'remove_node': {
      const node = resolveDecl(project, op.node);
      if (isContainerNodeType(node.nodeType)) {
        throw new Error(`RemoveNode called on '${op.node}' which is a container; containers remove via RemoveGroup/RemoveLoop`);
      }
      project.nodes = project.nodes.filter((n) => n.id !== node.id);
      project.edges = project.edges.filter((e) => e.source !== node.id && e.target !== node.id);
      return;
    }
    case 'remove_group':
      ungroup(project, op.group, 'Group');
      return;
    case 'remove_loop':
      ungroup(project, op.loopId, 'Loop');
      return;
    case 'rename_group':
      renameContainer(project, op.group, op.newLabel, 'Group');
      return;
    case 'rename_loop':
      renameContainer(project, op.loopId, op.newLabel, 'Loop');
      return;
    case 'move_node_scope':
      moveScope(project, op.node, op.targetGroup, 'Node');
      return;
    case 'move_group_scope':
      moveScope(project, op.group, op.targetGroup, 'Group');
      return;
    case 'move_loop_scope':
      moveScope(project, op.loopId, op.targetGroup, 'Loop');
      return;
    case 'add_edge': {
      const scope = resolveScopeGroup(project, op.scopeGroup);
      const src = resolveEndpoint(project, op.source, op.sourcePort, scope);
      const tgt = resolveEndpoint(project, op.target, op.targetPort, scope);
      // The server replaces an existing driver on the target port (one driver
      // per input); mirror it so the projection and the round-trip agree.
      project.edges = project.edges.filter(
        (e) => !(e.target === tgt.id && (e.targetHandle ?? '') === tgt.handle),
      );
      project.edges.push({
        // The compiler's `wire_id`: the keys read are part of it.
        id: `e-${src.id}-${src.handle}-${tgt.id}-${tgt.handle}${op.path && op.path.length > 0 ? `.${op.path.join('.')}` : ''}`,
        source: src.id,
        target: tgt.id,
        sourceHandle: src.handle,
        targetHandle: tgt.handle,
        ...(op.path && op.path.length > 0 ? { path: op.path } : {}),
      });
      return;
    }
    case 'remove_edge': {
      const scope = resolveScopeGroup(project, op.scopeGroup);
      const src = resolveEndpoint(project, op.source, op.sourcePort, scope);
      const tgt = resolveEndpoint(project, op.target, op.targetPort, scope);
      const before = project.edges.length;
      project.edges = project.edges.filter(
        (e) => !(e.source === src.id && (e.sourceHandle ?? '') === src.handle
          && e.target === tgt.id && (e.targetHandle ?? '') === tgt.handle),
      );
      if (project.edges.length === before) {
        throw new Error(`connection not found: ${op.target}.${op.targetPort} = ${op.source}.${op.sourcePort}`);
      }
      return;
    }
    case 'update_node_ports': {
      const node = resolveDecl(project, op.node);
      if (isContainerNodeType(node.nodeType)) {
        throw new Error(`UpdateNodePorts called on '${op.node}' which is a container`);
      }
      updateNodePortSurface(
        node, op.inputs, op.outputs,
        op.removedInputs, op.removedOutputs,
        op.revertedInputs, op.revertedOutputs,
      );
      dropDanglingPortEdges(node, project);
      return;
    }
    case 'update_group_ports': {
      const decl = resolveContainer(project, op.group);
      if (containerKindOf(decl.nodeType) !== 'Group') {
        throw new Error(`UpdateGroupPorts called on '${op.group}' which is not a Group decl`);
      }
      updatePorts(decl, op.inputs, op.outputs);
      syncPassThrough(decl);
      dropDanglingPortEdges(decl, project);
      return;
    }
    case 'update_loop_ports': {
      const decl = resolveContainer(project, op.loopId);
      if (containerKindOf(decl.nodeType) !== 'Loop') {
        throw new Error(`UpdateLoopPorts called on '${op.loopId}' which is not a Loop decl`);
      }
      // Signatures never carry ghost inputs; re-derive them from the carry
      // list BEFORE the sweep so a surviving carry's wires stay.
      updatePorts(decl, op.inputs, op.outputs);
      syncLoopCarryInputs(decl);
      syncPassThrough(decl);
      dropDanglingPortEdges(decl, project);
      return;
    }
    case 'set_metadata_value': {
      const decl = resolveContainer(project, op.group);
      const port = decl.metadataPorts?.find((m) => m.name === op.port);
      if (!port) throw new Error(`'${op.group}' has no metadata port '${op.port}'`);
      if (op.value === null) {
        delete port.value;
        return;
      }
      // A value written out in full shows at once. A value gathered from
      // other metadata ports (`[...other.tools, pause.tool]`, what a
      // duplicate writes) is no value token: only the compiler resolves
      // it, so the value stays unknown until the next parse brings it.
      // The server, not this preview, says whether the text is right.
      try {
        port.value = parseConfigToken(op.value);
      } catch {
        delete port.value;
      }
      return;
    }
    case 'set_group_description': {
      // Descriptions live in source comments, not on the rendered node; the
      // op only needs its target to exist.
      resolveContainer(project, op.group);
      return;
    }
  }
}

/** Deep clone that READS THROUGH proxies. The truth project arrives wrapped
 *  in Svelte's $state proxy in production, and `structuredClone` throws on
 *  any Proxy ("could not be cloned"), which would crash the projection on
 *  its very first derive. Project data is plain JSON-shaped values, so a
 *  recursive own-property walk is a faithful clone (and the projection's
 *  output is un-proxied plain data as a bonus). */
function plainDeep<T>(value: T): T {
  if (Array.isArray(value)) return value.map(plainDeep) as unknown as T;
  if (value !== null && typeof value === 'object') {
    const out: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(value as Record<string, unknown>)) out[k] = plainDeep(v);
    return out as T;
  }
  return value;
}

function cloneProject(project: ProjectDefinition): ProjectDefinition {
  return plainDeep(project);
}

/** Apply one op batch atomically: all ops land or none do (the server applies
 *  an `applyEdits` batch as one transaction, so the projection must too). */
export function applyOpsToProject(
  project: ProjectDefinition,
  ops: EditOp[],
  catalog: ProjectionCatalog,
): ProjectDefinition {
  const next = cloneProject(project);
  for (const op of ops) applyOp(next, op, catalog);
  return next;
}

/** Fold the pending-op queue over a truth project. Returns the projected
 *  visible project plus the kept/dropped partition. Dropping is exact
 *  dependency handling: an op that consumed a dropped op's product fails to
 *  apply against the post-drop state and is dropped with that reason. */
export function foldOps(
  truthProject: ProjectDefinition,
  pendingOps: PendingOp[],
  catalog: ProjectionCatalog,
): FoldResult {
  let working = cloneProject(truthProject);
  const kept: PendingOp[] = [];
  const dropped: Array<{ op: PendingOp; reason: string }> = [];
  for (const pending of pendingOps) {
    try {
      working = applyOpsToProject(working, pending.ops, catalog);
      kept.push(pending);
    } catch (err) {
      dropped.push({ op: pending, reason: err instanceof Error ? err.message : String(err) });
    }
  }
  return { project: working, kept, dropped };
}
