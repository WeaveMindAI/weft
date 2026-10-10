// A run spec: how to run a program, as one value. The TypeScript side of
// `weft_core::run_spec`: the file in `examples/<name>.json`, the body a
// run is started with, and the frozen example a person reviews. The
// editor's spec dialog builds one, resolves it live through the parse
// server (the same resolver the dispatcher runs), and hands it to the
// host, which runs it through the CLI: a named spec as `weft run
// <name>`, a one-off as the same flags typed by hand.

/// The JSON a `.weft` value carries; the spec never inspects it.
export type JsonValue = unknown;

export type PortValues = Record<string, Record<string, JsonValue>>;

/// JSON.parse validates syntax; scanning its string tokens also rejects keys
/// it would silently overwrite, including escaped spellings of the same key.
export function parseSuppliedJson(text: string): unknown {
  const value: unknown = JSON.parse(text);
  const objects: (Set<string> | null)[] = [];
  let quoted: string | undefined;
  for (const [token] of text.matchAll(/"(?:[^"\\]|\\.)*"|[{}\[\]:,]/g)) {
    if (token === '{') objects.push(new Set());
    else if (token === '[') objects.push(null);
    else if (token === '}' || token === ']') objects.pop();
    else if (token === ':' && quoted !== undefined) {
      const keys = objects.at(-1);
      const key = JSON.parse(quoted) as string;
      if (keys?.has(key)) throw new Error(`duplicate key '${key}'`);
      keys?.add(key);
    }
    quoted = token.startsWith('"') ? token : undefined;
  }
  return value;
}

// SYNC: RunSpec <-> crates/weft-core/src/run_spec.rs RunSpec
export interface RunSpec {
  name: string;
  from?: PortValues;
  target?: string[];
  before?: string[];
  /// Starts whose feeders run too (`--feed`).
  feed?: string[];
  group?: [string, Record<string, JsonValue>];
  emit?: PortValues;
  fire?: [string, JsonValue];
  answers?: Answer[];
  caller?: JsonValue[];
  frozen_from?: FrozenFrom;
  expected?: Expected;
  /// Which instance of the program the run is for. Needed only when the
  /// run reaches something that exists once per instance.
  instance?: string;
  /// How the run asks to be kept (`weft run --durable --fast
  /// --keep-for`); each part absent follows the trigger it fires, or the
  /// default (fast, as long as the project keeps runs) for a run that
  /// fires nothing. A run started by hand is always recorded.
  settings?: RunSettings;
}

// SYNC: RunSettings <-> crates/weft-core/src/run_settings.rs SettingsChoice
export interface RunSettings {
  keeping?: 'fast' | 'durable';
  /** How long the run is kept once it ended: a whole number and a unit
   *  (`30m`, `12h`, `7d`), or `forever`. */
  keep_for?: string;
  /** How long, in seconds, a wait holds the run's worker while the run
   *  cannot pause, from the last time anything moved in it. */
  hold_secs?: number;
}

/// The longest hold a run may ask for: 30 days.
// SYNC: MAX_HOLD_SECS <-> crates/weft-core/src/run_settings.rs MAX_HOLD_SECS
const MAX_HOLD_SECS = 30 * 24 * 3600;

/// How long to keep a run, as written: a whole number and a unit, or
/// `forever`.
// SYNC: KEEP_FOR_PATTERN <-> crates/weft-core/src/run_settings.rs KeepFor::from_str
const KEEP_FOR_PATTERN = /^(forever|[0-9]+[mhd])$/;

/// An instance id: the grammar of one storage key segment.
// SYNC: INSTANCE_ID_PATTERN <-> crates/weft-core/src/storage/key.rs valid_segment, crates/weft-core/src/instance.rs InstanceId::new
export const INSTANCE_ID_PATTERN = /^(?!\.{1,2}$)[A-Za-z0-9._-]{1,128}$/;

/// Validate files before they enter menus or dialogs. A type assertion alone
/// cannot reject obsolete fields or malformed nested port maps.
export function parseRunSpec(value: unknown): RunSpec {
  const object = (value: unknown, where: string): Record<string, unknown> => {
    if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error(`${where}: expected an object`);
    return value as Record<string, unknown>;
  };
  const fields = (value: Record<string, unknown>, allowed: string[], where: string) => {
    for (const key of Object.keys(value)) if (!allowed.includes(key)) throw new Error(`${where}: unknown field '${key}'`);
  };
  const string = (value: unknown, where: string) => {
    if (typeof value !== 'string') throw new Error(`${where}: expected a string`);
  };
  const array = (value: unknown, where: string): unknown[] => {
    if (!Array.isArray(value)) throw new Error(`${where}: expected a list`);
    return value;
  };
  const frames = (value: unknown, where: string) => {
    if (value === undefined) return;
    for (const frame of array(value, where)) {
      const item = object(frame, where);
      // An iteration frame or a call frame, nothing else.
      if ('site' in item) {
        if (typeof item.site !== 'string' || item.site.length === 0) throw new Error(`${where}: invalid call site`);
        continue;
      }
      if (!Number.isInteger(item.index) || Number(item.index) < 0 || Number(item.index) > 0xffffffff) throw new Error(`${where}: invalid iteration index`);
    }
  };
  const spec = object(value, 'spec');
  fields(spec, ['name', 'from', 'target', 'before', 'feed', 'group', 'emit', 'fire', 'answers', 'caller', 'frozen_from', 'expected', 'instance', 'settings'], 'spec');
  string(spec.name, 'name');
  if (spec.settings !== undefined) {
    const settings = object(spec.settings, 'settings');
    fields(settings, ['keeping', 'keep_for', 'hold_secs'], 'settings');
    // A key left out (or null, which the runtime reads as left out) follows
    // the fired trigger.
    if (settings.keeping != null && settings.keeping !== 'fast' && settings.keeping !== 'durable') {
      throw new Error(`settings.keeping: '${String(settings.keeping)}' is neither 'fast' nor 'durable'`);
    }
    if (settings.keep_for != null && (typeof settings.keep_for !== 'string' || !KEEP_FOR_PATTERN.test(settings.keep_for.trim()))) {
      throw new Error(`settings.keep_for: '${String(settings.keep_for)}' is not how long to keep a run (\`30m\`, \`12h\`, \`7d\`, or \`forever\`)`);
    }
    if (settings.hold_secs != null && (!Number.isInteger(settings.hold_secs) || (settings.hold_secs as number) < 0 || (settings.hold_secs as number) > MAX_HOLD_SECS)) {
      throw new Error(`settings.hold_secs: '${String(settings.hold_secs)}' is not a whole number of seconds from 0 to ${MAX_HOLD_SECS} (30 days)`);
    }
  }
  if (spec.instance != null) {
    string(spec.instance, 'instance');
    if (!INSTANCE_ID_PATTERN.test(spec.instance as string)) throw new Error(`instance: '${spec.instance}' is not a valid instance id`);
  }
  for (const key of ['target', 'before', 'feed']) if (spec[key] !== undefined) {
    for (const id of array(spec[key], key)) string(id, key);
  }
  if (spec.group != null) {
    const group = array(spec.group, 'group');
    if (group.length !== 2) throw new Error('group: expected [group, input ports]');
    string(group[0], 'group name');
    object(group[1], 'group input ports');
  }
  for (const key of ['from', 'emit']) if (spec[key] !== undefined) {
    for (const [node, ports] of Object.entries(object(spec[key], key))) object(ports, `${key}.${node}`);
  }
  if (spec.fire != null) {
    const fire = array(spec.fire, 'fire');
    if (fire.length !== 2) throw new Error('fire: expected [trigger, payload]');
    string(fire[0], 'fire trigger');
  }
  // SYNC: answer refusals <-> crates/weft-core/src/run_spec.rs Answer TryFrom<AnswerFile>
  if (spec.answers !== undefined) for (const value of array(spec.answers, 'answers')) {
    const answer = object(value, 'answer');
    fields(answer, ['node', 'frames', 'payload', 'skipped', 'gave_up', 'question'], 'answer');
    string(answer.node, 'answer node');
    frames(answer.frames, 'answer frames');
    if (answer.skipped !== undefined && typeof answer.skipped !== 'boolean') throw new Error('answer: skipped must be true or false');
    // A null `gave_up` is left out, as the Rust side reads it.
    if (answer.gave_up != null) string(answer.gave_up, 'answer gave_up');
    // `payload: null` is a given null: only an absent payload is none.
    const said = [
      ...('payload' in answer ? ['a payload'] : []),
      ...(answer.skipped === true ? ['"skipped": true'] : []),
      ...(answer.gave_up != null ? ['"gave_up"'] : []),
    ];
    if (said.length > 1) throw new Error(`the answer to '${answer.node}' says its wait ended more than one way (${said.join(' and ')}); a wait ends once, so keep only one`);
    if (said.length === 0) throw new Error(`the answer to '${answer.node}' says nothing about how its wait ended; give the value the person answered ("payload"), "skipped": true if they skipped it, or "gave_up" with the error if the run gave it up`);
  }
  if (spec.caller !== undefined) array(spec.caller, 'caller');
  if (spec.frozen_from != null) {
    const origin = object(spec.frozen_from, 'frozen_from');
    fields(origin, ['version', 'execution_id', 'definition_hash'], 'frozen_from');
    for (const key of ['version', 'execution_id', 'definition_hash']) string(origin[key], `frozen_from.${key}`);
  }
  if (spec.expected != null) {
    const expected = object(spec.expected, 'expected');
    fields(expected, ['wires', 'focus', 'nodes'], 'expected');
    if (expected.nodes !== undefined) for (const node of array(expected.nodes, 'expected.nodes')) string(node, 'expected.nodes');
    if (expected.focus !== undefined) for (const node of array(expected.focus, 'expected.focus')) string(node, 'expected.focus');
    for (const value of array(expected.wires, 'expected.wires')) {
      const wire = object(value, 'wire');
      fields(wire, ['node', 'port', 'frames', 'value', 'ordinal', 'closed', 'failure'], 'wire');
      if (wire.ordinal !== undefined && (!Number.isSafeInteger(wire.ordinal) || Number(wire.ordinal) < 0)) throw new Error('wire: invalid ordinal');
      if (wire.closed !== undefined && typeof wire.closed !== 'boolean') throw new Error('wire: closed must be boolean');
      if (wire.failure != null) {
        const failure = object(wire.failure, 'wire failure');
        fields(failure, ['node', 'error'], 'wire failure');
        string(failure.node, 'wire failure node');
        string(failure.error, 'wire failure text');
      }
      string(wire.node, 'wire node');
      string(wire.port, 'wire port');
      frames(wire.frames, 'wire frames');
      if (!('value' in wire)) throw new Error('wire: missing value');
    }
  }
  const normalized = { ...spec };
  for (const key of ['group', 'fire', 'frozen_from', 'expected', 'instance']) if (normalized[key] === null) delete normalized[key];
  // A null setting is left out, on a copy: the caller's spec is never
  // changed by reading it.
  if (normalized.settings !== undefined) {
    const settings = { ...(normalized.settings as Record<string, unknown>) };
    for (const key of ['keeping', 'keep_for', 'hold_secs']) if (settings[key] === null) delete settings[key];
    normalized.settings = settings;
  }
  return normalized as unknown as RunSpec;
}

/** How one wait ended, as a frozen example holds it: the value the person
 *  gave as `payload` (`null` included), `skipped: true` for a person
 *  skipping it, or `gave_up` with the error the waiting step failed with
 *  when the run gave the wait up. Exactly one of the three. */
// SYNC: Answer <-> crates/weft-core/src/run_spec.rs Answer
export type Answer = {
  node: string;
  frames?: Array<{ index: number }>;
  question?: JsonValue;
} & (
  | { payload: JsonValue; skipped?: false; gave_up?: never }
  | { skipped: true; payload?: never; gave_up?: never }
  | { gave_up: string; payload?: never; skipped?: false }
);

export interface FrozenFrom {
  version: string;
  execution_id: string;
  definition_hash: string;
}

// SYNC: Expected <-> crates/weft-core/src/run_spec.rs Expected
export interface Expected {
  nodes?: string[];
  focus?: string[];
  wires: ExpectedWire[];
}

// SYNC: ExpectedWire <-> crates/weft-core/src/run_spec.rs ExpectedWire
// `node` is the address a person types (`one.strip`, through the site
// for a node in an included file); `frames` holds loop positions only.
export interface ExpectedWire {
  ordinal?: number;
  closed?: boolean;
  failure?: Failure | null;
  node: string;
  port: string;
  frames?: Array<{ index: number }>;
  value: JsonValue;
}

// SYNC: ProgramIdentity <-> crates/weft-core/src/project/hash.rs ProgramIdentity
export interface ProgramIdentity {
  definition_hash: string;
  binary_hash: string;
  implementations: Record<string, string>;
}

// SYNC: BakeSummary <-> crates/weft-core/src/run_spec.rs BakeSummary
export interface BakeSummary {
  program: ProgramIdentity;
  captured: string[];
  execution_id: string;
  at_unix: number;
}

/// Every entry is a place: a `locatedKey` (`one/Clean.strip` for a node
/// of an included file under the site `one`, the bare id at the top),
/// since a body reached through two sites is two places of a run.
// SYNC: RunSelection <-> crates/weft-core/src/project/selection.rs RunSelection
export interface RunSelection {
  nodes: string[];
  edges: string[];
  boundary_ports: Record<string, string[]>;
  gates: string[];
  suppliers: string[];
  input: PortValues;
  input_origins: Record<string, Record<string, string>>;
}

// SYNC: Resolved, CrossingPort, Refusal <-> crates/weft-core/src/run_spec.rs Resolved, CrossingPort, Refusal
export interface Resolved {
  selection: RunSelection;
  kicks: Array<{ node: string; frames?: Frame[]; firing: boolean; payload: JsonValue | null; port_snapshot?: JsonValue }>;
  provided: Array<{ source_node: string; source_port: string; frames?: Frame[]; value: JsonValue; consumers: Array<[string, string]> }>;
  crossings: CrossingPort[];
  warnings: string[];
}

export interface CrossingPort {
  /// The receiving node as the program spells it (`one.strip`), the
  /// same key a `from` entry uses.
  node: string;
  port: string;
  source_node: string;
  source_port: string;
  /// Whether something in the run needs this input (`needed_by` is set).
  required: boolean;
  /// The required input it ends up feeding: its own, or one inside the
  /// group, loop or included file it enters.
  needed_by?: string;
  /// Where to hand the missing value: the first start the value would pass.
  hand_at?: StartPort;
  supplied: boolean;
}

// SYNC: StartPort <-> crates/weft-core/src/run_spec.rs StartPort
export interface StartPort {
  node: string;
  port: string;
}

export interface Refusal {
  errors: string[];
}

// SYNC: ResolveSpecResponse <-> crates/weft-cli/src/commands/parse.rs ResolveSpecResponse
export interface ResolveSpecResponse {
  resolved?: Resolved;
  refusal?: Refusal;
}

/// Whether a spec is a frozen example (`expected` filled).
export function isFrozen(spec: RunSpec): boolean {
  return spec.expected !== undefined;
}

/// The `weft run` flags that say what a spec says, for a one-off run of
/// a spec that is not on disk. The named form (`weft run <name>`) is for
/// a saved one; the two are the same request to the dispatcher.
export function specToRunArgs(spec: RunSpec, seeded = false): string[] {
  const args: string[] = [];
  if (seeded) args.push('--seed');
  for (const [node, ports] of Object.entries(spec.from ?? {})) {
    args.push('--from', Object.keys(ports).length ? `${node}=${JSON.stringify(ports)}` : node);
  }
  for (const id of spec.target ?? []) args.push('--target', id);
  for (const id of spec.before ?? []) args.push('--before', id);
  for (const id of spec.feed ?? []) args.push('--feed', id);
  if (spec.group) args.push('--group', Object.keys(spec.group[1]).length ? `${spec.group[0]}=${JSON.stringify(spec.group[1])}` : spec.group[0]);
  if (spec.fire) args.push('--fire', `${spec.fire[0]}=${JSON.stringify(spec.fire[1])}`);
  for (const [node, ports] of Object.entries(spec.emit ?? {})) args.push('--emit', `${node}=${JSON.stringify(ports)}`);
  if (spec.instance) args.push('--instance', spec.instance);
  if (spec.settings?.keeping === 'durable') args.push('--durable');
  if (spec.settings?.keeping === 'fast') args.push('--fast');
  if (spec.settings?.keep_for) args.push('--keep-for', spec.settings.keep_for);
  if (spec.settings?.hold_secs != null) args.push('--hold-secs', String(spec.settings.hold_secs));
  return args;
}

export function specSummary(spec: RunSpec): string {
  const parts: string[] = [];
  if (spec.group) parts.push(`group ${spec.group[0]}`);
  if (Object.keys(spec.from ?? {}).length) parts.push(`from ${Object.keys(spec.from!).join(', ')}`);
  if (spec.target?.length) parts.push(`until ${spec.target.join(', ')}`);
  if (spec.before?.length) parts.push(`before ${spec.before.join(', ')}`);
  if (spec.feed?.length) parts.push(`feeding ${spec.feed.join(', ')}`);
  if (spec.fire) parts.push(`fires ${spec.fire[0]}`);
  const count = Object.values(spec.from ?? {}).reduce((n, ports) => n + Object.keys(ports).length, 0) + Object.keys(spec.group?.[1] ?? {}).length;
  if (count) parts.push(`${count} input backups`);
  if (isFrozen(spec)) parts.push('frozen');
  return parts.length === 0 ? 'whole graph' : parts.join(', ');
}

/// A node or group addressed the way the source reads, through the
/// call sites descended into: `addressOf(['c'], 'C.inner')` is
/// `c.inner`, and `addressOf(['c', 'C.inner'], 'Inner.deep')` is
/// `c.inner.deep`. Each site id after the first is scoped under the
/// body it sits in (`C.inner`), and the id being addressed under the
/// body the view shows (`Inner.deep`), so the body prefix comes off
/// each: what is left is the chain of names a person wrote. This is
/// the spelling `weft run --group` and `weft events --node` take, and
/// the key the dispatcher holds a trigger's registration under, so a
/// display poll built here has to spell exactly what the Rust side
/// spells. The editor only ever addresses nodes and groups; a group's
/// compiler-made boundaries (`__in`, `__out`), which the Rust side
/// reads as the group, never reach this function.
// SYNC: addressOf <-> crates/weft-core/src/project.rs address_of
export function addressOf(callPath: readonly string[], id: string): string {
  const local = (scoped: string) => scoped.slice(scoped.indexOf('.') + 1);
  if (callPath.length === 0) return id;
  return [callPath[0], ...callPath.slice(1).map(local), local(id)].join('.');
}

/// The group the user is standing in, addressed through the call sites
/// descended into (`['c', 'C.inner']` is the group `c.inner`); `null`
/// at the top level.
export function groupOfCallPath(callPath: readonly string[]): string | null {
  if (callPath.length === 0) return null;
  const last = callPath[callPath.length - 1];
  return addressOf(callPath.slice(0, -1), last);
}

/// Whether a spec is scoped to `group` or to something inside it.
export function specScopedTo(spec: RunSpec, group: string): boolean {
  const s = spec;
  const inside = (id: string) => id === group || id.startsWith(`${group}.`);
  return (
    (s.group !== undefined && inside(s.group[0])) ||
    Object.keys(s.from ?? {}).some(inside) ||
    (s.target ?? []).some(inside) ||
    (s.before ?? []).some(inside)
  );
}

/// The specs a Run menu lists, with the ones scoped to `group` first
/// (a person who clicked into an include sees its proofs where they
/// are standing), each group kept in name order.
export function orderSpecsForMenu(specs: RunSpec[], group: string | null): RunSpec[] {
  const byName = [...specs].sort((a, b) => a.name.localeCompare(b.name));
  if (!group) return byName;
  return [...byName.filter((s) => specScopedTo(s, group)), ...byName.filter((s) => !specScopedTo(s, group))];
}

/// The spec the dialog opens with for a node action: "Run from here"
/// on a node, "Run this group" on a group or an include.
export function specForAction(action: 'from' | 'group', nodeId: string): RunSpec {
  return action === 'from'
    ? { name: `from-${nodeId.replace(/\./g, '-')}`, from: { [nodeId]: {} } }
    : { name: `group-${nodeId.replace(/\./g, '-')}`, group: [nodeId, {}] };
}

/// Why `name` cannot name an example, or `undefined` when it can.
///
/// An example is a NAME, not a path: it becomes the file
/// `examples/<name>.json` and `weft run <name>` finds it by that name.
/// The refusal suggests the name the person probably meant, and the
/// suggestion is put through the same rule so that following it cannot
/// be refused in its turn.
// SYNC: exampleNameProblem <-> crates/weft-cli/src/commands/versions.rs validate_example_name
export function exampleNameProblem(name: string): string | undefined {
  // A leading dash is refused because the name is typed back as an
  // argument (`weft run <name>`, `weft freeze <name>`) and the argument
  // parser reads it as a flag, so the name would be accepted here and
  // then unusable everywhere it is used.
  if (name.startsWith('-')) return 'an example cannot start with a dash';
  const looksLikeAPath = name.includes('/') || name.includes('\\') || name.endsWith('.json');
  if (!looksLikeAPath && name !== '' && name !== '.' && name !== '..') return undefined;
  let suggestion = name.split(/[/\\]/).pop() ?? '';
  while (suggestion.endsWith('.json')) suggestion = suggestion.slice(0, -'.json'.length);
  if (suggestion !== '' && suggestion !== '.' && suggestion !== '..') {
    return `an example is named, not a path: use \`${suggestion}\``;
  }
  return 'an example needs a name (a word, not a path or a directory)';
}
import type { Failure, Frame } from './protocol';
