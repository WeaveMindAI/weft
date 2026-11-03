// A run spec: how a run was asked for, as one value. The TypeScript side
// of `weft_core::run_spec::RunSpec`: the file in `examples/<name>.json`,
// and what the runs view reads off a run to say which example or flags
// started it. Starting a run from a spec is the CLI's (`weft run`). This
// file also spells a place the way the program reads it (`addressOf`),
// for every run, display and status lookup the editor makes.

import type { Failure } from './protocol';

/// The JSON a `.weft` value carries; the spec never inspects it.
export type JsonValue = unknown;

export type PortValues = Record<string, Record<string, JsonValue>>;

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
  frozenFrom?: FrozenFrom;
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
  keepFor?: string;
  /** How long, in seconds, a wait holds the run's worker while the run
   *  cannot pause, from the last time anything moved in it. */
  holdSecs?: number;
  /** How long, in seconds, the run stays quiet on its worker before it
   *  pauses; anything that arrives meanwhile carries it on there. */
  pauseAfterSecs?: number;
}

/// An instance id: the grammar of one storage key segment.
// SYNC: INSTANCE_ID_PATTERN <-> crates/weft-core/src/storage/key.rs valid_segment, crates/weft-core/src/instance.rs InstanceId::new
export const INSTANCE_ID_PATTERN = /^(?!\.{1,2}$)[A-Za-z0-9._-]{1,128}$/;

/** How one wait ended, as a frozen example holds it: the value the person
 *  gave as `payload` (`null` included), `skipped: true` for a person
 *  skipping it, or `gaveUp` with the error the waiting node failed with
 *  when the run gave the wait up. Exactly one of the three. */
// SYNC: Answer <-> crates/weft-core/src/run_spec.rs Answer
export type Answer = {
  node: string;
  frames?: Array<{ index: number }>;
  question?: JsonValue;
} & (
  | { payload: JsonValue; skipped?: false; gaveUp?: never }
  | { skipped: true; payload?: never; gaveUp?: never }
  | { gaveUp: string; payload?: never; skipped?: false }
);

export interface FrozenFrom {
  version: string;
  runId: string;
  definitionHash: string;
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
/// spells. The editor only ever addresses nodes and groups; a loop's
/// boundaries (`__in`, `__out`), which the Rust side reads as the loop,
/// never reach this function.
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
