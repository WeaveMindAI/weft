import { callPathOf, type Frame, type ScopeDecidedEvent, type ValueRef } from '../../../protocol';
import type { Edge, NodeFiring, NodeFiringStatus, RunTerminal } from '../types';
import { containerStatus } from './container-status';
import { partOf } from './value-ref';

/** What one box (a group, or an include's call site) is painted from: a
 *  group is no node of a run, so the box has no firing of its own. Its
 *  run is its scope's copies, each decided by the engine (open, or
 *  skipped and why), and what its members and its neighbours did at that
 *  copy's frames. */
export type ScopeBox = {
	id: string;
	/** The view's call path: the copies painted here are the ones whose
	 *  call frames are this path. */
	callPath: readonly string[];
	/** Every copy of this scope the run decided (`scope_decided`), any
	 *  call path. */
	decisions: readonly ScopeDecidedEvent[];
	/** Every row that ran inside the box, at any depth. */
	inside: readonly NodeFiring[];
	/** The frames of the copy `row` (one of `inside`) ran in, for a box
	 *  the run decided nothing about (a group dissolved in the run's cut). */
	copyOf: (row: NodeFiring) => Frame[];
	/** The view's drawn wires (`host-bridge.ts legsOf`). */
	legs: readonly Edge[];
	/** The rows at this view's call path of the node a leg ends on, by its
	 *  id on the canvas (a loop box reads its own boundary's rows). */
	rowsOf: (nodeId: string, side: 'source' | 'target') => readonly NodeFiring[];
	inPorts: readonly string[];
	outPorts: readonly string[];
	terminal: RunTerminal | undefined;
};

/** One synthetic row per copy of the box, in the order the copies were
 *  decided: its status, its cost, and the values at its ports.
 *
 *  A box the run decided nothing about (dissolved in the run's cut, so no
 *  scope ran) is painted the way an ordinary container would be, from its
 *  members alone: one row per copy their frames say they ran in. */
export function scopeFirings(box: ScopeBox): NodeFiring[] {
	const here = box.decisions.filter((d) => sameSites(callPathOf(d.frames), box.callPath));
	const copies: Array<{ frames: Frame[]; decision?: ScopeDecidedEvent }> = here.length > 0
		? here.map((decision) => ({ frames: decision.frames, decision }))
		: distinctFrames(box.inside.map(box.copyOf)).map((frames) => ({ frames }));
	return copies.map(({ frames, decision }) => {
		const framesKey = JSON.stringify(frames);
		const related = box.inside.filter((e) => withinFrames(e.frames, frames));
		const own: NodeFiringStatus = decision?.state === 'skipped' ? 'skipped'
			: box.terminal ? box.terminal.state
			: 'running';
		const status = containerStatus(own, related);
		const decidedAt = decision ? decision.atUnix * 1000 : undefined;
		const startedAt = decidedAt ?? Math.min(...related.map((e) => e.startedAt));
		const ended = related.map((e) => e.completedAt).filter((t): t is number => t !== undefined);
		const terminal = status === 'completed' || status === 'failed' || status === 'skipped' || status === 'cancelled';
		const reused = decision ? decision.inheritedFrom : related[0]?.inheritedFrom;
		const ports = portValues(box, framesKey);
		return {
			// Frame-stack-keyed id so a copy's card keeps a stable identity
			// across ticks even as sibling copies' events interleave.
			id: `${box.id}-synth-${framesKey}`,
			nodeId: box.id,
			status,
			pulseId: `${box.id}-${framesKey}`,
			startedAt,
			completedAt: terminal ? Math.max(startedAt, ...ended) : undefined,
			input: ports.input,
			output: ports.output,
			closedPorts: ports.closedPorts,
			...(decision?.state === 'skipped' ? { skipReason: decision.reason } : {}),
			// Reused only when the decision and everything inside came from
			// the same seed run.
			inheritedFrom: reused !== undefined && related.every((e) => e.inheritedFrom === reused) ? reused : undefined,
			providedPorts: ports.providedPorts,
			inheritedPorts: ports.inheritedPorts,
			costUsd: related.reduce((sum, e) => sum + (e.costUsd || 0), 0),
			inheritedCostUsd: related.reduce((sum, e) => sum + (e.inheritedFrom ? e.costUsd : 0), 0),
			inheritedCostUnknown: related.some((e) => e.inheritedFrom && e.costUnknown),
			// An unknown member cost keeps the box honest too: the summed
			// figure alone would read as the full price.
			costUnknown: related.some((e) => e.costUnknown),
			credentialOwner: credentialOwner(related),
			logs: [],
			runId: '',
			frames,
			framesKey,
		};
	});
}

/** The values at the box's ports for the copy at `framesKey`, read off the
 *  wires on either face: an input is what its first reader inside got (or,
 *  for a box with no inside on this canvas, what the wire landing on it
 *  outside carried), an output what the wire leaving it inside carried
 *  (or what its first reader outside got). A port whose only wires take
 *  keys off the value on the reading side says nothing about the value
 *  itself, so it is left out. */
function portValues(box: ScopeBox, framesKey: string): {
	input: Record<string, ValueRef>;
	output: Record<string, ValueRef>;
	closedPorts: string[];
	providedPorts?: string[];
	inheritedPorts?: Record<string, string>;
} {
	const rowAt = (id: string, side: 'source' | 'target') => box.rowsOf(id, side).find((r) => r.framesKey === framesKey);
	const input: Record<string, ValueRef> = Object.create(null);
	const output: Record<string, ValueRef> = Object.create(null);
	const closedPorts: string[] = [];
	const providedPorts: string[] = [];
	const inheritedPorts: Record<string, string> = Object.create(null);
	/** What the reader at the end of `leg` got on its port, with how. */
	const read = (leg: Edge) => {
		if (leg.path && leg.path.length > 0) return undefined;
		const row = rowAt(leg.target, 'target');
		const port = leg.targetHandle ?? '';
		if (!row) return undefined;
		if (row.closedPorts?.includes(port)) return { closed: true as const, row, port };
		const values = row.input;
		if (!values || !Object.prototype.hasOwnProperty.call(values, port)) return undefined;
		return { closed: false as const, value: values[port], row, port };
	};
	/** What the producer at the start of `leg` sent, its keys taken. */
	const sent = (leg: Edge) => {
		const row = rowAt(leg.source, 'source');
		const values = row?.output;
		const port = leg.sourceHandle ?? '';
		if (!row || row.status !== 'completed' || !values || !Object.prototype.hasOwnProperty.call(values, port)) return undefined;
		return { value: partOf(values[port], leg.path ?? []) };
	};
	for (const port of box.inPorts) {
		const inner = `${port}__inner`;
		const inside = box.legs.filter((l) => l.source === box.id && l.sourceHandle === inner);
		const got = inside.map(read).find((r) => r !== undefined);
		if (got) {
			if (got.closed) closedPorts.push(port);
			else input[port] = got.value;
			if (got.row.providedPorts?.includes(got.port)) providedPorts.push(port);
			const origin = got.row.inheritedPorts?.[got.port];
			if (origin) inheritedPorts[port] = origin;
			continue;
		}
		const outside = box.legs.filter((l) => l.target === box.id && l.targetHandle === port);
		const value = outside.map(sent).find((v) => v !== undefined);
		if (value !== undefined) input[port] = value.value;
	}
	for (const port of box.outPorts) {
		const inner = `${port}__inner`;
		const inside = box.legs.filter((l) => l.target === box.id && l.targetHandle === inner);
		const value = inside.map(sent).find((v) => v !== undefined);
		if (value !== undefined) {
			output[port] = value.value;
			continue;
		}
		const outside = box.legs.filter((l) => l.source === box.id && l.sourceHandle === port);
		const got = outside.map(read).find((r) => r !== undefined && !r.closed);
		if (got && !got.closed) output[port] = got.value;
	}
	return {
		input,
		output,
		closedPorts,
		...(providedPorts.length > 0 ? { providedPorts } : {}),
		...(Object.keys(inheritedPorts).length > 0 ? { inheritedPorts } : {}),
	};
}

/** Is a row at `frames` inside the copy at `copy`: the same frame stack,
 *  or one that extends it (a nested loop's iterations, a call below). */
export function withinFrames(frames: readonly Frame[], copy: readonly Frame[]): boolean {
	return frames.length >= copy.length
		&& copy.every((f, i) => JSON.stringify(frames[i]) === JSON.stringify(f));
}

function sameSites(a: readonly string[], b: readonly string[]): boolean {
	return a.length === b.length && a.every((s, i) => s === b[i]);
}

function distinctFrames(all: Frame[][]): Frame[][] {
	const seen = new Map<string, Frame[]>();
	for (const frames of all) {
		const key = JSON.stringify(frames);
		if (!seen.has(key)) seen.set(key, frames);
	}
	return [...seen.values()];
}

/** Whose key the members spent: one distinct origin passes through,
 *  disagreeing members read 'mixed', no cost records means no origin. */
export function credentialOwner(rows: readonly NodeFiring[]): NodeFiring['credentialOwner'] {
	const origins = new Set(rows.map((e) => e.credentialOwner).filter((o) => o !== undefined));
	if (origins.size === 0) return undefined;
	return origins.size === 1 ? [...origins][0] : 'mixed';
}

/** The frames of the copy of a box that a row inside it ran in: the row's
 *  own, cut at the call the box's include site makes (a body runs one call
 *  frame deeper than its site) and stripped of the iterations of the
 *  `loopsBetween` loops between the box and the row. `sitesAbove` is the
 *  number of call frames above the box (the view's call path). */
export function copyFrames(frames: readonly Frame[], sitesAbove: number, loopsBetween: number): Frame[] {
	let sites = 0;
	let cut = frames.length;
	for (let i = 0; i < frames.length; i++) {
		if ('site' in frames[i]) {
			if (sites === sitesAbove) { cut = i; break; }
			sites++;
		}
	}
	const kept = frames.slice(0, cut);
	let drop = loopsBetween;
	while (drop > 0 && kept.length > 0 && 'index' in kept[kept.length - 1]) {
		kept.pop();
		drop--;
	}
	return kept;
}
