// The install's picks for the project's own connections, read once and
// shared by every node on the canvas. An access node's connection is kept
// by the install (never in the source: a connection's id means nothing on
// another install), keyed by the node's place, so a node reads its pick
// here at `addressOf(callPath, id)`. A change goes through the host and
// the picks are read again, which bumps `installPicksVersion` so the
// canvas redraws (the unpicked-access pin, the traced connection of a
// remote_select).
import { picksCall } from '../../../host';
import { installPicked, type ChangePicks, type ConnectionHandle, type Picks } from '../../../../protocol';
import { invalidateGrants } from './grants-cache.svelte';
import { FreshReads } from './fresh-reads';

let picks = $state<Picks>({});
let version = $state(0);
let loadError = $state<string | null>(null);

/// Bumped whenever the picks are (re)read: read it in an effect to redraw
/// on a change.
export function installPicksVersion(): number {
	return version;
}

/// Why the last read failed, if it did (the canvas says it cannot show
/// the connections rather than showing every node unconnected).
export function installPicksError(): string | null {
	return loadError;
}

const reads = new FreshReads<Picks | undefined>(
	() => picksCall<Picks>('GET'),
	(outcome) => {
		if (outcome.ok) {
			picks = outcome.value ?? {};
			loadError = null;
		} else {
			loadError = outcome.error instanceof Error ? outcome.error.message : String(outcome.error);
		}
		version++;
	},
);

/// Read the picks from the install (again). Concurrent callers share one
/// read, never one that started before a change landed.
export function loadInstallPicks(): Promise<void> {
	return reads.load();
}

/// The handle picked for `field` of the node at `place`, if any.
export function pickedAt(place: string, field: string): ConnectionHandle | undefined {
	return picks[place]?.[field];
}

/// What a connection field's literal stands for: the install's pick when
/// the literal is the install-picked marker or absent (a view the
/// compiler has not marked yet), the literal otherwise (`@instance_filled`).
export function effectiveAccessValue(literal: unknown, place: string, field: string): unknown {
	return literal == null || installPicked(literal) ? pickedAt(place, field) : literal;
}

/// Pick `connection` (of `service`) for `field` of the node at `place`, or
/// forget the pick (`connection` null). Answers the triggers the change
/// set up again.
export async function changeInstallPick(
	place: string,
	field: string,
	service: string,
	connection: string | null,
): Promise<string[]> {
	const body: ChangePicks =
		connection == null
			? { clear: [{ step: place, field }] }
			: { set: [{ step: place, field, connection, service }] };
	const changed = await picksCall<{ rearmed?: string[] }>('PUT', body);
	reads.wrote();
	invalidateGrants(service);
	await loadInstallPicks();
	return changed?.rearmed ?? [];
}
