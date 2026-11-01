// What the graph draws about triggers and infra, and what the contextual
// bar offers for a selection. The dispatcher computes every state (a
// trigger's, an infra copy's, an instance's) and the status answer
// carries it; everything here only reads those states, so the badge on
// a node and the button that acts on it can never disagree.

import type {
	ActionAvailability,
	Dropped,
	HeldWork,
	InfraCopyRead,
	InfraPlacementStatus,
	InstanceInfraEntry,
	InstanceTarget,
	OwnerTriggers,
	RunCheck,
	TargetCheck,
	TriggerCopy,
	TriggerEntry,
	TriggerState,
} from '../../protocol';
import { describeOwnerTriggers } from '../../protocol';
import { describeProgress, describeRestart } from '../../status';

/// How a badge is painted: `on` green, `off` grey, `busy` a change under
/// way, `behind` out of date with the source, `held` work waiting on a
/// trigger that is down, `failed` red, `warn` amber.
export type BadgeTone = 'on' | 'off' | 'busy' | 'behind' | 'held' | 'failed' | 'warn';

/// A trigger's or an infra node's badge: the word on the node, and the
/// detail shown on hover.
export interface LifecycleBadge {
	label: string;
	tone: BadgeTone;
	detail?: string;
}

/// "3 arrivals and 1 paused run", naming only what there is.
// SYNC: describeHeld <-> crates/weft-core/src/projects.rs HeldWork::describe
export function describeHeld(held: HeldWork): string {
	const counted = (n: number, one: string, many: string) => (n === 1 ? `1 ${one}` : `${n} ${many}`);
	if (held.arrivals === 0 && held.paused === 0) return 'nothing';
	if (held.paused === 0) return counted(held.arrivals, 'arrival', 'arrivals');
	if (held.arrivals === 0) return counted(held.paused, 'paused run', 'paused runs');
	return `${counted(held.arrivals, 'arrival', 'arrivals')} and ${counted(held.paused, 'paused run', 'paused runs')}`;
}

/// "3 fires or calls dropped in the last minute or two (refused before any
/// run started), the last because: ...".
// SYNC: describeDropped <-> crates/weft-core/src/projects.rs Dropped::describe
export function describeDropped(dropped: Dropped): string {
	const what = dropped.count === 1 ? '1 fire or call' : `${dropped.count} fires or calls`;
	return `${what} dropped in the last minute or two (refused before any run started), the last because: ${dropped.lastReason}`;
}

function heldCount(copy: TriggerCopy): number {
	return (copy.held?.arrivals ?? 0) + (copy.held?.paused ?? 0);
}

/// One activation as a badge reads it: `on`, `off`, `behind`, `held back`
/// (taken down, and its bring-back refused), `held N` (off with work
/// waiting for it), `turning on...`, and `dropping` for one that is
/// on while what arrives for it is dropped.
function copyBadge(copy: TriggerCopy): LifecycleBadge {
	switch (copy.state) {
		case 'turning_on':
			return { label: 'turning on...', tone: 'busy' };
		case 'on':
			return copy.dropped ? { label: 'dropping', tone: 'warn' } : { label: 'on', tone: 'on' };
		case 'behind':
			return copy.dropped ? { label: 'dropping', tone: 'warn' } : { label: 'behind', tone: 'behind' };
		case 'off':
			if (copy.heldBack) return { label: 'held back', tone: 'held' };
			return heldCount(copy) > 0 ? { label: `held ${heldCount(copy)}`, tone: 'held' } : { label: 'off', tone: 'off' };
	}
}

/// What hovering one activation tells: its state in words, what it
/// holds, what its fires wait on, and the version they run.
function copyDetail(copy: TriggerCopy): string {
	const lines: string[] = [];
	switch (copy.state) {
		case 'turning_on':
			lines.push(copy.progress ? `Turning on ${describeProgress(copy.progress, copy.progress.asOfUnix)}.` : 'Turning on.');
			break;
		case 'on': lines.push('On.'); break;
		case 'behind': lines.push('On, firing an older version of the program. Resync brings it up to date.'); break;
		case 'off': lines.push(copy.mode === 'registered' ? 'Off: never turned on.' : `Off: its door ${doorWords(copy.mode)}.`); break;
	}
	if (copy.downWith === 'health') {
		lines.push("Taken down by the health check: its infra's health rules bring it back once that infra is healthy; with rules that do not, Turn on does.");
	}
	if (copy.downWith === 'infra') lines.push('Taken down with its infra: it comes back once that infra runs again.');
	if (copy.downWith === 'maintenance') lines.push('Taken down while the install is in maintenance: `weft maintenance off` brings it back.');
	if (copy.heldBack) lines.push(`Held back: ${copy.heldBack}`);
	if (copy.held && heldCount(copy) > 0) lines.push(`Holding ${describeHeld(copy.held)} for when it is back.`);
	if (copy.waiting) lines.push(`${copy.waiting.fires} fire(s) waiting: ${copy.waiting.reason}`);
	if (copy.dropped) lines.push(describeDropped(copy.dropped));
	if (copy.version) lines.push(`Fires run version ${copy.version.slice(0, 8)}.`);
	return lines.join('\n');
}

function doorWords(mode: TriggerCopy['mode']): string {
	switch (mode) {
		case 'take': return 'holds what arrives';
		case 'hide': return 'holds what arrives, out of sight';
		case 'refuse': return 'refuses what arrives';
		default: return 'is shut';
	}
}

/// A trigger node's badge. A trigger that exists once per instance reads
/// "3 of 5 on", each instance's own state on hover.
export function triggerBadge(entry: TriggerEntry): LifecycleBadge {
	if (!entry.perInstance) {
		const copy = entry.shared ?? { state: 'off' as const, mode: 'registered' as const };
		return { ...copyBadge(copy), detail: copyDetail(copy) };
	}
	const copies = entry.copies ?? [];
	if (copies.length === 0) {
		return { label: 'off', tone: 'off', detail: 'One per instance, and no instance has turned it on.' };
	}
	const on = copies.filter((c) => c.state === 'on' || c.state === 'behind').length;
	const tone: BadgeTone = copies.some((c) => c.state === 'turning_on')
		? 'busy'
		: copies.some((c) => c.dropped && (c.state === 'on' || c.state === 'behind'))
			? 'warn'
			: copies.some((c) => c.state === 'behind')
			? 'behind'
			: on > 0 ? 'on' : copies.some((c) => heldCount(c) > 0) ? 'held' : 'off';
	const detail = copies.map((c) => `${c.instance}: ${copyBadge(c).label}`).join('\n');
	return { label: `${on} of ${copies.length} on`, tone, detail };
}

/// One infra copy's status as its pill reads it.
function infraStatusBadge(status: string, behind: boolean): LifecycleBadge {
	switch (status) {
		case 'running': return behind ? { label: 'behind', tone: 'behind' } : { label: 'running', tone: 'on' };
		case 'flaky': return { label: 'flaky', tone: 'warn' };
		case 'failed': return { label: 'failed', tone: 'failed' };
		case 'provisioning': return { label: 'starting...', tone: 'busy' };
		case 'stopping': return { label: 'stopping...', tone: 'busy' };
		case 'terminating': return { label: 'terminating...', tone: 'busy' };
		// SYNC: "not_started" <-> crates/weft-core/src/infra/wire.rs INFRA_NOT_STARTED
		case 'stopped':
		case 'not_started': return { label: 'stopped', tone: 'off' };
		default: throw new Error(`an infra status the editor cannot read: '${status}'`);
	}
}

/// An infra node's pill: `running`, `stopped`, `starting...`, `behind`,
/// and the like. A node that exists once per instance reads "3 of 5
/// running", each copy on hover. `nowUnix` is on this machine's clock,
/// which a status answer's progress was moved onto.
export function infraBadge(entry: InfraPlacementStatus, instanceCopies: InstanceInfraEntry[], nowUnix: number): LifecycleBadge {
	// SYNC: "per_instance" <-> crates/weft-core/src/infra/wire.rs INFRA_PER_INSTANCE
	if (entry.status === 'per_instance') {
		const copies = instanceCopies.filter((c) => c.node === entry.node);
		if (copies.length === 0) {
			return { label: 'no copies', tone: 'off', detail: 'One per instance, and no instance has a copy yet.' };
		}
		const running = copies.filter((c) => c.status === 'running').length;
		const badges = copies.map((c) => infraStatusBadge(c.status, !!c.behind));
		const tone: BadgeTone = badges.some((b) => b.tone === 'busy')
			? 'busy'
			: badges.some((b) => b.tone === 'failed')
				? 'failed'
				: badges.some((b) => b.tone === 'behind')
					? 'behind'
					: running > 0 ? 'on' : 'off';
		const detail = copies
			.flatMap((c, i) => [
				`${c.instance}: ${badges[i].label}`,
				...(c.lastRestart ? [`${describeRestart(c.lastRestart, `${c.node} (instance ${c.instance})`)}.`] : []),
			])
			.join('\n');
		return { label: `${running} of ${copies.length} running`, tone, detail };
	}
	const badge = infraStatusBadge(entry.status, !!entry.behind);
	const lines: string[] = [];
	if (entry.failureMessage) lines.push(`${entry.failureStage ? `${entry.failureStage}: ` : ''}${entry.failureMessage}`);
	if (entry.progress) lines.push(`${badge.label} ${describeProgress(entry.progress, nowUnix)}`);
	if (entry.lastRestart) lines.push(`${describeRestart(entry.lastRestart, entry.node)}.`);
	if (entry.behind) lines.push('Running from older infra than the source holds now. Upgrade brings it up to date.');
	if (entry.takesDown?.length) lines.push(`Taking it down takes down: ${describeOwnerTriggers(entry.takesDown)}.`);
	return lines.length ? { ...badge, detail: lines.join('\n') } : badge;
}

// ─── The contextual bar ────────────────────────────────────────────────

/// One selected node, as the contextual bar reads it: its place (what
/// every verb names it by), and what the source says it is.
export interface SelectedNode {
	place: string;
	isTrigger: boolean;
	isInfra: boolean;
	/// The source marks it (or derives it) once per instance. The status
	/// answer says the same for a node it knows; the source answers for
	/// one the install has not built yet.
	perInstance: boolean;
}

export type TriggerVerb = 'deactivate' | 'activate' | 'resync';
export type InfraVerb = 'infra_start' | 'infra_stop' | 'infra_terminate' | 'infra_upgrade';

/// One action of the bar, with the places it applies to among the
/// selected nodes (never empty: an action no selected node takes is not
/// listed).
export interface BarAction<V> {
	verb: V;
	places: string[];
}

export interface ContextActions {
	triggers: BarAction<TriggerVerb>[];
	infra: BarAction<InfraVerb>[];
	/// Selected nodes that exist once per instance, which the shared
	/// target cannot act on: they need an instance (or every one) picked.
	needInstance: string[];
	/// Selected shared nodes an instance target does not reach.
	sharedLeftOut: string[];
	/// Some selected trigger or infra node exists once per instance, so
	/// the bar shows the instance picker.
	anyPerInstance: boolean;
}

/// The order the bar lists the actions in.
const TRIGGER_VERBS: TriggerVerb[] = ['deactivate', 'activate', 'resync'];
const INFRA_VERBS: InfraVerb[] = ['infra_start', 'infra_stop', 'infra_terminate', 'infra_upgrade'];

/// The owners a target names: whether it reaches the shared ones, and the
/// instances it reaches (`null` for none; every one weft holds anything
/// for, for `all_instances` and `all`).
interface Owners {
	shared: boolean;
	instances: string[] | null;
}

// SYNC: ownersOf <-> crates/weft-core/src/instance.rs InstanceTarget::owners
function ownersOf(target: InstanceTarget | undefined, status: ActionAvailability): Owners {
	const every = () => status.instances.map((i) => i.instance);
	if (target === undefined) return { shared: true, instances: null };
	if (target === 'all_instances') return { shared: false, instances: every() };
	if (target === 'all') return { shared: true, instances: every() };
	return { shared: false, instances: target };
}

/// Which of one node's copies the owners reach: the program's own
/// (`'shared'`) for a shared node, the instances for a per-instance one,
/// or `null` when they reach none of its copies.
function reached(perInstance: boolean, owners: Owners): 'shared' | string[] | null {
	if (perInstance) return owners.instances;
	return owners.shared ? 'shared' : null;
}

/// Which trigger verbs one activation takes, from its state.
function triggerVerbsFor(state: TriggerState): TriggerVerb[] {
	switch (state) {
		case 'off': return ['activate'];
		case 'on': return ['deactivate'];
		case 'behind': return ['deactivate', 'resync'];
		// Its setup is running: the bottom bar offers that setup's cancel.
		case 'turning_on': return [];
	}
}

/// Which infra verbs one copy takes, from its status.
function infraVerbsFor(status: string, behind: boolean): InfraVerb[] {
	switch (status) {
		case 'not_started': return ['infra_start'];
		case 'stopped': return ['infra_start', 'infra_terminate'];
		case 'failed': return ['infra_start', 'infra_stop', 'infra_terminate'];
		case 'running':
		case 'flaky': return ['infra_stop', 'infra_terminate', ...(behind ? ['infra_upgrade' as const] : [])];
		// A change under way: the bottom bar offers its cancel.
		default: return [];
	}
}

/// Each lifecycle action once, with how many of the selected nodes it
/// applies to under `target` (`undefined` is the shared ones). A trigger
/// or infra copy the status answer does not list is one the install
/// never turned on or started.
export function contextActions(
	selected: SelectedNode[],
	status: ActionAvailability,
	target: InstanceTarget | undefined,
): ContextActions {
	const owners = ownersOf(target, status);
	const byVerb = new Map<TriggerVerb | InfraVerb, string[]>();
	const add = (verbs: Iterable<TriggerVerb | InfraVerb>, place: string) => {
		for (const verb of new Set(verbs)) byVerb.set(verb, [...(byVerb.get(verb) ?? []), place]);
	};
	const needInstance: string[] = [];
	const sharedLeftOut: string[] = [];
	let anyPerInstance = false;

	for (const node of selected) {
		if (node.isTrigger) {
			const entry = status.triggers.find((t) => t.node === node.place);
			const perInstance = entry?.perInstance ?? node.perInstance;
			anyPerInstance ||= perInstance;
			const reach = reached(perInstance, owners);
			if (reach === null) {
				(perInstance ? needInstance : sharedLeftOut).push(node.place);
				continue;
			}
			const states: TriggerState[] = reach === 'shared'
				? [entry?.shared?.state ?? 'off']
				: reach.map((id) => entry?.copies?.find((c) => c.instance === id)?.state ?? 'off');
			add(states.flatMap(triggerVerbsFor), node.place);
		} else if (node.isInfra) {
			const entry = status.infraNodes.find((n) => n.node === node.place);
			// SYNC: "per_instance" <-> crates/weft-core/src/infra/wire.rs INFRA_PER_INSTANCE
			const perInstance = entry ? entry.status === 'per_instance' : node.perInstance;
			anyPerInstance ||= perInstance;
			const reach = reached(perInstance, owners);
			if (reach === null) {
				(perInstance ? needInstance : sharedLeftOut).push(node.place);
				continue;
			}
			const copies = reach === 'shared'
				? [{ status: entry?.status ?? 'not_started', behind: !!entry?.behind }]
				: reach.map((id) => {
					const copy = status.instanceInfra.find((c) => c.node === node.place && c.instance === id);
					return { status: copy?.status ?? 'not_started', behind: !!copy?.behind };
				});
			add(copies.flatMap((c) => infraVerbsFor(c.status, c.behind)), node.place);
		}
	}

	return {
		triggers: TRIGGER_VERBS.filter((v) => byVerb.has(v)).map((verb) => ({ verb, places: byVerb.get(verb)! })),
		infra: INFRA_VERBS.filter((v) => byVerb.has(v)).map((verb) => ({ verb, places: byVerb.get(verb)! })),
		needInstance,
		sharedLeftOut,
		anyPerInstance,
	};
}

/// Which choices the bar's options hold for `actions`: how triggers go
/// down (turning off or resyncing), how infra goes down (stopping,
/// terminating, upgrading), and what triggers turned on do with what they
/// held. `any` is whether there is one at all: the panel shows a section
/// per choice, and the Options button shows only when `any` holds.
export function optionsFor(actions: ContextActions): {
	takesTriggersDown: boolean;
	turnsTriggersOn: boolean;
	infraGoingDown: BarAction<InfraVerb>[];
	any: boolean;
} {
	const takesTriggersDown = actions.triggers.some((a) => a.verb !== 'activate');
	const turnsTriggersOn = actions.triggers.some((a) => a.verb === 'activate');
	const infraGoingDown = actions.infra.filter((a) => a.verb !== 'infra_start');
	return { takesTriggersDown, turnsTriggersOn, infraGoingDown, any: takesTriggersDown || turnsTriggersOn || infraGoingDown.length > 0 };
}

/// The triggers taking `places` down takes down with them (each infra
/// entry's `takesDown`), each owner once with each of its triggers once:
/// what the infra options name.
export function takenDownWith(places: string[], status: ActionAvailability): OwnerTriggers[] {
	const byOwner = new Map<string, OwnerTriggers>();
	for (const place of places) {
		for (const { owner, triggers } of status.infraNodes.find((n) => n.node === place)?.takesDown ?? []) {
			const key = owner.kind === 'shared' ? '' : owner.instance;
			const known = byOwner.get(key) ?? { owner, triggers: [] };
			byOwner.set(key, { owner, triggers: [...new Set([...known.triggers, ...triggers])] });
		}
	}
	return [...byOwner.values()];
}

/// Why a lifecycle action of the contextual bar cannot go, shown as its
/// tooltip, or `undefined` when it can.
export function actionUnavailable(gates: { blocked: boolean; offered: boolean }): string | undefined {
	if (gates.blocked) return 'Something is under way; it shows on the bar below.';
	if (!gates.offered) return 'weft does not take this right now; the bar below says what it is busy with.';
	return undefined;
}

/// The infra copies of `read` that are not running in `status`, spelled
/// for a person: `db`, or `db (instance 'acme')` for an instance's copy.
export function infraNotRunning(
	read: readonly InfraCopyRead[],
	status: Pick<ActionAvailability, 'infraNodes' | 'instanceInfra'>,
): string[] {
	return read
		.filter((copy) => {
			const state = copy.instance === undefined
				? status.infraNodes.find((place) => place.node === copy.node)?.status
				: status.instanceInfra.find((entry) => entry.node === copy.node && entry.instance === copy.instance)?.status;
			return state !== 'running';
		})
		.map((copy) => (copy.instance === undefined ? copy.node : `${copy.node} (instance '${copy.instance}')`));
}

/// Why the contextual bar's Run cannot go, or `undefined` when it can.
/// `runBlocked` is what stops it whatever weft says (this view is no
/// place in the program); `check` is weft's answer for the selection
/// (`weft run --check`), `undefined` while it is asked: its refusal, or
/// else the infra it says the run reads that `status` does not show
/// running, which the install would refuse the run for. A run of several
/// targets is one run, refused whole: when the check says each target
/// alone, the reason names each target that stops it.
export function runUnavailable(gates: {
	blocked: boolean;
	offered: boolean;
	runBlocked: string | null;
	check: RunCheck | undefined;
	status: Pick<ActionAvailability, 'infraNodes' | 'instanceInfra'>;
}): string | undefined {
	const action = actionUnavailable(gates);
	if (action) return action;
	if (gates.runBlocked) return gates.runBlocked;
	const check = gates.check;
	if (check === undefined) return 'Checking what this run needs...';
	const down = check.ok ? infraNotRunning(check.infraRead ?? [], gates.status) : [];
	if (check.ok && down.length === 0) return undefined;
	const each = check.targets && check.targets.length > 1 ? eachTargetBlocked(check.targets, gates.status) : undefined;
	if (each) return each;
	if (!check.ok) return check.refusal;
	return `Start ${down.join(', ')} first: this run reads ${down.length === 1 ? 'it' : 'them'}, and ${down.length === 1 ? 'it is' : 'they are'} not running.`;
}

/// A run of several targets, said target by target: each one that could
/// not run alone with its reasons, then the ones that could. `undefined`
/// when none of them is blocked alone, so the run's own refusal speaks.
function eachTargetBlocked(targets: readonly TargetCheck[], status: Pick<ActionAvailability, 'infraNodes' | 'instanceInfra'>): string | undefined {
	const blocked: string[] = [];
	const free: string[] = [];
	for (const target of targets) {
		const reasons: string[] = [];
		const down = infraNotRunning(target.infraRead ?? [], status);
		if (down.length > 0) reasons.push(`needs ${down.join(', ')} started`);
		const triggers = target.unfiredTriggers ?? [];
		if (triggers.length > 0) {
			reasons.push(`only gets its input from the ${triggers.length === 1 ? 'trigger' : 'triggers'} ${triggers.join(', ')}, and Run does not fire triggers`);
		}
		if (target.refusal) reasons.push(`cannot start: ${target.refusal}`);
		if (reasons.length > 0) blocked.push(`${target.target} ${reasons.join(', and ')}`);
		else free.push(target.target);
	}
	if (blocked.length === 0) return undefined;
	const head = `These run together as one run, which cannot start: ${blocked.join('; ')}.`;
	if (free.length === 0) return head;
	return free.length === 1
		? `${head} ${free[0]} could run on its own: select it without the others.`
		: `${head} ${free.join(', ')} could each run on their own: select them without the others.`;
}
