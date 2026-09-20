// Which verbs the action bar offers, in one pure decision.
//
// The dispatcher's reconciliation table is the authority: it is what its
// own verb handlers enforce against, so a button and the endpoint behind
// it can never disagree, and it knows things the editor cannot (which
// infra a trigger actually depends on, whether a build is in flight).
//
// On top of it sit the STARTER verbs, the four whose action builds and
// REGISTERS on demand. A registration the dispatcher has on file may not
// gate those, because it answers from the definition it last registered
// and that goes stale the moment the user writes a node: it has never
// seen the database they just added, so it reports a project with no
// infra and offers no way to start any. Those four light from the source
// in front of the user, each under the one gate the source can answer
// for, and the table only ever adds verbs on top.
//
// What the table says about the project's LIFETIME is a different
// matter, and it wins outright: a project mid-activation or mid-infra-
// operation offers one cancel verb and nothing else. The source cannot
// know that, so `transitional` below suppresses every starter verb.
// SYNC: the verb gates <-> crates/weft-dispatcher/src/api/project.rs compute_available_actions, packages/weft-graph/src/protocol.ts ActionVerb

import type { ActionVerb, BackendSnapshot, InfraRollup } from '../../protocol';

/// What the source in front of the user says about the project, plus
/// the lifetime facts only the dispatcher can report.
export interface VerbInputs {
	/// The verbs the dispatcher offers right now. Empty on a project it
	/// has never registered, and on one the daemon did not answer about.
	available: readonly ActionVerb[];
	/// Does the SOURCE declare infra nodes / trigger nodes.
	hasInfra: boolean;
	hasTriggers: boolean;
	/// The project-wide infra state, as the last status fetch reported it.
	infraRollup: InfraRollup;
	/// The lifecycle status the last status fetch reported.
	status: BackendSnapshot['status'];
	/// An infra operation the rollup cannot see yet is in flight (a
	/// claimed stop still draining, a provisioning run before any node
	/// row flips). The dispatcher's own field, for the window where the
	/// rollup still reads `stopped` and the door already refuses.
	infraBusy: boolean;
}

/// Infra is the first lifetime, so anything that reaches the program
/// waits until the infra the source declares is up. Nothing here ever
/// starts it: infra is the user's own verb.
export function sourceInfraReady(inputs: VerbInputs): boolean {
	return !inputs.hasInfra || inputs.infraRollup === 'running';
}

/// Is the project in the middle of something that collapses the whole
/// table to one cancel verb?
///
/// The dispatcher returns exactly that during an activation, a
/// deactivation, and any infra operation in flight. The source says
/// nothing about it, so a starter verb offered here would be a button
/// whose click can only 409. The table still speaks for itself: the
/// cancel verb it names is offered, because it comes from `available`.
export function transitional(inputs: VerbInputs): boolean {
	if (inputs.status === 'activating' || inputs.status === 'deactivating') return true;
	const infraLive = inputs.hasInfra || inputs.infraRollup !== 'none';
	return infraLive
		&& (inputs.infraBusy
			|| inputs.infraRollup === 'provisioning'
			|| inputs.infraRollup === 'stopping'
			|| inputs.infraRollup === 'terminating');
}

/// The starter fallback, one entry per verb, each under the condition
/// the SOURCE can answer for and nothing else.
function starterGate(verb: ActionVerb, inputs: VerbInputs): boolean | undefined {
	switch (verb) {
		// Infra is its own lifetime and waits on nothing. It does need
		// something to provision, which only the source can offer: live
		// infra whose node was deleted has nothing left to start from,
		// only stop and terminate.
		case 'infra_start':
			return inputs.hasInfra;
		// The bar gates a run on the infra it would touch at its own call
		// site (the whole graph, or an aimed run's subgraph), so there is
		// nothing to repeat here.
		case 'run':
			return true;
		// A trigger needs the infra FEEDING IT running, which is a walk
		// the dispatcher does and the editor does not. So the source only
		// answers whether there is a trigger at all, and an activate with
		// infra down is refused by the door, naming the nodes to start.
		case 'activate':
		case 'reactivate':
			return inputs.hasTriggers;
		default:
			return undefined;
	}
}

/// Whether a verb is currently offered at all. The slots layer their own
/// conditions (an overlay in flight, an empty graph) on top of this.
export function isVerbOffered(verb: ActionVerb, inputs: VerbInputs): boolean {
	if (inputs.available.includes(verb)) return true;
	if (transitional(inputs)) return false;
	return starterGate(verb, inputs) === true;
}
