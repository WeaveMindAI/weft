// The editor's side of the connect library: every question a connection
// picker asks, answered through the host bridge (`/access/*` as the
// tenant). The list reads through the shared grant cache, and every write
// that changes a service's list invalidates it, so a node's live
// permission check sees a fresh connect or a forget at once.
import { accessCall, openExternalUrl } from '../../../host';
import type { ConnectTransport, ResourceTransport } from '@weft/connect';
import type {
	CompletedConnect,
	ConsentOutcome,
	DoorsStatus,
	LookupItem,
	LookupPage,
	PickerOutcome,
	StartedConsent,
} from '@weft/connect';
import { grantsForService, invalidateGrants } from './grants-cache.svelte';

export const editorConnect: ConnectTransport = {
	// The author may use the runtime's own key (its credit is theirs).
	allowsSharedKey: true,
	connections: (service) => grantsForService(service),
	async forget(connection) {
		await accessCall<null>('DELETE', `grants/${encodeURIComponent(connection.id)}`);
		invalidateGrants(connection.service);
	},
	// SYNC: the doors request body <-> crates/weft-core/src/access/wire.rs DoorsRequest
	doors: (spec) => accessCall<DoorsStatus>('POST', 'doors', { spec }),
	async connectDirect(request) {
		// The host stamps the active project into the body; the editor's
		// connections are the author's, so it names none of its own.
		const done = await accessCall<CompletedConnect>('POST', 'connect/direct', { ...request, project_id: null });
		invalidateGrants(request.spec.service);
		return done;
	},
	beginConsent: (request) => accessCall<StartedConsent>('POST', 'connect/begin', request),
	async consentOutcome(state) {
		const outcome = await accessCall<ConsentOutcome>('GET', `connect/status?state=${encodeURIComponent(state)}`);
		if (outcome?.grant) invalidateGrants(outcome.grant.service);
		return outcome;
	},
	mintApp: (spec, permissions) => accessCall<{ values: Record<string, string> }>('POST', 'mint-app', { spec, permissions }),
	openExternal: openExternalUrl,
};

/** The editor's side of a `remote_select` field: every source signed
 *  with `accessRef`, the author's connection the node traced through the
 *  graph (none: only a public list and a pasted link work). */
export function editorResources(accessRef: { accessId: string; service: string } | null): ResourceTransport {
	// SYNC: the granted / lookup / picker bodies <-> crates/weft-core/src/access/lookup.rs GrantedQuery, LookupRequest; crates/weft-access-store/src/flows.rs BeginPicker
	const signed = () => {
		if (!accessRef) throw new Error('connect an account first');
		return accessRef;
	};
	return {
		granted: (_index, source) =>
			accessCall<LookupItem[]>('POST', 'granted', {
				access_id: signed().accessId,
				service: signed().service,
				from: source.from,
				label: source.label,
				value: source.value,
			}),
		list: (_index, source, query, parents, cursor) => {
			const { kind: _kind, requires: _requires, ...lookup } = source;
			// A public list signs with nothing: no connection, and no service
			// to name, so both are left out.
			return accessCall<LookupPage>('POST', 'lookup', {
				...(accessRef ? { access_id: accessRef.accessId, service: accessRef.service } : {}),
				lookup,
				query,
				parents,
				cursor,
			});
		},
		beginPicker: (_index, source) =>
			accessCall<{ state: string; url: string }>('POST', 'picker/begin', {
				access_id: signed().accessId,
				service: signed().service,
				script: source.script,
				code: source.code,
				mime_types: source.mime_types ?? [],
				grants: source.grants ?? [],
			}),
		pickerOutcome: (state) => accessCall<PickerOutcome>('GET', `connect/status?state=${encodeURIComponent(state)}`),
		openExternal: openExternalUrl,
	};
}
