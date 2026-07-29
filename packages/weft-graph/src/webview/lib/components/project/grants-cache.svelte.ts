// The ONE per-service grant-list fetch, shared by every component that
// reads the store's connections (the access widget's list, the node's
// live permission/value checks). Concurrent readers share one in-flight
// request; a failed fetch is never cached (the rejection reaches every
// caller, and the next read retries). Writers (Forget, a successful
// connect) invalidate their service so the next read refetches.
import { accessCall } from '../../../vscode';
import type { GrantSummary } from '../../../../protocol';

const cache = new Map<string, Promise<GrantSummary[]>>();

/// Bumped on every invalidation, so a live surface (a node's open
/// permission check) can DEPEND on it and re-read after a Forget or a
/// fresh connect instead of showing the old answer until something
/// unrelated re-renders it. Svelte 5 module-level $state is reactive
/// across importers.
let generation = $state(0);

/// The reactive invalidation counter; read it in a $derived/$effect to
/// re-run when any service's grant list changes.
export function grantsGeneration(): number {
	return generation;
}

/// The store's connection rows for one service, cached until
/// `invalidateGrants(service)`.
export function grantsForService(service: string): Promise<GrantSummary[]> {
	let entry = cache.get(service);
	if (!entry) {
		entry = accessCall<GrantSummary[]>(
			'GET',
			`grants?service=${encodeURIComponent(service)}`,
		);
		entry.catch(() => {
			if (cache.get(service) === entry) cache.delete(service);
		});
		cache.set(service, entry);
	}
	return entry;
}

/// Drop the cached list for a service; called after any write that
/// changes it (a Forget, a successful connect).
export function invalidateGrants(service: string) {
	cache.delete(service);
	generation++;
}
