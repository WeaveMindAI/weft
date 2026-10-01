<script lang="ts">
	// The editor's `remote_select` field: the connect library's
	// `ResourceSelect`, with the transport that signs every source with the
	// author's connection the parent traced through the graph (the
	// editor's `/access/*` as the tenant). An instance's page draws the same
	// control over the instance door.
	import { ResourceSelect } from '@weft/connect/svelte';
	import type { FieldDefinition } from '../../types';
	import { editorResources } from './editor-connect';

	let {
		field,
		value,
		accessRef,
		accessIsOwnField = false,
		grantedScopes = null,
		parents = {},
		onUpdate,
	}: {
		field: FieldDefinition;
		/// The picked id, or unset.
		value: string | undefined;
		/// The feeding access node's connection, traced structurally
		/// through the graph's edges by the parent; null = none picked.
		accessRef: { accessId: string; service: string } | null;
		/// Whether the access input is a connection picker on THIS node
		/// (vs a wired Access port): decides the connect-first wording.
		accessIsOwnField?: boolean;
		/// The traced connection's granted permission set, when the
		/// parent fetched it; null = unknown, sources stay.
		grantedScopes?: string[] | null;
		/// Picked parent ids for drill-down (`dependsOn`).
		parents?: Record<string, string>;
		onUpdate: (id: string | null) => void;
	} = $props();

	const transport = $derived(editorResources(accessRef));
</script>

<div class="nodrag nopan">
	<ResourceSelect
		sources={field.sources ?? []}
		{value}
		connection={accessRef != null ? 'own' : 'none'}
		{transport}
		{onUpdate}
		placeholder={field.placeholder ?? null}
		freeText={field.freeText ?? false}
		{grantedScopes}
		{parents}
		connectFirst={accessIsOwnField
			? `Connect an account first (pick a connection on this node's \`${field.access}\` field).`
			: `Connect an account first (wire this node's \`${field.access}\` input to a connected access node).`}
	/>
</div>
