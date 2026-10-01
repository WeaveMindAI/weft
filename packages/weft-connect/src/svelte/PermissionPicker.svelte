<script lang="ts">
	// The permission picker: a tick and one human sentence per entry,
	// driven entirely by the service's declared catalogue. Ticked once
	// at connect time; the ticks drive the consent URL, the generated
	// guide, and what the connection records. Never a live setting.
	import type { Permission } from '../core/wire';

	let {
		permissions,
		ticked = $bindable([]),
		allPermissionsUrl = null,
		nodeType = null,
		openExternal,
	}: {
		permissions: Permission[];
		ticked: string[];
		/// The provider's complete permission list, when this catalogue
		/// is a curated subset; renders the add-your-own hint below.
		allPermissionsUrl?: string | null;
		/// The access node type whose metadata a missing permission is
		/// added to (named in the hint). Absent on an instance's page, where
		/// nobody edits the program.
		nodeType?: string | null;
		openExternal: (url: string) => void;
	} = $props();

	function toggle(id: string) {
		ticked = ticked.includes(id) ? ticked.filter((t) => t !== id) : [...ticked, id];
	}
</script>

<div class="wc-stack">
	<div class="wc-muted wc-strong">Permissions</div>
	{#each permissions as p (p.id)}
		<label class="wc-tick" title={p.id}>
			<input type="checkbox" checked={ticked.includes(p.id)} onchange={() => toggle(p.id)} />
			<span>
				<span class="wc-strong">{p.label}</span>
				<span class="wc-muted"> {p.description}</span>
			</span>
		</label>
	{/each}
	{#if allPermissionsUrl}
		<!-- The catalogue is a curated subset (only what shipped nodes
		     use); the provider's full pool is one click away, and the
		     fix for a missing permission is naming it in the node's
		     metadata. Shown exactly where the person would hit the wall. -->
		<div class="wc-muted">
			Need a permission that is not in this list? The provider's complete set is
			<button type="button" class="wc-link" onclick={(e) => { e.stopPropagation(); openExternal(allPermissionsUrl!); }}>here</button>{#if nodeType};
			add the one you need under `permissions` in the {nodeType} node's metadata{/if}.
		</div>
	{/if}
</div>

<style>
	.wc-stack > * + * {
		margin-top: 0.25rem;
	}
	.wc-muted {
		font-size: var(--wc-font-size, 10px);
		color: var(--wc-muted-fg, #71717a);
	}
	.wc-strong {
		font-weight: 500;
	}
	.wc-tick {
		display: flex;
		align-items: flex-start;
		gap: 0.375rem;
		font-size: var(--wc-font-size, 10px);
		cursor: pointer;
	}
	.wc-tick input {
		margin-top: 0.125rem;
	}
	.wc-link {
		color: var(--wc-accent, #3b82f6);
		background: none;
		border: none;
		padding: 0;
		font: inherit;
		cursor: pointer;
	}
	.wc-link:hover {
		text-decoration: underline;
	}
</style>
