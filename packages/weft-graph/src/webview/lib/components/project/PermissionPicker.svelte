<script lang="ts">
	// The permission picker: a tick and one human sentence per entry,
	// driven entirely by the service's declared catalogue. Ticked once
	// at connect time; the ticks drive the consent URL, the generated
	// guide, and what the connection records. Never a live setting.
	import { openExternalUrl } from '../../../host';
	import type { Permission } from '../../../../protocol';

	let {
		permissions,
		ticked = $bindable([]),
		allPermissionsUrl = null,
		nodeType = null,
	}: {
		permissions: Permission[];
		ticked: string[];
		/// The provider's complete permission list, when this catalogue
		/// is a curated subset; renders the add-your-own hint below.
		allPermissionsUrl?: string | null;
		/// The access node type whose metadata a missing permission is
		/// added to (named in the hint).
		nodeType?: string | null;
	} = $props();

	function toggle(id: string) {
		ticked = ticked.includes(id) ? ticked.filter((t) => t !== id) : [...ticked, id];
	}
</script>

<div class="space-y-1">
	<div class="text-[10px] text-muted-foreground font-medium">Permissions</div>
	{#each permissions as p (p.id)}
		<label class="flex items-start gap-1.5 text-[10px] cursor-pointer" title={p.id}>
			<input
				type="checkbox"
				class="mt-0.5"
				checked={ticked.includes(p.id)}
				onchange={() => toggle(p.id)}
			/>
			<span>
				<span class="font-medium">{p.label}</span>
				<span class="text-muted-foreground"> {p.description}</span>
			</span>
		</label>
	{/each}
	{#if allPermissionsUrl}
		<!-- The catalogue is a curated subset (only what shipped nodes
		     use); the provider's full pool is one click away, and the
		     fix for a missing permission is naming it in THIS node's
		     metadata. Shown exactly where the user would hit the wall. -->
		<div class="text-[10px] text-muted-foreground pt-0.5">
			Need a permission that is not in this list? The provider's complete set is
			<button type="button" class="text-blue-500 hover:underline" onclick={(e) => { e.stopPropagation(); openExternalUrl(allPermissionsUrl!); }}>here</button>{#if nodeType};
			add the one you need under `permissions` in the {nodeType} node's metadata{/if}.
		</div>
	{/if}
</div>
