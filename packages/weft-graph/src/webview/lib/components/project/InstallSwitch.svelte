<script lang="ts">
	// Which install the graph shows the project on: the local one (the files
	// on disk, editable) or a target of the project (the program that install
	// holds, read-only but for its connections, with every read, run and verb
	// acting there). Drawn only when the project names a target.
	import { Cloud, Laptop, LoaderCircle } from "@lucide/svelte";
	import { LOCAL_INSTALL, type InstallView } from "../../../../protocol";

	let { view, onSwitch }: { view: InstallView; onSwitch: (install: string) => void } = $props();

	const ordered = $derived([
		...view.installs.filter((i) => i.name === LOCAL_INSTALL),
		...view.installs.filter((i) => i.name !== LOCAL_INSTALL),
	]);

	function hint(name: string, loggedIn: boolean): string {
		if (name === LOCAL_INSTALL) return "The files on disk, on this machine's install. Editable.";
		if (!loggedIn) return `You hold no key for ${name} yet: run \`weft login ${name}\` in the project.`;
		return `What ${name} runs: its program read-only, its connections, runs and status. Every action acts on ${name}.`;
	}
</script>

<div class="flex flex-col items-end gap-1">
	<div
		role="radiogroup"
		aria-label="Install"
		class="flex items-center rounded-md border bg-white border-zinc-200 shadow-sm p-0.5 text-xs font-medium"
	>
		{#each ordered as install (install.name)}
			{@const active = install.name === view.active}
			{@const remote = install.name !== LOCAL_INSTALL}
			<button
				type="button"
				role="radio"
				aria-checked={active}
				disabled={view.switching !== null}
				onclick={() => onSwitch(install.name)}
				title={hint(install.name, install.loggedIn)}
				class="flex items-center gap-1.5 px-2 py-1 rounded transition
					{active
						? remote ? 'bg-sky-600 text-white' : 'bg-zinc-800 text-white'
						: 'text-zinc-600 hover:bg-zinc-100'}
					{install.loggedIn ? '' : 'opacity-60'}"
			>
				{#if view.switching === install.name}
					<LoaderCircle class="w-3.5 h-3.5 animate-spin" />
				{:else if remote}
					<Cloud class="w-3.5 h-3.5" />
				{:else}
					<Laptop class="w-3.5 h-3.5" />
				{/if}
				<span>{install.name}</span>
			</button>
		{/each}
	</div>
	{#if view.error}
		<div class="max-w-xs rounded-md border border-red-200 bg-red-50 px-2 py-1 text-[11px] text-red-700 shadow-sm">
			{view.error}
		</div>
	{/if}
</div>
