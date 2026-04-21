<script lang="ts">
	import type { Snippet } from 'svelte';

	let { tabs, activeTab = $bindable(), projectId, collapsed = $bindable(false), children }: {
		tabs: Array<{ id: string; label: string; icon: Snippet; badge?: () => string | number | null }>;
		activeTab: string;
		projectId: string;
		collapsed: boolean;
		children: Snippet;
	} = $props();

	function storageKey(suffix: string) {
		return `rightSidebar:${projectId}:${suffix}`;
	}

	function loadNumber(key: string, fallback: number): number {
		try {
			const v = localStorage.getItem(key);
			return v === null ? fallback : Number(v);
		} catch { return fallback; }
	}

	let panelWidth = $state(loadNumber(storageKey('width'), 320));

	$effect(() => {
		try { localStorage.setItem(storageKey('width'), String(panelWidth)); } catch {}
	});

	const MIN_WIDTH_PX = 220;
	const MAX_WIDTH_PCT = 60;
	let isDragging = $state(false);
	let containerEl: HTMLElement | null = null;

	function onDragStart(e: MouseEvent) {
		e.preventDefault();
		isDragging = true;

		function onMove(ev: MouseEvent) {
			if (!containerEl) return;
			const parentWidth = containerEl.parentElement?.clientWidth ?? window.innerWidth;
			const maxPx = (MAX_WIDTH_PCT / 100) * parentWidth;
			const rect = containerEl.getBoundingClientRect();
			const newWidth = rect.right - ev.clientX;
			panelWidth = Math.max(MIN_WIDTH_PX, Math.min(maxPx, newWidth));
		}

		function onUp() {
			isDragging = false;
			window.removeEventListener('mousemove', onMove);
			window.removeEventListener('mouseup', onUp);
		}

		window.addEventListener('mousemove', onMove);
		window.addEventListener('mouseup', onUp);
	}

	function selectTab(id: string) {
		if (activeTab === id && !collapsed) {
			collapsed = true;
		} else {
			activeTab = id;
			collapsed = false;
		}
	}
</script>

<!-- svelte-ignore a11y_no_static_element_interactions -->
<div
	bind:this={containerEl}
	class="relative flex h-full bg-[#f8f9fa] border-l border-zinc-200 overflow-hidden select-none"
	style="width: {collapsed ? '36px' : `${panelWidth}px`}; min-width: {collapsed ? '36px' : `${panelWidth}px`}; flex-shrink: 0; transition: {isDragging ? 'none' : 'width 200ms ease'};"
>
	{#if !collapsed}
		<!-- Main content area -->
		<div class="flex flex-col flex-1 min-w-0 overflow-hidden">
			<!-- Tab strip -->
			<div class="h-9 border-b border-zinc-200 flex items-stretch shrink-0 bg-[#f3f4f6]">
				{#each tabs as tab}
					<button
						class="flex items-center gap-1.5 px-3 text-[11px] font-semibold uppercase tracking-wider transition-colors border-b-2 {activeTab === tab.id ? 'text-zinc-800 border-zinc-800 bg-[#f8f9fa]' : 'text-zinc-400 border-transparent hover:text-zinc-600 hover:bg-zinc-100'}"
						onclick={() => { activeTab = tab.id; }}
					>
						<span class="w-3.5 h-3.5 flex items-center justify-center">
							{@render tab.icon()}
						</span>
						{tab.label}
						{#if tab.badge}
							{@const b = tab.badge()}
							{#if b != null}
								<span class="text-[10px] font-normal text-zinc-400">{b}</span>
							{/if}
						{/if}
					</button>
				{/each}
				<!-- Collapse button at far right -->
				<button
					class="ml-auto flex items-center justify-center w-8 rounded hover:bg-zinc-200 transition-colors text-zinc-400 hover:text-zinc-600"
					onclick={() => collapsed = true}
					title="Collapse panel"
				>
					<svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
						<polyline points="9 18 15 12 9 6"/>
					</svg>
				</button>
			</div>

			<!-- Tab content -->
			<div class="flex-1 overflow-hidden">
				{@render children()}
			</div>
		</div>

		<!-- Drag handle on left edge -->
		<!-- svelte-ignore a11y_no_static_element_interactions -->
		<div
			class="absolute top-0 left-0 w-1 h-full cursor-col-resize z-10 group"
			onmousedown={onDragStart}
		>
			<div class="absolute top-0 left-0 w-1 h-full bg-transparent group-hover:bg-zinc-300 transition-colors {isDragging ? 'bg-zinc-400' : ''}"></div>
		</div>
	{:else}
		<!-- Collapsed: vertical icon bar -->
		<div class="flex flex-col items-center py-2 gap-1 w-full">
			{#each tabs as tab}
				<button
					class="flex items-center justify-center w-7 h-7 rounded transition-colors {activeTab === tab.id ? 'bg-zinc-200 text-zinc-800' : 'text-zinc-400 hover:bg-zinc-200 hover:text-zinc-600'}"
					onclick={() => selectTab(tab.id)}
					title={tab.label}
				>
					<span class="w-[15px] h-[15px] flex items-center justify-center">
						{@render tab.icon()}
					</span>
				</button>
			{/each}
		</div>
	{/if}
</div>
