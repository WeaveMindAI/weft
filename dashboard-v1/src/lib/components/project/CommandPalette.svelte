<script lang="ts">
	import type { NodeCategory } from "$lib/types";
	import { NODE_TYPE_CONFIG, ALL_NODES, type NodeType } from "$lib/nodes";
	import { browser } from "$app/environment";
	import { STORAGE_KEYS } from "$lib/utils";
	import * as te from "$lib/telemetry-events";
	import {
		Search, BrainCircuit, ChartBar, GitFork, Server, Wrench, Bug, Zap,
		Save, Play, Upload, Download, Undo2, Redo2, Copy, Trash2, CheckSquare, Maximize2, LayoutDashboard,
	} from '@lucide/svelte';
	import type { Component } from 'svelte';
	
	let {
		open = $bindable(false),
		onAddNode,
		onAction,
		playground = false,
	}: {
		open: boolean;
		onAddNode: (type: NodeType) => void;
		onAction?: (action: string) => void;
		playground?: boolean;
	} = $props();
	
	let searchValue = $state("");
	let inputRef: HTMLInputElement | null = $state(null);
	let selectedIndex = $state(0);
	
	// Get the currently highlighted item for preview
	let previewedItem = $derived.by(() => {
		if (filteredItems.length === 0) return null;
		const item = filteredItems[selectedIndex];
		if (!item) return null;
		if (item.type === 'node') {
			return { type: 'node' as const, config: NODE_TYPE_CONFIG[item.nodeType], nodeType: item.nodeType };
		}
		return null; // Don't preview actions
	});
	
	// Focus input when opened
	$effect(() => {
		if (open && inputRef) {
			setTimeout(() => inputRef?.focus(), 10);
			selectedIndex = 0;
			searchValue = "";
		}
	});
	
	// Favorites and recents from localStorage
	let favorites = $state<NodeType[]>([]);
	let recents = $state<NodeType[]>([]);
	
	$effect(() => {
		if (browser) {
			const savedFavorites = localStorage.getItem(STORAGE_KEYS.nodeFavorites);
			const savedRecents = localStorage.getItem(STORAGE_KEYS.nodeRecents);
			if (savedFavorites) favorites = JSON.parse(savedFavorites);
			if (savedRecents) recents = JSON.parse(savedRecents);
		}
	});
	
	// Save recents to localStorage
	function addToRecents(type: NodeType) {
		recents = [type, ...recents.filter(t => t !== type)].slice(0, 5);
		if (browser) {
			localStorage.setItem(STORAGE_KEYS.nodeRecents, JSON.stringify(recents));
		}
	}
	
	// Auto-generate categories from node definitions
	const CATEGORY_CONFIG: Record<NodeCategory, { icon: Component; order: number }> = {
		Triggers: { icon: Zap, order: 0 },
		AI: { icon: BrainCircuit, order: 1 },
		Data: { icon: ChartBar, order: 2 },
		Flow: { icon: GitFork, order: 3 },
		Infrastructure: { icon: Server, order: 4 },
		Utility: { icon: Wrench, order: 5 },
		Debug: { icon: Bug, order: 6 },
	};
	
	// Build categories from ALL_NODES (excluding hidden nodes)
	const nodeCategories = $derived.by(() => {
		const categoryMap = new Map<NodeCategory, NodeType[]>();
		
		for (const node of ALL_NODES) {
			// Skip nodes that are hidden from palette
			if (node.features?.hidden) continue;
			
			const category = node.category;
			if (!categoryMap.has(category)) {
				categoryMap.set(category, []);
			}
			categoryMap.get(category)!.push(node.type as NodeType);
		}
		
		return Array.from(categoryMap.entries())
			.map(([name, types]) => ({
				name,
				types,
				icon: CATEGORY_CONFIG[name]?.icon || Search,
				order: CATEGORY_CONFIG[name]?.order ?? 99,
			}))
			.sort((a, b) => a.order - b.order);
	});
	
	// Actions available in the palette
	const playgroundHiddenActions = new Set(['save', 'run', 'export_json', 'export_weft', 'import']);

	const actions: { id: string; label: string; icon: Component; shortcut?: string }[] = [
		{ id: "save", label: "Save Project", icon: Save, shortcut: "Ctrl+S" },
		{ id: "run", label: "Run Project", icon: Play, shortcut: "Ctrl+Enter" },
		{ id: "export_json", label: "Export as JSON", icon: Upload },
		{ id: "export_weft", label: "Export as Weft", icon: Upload },
		{ id: "import", label: "Import from JSON/Weft", icon: Download },
		{ id: "undo", label: "Undo", icon: Undo2, shortcut: "Ctrl+Z" },
		{ id: "redo", label: "Redo", icon: Redo2, shortcut: "Ctrl+Shift+Z" },
		{ id: "duplicate", label: "Duplicate Selected", icon: Copy, shortcut: "Ctrl+D" },
		{ id: "delete", label: "Delete Selected", icon: Trash2, shortcut: "Del" },
		{ id: "selectAll", label: "Select All Nodes", icon: CheckSquare, shortcut: "Ctrl+A" },
		{ id: "fitView", label: "Fit View", icon: Maximize2 },
		{ id: "autoOrganize", label: "Auto Organize Layout", icon: LayoutDashboard },
	];
	
	// Build flat list of all items for keyboard navigation
	type PaletteItem = { type: 'node'; nodeType: NodeType } | { type: 'action'; actionId: string };
	
	// Score an item for search ranking. Lower = better match.
	// 0: exact label match, 1: label starts with query, 2: label word starts with query,
	// 3: label contains query, 4: tag match, 5: description match, -1: no match.
	function scoreNode(config: { label: string; description: string; tags: string[] }, query: string): number {
		const label = config.label.toLowerCase();
		if (label === query) return 0;
		if (label.startsWith(query)) return 1;
		// Check if any word in the label starts with the query
		const words = label.split(/\s+/);
		if (words.some(w => w.startsWith(query))) return 2;
		if (label.includes(query)) return 3;
		if (config.tags.some(t => t.toLowerCase().includes(query))) return 4;
		if (config.description.toLowerCase().includes(query)) return 5;
		return -1;
	}

	const visibleActions = $derived(playground ? actions.filter(a => !playgroundHiddenActions.has(a.id)) : actions);

	let filteredItems = $derived.by(() => {
		const query = searchValue.toLowerCase().trim();

		if (!query) {
			// No query: show everything in default order (actions first, then nodes by category)
			const items: PaletteItem[] = [];
			for (const action of visibleActions) {
				items.push({ type: 'action', actionId: action.id });
			}
			for (const category of nodeCategories) {
				for (const nodeType of category.types) {
					items.push({ type: 'node', nodeType });
				}
			}
			return items;
		}

		// With query: score and sort all items
		const scored: { item: PaletteItem; score: number }[] = [];

		for (const action of visibleActions) {
			if (action.label.toLowerCase().includes(query)) {
				const label = action.label.toLowerCase();
				const score = label.startsWith(query) ? 1 : label.includes(query) ? 3 : 5;
				scored.push({ item: { type: 'action', actionId: action.id }, score: score + 0.5 }); // +0.5 to rank nodes above actions at same tier
			}
		}

		for (const category of nodeCategories) {
			for (const nodeType of category.types) {
				const config = NODE_TYPE_CONFIG[nodeType];
				const score = scoreNode(config, query);
				if (score >= 0) {
					scored.push({ item: { type: 'node', nodeType }, score });
				}
			}
		}

		scored.sort((a, b) => a.score - b.score);
		return scored.map(s => s.item);
	});
	
	// Reset selection when filter changes
	$effect(() => {
		filteredItems; // dependency
		selectedIndex = 0;
	});
	
	function handleSelectNode(type: NodeType) {
		te.palette.nodeSelected(type);
		addToRecents(type);
		onAddNode(type);
		open = false;
		searchValue = "";
	}

	function handleSelectAction(actionId: string) {
		te.palette.actionSelected(actionId);
		onAction?.(actionId);
		open = false;
		searchValue = "";
	}
	
	function selectItem(item: PaletteItem) {
		if (item.type === 'node') {
			handleSelectNode(item.nodeType);
		} else {
			handleSelectAction(item.actionId);
		}
	}
	
	// Scroll selected item into view
	function scrollSelectedIntoView() {
		if (browser) {
			const selected = document.querySelector('[data-selected="true"]');
			selected?.scrollIntoView({ block: 'nearest' });
		}
	}
	
	// Handle keyboard navigation inside the palette
	function handlePaletteKeyDown(e: KeyboardEvent) {
		if (e.key === 'Escape') {
			e.preventDefault();
			e.stopPropagation();
			open = false;
			return;
		}
		if (e.key === 'ArrowDown') {
			e.preventDefault();
			selectedIndex = Math.min(selectedIndex + 1, filteredItems.length - 1);
			setTimeout(scrollSelectedIntoView, 0);
			return;
		}
		if (e.key === 'ArrowUp') {
			e.preventDefault();
			selectedIndex = Math.max(selectedIndex - 1, 0);
			setTimeout(scrollSelectedIntoView, 0);
			return;
		}
		if (e.key === 'Enter' && filteredItems.length > 0) {
			e.preventDefault();
			selectItem(filteredItems[selectedIndex]);
			return;
		}
	}
	
	// Global keyboard shortcut to open (Ctrl+P - cross-platform)
	function handleGlobalKeyDown(e: KeyboardEvent) {
		const target = e.target as HTMLElement;
		
		// Skip if user is typing in an editable element (input, textarea, contenteditable)
		const isEditableElement = 
			target.tagName === 'INPUT' || 
			target.tagName === 'TEXTAREA' || 
			target.isContentEditable ||
			target.closest('.edit-textarea') ||
			target.closest('.annotation-node.editing');
		
		// Ctrl+P to toggle palette (always works)
		if (e.ctrlKey && e.key === 'p') {
			e.preventDefault();
			e.stopPropagation();
			open = !open;
			return;
		}
		
		// Skip other shortcuts if in editable element
		if (isEditableElement) return;
		
		// Prevent browser defaults for our shortcuts when palette is closed
		if (!open && e.ctrlKey) {
			if (e.key === 's') {
				e.preventDefault();
				onAction?.('save');
			} else if (e.key === 'z' && !e.shiftKey) {
				e.preventDefault();
				onAction?.('undo');
			} else if (e.key === 'z' && e.shiftKey) {
				e.preventDefault();
				onAction?.('redo');
			} else if (e.key === 'a') {
				e.preventDefault();
				onAction?.('selectAll');
			} else if (e.key === 'd') {
				e.preventDefault();
				onAction?.('duplicate');
			} else if (e.key === 'Enter') {
				e.preventDefault();
				onAction?.('run');
			}
		}
		
		// Delete key (no modifier)
		if (!open && e.key === 'Delete') {
			e.preventDefault();
			onAction?.('delete');
		}
	}
	
	$effect(() => {
		if (browser) {
			// Use capture phase to intercept before browser handles it
			window.addEventListener('keydown', handleGlobalKeyDown, true);
			return () => window.removeEventListener('keydown', handleGlobalKeyDown, true);
		}
	});
</script>

{#if open}
	<!-- Backdrop -->
	<!-- svelte-ignore a11y_no_static_element_interactions -->
	<div 
		class="fixed inset-0 z-[100] bg-black/50"
		onclick={() => open = false}
		onkeydown={handlePaletteKeyDown}
	></div>
	
	<!-- Palette Container -->
	<!-- svelte-ignore a11y_no_static_element_interactions -->
	<div 
		class="fixed top-[15%] left-1/2 -translate-x-1/2 z-[101] flex gap-3"
		onkeydown={handlePaletteKeyDown}
	>
		<!-- Main Palette -->
		<div class="w-[420px] bg-popover border rounded-xl shadow-2xl overflow-hidden">
			<!-- Search Input -->
			<div class="flex items-center border-b px-3">
				<span class="text-muted-foreground mr-2"><Search size={16} /></span>
				<input
					bind:this={inputRef}
					bind:value={searchValue}
					type="text"
					placeholder="Search nodes and actions..."
					class="flex-1 py-3 bg-transparent outline-none text-sm"
				/>
				<kbd class="text-xs text-muted-foreground bg-muted px-1.5 py-0.5 rounded">Esc</kbd>
			</div>
			
			<!-- Results -->
			<div class="max-h-96 overflow-y-auto p-2">
				{#if filteredItems.length === 0}
					<div class="text-center text-muted-foreground py-6 text-sm">
						No results found
					</div>
				{:else if searchValue.trim()}
					<!-- Ranked results when searching -->
					{#each filteredItems as item, itemIndex}
						{#if item.type === 'action'}
							{@const action = actions.find(a => a.id === item.actionId)}
							{#if action}
							<button
								class="w-full flex items-center gap-2 px-3 py-2 rounded-lg text-sm text-left transition-colors {itemIndex === selectedIndex ? 'bg-accent text-accent-foreground' : 'hover:bg-muted'}"
								data-selected={itemIndex === selectedIndex}
								onclick={() => handleSelectAction(action.id)}
								onmouseenter={() => selectedIndex = itemIndex}
							>
								<span class="w-4 h-4 flex items-center justify-center text-muted-foreground"><action.icon size={14} /></span>
								<span class="flex-1">{action.label}</span>
								{#if action.shortcut}
									<kbd class="text-xs text-muted-foreground bg-muted px-1.5 py-0.5 rounded">{action.shortcut}</kbd>
								{/if}
							</button>
							{/if}
						{:else}
							{@const config = NODE_TYPE_CONFIG[item.nodeType]}
							{@const NodeIcon = config.icon}
							<button
								class="w-full flex items-center gap-2 px-3 py-2 rounded-lg text-sm text-left transition-colors {itemIndex === selectedIndex ? 'bg-accent text-accent-foreground' : 'hover:bg-muted'}"
								data-selected={itemIndex === selectedIndex}
								onclick={() => handleSelectNode(item.nodeType)}
								onmouseenter={() => selectedIndex = itemIndex}
							>
								<span class="w-4 h-4 flex items-center justify-center text-muted-foreground">{#if NodeIcon}<NodeIcon size={14} />{/if}</span>
								<span class="flex-1">{config.label}</span>
								<span class="text-xs text-muted-foreground">{config.category}</span>
							</button>
						{/if}
					{/each}
				{:else}
					<!-- Default view: grouped by category -->
					<!-- Actions Section -->
					<div class="text-xs font-medium text-muted-foreground px-2 py-1">Actions</div>
					{#each actions as action}
						{@const itemIndex = filteredItems.findIndex(item => item.type === 'action' && item.actionId === action.id)}
						<button
							class="w-full flex items-center gap-2 px-3 py-2 rounded-lg text-sm text-left transition-colors {itemIndex === selectedIndex ? 'bg-accent text-accent-foreground' : 'hover:bg-muted'}"
							data-selected={itemIndex === selectedIndex}
							onclick={() => handleSelectAction(action.id)}
							onmouseenter={() => selectedIndex = itemIndex}
						>
							<span class="w-4 h-4 flex items-center justify-center text-muted-foreground"><action.icon size={14} /></span>
							<span class="flex-1">{action.label}</span>
							{#if action.shortcut}
								<kbd class="text-xs text-muted-foreground bg-muted px-1.5 py-0.5 rounded">{action.shortcut}</kbd>
							{/if}
						</button>
					{/each}
					
					<!-- Nodes by Category -->
					{#each nodeCategories as category}
						{#if category.types.length > 0}
							<div class="text-xs font-medium text-muted-foreground px-2 py-1 mt-2 flex items-center gap-1.5"><category.icon size={12} />{category.name}</div>
							{#each category.types as type}
							{@const config = NODE_TYPE_CONFIG[type]}
							{@const itemIndex = filteredItems.findIndex(item => item.type === 'node' && item.nodeType === type)}
							{@const NodeIcon = config.icon}
							<button
								class="w-full flex items-center gap-2 px-3 py-2 rounded-lg text-sm text-left transition-colors {itemIndex === selectedIndex ? 'bg-accent text-accent-foreground' : 'hover:bg-muted'}"
								data-selected={itemIndex === selectedIndex}
								onclick={() => handleSelectNode(type)}
								onmouseenter={() => selectedIndex = itemIndex}
							>
								<span class="w-4 h-4 flex items-center justify-center text-muted-foreground">{#if NodeIcon}<NodeIcon size={14} />{/if}</span>
								<span class="flex-1">{config.label}</span>
							</button>
						{/each}
						{/if}
					{/each}
				{/if}
			</div>
		</div>
		
		<!-- Preview Panel (only shows when a node is selected) -->
		{#if previewedItem}
			{@const PreviewIcon = previewedItem.config.icon}
			<div class="w-64 bg-popover border rounded-xl shadow-2xl p-4 self-start">
				<div class="flex items-center gap-3 mb-3">
					<span class="w-6 h-6 flex items-center justify-center">{#if PreviewIcon}<PreviewIcon size={20} />{/if}</span>
					<div>
						<div class="font-semibold">{previewedItem.config.label}</div>
						<div class="text-xs text-muted-foreground">{previewedItem.nodeType}</div>
					</div>
				</div>
				
				<p class="text-sm text-muted-foreground mb-3">{previewedItem.config.description}</p>
				
				<!-- Inputs -->
				{#if previewedItem.config.defaultInputs.length > 0}
					<div class="mb-2">
						<div class="text-xs font-medium text-green-600 mb-1">Inputs</div>
						<div class="flex flex-wrap gap-1">
							{#each previewedItem.config.defaultInputs as input}
								<span class="text-xs px-1.5 py-0.5 bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400 rounded">
									{input.name}
								</span>
							{/each}
						</div>
					</div>
				{/if}
				
				<!-- Outputs -->
				{#if previewedItem.config.defaultOutputs.length > 0}
					<div class="mb-2">
						<div class="text-xs font-medium text-blue-600 mb-1">Outputs</div>
						<div class="flex flex-wrap gap-1">
							{#each previewedItem.config.defaultOutputs as output}
								<span class="text-xs px-1.5 py-0.5 bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-400 rounded">
									{output.name}
								</span>
							{/each}
						</div>
					</div>
				{/if}
				
				<!-- Tags -->
				{#if previewedItem.config.tags.length > 0}
					<div class="flex flex-wrap gap-1 mt-3 pt-3 border-t">
						{#each previewedItem.config.tags as tag}
							<span class="text-xs px-1.5 py-0.5 bg-muted text-muted-foreground rounded">{tag}</span>
						{/each}
					</div>
				{/if}
			</div>
		{/if}
	</div>
{/if}
