<script lang="ts">
	import * as Dialog from '$lib/components/ui/dialog';
	import { Button } from '$lib/components/ui/button/index.js';
	import { Plus, Trash2, Check, ChevronDown } from '@lucide/svelte';
	import type { PortDefinition } from '$lib/types';
	import { NODE_TYPE_CONFIG } from '$lib/nodes';
	import { authFetch } from '$lib/config';

	interface TestConfig {
		id: string;
		projectId: string;
		name: string;
		description: string;
		mocks: Record<string, Record<string, unknown>>;
		createdAt: string;
		updatedAt: string;
	}

	interface ProjectNode {
		id: string;
		nodeType: string;
		label?: string | null;
		outputs: PortDefinition[];
		isTrigger?: boolean;
		isInfrastructure?: boolean;
	}

	let {
		projectId,
		projectNodes,
		open = $bindable(false),
		selectedConfigId = null,
		onSelect,
	}: {
		projectId: string;
		projectNodes: ProjectNode[];
		open?: boolean;
		selectedConfigId?: string | null;
		onSelect: (configId: string | null, mocks: Record<string, Record<string, unknown>> | null) => void;
	} = $props();

	let configs = $state<TestConfig[]>([]);
	let loading = $state(true);
	let creating = $state(false);
	let editingId = $state<string | null>(null);
	let editName = $state('');
	let editDescription = $state('');
	let editMocks = $state<Record<string, Record<string, unknown>>>({});
	let addNodeDropdown = $state(false);

	// Nodes available for mocking (exclude triggers, infra, and already-mocked nodes)
	const mockableNodes = $derived(
		projectNodes.filter(n => !n.isTrigger && !n.isInfrastructure && n.outputs.length > 0)
	);
	const unmockedNodes = $derived(
		mockableNodes.filter(n => !editMocks[n.id])
	);

	async function loadConfigs() {
		loading = true;
		try {
			const res = await authFetch(`/api/projects/${projectId}/test-configs`);
			if (res.ok) configs = await res.json();
		} catch (e) {
			console.error('Failed to load test configs:', e);
		}
		loading = false;
	}

	$effect(() => { if (open) loadConfigs(); });

	async function createConfig() {
		creating = true;
		try {
			const res = await authFetch(`/api/projects/${projectId}/test-configs`, {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({ name: 'New test', mocks: {} }),
			});
			if (res.ok) {
				const config: TestConfig = await res.json();
				configs = [...configs, config];
				startEdit(config);
			}
		} catch (e) {
			console.error('Failed to create test config:', e);
		}
		creating = false;
	}

	function startEdit(config: TestConfig) {
		editingId = config.id;
		editName = config.name;
		editDescription = config.description ?? '';
		editMocks = JSON.parse(JSON.stringify(config.mocks));
		addNodeDropdown = false;
	}

	function addNodeMock(nodeId: string) {
		const node = projectNodes.find(n => n.id === nodeId);
		if (!node) return;
		const portDefaults: Record<string, unknown> = {};
		for (const port of node.outputs) {
			portDefaults[port.name] = defaultForType(port.portType);
		}
		editMocks = { ...editMocks, [nodeId]: portDefaults };
		addNodeDropdown = false;
	}

	function removeNodeMock(nodeId: string) {
		const next = { ...editMocks };
		delete next[nodeId];
		editMocks = next;
	}

	function updatePortValue(nodeId: string, portName: string, value: unknown) {
		editMocks = {
			...editMocks,
			[nodeId]: { ...editMocks[nodeId], [portName]: value },
		};
	}

	function defaultForType(portType: string): unknown {
		if (portType.startsWith('List')) return [];
		if (portType.startsWith('Dict') || portType === 'JsonDict') return {};
		if (portType === 'Number') return 0;
		if (portType === 'Boolean') return true;
		if (portType === 'Null') return null;
		return '';
	}

	function parsePortInput(raw: string, portType: string): unknown {
		if (portType === 'Number') { const n = Number(raw); return isNaN(n) ? 0 : n; }
		if (portType === 'Boolean') return raw === 'true';
		if (portType === 'Null') return null;
		if (portType.startsWith('List') || portType.startsWith('Dict') || portType === 'JsonDict') {
			try { return JSON.parse(raw); } catch { return raw; }
		}
		return raw;
	}

	function portInputType(portType: string): string {
		if (portType === 'Number') return 'number';
		if (portType === 'Boolean') return 'checkbox';
		return 'text';
	}

	function isJsonPort(portType: string): boolean {
		return portType.startsWith('List') || portType.startsWith('Dict') || portType === 'JsonDict';
	}

	async function saveEdit() {
		if (!editingId) return;
		try {
			const res = await authFetch(`/api/projects/${projectId}/test-configs/${editingId}`, {
				method: 'PUT',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({ name: editName, description: editDescription || null, mocks: editMocks }),
			});
			if (res.ok) {
				const updated: TestConfig = await res.json();
				configs = configs.map(c => c.id === updated.id ? updated : c);
				editingId = null;
				if (selectedConfigId === updated.id) onSelect(updated.id, updated.mocks);
			}
		} catch (e) {
			console.error('Failed to save test config:', e);
		}
	}

	async function deleteConfig(id: string) {
		try {
			const res = await authFetch(`/api/projects/${projectId}/test-configs/${id}`, { method: 'DELETE' });
			if (res.ok) {
				configs = configs.filter(c => c.id !== id);
				if (editingId === id) editingId = null;
				if (selectedConfigId === id) onSelect(null, null);
			}
		} catch (e) {
			console.error('Failed to delete test config:', e);
		}
	}

	function selectConfig(config: TestConfig) {
		onSelect(selectedConfigId === config.id ? null : config.id, selectedConfigId === config.id ? null : config.mocks);
	}

	function deactivateTests() {
		onSelect(null, null);
		open = false;
	}

	function getNodeLabel(nodeId: string): string {
		const node = projectNodes.find(n => n.id === nodeId);
		if (!node) return nodeId;
		if (node.label) return node.label;
		const config = NODE_TYPE_CONFIG[node.nodeType as keyof typeof NODE_TYPE_CONFIG];
		return config?.label || node.nodeType;
	}

	function getNodePorts(nodeId: string): PortDefinition[] {
		return projectNodes.find(n => n.id === nodeId)?.outputs ?? [];
	}
</script>

<Dialog.Root bind:open>
	<Dialog.Content class="!w-[90vw] !max-w-[90vw] h-[90vh] max-h-[90vh] flex flex-col">
		<Dialog.Header>
			<Dialog.Title>Test Configs</Dialog.Title>
			<Dialog.Description>
				Mock node outputs for testing. Select a config to enable test mode.
			</Dialog.Description>
		</Dialog.Header>

		<div class="flex-1 overflow-y-auto min-h-0 space-y-3 py-2">
			{#if loading}
				<div class="text-sm text-zinc-400 text-center py-8">Loading...</div>
			{:else if configs.length === 0}
				<div class="text-sm text-zinc-400 text-center py-8">
					No test configs yet. Create one to start mocking.
				</div>
			{:else}
				{#each configs as config (config.id)}
					<div class="border rounded-lg {selectedConfigId === config.id ? 'border-amber-400 bg-amber-50' : 'border-zinc-200'}">
						{#if editingId === config.id}
							<!-- Edit mode -->
							<div class="p-3 space-y-3">
								<div class="flex gap-2">
									<input type="text" class="flex-1 text-sm font-medium border rounded px-2 py-1" bind:value={editName} placeholder="Config name" />
									<input type="text" class="flex-1 text-xs border rounded px-2 py-1 text-zinc-500" bind:value={editDescription} placeholder="Description (optional)" />
								</div>

								<!-- Mocked nodes -->
								{#each Object.keys(editMocks) as nodeId (nodeId)}
									<div class="border border-zinc-200 rounded p-2 bg-white">
										<div class="flex items-center justify-between mb-2">
											<span class="text-xs font-semibold text-zinc-700">{getNodeLabel(nodeId)} <span class="text-zinc-400 font-normal">({nodeId})</span></span>
											<button class="text-zinc-400 hover:text-red-500 p-0.5" onclick={() => removeNodeMock(nodeId)} title="Remove mock">
												<Trash2 class="w-3 h-3" />
											</button>
										</div>
										{#each getNodePorts(nodeId) as port (port.name)}
											<div class="flex items-center gap-2 mb-1.5">
												<span class="text-[11px] text-zinc-500 w-24 shrink-0 text-right">{port.name} <span class="text-zinc-300">({port.portType})</span></span>
												{#if port.portType === 'Boolean'}
													<input
														type="checkbox"
														checked={editMocks[nodeId]?.[port.name] === true}
														onchange={(e) => updatePortValue(nodeId, port.name, (e.target as HTMLInputElement).checked)}
														class="rounded"
													/>
												{:else if isJsonPort(port.portType)}
													<textarea
														class="flex-1 text-xs font-mono border rounded px-2 py-1 min-h-[40px] resize-y"
														value={typeof editMocks[nodeId]?.[port.name] === 'string' ? (editMocks[nodeId][port.name] as string) : JSON.stringify(editMocks[nodeId]?.[port.name] ?? defaultForType(port.portType), null, 2)}
														oninput={(e) => updatePortValue(nodeId, port.name, parsePortInput((e.target as HTMLTextAreaElement).value, port.portType))}
													></textarea>
												{:else if port.portType === 'Number'}
													<input
														type="number"
														class="flex-1 text-xs border rounded px-2 py-1"
														value={editMocks[nodeId]?.[port.name] ?? 0}
														oninput={(e) => updatePortValue(nodeId, port.name, Number((e.target as HTMLInputElement).value))}
													/>
												{:else}
													<input
														type="text"
														class="flex-1 text-xs border rounded px-2 py-1"
														value={editMocks[nodeId]?.[port.name] ?? ''}
														oninput={(e) => updatePortValue(nodeId, port.name, (e.target as HTMLInputElement).value)}
													/>
												{/if}
											</div>
										{/each}
										{#if getNodePorts(nodeId).length === 0}
											<div class="text-[11px] text-zinc-400 italic">No output ports (node not in project?)</div>
										{/if}
									</div>
								{/each}

								<!-- Add node -->
								<div class="relative">
									{#if unmockedNodes.length > 0}
										<button
											class="text-xs text-zinc-500 hover:text-zinc-700 flex items-center gap-1"
											onclick={() => { addNodeDropdown = !addNodeDropdown; }}
										>
											<Plus class="w-3 h-3" /> Add node to mock
											<ChevronDown class="w-3 h-3" />
										</button>
										{#if addNodeDropdown}
											<div class="absolute left-0 top-6 bg-white border border-zinc-200 rounded shadow-lg z-10 max-h-[200px] overflow-y-auto min-w-[200px]">
												{#each unmockedNodes as node (node.id)}
													<button
														class="w-full text-left px-3 py-1.5 text-xs hover:bg-zinc-50 flex items-center justify-between"
														onclick={() => addNodeMock(node.id)}
													>
														<span class="font-medium">{node.label || NODE_TYPE_CONFIG[node.nodeType as keyof typeof NODE_TYPE_CONFIG]?.label || node.nodeType}</span>
														<span class="text-zinc-400 ml-2">{node.id}</span>
													</button>
												{/each}
											</div>
										{/if}
									{/if}
								</div>

								<div class="flex gap-2 pt-1">
									<Button size="sm" variant="default" onclick={saveEdit}>
										<Check class="w-3 h-3 mr-1" /> Save
									</Button>
									<Button size="sm" variant="outline" onclick={() => { editingId = null; }}>Cancel</Button>
								</div>
							</div>
						{:else}
							<!-- Display mode -->
							<!-- svelte-ignore a11y_click_events_have_key_events a11y_no_static_element_interactions -->
							<div role="button" tabindex="0" class="w-full text-left p-3 cursor-pointer hover:bg-zinc-50/50" onclick={() => selectConfig(config)} onkeydown={(e) => { if (e.key === 'Enter' || e.key === ' ') selectConfig(config); }}>
								<div class="flex items-center justify-between">
									<div>
										<div class="flex items-center gap-2">
											<span class="text-sm font-medium">{config.name}</span>
											{#if selectedConfigId === config.id}
												<span class="text-[10px] bg-amber-500 text-white px-1.5 py-0.5 rounded font-medium">ACTIVE</span>
											{/if}
										</div>
										{#if config.description}
											<div class="text-xs text-zinc-400 mt-0.5">{config.description}</div>
										{/if}
										<div class="text-xs text-zinc-400 mt-1">
											{Object.keys(config.mocks).length} {Object.keys(config.mocks).length === 1 ? 'node' : 'nodes'} mocked
										</div>
									</div>
									<!-- svelte-ignore a11y_no_static_element_interactions a11y_click_events_have_key_events --><div class="flex items-center gap-1" onclick={(e) => e.stopPropagation()}>
										<button class="p-1 rounded hover:bg-zinc-100 text-zinc-400 hover:text-zinc-600" onclick={() => startEdit(config)} title="Edit">
											<svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M11 4H4a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7"/><path d="M18.5 2.5a2.121 2.121 0 0 1 3 3L12 15l-4 1 1-4 9.5-9.5z"/></svg>
										</button>
										<button class="p-1 rounded hover:bg-red-50 text-zinc-400 hover:text-red-500" onclick={() => deleteConfig(config.id)} title="Delete">
											<Trash2 class="w-3.5 h-3.5" />
										</button>
									</div>
								</div>
							</div>
						{/if}
					</div>
				{/each}
			{/if}
		</div>

		<div class="flex items-center justify-between pt-3 border-t">
			<Button size="sm" variant="outline" onclick={createConfig} disabled={creating}>
				<Plus class="w-3 h-3 mr-1" /> New test config
			</Button>
			<div class="flex gap-2">
				{#if selectedConfigId}
					<Button size="sm" variant="ghost" onclick={deactivateTests}>Deactivate</Button>
				{/if}
				<Button size="sm" variant="outline" onclick={() => { open = false; }}>Close</Button>
			</div>
		</div>
	</Dialog.Content>
</Dialog.Root>
