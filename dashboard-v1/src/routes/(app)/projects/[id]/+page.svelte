<script lang="ts">
	import { page } from "$app/stores";
	import { goto } from "$app/navigation";
	import { browser } from "$app/environment";
	import { projects } from "$lib/stores/projects";
	import type { ProjectDefinition, NodeInstance, ValidationError } from "$lib/types";
	import RunnerView from '$lib/components/project/RunnerView.svelte';
	import LoomEditorModal from '$lib/components/project/LoomEditorModal.svelte';
	import TestConfigModal from '$lib/components/project/TestConfigModal.svelte';
	import PublishProjectModal from '$lib/components/project/PublishProjectModal.svelte';
	import { listPublications } from '$lib/publish/client';
	import { NODE_TYPE_CONFIG, ALL_NODES } from "$lib/nodes";
	import { validateProject, validateProjectAtLevel } from "$lib/validation";
	import * as te from "$lib/telemetry-events";
	
	import { 
		getUserId, 
		loadExecutionState as loadExecutionStateUtil, 
		saveExecutionState as saveExecutionStateUtil,
		saveRunningExecution,
		loadRunningExecution,
		clearRunningExecution,
	} from "$lib/utils";
	import { authFetch, api } from "$lib/config";
	import { Button } from "$lib/components/ui/button";
	import ProjectEditor from "$lib/components/project/ProjectEditor.svelte";
	import { buildNodeCatalog, buildShoppableNodes, resolveShoppedNodes, formatNodeCatalog } from '$lib/ai/node-catalog';
	import { updateNodeConfig as weftUpdateConfig, addNode as weftAddNode, removeNode as weftRemoveNode } from '$lib/ai/weft-editor';
	import { stripSensitiveFields, countSensitiveValues, computeVisitorAccess } from '$lib/ai/sanitize';
	import { publishProject as publishProjectApi } from '$lib/publish/client';
	import type { PublishedProject } from '$lib/publish/client';
	import { parseWeft } from '$lib/ai/weft-parser';
	import { parseLoom, hasLoomMarker, applyLoomPatchText } from '$lib/ai/loom-parser';
	import { onMount, untrack } from "svelte";
	import { toast } from "svelte-sonner";
	import {
		hasInfraDivergence,
		buildInfraReconciliationPlan,
		type FrontendInfraNode,
		type BackendTrigger,
		type ReconciliationPlan,
	} from '$lib/utils/infra-reconciliation';
	import ReconciliationDialog, { type ReconciliationResult } from '$lib/components/project/ReconciliationDialog.svelte';
	import type { NodeType } from '$lib/nodes';

	// Helper to check if a node is a trigger (based on features.isTrigger)
	const triggerNodeTypes = new Set(ALL_NODES.filter(n => n.features?.isTrigger).map(n => n.type));

	let project = $derived($projects.find((w) => w.id === $page.params.id));

	// Any time the URL param changes OR the store doesn't have the row yet,
	// try a direct fetch by id. This runs on first mount (before the store
	// has finished loading) and after navigation to a deployment project
	// that is excluded from the builder sidebar's list fetch.
	//
	// `loadingProjectById` tracks whether a direct fetch is in flight so
	// the template can show a loading state instead of "Project not found"
	// while the store is still being populated.
	let fetchAttempted = $state(new Set<string>());
	let loadingProjectById = $state(false);
	let fetchByIdErrorDetail = $state<string | null>(null);
	$effect(() => {
		const pid = $page.params.id;
		if (!pid || !browser) return;
		if (project) return;                // already in store
		if (fetchAttempted.has(pid)) return; // don't loop on 404
		fetchAttempted.add(pid);
		loadingProjectById = true;
		fetchByIdErrorDetail = null;
		// Do the direct fetch ourselves so we can capture the HTTP status
		// and surface it in the error screen. `projects.fetchById` swallows
		// errors and returns null, which hides the real cause.
		(async () => {
			try {
				const url = `/api/projects/${pid}`;
				const res = await authFetch(url, { credentials: 'include' });
				if (!res.ok) {
					const body = await res.text().catch(() => '');
					fetchByIdErrorDetail = `GET ${url} → ${res.status} ${res.statusText}${body ? ` · ${body.slice(0, 200)}` : ''}`;
					console.warn('[project page] fetchById failed:', fetchByIdErrorDetail);
					loadingProjectById = false;
					return;
				}
				// Delegate the store update to the real store method so the
				// derived `project` actually picks it up.
				await projects.fetchById(pid);
			} catch (e) {
				fetchByIdErrorDetail = `fetch threw: ${e instanceof Error ? e.message : String(e)}`;
				console.error('[project page] fetchById exception:', e);
			} finally {
				loadingProjectById = false;
			}
		})();
	});

	// Whenever the hydrated project changes identity (first load, navigation
	// between scopes, or deployment fetch-by-id completes), refresh the
	// publication list so the scope switcher populates with current data.
	// Runs AFTER the project is present so refreshPublications can filter
	// against the correct project id.
	let lastRefreshedProjectId = $state<string | null>(null);
	$effect(() => {
		if (!project) return;
		if (lastRefreshedProjectId === project.id) return;
		lastRefreshedProjectId = project.id;
		refreshPublications();
	});

	// Key to force ProjectEditor remount (only used for import, not AI apply)
	let editorKey = $state(0);
	let shouldAutoOrganize = $state($page.url.searchParams.get('autoOrganize') === '1');
	let fitViewAfterOrganize = $state($page.url.searchParams.get('autoOrganize') === '1');
	let editorRef: any = $state();

	// Builder/Runner mode toggle
	// Persisted per project in localStorage
	function getModeStorageKey(projectId: string) {
		return `weavemind-mode-${projectId}`;
	}

	function getInitialMode(wf: ProjectDefinition): 'builder' | 'runner' {
		if (!browser) return 'builder';
		const stored = localStorage.getItem(getModeStorageKey(wf.id));
		if (stored === 'builder' || stored === 'runner') return stored;
		// Default: runner if project has nodes, builder if empty
		return wf.nodes.length > 0 ? 'runner' : 'builder';
	}

	let viewMode = $state<'builder' | 'runner'>('builder');
	let testMode = $state(false);
	let selectedTestConfigId = $state<string | null>(null);
	let selectedTestConfigMocks = $state<Record<string, Record<string, unknown>> | null>(null);
	let showLoomEditor = $state(false);
	let showTestConfigModal = $state(false);
	let showPublishModalFromRunner = $state(false);
	let hasPublications = $state(false);
	/** When the current project IS a deployment, this holds the public URL
	 *  (`/p/<username>/<slug>`) so the runner's live-deployment banner can
	 *  display which audience is affected by admin-mode edits. Builder
	 *  projects don't populate this — edits on builder projects don't
	 *  propagate to visitors until a re-publish. */
	let deploymentPublicUrl = $state<string | null>(null);

	// Scope switcher: list of related projects the user can switch between.
	// Populated from `listPublications()` joined against the current project
	// and its origin builder. Builder project + its deployments appear as
	// one group so the admin can flip between editing code in the builder
	// and managing a deployment without clicking through modals.
	type ScopeOption = {
		projectId: string;
		label: string;         // display name
		kind: 'builder' | 'deployment';
		slug?: string;         // for deployments
	};
	let scopeOptions = $state<ScopeOption[]>([]);
	let scopeMenuOpen = $state(false);

	async function refreshPublications() {
		if (!project) return;
		try {
			const list = await listPublications();
			hasPublications = list.some(p => p.project_id === project!.id);

			// If the current project IS a deployment, resolve its public URL
			// from the publication row. Builder projects leave this null so
			// the live-deployment banner stays hidden there — builder edits
			// don't reach visitors until a re-publish.
			if (project!.isDeployment) {
				const own = list.find(p => p.project_id === project!.id);
				deploymentPublicUrl = own ? `/p/${own.username}/${own.slug}` : null;
			} else {
				deploymentPublicUrl = null;
			}

			// Resolve the builder project id for the current scope. If we
			// ARE the builder, it's our own id. If we're a deployment, it's
			// originProjectId. Then all deployments that share this builder
			// are siblings.
			const builderId = project!.isDeployment ? (project!.originProjectId ?? null) : project!.id;
			if (!builderId) {
				scopeOptions = [];
				return;
			}
			const siblings = list.filter(p => {
				// A publication is a sibling if its target project is a
				// deployment cloned from the same builder. We can't see the
				// origin_project_id from the publication row directly, but
				// publications reference the deployment project via project_id.
				// Two publications with the same builder origin show up here
				// as long as listPublications returns them (they're scoped to
				// the user). We filter them down via the builder id after.
				return p.project_id !== builderId && p.project_id !== null;
			});

			// Build the scope list: builder at the top, then each deployment.
			const options: ScopeOption[] = [];
			if (project!.isDeployment) {
				// We need the builder project's name. Fetch it on demand if
				// not in the store.
				const builder = $projects.find(p => p.id === builderId);
				options.push({
					projectId: builderId,
					label: builder?.name ?? 'Builder project',
					kind: 'builder',
				});
			} else {
				options.push({
					projectId: project!.id,
					label: project!.name,
					kind: 'builder',
				});
			}
			for (const pub of siblings) {
				if (!pub.project_id) continue;
				options.push({
					projectId: pub.project_id,
					label: `${pub.project_name}: /p/${pub.slug}`,
					kind: 'deployment',
					slug: pub.slug,
				});
			}
			scopeOptions = options;
		} catch {
			// Silently ignore: scope switcher stays empty, button label
			// falls back to "Publish".
		}
	}

	function switchScope(targetProjectId: string) {
		scopeMenuOpen = false;
		if (targetProjectId === $page.params.id) return;
		window.location.href = `/projects/${targetProjectId}`;
	}

	/** Prepare a publish payload from the current editor state. Reads the
	 *  live weft / loom / layout from the editor, optionally sanitizes the
	 *  weft, and computes the visitor access allowlist from the current
	 *  setup manifest. This is shared between "publish new" and "overwrite
	 *  existing" so both paths apply identical sanitization. */
	function buildPublishPayload(stripSensitive: boolean): {
		weftCode: string;
		loomCode: string;
		layoutCode: string | null;
		visitorAccess: ReturnType<typeof computeVisitorAccess>;
	} | null {
		if (!project) return null;
		let weftCode = editorRef?.getWeftCode() || project.weftCode || '';
		if (stripSensitive) {
			weftCode = stripSensitiveFields(weftCode, project.nodes);
		}
		// loomCode is the single source of truth. `setupManifest` is
		// a parsed view derived on hydration, never the authority.
		const loomCode = project.loomCode || '';
		const layoutCode = editorRef?.getLayoutCode?.() || project.layoutCode || null;
		const visitorAccess = computeVisitorAccess(project.setupManifest);
		return { weftCode, loomCode, layoutCode, visitorAccess };
	}

	/** Modal callback: publish a brand-new slug. */
	async function publishWithSanitize(args: {
		slug: string;
		description: string;
		stripSensitive: boolean;
		rateLimitPerMinute: number | null;
	}): Promise<void> {
		if (!project) return;
		const payload = buildPublishPayload(args.stripSensitive);
		if (!payload) return;
		await publishProjectApi({
			projectId: project.id,
			slug: args.slug,
			description: args.description,
			weftCode: payload.weftCode,
			loomCode: payload.loomCode,
			layoutCode: payload.layoutCode,
			visitorAccess: payload.visitorAccess,
			rateLimitPerMinute: args.rateLimitPerMinute,
		});
	}

	/** Modal callback: overwrite an existing slug (re-publish in place). */
	async function overwriteWithSanitize(args: {
		row: PublishedProject;
		stripSensitive: boolean;
		rateLimitPerMinute: number | null;
	}): Promise<void> {
		if (!project) return;
		const payload = buildPublishPayload(args.stripSensitive);
		if (!payload) return;
		await publishProjectApi({
			projectId: project.id,
			slug: args.row.slug,
			description: args.row.description,
			weftCode: payload.weftCode,
			loomCode: payload.loomCode,
			layoutCode: payload.layoutCode,
			visitorAccess: payload.visitorAccess,
			rateLimitPerMinute: args.rateLimitPerMinute,
		});
	}
	let hasSeenModeToggle = $state(false);

	$effect(() => {
		if (browser) {
			untrack(() => {
				if (project) {
					viewMode = getInitialMode(project);
					hasSeenModeToggle = localStorage.getItem(`weavemind-mode-seen-${project.id}`) === '1';

					const savedConfigId = localStorage.getItem(`weavemind-test-config-${project.id}`);
					if (savedConfigId) {
						authFetch(`/api/projects/${project.id}/test-configs/${savedConfigId}`)
							.then(res => res.ok ? res.json() : null)
							.then(config => {
								if (config) {
									selectedTestConfigId = config.id;
									selectedTestConfigMocks = config.mocks;
									testMode = true;
								} else {
									localStorage.removeItem(`weavemind-test-config-${project.id}`);
									testMode = false;
								}
							})
							.catch(() => {
								testMode = false;
							});
					} else {
						testMode = false;
					}
				}
			});
		}
	});

	function setTestMode(enabled: boolean) {
		testMode = enabled;
		if (!enabled) {
			selectedTestConfigId = null;
			selectedTestConfigMocks = null;
			if (project && browser) localStorage.removeItem(`weavemind-test-config-${project.id}`);
		}
	}

	function selectTestConfig(configId: string | null, mocks: Record<string, Record<string, unknown>> | null) {
		selectedTestConfigId = configId;
		selectedTestConfigMocks = mocks;
		testMode = configId !== null;
		if (project && browser) {
			if (configId) {
				localStorage.setItem(`weavemind-test-config-${project.id}`, configId);
			} else {
				localStorage.removeItem(`weavemind-test-config-${project.id}`);
			}
		}
	}

	function setViewMode(mode: 'builder' | 'runner') {
		// Deployment projects are read-only from the builder side. Silently
		// ignore attempts to switch to builder mode and keep the runner
		// view mounted. We also show a toast/banner so the user knows why.
		if (mode === 'builder' && project?.isDeployment) {
			return;
		}
		viewMode = mode;
		if (project && browser) {
			localStorage.setItem(getModeStorageKey(project.id), mode);
			localStorage.setItem(`weavemind-mode-seen-${project.id}`, '1');
			hasSeenModeToggle = true;
		}
	}

	// Force deployment projects into runner mode, always. This runs
	// whenever the project changes, so if the user navigates from a
	// builder project to a deployment project, they land in runner mode.
	$effect(() => {
		if (project?.isDeployment && viewMode !== 'runner') {
			viewMode = 'runner';
		}
	});

	function handleSaveLoom(loomCode: string) {
		if (!project) return;
		// Raw loom text straight from the editor. hydrateProject
		// re-parses it on the next store read to derive the
		// setupManifest view. No serializer round-trip.
		projects.update(project.id, { loomCode });
	}

	function handleRunnerUpdateNodeConfig(nodeId: string, config: Record<string, unknown>) {
		if (!project) return;
		// Find which fields actually changed
		const node = project.nodes.find(n => n.id === nodeId);
		if (!node) return;
		const updates: Array<{ nodeId: string; fieldKey: string; value: unknown }> = [];
		for (const [key, value] of Object.entries(config)) {
			if (node.config[key] !== value) {
				updates.push({ nodeId, fieldKey: key, value });
			}
		}
		if (updates.length > 0) {
			editorRef?.updateNodeConfigs(updates);
		}
	}

	// Project fingerprint: detect when project changes while trigger is active
	// Only includes fields that affect execution logic (not UI state like expanded, width, height)
	const UI_ONLY_NODE_TYPES = new Set(['Annotation', 'Group']);
	const UI_ONLY_CONFIG_KEYS = new Set(['expanded', 'width', 'height', 'textareaHeights']);

	function stripUiConfig(config: Record<string, unknown>): Record<string, unknown> {
		const out: Record<string, unknown> = {};
		for (const [k, v] of Object.entries(config)) {
			if (!UI_ONLY_CONFIG_KEYS.has(k)) out[k] = v;
		}
		return out;
	}

	function projectFingerprint(wf: ProjectDefinition): string {
		const data = {
			nodes: wf.nodes
				.filter(n => !UI_ONLY_NODE_TYPES.has(n.nodeType))
				.map(n => ({ id: n.id, nodeType: n.nodeType, config: stripUiConfig(n.config), inputs: n.inputs, outputs: n.outputs })),
			edges: wf.edges.map(e => ({ id: e.id, source: e.source, target: e.target, sourceHandle: e.sourceHandle, targetHandle: e.targetHandle })),
		};
		return JSON.stringify(data);
	}

	function projectHash(wf: ProjectDefinition): string {
		const s = projectFingerprint(wf);
		let hash = 5381;
		for (let i = 0; i < s.length; i++) {
			hash = ((hash * 33) ^ s.charCodeAt(i)) >>> 0;
		}
		return hash.toString(16).padStart(8, '0');
	}

	// Project activation state
	let isProjectActive = $state(false);

	// Hash of the project at activation time, stored server-side and returned by list_triggers.
	// Compare against current hash to detect drift.
	let backendProjectHash = $state<string | null>(null);
	let triggerStale = $derived(
		isProjectActive && backendProjectHash !== null && project
			? projectHash(project) !== backendProjectHash
			: false
	);

	let isActivating = $state(false);
	let isCheckingStatus = $state(true); // Start as true until first check completes
	let statusCheckFailed = $state(false); // True if we couldn't reach the API
	let backendTriggers = $state<BackendTrigger[]>([]);

	// Trigger detection: frontend graph vs backend state
	let hasTriggersInFrontend = $derived(project?.nodes.some(n => triggerNodeTypes.has(n.nodeType)) ?? false);
	let hasTriggersInBackend = $derived(backendTriggers.some(t => t.projectId === (project?.id ?? '') && (t.status === 'Running' || t.status === 'Activating' || t.status === 'Deactivating')));
	let hasTriggers = $derived(hasTriggersInFrontend || hasTriggersInBackend);

	// Infrastructure state, start as 'loading' until we confirm actual status from backend
	const infraNodeTypes = new Set(ALL_NODES.filter(n => n.features?.isInfrastructure).map(n => n.type));
	let infraStatus = $state<'none' | 'starting' | 'stopping' | 'terminating' | 'running' | 'stopped' | 'terminated' | 'failed' | 'loading' | 'error'>('loading');
	let infraPollTimer: ReturnType<typeof setInterval> | null = null;
	let infraNodes = $state<Array<{ nodeId: string; nodeType: string; instanceId: string; status: string; backend?: string }>>([]);
	let isInfraLoading = $state(false);

	// Infrastructure detection: frontend graph vs backend state
	let hasInfraInFrontend = $derived(project?.nodes.some(n => infraNodeTypes.has(n.nodeType)) ?? false);
	let hasInfraInBackend = $derived(infraStatus !== 'none' && infraStatus !== 'terminated' && infraStatus !== 'loading');
	let hasInfrastructure = $derived(hasInfraInFrontend || hasInfraInBackend);

	// Detect divergence between frontend graph and backend infra state
	let frontendInfraNodes = $derived<FrontendInfraNode[]>(
		(project?.nodes ?? []).filter(n => infraNodeTypes.has(n.nodeType)).map(n => ({
			id: n.id,
			nodeType: n.nodeType,
			config: n.config as Record<string, unknown>,
		}))
	);
	let infraDiverged = $derived(
		infraStatus === 'running' && hasInfraDivergence(frontendInfraNodes, infraNodes)
	);

	async function checkInfraStatus(retryCount = 0) {
		if (!project) {
			infraStatus = 'none';
			return;
		}
		try {
			const response = await authFetch(api.getInfraStatus(project.id));
			if (response.ok) {
				const data = await response.json();
				infraStatus = data.status as typeof infraStatus;
				infraNodes = data.nodes || [];
			} else if (retryCount < 3) {
				const delay = Math.pow(2, retryCount) * 1000;
				console.warn(`Infra status check failed (${response.status}), retrying in ${delay}ms...`);
				await new Promise(r => setTimeout(r, delay));
				return checkInfraStatus(retryCount + 1);
			} else {
				console.error('Infra status check failed after 3 retries, status unknown');
				infraStatus = 'error';
			}
		} catch {
			if (retryCount < 3) {
				const delay = Math.pow(2, retryCount) * 1000;
				console.warn(`Infra status check network error, retrying in ${delay}ms...`);
				await new Promise(r => setTimeout(r, delay));
				return checkInfraStatus(retryCount + 1);
			} else {
				console.error('Infra status check network error after 3 retries, status unknown');
				infraStatus = 'error';
			}
		}
	}

	function startInfraPolling(showToast = true) {
		stopInfraPolling();
		const transitionalStates = new Set(['starting', 'stopping', 'terminating']);

		async function poll() {
			await checkInfraStatus();

			// Keep polling while the backend reports a transitional state.
			// No timeout: provisioning blocks until the sidecar's ping returns ready.
			// Users can terminate manually if stuck.
			if (!transitionalStates.has(infraStatus)) {
				stopInfraPolling();
				isInfraLoading = false;
				if (showToast) {
					if (infraStatus === 'running') {
						toast.success('Infrastructure is running');
					} else if (infraStatus === 'stopped') {
						toast.success('Infrastructure stopped');
					} else if (infraStatus === 'terminated') {
						toast.success('Infrastructure terminated');
					} else if (infraStatus === 'failed') {
						toast.error('Infrastructure operation failed');
					}
				}
				if (infraStatus === 'terminated') {
					executionState = {
						isRunning: false,
						isStarting: false,
						isStopping: false,
						activeEdges: new Set(),
						nodeOutputs: {},
						nodeStatuses: {},
						nodeExecutions: {},
					};
				}
			}
		}

		infraPollTimer = setInterval(poll, 2000);
	}

	function stopInfraPolling() {
		if (infraPollTimer) {
			clearInterval(infraPollTimer);
			infraPollTimer = null;
		}
	}

	// Live data polling for infra nodes with hasLiveData feature
	let infraLiveData = $state<Record<string, import('$lib/types').LiveDataItem[]>>({});
	let liveDataPollTimer: ReturnType<typeof setInterval> | null = null;

	function startLiveDataPolling() {
		stopLiveDataPolling();
		async function pollLiveData() {
			if (!project || infraStatus !== 'running') return;
			const liveDataNodes = (project.nodes ?? []).filter(n => {
				const config = NODE_TYPE_CONFIG[n.nodeType as import('$lib/nodes').NodeType];
				return config?.features?.hasLiveData && config?.features?.isInfrastructure;
			});
			if (liveDataNodes.length === 0) return;

			const newData: Record<string, import('$lib/types').LiveDataItem[]> = {};
			await Promise.allSettled(liveDataNodes.map(async (n) => {
				try {
					const resp = await authFetch(api.getInfraLiveData(project!.id, n.id));
					if (resp.ok) {
						const data = await resp.json();
						newData[n.id] = data.items || [];
					}
				} catch { /* ignore per-node errors */ }
			}));
			infraLiveData = newData;
		}
		pollLiveData();
		liveDataPollTimer = setInterval(pollLiveData, 3000);
	}

	function stopLiveDataPolling() {
		if (liveDataPollTimer) {
			clearInterval(liveDataPollTimer);
			liveDataPollTimer = null;
		}
		infraLiveData = {};
	}

	// Start/stop live data polling based on infra status
	$effect(() => {
		if (infraStatus === 'running') {
			startLiveDataPolling();
		} else {
			stopLiveDataPolling();
		}
		return () => stopLiveDataPolling();
	});

	// Reconciliation dialog state
	let showReconciliationDialog = $state(false);
	let reconciliationPlan = $state<ReconciliationPlan | null>(null);

	function requestStartInfra() {
		if (!project || isInfraLoading) return;

		// If backend has existing infra, build reconciliation plan
		if (hasInfraInBackend && infraNodes.length > 0) {
			const plan = buildInfraReconciliationPlan(frontendInfraNodes, infraNodes);
			if (plan.hasChanges) {
				reconciliationPlan = plan;
				showReconciliationDialog = true;
				return;
			}
		}

		// No changes or no existing infra: start directly
		startInfrastructure();
	}

	async function confirmReconciliation(result: ReconciliationResult) {
		showReconciliationDialog = false;
		if (!project || !reconciliationPlan) return;

		let graphChanged = false;

		// Restore nodes: re-add backend nodes to the frontend project graph
		if (result.restoreNodeIds.length > 0) {
			const nodesToRestore = reconciliationPlan.entries
				.filter(e => result.restoreNodeIds.includes(e.nodeId));

			for (const entry of nodesToRestore) {
				const typeConfig = NODE_TYPE_CONFIG[entry.nodeType as NodeType];
				if (!typeConfig) continue;

				// Create a new NodeInstance with default config from the node type
				const restoredNode: NodeInstance = {
					id: entry.nodeId,
					nodeType: entry.nodeType,
					label: null,
					config: {},
					position: { x: 100 + Math.random() * 200, y: 100 + Math.random() * 200 },
					inputs: [...typeConfig.defaultInputs],
					outputs: [...typeConfig.defaultOutputs],
					features: typeConfig.features || {},
				};
				project.nodes = [...project.nodes, restoredNode];
			}
			graphChanged = true;
		}

		// Remove provision nodes: delete new nodes the user chose to discard
		if (result.removeNodeIds.length > 0) {
			const removeSet = new Set(result.removeNodeIds);
			project.nodes = project.nodes.filter(n => !removeSet.has(n.id));
			project.edges = project.edges.filter(
				e => !removeSet.has(e.source) && !removeSet.has(e.target)
			);
			graphChanged = true;
		}

		// Save the project if graph changed
		if (graphChanged) {
			let code = project!.weftCode || '';
			// Add restored nodes to weftCode
			for (const entry of (reconciliationPlan?.entries ?? []).filter(e => result.restoreNodeIds.includes(e.nodeId))) {
				code = weftAddNode(code, entry.nodeType, entry.nodeId);
			}
			// Remove discarded nodes from weftCode
			for (const nodeId of result.removeNodeIds) {
				code = weftRemoveNode(code, nodeId);
			}
			await projects.update(project!.id, { weftCode: code });
			editorKey++;
			toast.success('Project updated');
		}

		reconciliationPlan = null;

		// If user made overrides, only apply graph changes, don't restart infra.
		// They can click Start/Restart again once the graph matches their intent.
		if (graphChanged) {
			await checkInfraStatus();
			return;
		}

		// No overrides: user accepted the diff as-is → terminate old + start fresh
		if (result.hasRemainingChanges) {
			isInfraLoading = true;

			try {
				const resp = await authFetch(api.terminateInfra(project.id), { method: 'POST' });
				if (resp.ok) {
					const data = await resp.json();
					infraStatus = data.status as typeof infraStatus;
				}
				while (infraStatus === 'terminating' || infraStatus === 'stopping') {
					await new Promise(r => setTimeout(r, 2000));
					await checkInfraStatus();
				}
			} catch (err) {
				console.error('Failed to terminate existing infra:', err);
			}

			isInfraLoading = false;
			await startInfrastructure();
		} else {
			await checkInfraStatus();
		}
	}

	function cancelReconciliation() {
		showReconciliationDialog = false;
		reconciliationPlan = null;
	}

	async function startInfrastructure() {
		if (!project || isInfraLoading) return;
		te.infra.started(project.id);
		isInfraLoading = true;

		try {
			const userId = sessionStorage.getItem('weavemind_user_id') || 'local';
			const controller = new AbortController();
			const timeout = setTimeout(() => controller.abort(), 65_000);
			const response = await authFetch(api.startInfra(project.id), {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({ weftCode: editorRef?.getWeftCode() || project?.weftCode || '', userId }),
				signal: controller.signal,
			});
			clearTimeout(timeout);
			if (response.ok) {
				const data = await response.json();
				infraStatus = data.status;
				infraNodes = data.nodes || [];
				if (data.status === 'starting') {
					toast.info('Infrastructure starting...');
					startInfraPolling();
				} else if (data.status === 'limit_reached') {
					toast.error('Global infrastructure limit reached. Please contact us at support@weavemind.ai to increase your limit.');
					isInfraLoading = false;
				} else if (data.status === 'user_limit_reached') {
					toast.error('You have reached your infrastructure limit (2 running). Please stop or terminate existing infrastructure, or contact us at support@weavemind.ai.');
					isInfraLoading = false;
				} else {
					toast.success('Infrastructure started');
					isInfraLoading = false;
				}
			} else {
				const errorText = await response.text();
				toast.error('Failed to start infrastructure', { description: errorText });
				// Don't set infraStatus to 'failed' for payment/auth errors (4xx)
				// because it shows retry/terminate buttons which don't apply.
				// Only set 'failed' for server errors (5xx) where a retry might help.
				if (response.status >= 500) {
					infraStatus = 'failed';
				}
				isInfraLoading = false;
			}
		} catch (err) {
			const isTimeout = err instanceof DOMException && err.name === 'AbortError';
			toast.error(isTimeout ? 'Infrastructure request timed out' : 'Failed to start infrastructure', {
				description: isTimeout ? 'The server took too long to respond. You can use "Force Retry" to clear stuck state.' : (err instanceof Error ? err.message : String(err)),
			});
			infraStatus = 'failed';
			isInfraLoading = false;
		}
	}

	async function forceRetryInfra() {
		if (!project || isInfraLoading) return;
		isInfraLoading = true;
		infraStatus = 'starting';

		try {
			const resp = await authFetch(api.forceRetryInfra(project.id), { method: 'POST' });
			if (resp.ok) {
				const data = await resp.json();
				if (data.killed > 0) {
					toast.info(`Cleared ${data.killed} stuck operation(s). Retrying...`);
					await new Promise(r => setTimeout(r, 2000));
				}
			}
		} catch {
			// Best effort, continue to retry anyway
		}

		isInfraLoading = false;
		await startInfrastructure();
	}

	async function deactivateTriggersIfActive() {
		if (!project || !isProjectActive) return;
		try {
			const response = await authFetch(api.unregisterProjectTriggers(project.id), {
				method: 'DELETE',
				headers: { 'Content-Type': 'application/json' },
				credentials: 'include',
			});
			if (response.ok) {
				isProjectActive = false;
				await checkProjectActiveStatus();
			}
		} catch (err) {
			console.error('Failed to deactivate triggers:', err);
		}
	}

	async function stopInfrastructure() {
		if (!project) return;
		isInfraLoading = true;
		await deactivateTriggersIfActive();
		try {
			const response = await authFetch(api.stopInfra(project.id), { method: 'POST' });
			if (response.ok) {
				const data = await response.json();
				infraStatus = data.status as typeof infraStatus;
			} else {
				toast.error('Failed to stop infrastructure');
				await checkInfraStatus();
				isInfraLoading = false;
				return;
			}
		} catch (err) {
			toast.error('Failed to stop infrastructure', {
				description: err instanceof Error ? err.message : String(err),
			});
			await checkInfraStatus();
			isInfraLoading = false;
			return;
		}
		startInfraPolling();
	}

	async function terminateInfrastructure() {
		if (!project) return;
		// Stop any in-progress polling (e.g. from a starting operation)
		stopInfraPolling();
		isInfraLoading = true;
		await deactivateTriggersIfActive();
		try {
			const response = await authFetch(api.terminateInfra(project.id), { method: 'POST' });
			if (response.ok) {
				const data = await response.json();
				infraStatus = data.status as typeof infraStatus;
			} else {
				toast.error('Failed to terminate infrastructure');
				await checkInfraStatus();
				isInfraLoading = false;
				return;
			}
		} catch (err) {
			toast.error('Failed to terminate infrastructure', {
				description: err instanceof Error ? err.message : String(err),
			});
			await checkInfraStatus();
			isInfraLoading = false;
			return;
		}
		startInfraPolling();
	}

	onMount(() => {
		// Touch last_opened_at so the project list sorts by recency
		if (browser && $page.params.id) {
			authFetch(`/api/projects/${$page.params.id}`, { method: 'PATCH', credentials: 'include' }).catch(() => {});
		}

		// Check if this project has any publications so we can show
		// "Manage deployments" instead of "Publish" on the button.
		// Also re-run whenever `project` changes (see $effect below) so
		// the scope switcher picks up fresh data after a direct deployment
		// fetch or a navigation between scopes.
		refreshPublications();

		// AI builder bridge: respond to parent (website) postMessage requests
		if (browser && window.parent !== window) {
			window.addEventListener('message', handleAiBuilderMessage);
		}

		return () => {
			shouldStopPolling = true;
			stopInfraPolling();
			if (browser && window.parent !== window) {
				window.removeEventListener('message', handleAiBuilderMessage);
			}
		};
	});

	let aiChatActive = $state(false);

	async function handleAiBuilderMessage(event: MessageEvent) {
		const { type } = event.data || {};

		if (type === 'aiChatActive') {
			aiChatActive = event.data.active ?? false;
			return;
		}


		if (type === 'requestProjectContext') {
			let projectContext: string | null = null;
			let loomContext: string | null = null;
			if (project) {
				const liveCode = editorRef?.getWeftCode() || project?.weftCode || '';
				projectContext = '````weft\n' + liveCode + '\n````';
				if (project.loomCode) loomContext = project.loomCode;
			}
			window.parent.postMessage({
				type: 'projectContext',
				requestId: event.data.requestId,
				projectId: project?.id ?? null,
				projectContext,
				loomContext,
				nodeCount: project?.nodes?.length ?? 0,
			}, '*');
		}

		if (type === 'requestDsl') {
			const liveWeft = editorRef?.getWeftCode() || project?.weftCode || '';
			const weft = liveWeft ? ('````weft\n' + liveWeft + '\n````') : '';
			const loom = project?.loomCode ?? '';
			window.parent.postMessage({
				type: 'dslResult',
				requestId: event.data.requestId,
				weft,
				loom,
			}, '*');
		}

		if (type === 'requestNodeCatalog') {
			window.parent.postMessage({
				type: 'nodeCatalog',
				requestId: event.data.requestId,
				catalog: buildNodeCatalog(),
				shoppableNodes: buildShoppableNodes(),
			}, '*');
		}

		if (type === 'resolveShoppedNodes') {
			const { nodeTypes, requestId } = event.data;
			const resolved = resolveShoppedNodes(nodeTypes);
			window.parent.postMessage({
				type: 'resolveShoppedNodesResult',
				requestId,
				nodes: resolved,
				formatted: formatNodeCatalog(resolved),
			}, '*');
		}

		if (type === 'parseWeft') {
			const { rawResponse, requestId } = event.data;
			const result = parseWeft(rawResponse);
			window.parent.postMessage({
				type: 'weftParseResult',
				requestId,
				projects: result.projects.map((w: any) => ({
					project: JSON.parse(JSON.stringify(w.project)),
					errors: w.errors,
				})),
				errors: result.errors,
			}, '*');
		}

		if (type === 'validateProject') {
			const { nodes, edges, requestId, level } = event.data;
			const result = level
				? validateProjectAtLevel(nodes, edges, level)
				: validateProject(nodes, edges);
			// Convert Map to plain object for postMessage serialization
			const errorEntries: Record<string, ValidationError[]> = {};
			for (const [nodeId, errors] of result.nodeErrors) {
				errorEntries[nodeId] = errors;
			}
			window.parent.postMessage({
				type: 'validationResult',
				requestId,
				valid: result.valid,
				nodeErrors: errorEntries,
			}, '*');
		}

		if (type === 'applyWeftCode') {
			const { weftCode, requestId } = event.data;
			if (weftCode && project) {
				handleApplyWeftCode(weftCode).then(() => {
					window.parent.postMessage({ type: 'projectApplied', requestId, success: true }, '*');
				}).catch((err: Error) => {
					window.parent.postMessage({ type: 'projectApplied', requestId, success: false, error: err.message }, '*');
				});
			}
		}

		if (type === 'applyRunnerConfig') {
			// Tangle applies field values.
			// Payload: { requestId, updates: Array<{ fieldRef: "nodeId::fieldKey", value }> }
			const { updates, requestId } = event.data;
			if (project && Array.isArray(updates)) {
				let updatedNodes = [...project.nodes];
				const applied: string[] = [];
				const skipped: string[] = [];
				for (const { fieldRef, value } of updates) {
					if (typeof fieldRef !== 'string' || !fieldRef.includes('::')) {
						skipped.push(String(fieldRef));
						continue;
					}
					const [nodeId, fieldKey] = fieldRef.split('::');
					const node = updatedNodes.find(n => n.id === nodeId);
					if (!node) { skipped.push(fieldRef); continue; }
					updatedNodes = updatedNodes.map(n =>
						n.id === nodeId ? { ...n, config: { ...n.config, [fieldKey]: value } } : n
					);
					applied.push(fieldRef);
				}
				let code = project.weftCode || '';
				for (const ref of applied) {
					const [nodeId, fieldKey] = ref.split('::');
					const upd = updates.find((u: any) => u.fieldRef === ref);
					if (upd) code = weftUpdateConfig(code, nodeId, fieldKey, upd.value);
				}
				project.nodes = updatedNodes;
				await projects.update(project.id, { weftCode: code });
				// Also update the editor's internal node state so the builder view reflects changes
				const configUpdates = applied.map(ref => {
					const [nodeId, fieldKey] = ref.split('::');
					const upd = updates.find((u: any) => u.fieldRef === ref);
					return { nodeId, fieldKey, value: upd?.value };
				});
				editorRef?.updateNodeConfigs(configUpdates);
				window.parent.postMessage({ type: 'runnerConfigApplied', requestId, success: true, applied, skipped }, '*');
			} else {
				window.parent.postMessage({ type: 'runnerConfigApplied', requestId, success: false, error: 'No project or invalid updates' }, '*');
			}
		}

		if (type === 'parseLoom') {
			const { rawResponse, requestId } = event.data;
			const { manifest, errors } = parseLoom(rawResponse);
			window.parent.postMessage({
				type: 'loomParseResult',
				requestId,
				manifest: manifest ? JSON.parse(JSON.stringify(manifest)) : null,
				errors,
			}, '*');
		}

		if (type === 'applyLoomPatch') {
			const { patchBody, requestId } = event.data;
			// Apply patches against the raw loom source. loomCode is
			// the source of truth; no serializer detour.
			const currentLoom = project?.loomCode ?? '';
			const { patched, errors } = applyLoomPatchText(currentLoom, patchBody);
			window.parent.postMessage({
				type: 'loomPatchResult',
				requestId,
				patched,
				errors,
			}, '*');
		}

		if (type === 'applyLoom') {
			// The parent website's AI agent now sends raw loom TEXT
			// directly, not a parsed manifest. Writing the text
			// straight to `loomCode` lets hydrateProject re-parse it
			// into a fresh manifest on the next store read, without
			// round-tripping through a serializer that would lose
			// multi-line text blocks and comments.
			const { loomCode: incomingLoom, requestId } = event.data as { loomCode?: string; requestId: string };
			if (project && typeof incomingLoom === 'string') {
				await projects.update(project.id, { loomCode: incomingLoom });
				window.parent.postMessage({ type: 'loomApplied', requestId, success: true }, '*');
			} else {
				window.parent.postMessage({ type: 'loomApplied', requestId, success: false, error: 'No project loaded or invalid loom payload' }, '*');
			}
		}

		// Weft streaming: AI streams raw weft text into the code editor
		if (type === 'weftStreamStart') {
			const { mode } = event.data;
			editorRef?.weftStreamStart(mode ?? 'weft');
		}

		if (type === 'weftStreamDelta') {
			const { delta, mode, at } = event.data;
			editorRef?.weftStreamDelta(delta ?? '', mode ?? 'weft', at);
		}

		if (type === 'weftStreamPatchSearch') {
			const { searchText, requestId } = event.data;
			const result = editorRef?.weftStreamPatchSearch(searchText ?? '') ?? { error: 'Editor not ready' };
			window.parent.postMessage({ type: 'weftPatchSearchResult', requestId, ...result }, '*');
		}

		if (type === 'weftStreamEnd') {
			const { requestId } = event.data;
			const result = (await editorRef?.weftStreamEnd()) ?? { errors: [], warnings: [], opaqueBlocks: [] };
			window.parent.postMessage({
				type: 'weftStreamResult',
				requestId,
				errors: result.errors,
				warnings: result.warnings,
				opaqueBlocks: result.opaqueBlocks,
			}, '*');
		}
	}

	// Check trigger status on initial load only (not on every save)
	let triggerCheckDone = false;
	$effect(() => {
		if (browser && project && !triggerCheckDone) {
			triggerCheckDone = true;
			checkProjectActiveStatus();
		}
	});

	// Auto-detect trigger-fired executions when project is active
	$effect(() => {
		if (!browser || !isProjectActive) return;

		const interval = setInterval(checkForTriggerExecution, 3000);
		return () => clearInterval(interval);
	});

	async function checkForTriggerExecution() {
		if (!project || executionState.isRunning || isPollingActive) return;
		try {
			const res = await authFetch(`/api/executions?projectId=${project.id}&limit=5`);
			if (!res.ok) return;
			const executions: Array<{ id: string; status: string; startedAt: string }> = await res.json();
			const now = Date.now();
			// Include running ones AND recently completed ones (within 60s) we haven't shown yet
			const candidates = executions
				.filter(e => {
					if (e.id === currentExecutionId || handledExecutionIds.has(e.id)) return false;
					if (e.status === 'running' || e.status === 'waiting_for_input') return true;
					// Catch executions that finished before our poller noticed them running
					const age = now - new Date(e.startedAt).getTime();
					return age < 60_000;
				})
				.sort((a, b) => new Date(b.startedAt).getTime() - new Date(a.startedAt).getTime());
			if (candidates.length === 0) return;
			const latest = candidates[0];
			console.debug('[TriggerPoll] New trigger-fired execution detected:', latest.id, 'status:', latest.status);
			currentExecutionId = latest.id;
			handledExecutionIds.add(latest.id);
			saveRunningExecution(project.id, latest.id);

			const isStillRunning = latest.status === 'running' || latest.status === 'waiting_for_input';
			if (isStillRunning) {
				await executeWithBackend(latest.id, true);
			} else {
				// Execution already finished -- load final state from DB (Restate purges completed state)
				const loaded = await loadExecutionFromApi(latest.id);
				if (loaded) {
					executionState = loaded;
				}
			}
		} catch (e) {
			console.debug('[TriggerPoll] Poll failed:', e);
		}
	}

	// Always check infra status on load (backend may have running infra even if frontend has no infra nodes)
	let infraCheckDone = false;
	$effect(() => {
		if (browser && project && !infraCheckDone) {
			infraCheckDone = true;
			checkInfraStatus().then(() => {
				const transitional = new Set(['starting', 'stopping', 'terminating']);
				if (transitional.has(infraStatus)) {
					isInfraLoading = true;
					startInfraPolling(false);
				}
			});
		}
	});

	async function checkProjectActiveStatus(retryCount = 0) {
		if (!project) return;
		// Only show loading indicator on the very first check, not on subsequent polls
		const isFirstCheck = isCheckingStatus;
		statusCheckFailed = false;
		try {
			const url = api.listTriggers();
			const response = await authFetch(url, { credentials: 'include' });
			if (response.ok) {
				const data = await response.json();
				const triggers: BackendTrigger[] = data.triggers || [];
				backendTriggers = triggers;
				const myTriggers = triggers.filter((t) => t.projectId === project!.id);
				const hasRunning = myTriggers.some((t) => t.status === 'Running');
				const hasActivating = myTriggers.some((t) => t.status === 'Activating' || t.status === 'SetupPending');
				const hasDeactivating = myTriggers.some((t) => t.status === 'Deactivating');
				isProjectActive = hasRunning || hasActivating || hasDeactivating;
				statusCheckFailed = false;
				// Extract the project hash from the backend for stale detection
				if (isProjectActive && myTriggers.length > 0) {
					const firstHash = myTriggers.find(t => t.projectHash)?.projectHash ?? null;
					backendProjectHash = firstHash;
				} else {
					backendProjectHash = null;
				}
				// Poll while in transitional state until it resolves
				if ((hasActivating || hasDeactivating) && !hasRunning) {
					setTimeout(() => checkProjectActiveStatus(), 2000);
				}
			} else if (response.status === 429 || response.status >= 500) {
				// Rate limited or server error - retry with backoff
				if (retryCount < 3) {
					const delay = Math.pow(2, retryCount) * 1000; // 1s, 2s, 4s
					console.warn(`Status check failed (${response.status}), retrying in ${delay}ms...`);
					await new Promise(r => setTimeout(r, delay));
					return checkProjectActiveStatus(retryCount + 1);
				} else {
					console.error('Status check failed after retries');
					statusCheckFailed = true;
				}
			}
		} catch (err) {
			console.error('Failed to check project status:', err);
			// Network error - retry with backoff
			if (retryCount < 3) {
				const delay = Math.pow(2, retryCount) * 1000;
				console.warn(`Status check network error, retrying in ${delay}ms...`);
				await new Promise(r => setTimeout(r, delay));
				return checkProjectActiveStatus(retryCount + 1);
			} else {
				statusCheckFailed = true;
			}
		} finally {
			if (isFirstCheck) isCheckingStatus = false;
		}
	}

	function showValidationErrors(nodeErrors: Map<string, ValidationError[]>) {
		// Show a toast for each node with errors
		for (const [nodeId, errors] of nodeErrors) {
			const node = project?.nodes.find(n => n.id === nodeId);
			const nodeLabel = node?.label || NODE_TYPE_CONFIG[node?.nodeType || '']?.label || 'Unknown node';
			
			for (const error of errors) {
				const fieldInfo = error.field ? ` (${error.field})` : error.port ? ` (${error.port} port)` : '';
				toast.error(`${nodeLabel}${fieldInfo}`, {
					description: error.message,
					duration: 5000,
				});
			}
		}
	}

	/**
	 * Check if the project uses platform credits and verify the user has sufficient balance.
	 * Returns true if execution can proceed, false if blocked (toast shown).
	 */
	async function checkCreditsOrBlock(nodes: import('$lib/types').NodeInstance[], userId: string): Promise<boolean> {
		const usesPlatformCredits = nodes.some(n => {
			const def = NODE_TYPE_CONFIG[n.nodeType];
			const hasApiKeyField = def?.fields?.some(f => f.type === 'api_key');
			if (!hasApiKeyField) return false;
			const apiKey = n.config?.apiKey;
			return !apiKey || apiKey === '__PLATFORM__';
		});
		if (!usesPlatformCredits || userId === 'local') return true;
		try {
			const creditsRes = await authFetch(api.getCredits(userId));
			if (!creditsRes.ok) {
				toast.error('Unable to verify credits', {
					description: 'Credit verification failed. Retry, or switch nodes to your own API key.',
					duration: 5000,
				});
				return false;
			}
			const creditsData = await creditsRes.json();
			if ((creditsData.balance ?? 0) <= 0) {
				toast.error('Insufficient credits', {
					description: 'Add credits or switch nodes to use your own API key.',
					duration: 5000,
				});
				return false;
			}
		} catch (e) {
			console.warn('[Credits] Credit check failed, blocking:', e);
			toast.error('Unable to verify credits', {
				description: 'Credit verification failed. Retry, or switch nodes to your own API key.',
				duration: 5000,
			});
			return false;
		}
		return true;
	}

	async function toggleProjectActivation() {
		if (!project || isActivating) return;

		// If status check failed, retry the check first instead of blindly activating
		if (statusCheckFailed) {
			await checkProjectActiveStatus();
			return;
		}

		isActivating = true;
		const userId = getUserId();
		const wasActive = isProjectActive;
		if (wasActive) {
			te.trigger.deactivated(project.id);
		} else {
			const triggerType = project.nodes.find(n => triggerNodeTypes.has(n.nodeType))?.nodeType || 'unknown';
			te.trigger.activated(project.id, triggerType);
		}

		try {
			if (wasActive) {
				// Deactivate - stop all triggers for this project by project ID
				const response = await authFetch(api.unregisterProjectTriggers(project.id), {
					method: 'DELETE',
					headers: { 'Content-Type': 'application/json' },
					credentials: 'include',
				});
				if (!response.ok) {
					throw new Error(`Failed to deactivate project: ${response.statusText}`);
				}
				isProjectActive = false;
				// Re-fetch backend triggers so derived state (hasTriggers) updates
				await checkProjectActiveStatus();
				toast.success('Project deactivated');
			} else {
				// Activate - wake all triggers
				const triggerNodes = project.nodes.filter(n => triggerNodeTypes.has(n.nodeType));
				if (triggerNodes.length === 0) {
					toast.error('No trigger nodes found in this project');
					return;
				}
				
				// Validate project before activation
				const validation = validateProject(project.nodes, project.edges);
				if (!validation.valid) {
					showValidationErrors(validation.nodeErrors);
					return;
				}

				// Credit check before activation
				if (!(await checkCreditsOrBlock(project.nodes, userId))) return;

				for (const node of triggerNodes) {
					const triggerId = `${project.id}-${node.id}`;
					const nodeConfig = NODE_TYPE_CONFIG[node.nodeType];
					const triggerCategory = nodeConfig?.features?.triggerCategory;
					
					// Resolve connected config nodes (e.g. EmailConfig -> EmailReceive)
					// so the trigger backend receives the full merged config
					let resolvedConfig: Record<string, unknown> = {};
					for (const edge of project.edges) {
						if (edge.target === node.id && edge.targetHandle === 'config') {
							const sourceNode = project.nodes.find(n => n.id === edge.source);
							if (sourceNode) {
								resolvedConfig = { ...sourceNode.config };
							}
						}
					}
					
					// Include nodeType in config so the backend knows which node to instantiate
					const configWithNodeType = {
						...resolvedConfig,
						...node.config,
						nodeType: node.nodeType,
					};
					
					// Send weftCode, backend compiles and extracts trigger subgraph
					const weftCode = editorRef?.getWeftCode() || project?.weftCode || '';

					const response = await authFetch(api.registerTrigger(), {
						method: 'POST',
						headers: { 'Content-Type': 'application/json' },
						credentials: 'include',
						body: JSON.stringify({
							triggerId,
							triggerCategory,
							projectId: project.id,
							triggerNodeId: node.id,
							config: configWithNodeType,
							credentials: null,
							userId,
							weftCode,
							projectHash: projectHash(project),
						}),
					});
					
					if (!response.ok) {
						const result = await response.json().catch(() => ({}));
						throw new Error(result.message || response.statusText || 'Unknown error');
					}
				}
				
				isProjectActive = true;
				// Re-fetch backend triggers so derived state is accurate
				await checkProjectActiveStatus();
				toast.success('Project activated');
			}
		} catch (err) {
			console.error('Failed to toggle project activation:', err);
			toast.error(`Failed to ${wasActive ? 'deactivate' : 'activate'} project`, {
				description: err instanceof Error ? err.message : String(err)
			});
			// Don't change the state on error - it stays as it was
		} finally {
			isActivating = false;
		}
	}

	async function resyncTrigger() {
		if (!project || isActivating || !isProjectActive) return;
		isActivating = true;
		try {
			// Deactivate
			const response = await authFetch(api.unregisterProjectTriggers(project.id), {
				method: 'DELETE',
				headers: { 'Content-Type': 'application/json' },
				credentials: 'include',
			});
			if (!response.ok) {
				throw new Error(`Failed to deactivate: ${response.statusText}`);
			}
			isProjectActive = false;
		} catch (err) {
			console.error('Resync failed during deactivation:', err);
			toast.error('Resync failed', { description: err instanceof Error ? err.message : String(err) });
			isActivating = false;
			return;
		}
		// Reactivate with current project state
		isActivating = false;
		await toggleProjectActivation();
	}

	// Wrapper to add execution-specific fields to loaded state
	function loadExecutionState(projectId: string) {
		const state = loadExecutionStateUtil(projectId);
		if (state) {
			return {
				isRunning: false,
				activeEdges: new Set<string>(),
				nodeOutputs: state.nodeOutputs,
				nodeStatuses: {},
				nodeExecutions: {},
			};
		}
		return null;
	}

	let executionState = $state<{
		isRunning: boolean;
		isStarting: boolean;
		isStopping: boolean;
		activeEdges: Set<string>;
		nodeOutputs: Record<string, unknown>;
		nodeStatuses: Record<string, string>;
		nodeExecutions: import('$lib/types').NodeExecutionTable;
	}>({
		isRunning: false,
		isStarting: false,
		isStopping: false,
		activeEdges: new Set(),
		nodeOutputs: {},
		nodeStatuses: {},
		nodeExecutions: {},
	});

	async function loadExecutionFromApi(executionId: string) {
		try {
			const response = await authFetch(`/api/executions/${executionId}`);
			if (!response.ok) return null;
			const execution = await response.json();
			
			const nodeStatuses = execution.nodeStatuses || {};
			const nodeOutputs = execution.nodeOutputs || {};
			const nodeExecutions = execution.nodeExecutions || {};

			const isActive = execution.status === 'running' || execution.status === 'waiting_for_input';
			
			return {
				isRunning: isActive,
				isStarting: false,
				isStopping: false,
				activeEdges: new Set<string>(),
				nodeOutputs,
				nodeStatuses: nodeStatuses as Record<string, string>,
				nodeExecutions: nodeExecutions as import('$lib/types').NodeExecutionTable,
			};
		} catch (e) {
			console.error('Failed to load execution from API:', e);
			return null;
		}
	}

	$effect(() => {
		const projectId = $page.params.id;
		const executionIdFromUrl = $page.url.searchParams.get('executionId');
		
		if (projectId && browser) {
			// If executionId is in URL, load that execution's state
			if (executionIdFromUrl) {
				loadExecutionFromApi(executionIdFromUrl).then((loaded) => {
					if (loaded) {
						console.debug('[Execution] Loaded execution state from API:', executionIdFromUrl);
						executionState = loaded;
						currentExecutionId = executionIdFromUrl;
						
						// If execution is still running, start polling (preserve the loaded state)
						if (loaded.isRunning) {
							console.debug('[Execution] Execution is running, starting polling...');
							executeWithBackend(executionIdFromUrl, true);
						}
					}
				});
				return;
			}
			
			// Check if there's a running execution to resume
			const runningId = loadRunningExecution(projectId);
			if (runningId) {
				console.debug('[Execution] Found running execution, resuming polling:', runningId);
				currentExecutionId = runningId;
				resumePolling(runningId);
			} else {
				// Load completed execution state
				const loaded = loadExecutionState(projectId);
				if (loaded) {
					console.debug('[Execution] Loaded previous execution state from localStorage:', loaded);
					executionState = {
						isRunning: false,
						isStarting: false,
						isStopping: false,
						activeEdges: new Set<string>(),
						nodeOutputs: loaded.nodeOutputs,
						nodeStatuses: {},
						nodeExecutions: loaded.nodeExecutions as import('$lib/types').NodeExecutionTable,
					};
				}
			}
		}
	});

	async function resumePolling(executionId: string) {
		if (!project) return;
		
		try {
			const statusRes = await authFetch(api.getStatus(executionId));
			if (statusRes.ok) {
				const status = (await statusRes.json()) as string;
				if (status === 'running' || status === 'waiting_for_input') {
					console.debug('[Execution] Execution still running, resuming...');
					executionState = { ...executionState, isRunning: true };
					await executeWithBackend(executionId);
				} else {
					console.debug('[Execution] Execution already completed:', status);
					clearRunningExecution(project.id);
				}
			} else {
				clearRunningExecution(project.id);
			}
		} catch (e) {
			console.error('[Execution] Failed to check execution status:', e);
			clearRunningExecution(project!.id);
		}
	}

	function handleSave(data: { name?: string; description?: string; weftCode?: string; loomCode?: string; layoutCode?: string }) {
		if (!project) return;
		projects.update(project.id, data);
	}

	async function handleApplyWeftCode(weftCode: string) {
		if (!project) return;
		// Parse to extract name/description and validate
		const parsed = parseWeft(weftCode);
		const w = parsed.projects[0]?.project;
		const name = w?.name || project.name;
		const description = w?.description ?? project.description;
		await projects.update(project.id, { name, description: description ?? undefined, weftCode });
		if (editorRef) {
			const updated = { ...project, nodes: w?.nodes ?? [], edges: w?.edges ?? [], name, description, weftCode };
			await editorRef.patchFromProject(updated);
		} else {
			shouldAutoOrganize = true;
			fitViewAfterOrganize = false;
			editorKey++;
		}
	}

	async function exportProject(stripSensitive: boolean = false) {
		if (!project) return;

		let weftCode = editorRef?.getWeftCode() || project.weftCode || '';
		if (stripSensitive) {
			// Shared strip-at-publish/export/share implementation. See
			// `$lib/ai/sanitize` for the rule set.
			weftCode = stripSensitiveFields(weftCode, project.nodes);
		}
		const loomCode = project.loomCode || undefined;
		const layoutCode = editorRef?.getLayoutCode?.() || project.layoutCode || undefined;

		let testConfigs: Array<{ name: string; description: string; mocks: Record<string, unknown> }> | undefined;
		try {
			const res = await authFetch(`/api/projects/${project.id}/test-configs`);
			if (res.ok) {
				const configs = await res.json();
				testConfigs = configs.map((c: { name: string; description: string; mocks: Record<string, unknown> }) => ({ name: c.name, description: c.description, mocks: c.mocks }));
				if (testConfigs!.length === 0) testConfigs = undefined;
			}
		} catch (e) {
			console.warn('Failed to fetch test configs for export:', e);
		}

		const exported = {
			name: project.name,
			description: project.description,
			weftCode,
			loomCode,
			layoutCode,
			testConfigs,
		};

		const content = JSON.stringify(exported, null, 2);
		const filename = `${project.name.replace(/\s+/g, '_')}.json`;
		const blob = new Blob([content], { type: 'application/json' });
		const url = URL.createObjectURL(blob);
		const a = document.createElement('a');
		a.href = url;
		a.download = filename;
		a.click();
		URL.revokeObjectURL(url);
	}

	async function shareProject() {
		if (!project) return;

		// Same shared sanitizer as export/publish.
		let weftCode = editorRef?.getWeftCode() || project.weftCode || '';
		weftCode = stripSensitiveFields(weftCode, project.nodes);

		const loomCode = project.loomCode || undefined;
		const layoutCode = editorRef?.getLayoutCode?.() || project.layoutCode || undefined;

		let testConfigs: Array<{ name: string; description: string; mocks: Record<string, unknown> }> | undefined;
		try {
			const res = await authFetch(`/api/projects/${project.id}/test-configs`);
			if (res.ok) {
				const configs = await res.json();
				testConfigs = configs.map((c: { name: string; description: string; mocks: Record<string, unknown> }) => ({ name: c.name, description: c.description, mocks: c.mocks }));
				if (testConfigs!.length === 0) testConfigs = undefined;
			}
		} catch (e) {
			console.warn('Failed to fetch test configs for share:', e);
		}

		if (browser && window.parent !== window) {
			window.parent.postMessage({
				type: 'shareProject',
				projectData: {
					projectName: project.name,
					description: project.description,
					weftCode,
					loomCode,
					layoutCode,
					testConfigs,
				},
			}, '*');
		}
	}

	function importProject() {
		const input = document.createElement('input');
		input.type = 'file';
		input.accept = '.json';
		input.onchange = async (e) => {
			const file = (e.target as HTMLInputElement).files?.[0];
			if (!file || !project) return;
			try {
				const text = await file.text();
				const imported = JSON.parse(text);

				// Code-first format: { weftCode, loomCode, name?, description?, testConfigs? }
				if (imported.weftCode) {
					await projects.update(project.id, {
						name: imported.name || project.name,
						description: imported.description ?? project.description,
						weftCode: imported.weftCode,
						loomCode: imported.loomCode,
					});

					if (Array.isArray(imported.testConfigs)) {
						let testConfigsFailed = 0;
						for (const tc of imported.testConfigs) {
							try {
								await authFetch(`/api/projects/${project.id}/test-configs`, {
									method: 'POST',
									headers: { 'Content-Type': 'application/json' },
									body: JSON.stringify({ name: tc.name, description: tc.description, mocks: tc.mocks }),
								});
							} catch (e) {
								console.warn('Failed to import test config:', tc.name, e);
								testConfigsFailed++;
							}
						}
						if (testConfigsFailed > 0) {
							toast.warning(`${testConfigsFailed} test config(s) failed to import`);
						}
					}

					toast.success('Project imported successfully');
					shouldAutoOrganize = true;
					fitViewAfterOrganize = false;
					editorKey++;
				} else {
					toast.error('Unrecognized project format');
				}
			} catch (err: any) {
				toast.error('Failed to import project', {
					description: err instanceof Error ? err.message : String(err)
				});
			}
		};
		input.click();
	}

	let nodeOutputs = $state<Record<string, unknown>>({});

	let currentExecutionId: string | null = null;
	let handledExecutionIds = new Set<string>();  // prevent re-picking completed trigger executions
	let shouldStopPolling = false;
	let isPollingActive = false;

	async function runProject(overrideProject?: ProjectDefinition) {
		if (!project) return;

		// Visitor-preview override: when the runner is in visitor preview
		// mode, it passes a transient clone of the project with the
		// deployer's preview-time field edits applied. We use the override's
		// node configs as the source of truth for this one execution without
		// persisting anything back to the live weft.
		const executionProject = overrideProject ?? project;

		// Prevent running if already running
		if (executionState.isRunning) {
			alert('A project is already running. Please wait for it to complete or stop it first.');
			return;
		}

		// Validate project before running
		const validation = validateProject(executionProject.nodes, executionProject.edges);
		if (!validation.valid) {
			showValidationErrors(validation.nodeErrors);
			return;
		}

		// Credit check: verify balance before execution
		{
			const userId = sessionStorage.getItem('weavemind_user_id') || 'local';
			if (!(await checkCreditsOrBlock(executionProject.nodes, userId))) return;
		}

		// Build the execution's weft code. Start from the live editor's weft
		// (the persisted source of truth). If the runner handed us a preview
		// override, diff its node.config values against the live project and
		// apply each changed field through the same updateNodeConfig helper
		// the editor uses. This gives the backend a complete, valid weft
		// source to compile from without touching the persisted copy.
		let liveWeft = editorRef?.getWeftCode() || project?.weftCode || '';
		if (overrideProject) {
			for (const previewNode of overrideProject.nodes) {
				const liveNode = project.nodes.find(n => n.id === previewNode.id);
				if (!liveNode) continue;
				for (const [key, value] of Object.entries(previewNode.config)) {
					if (liveNode.config[key] === value) continue;
					liveWeft = weftUpdateConfig(liveWeft, previewNode.id, key, value);
				}
			}
		}

		// TODO: Replace with smarter solution (e.g., upload large files separately, stream data)
		// Check total weft code size - Restate has a 10MB message limit
		const MAX_PAYLOAD_SIZE = 9.5 * 1024 * 1024; // 9.5MB to be safe
		const weftCodeForSize = liveWeft;
		const payloadSize = new Blob([weftCodeForSize]).size;
		console.log('[Execution] Weft code size:', (payloadSize / 1024 / 1024).toFixed(2), 'MB');
		if (payloadSize > MAX_PAYLOAD_SIZE) {
			alert(`Project data is too large (${(payloadSize / 1024 / 1024).toFixed(1)}MB). Maximum is 9.5MB total.\n\nPlease reduce the size of file inputs or use shorter audio files.`);
			return;
		}

		// Generate execution ID and start backend execution
		currentExecutionId = crypto.randomUUID();
		shouldStopPolling = false;
		saveRunningExecution(project.id, currentExecutionId);
		te.execution.started(project.id, project.nodes.length, hasInfraInFrontend, hasTriggersInFrontend);

		// Show transition state while backend processes the request
		executionState = { ...executionState, isStarting: true };

		console.debug('[Execution] Starting backend project execution. ID:', currentExecutionId);

		// The executions row is created by weft-api inside the same
		// transaction as the billing event, so no client-side pre-flight
		// is needed. The orchestrator's /start handler triggers it.

		// The executor (axum) is a separate service from the
		// dashboard SvelteKit server and runs its own auth path. It
		// needs an explicit userId in the start payload for credit
		// attribution. We send what the dashboard's auth handshake
		// stashed in sessionStorage. In cloud mode this matches the
		// JWT claim; in local OSS mode it falls back to "local".
		const userId = sessionStorage.getItem('weavemind_user_id') || 'local';

		// Use the (possibly override-patched) weft we prepared above.
		const weftCode = liveWeft;
		if (!weftCode) {
			alert('No weft code available. Please save the project first.');
			return;
		}

		// Prepare payload: backend compiles weftCode, project shell carries metadata only
		const statusCallbackUrl = `${window.location.origin}/api/executions/${currentExecutionId}`;
		const payload = {
			project: {
				id: project.id,
				name: project.name,
				description: project.description,
				nodes: [],
				edges: [],
				createdAt: project.createdAt,
				updatedAt: project.updatedAt,
			},
			weftCode,
			input: {},
			userId: userId,
			statusCallbackUrl: statusCallbackUrl,
			testMode,
			mocks: testMode && selectedTestConfigMocks ? selectedTestConfigMocks : undefined,
		};

		console.debug('[Execution] weftCode:', weftCode.length, 'bytes, target:', api.startExecution(currentExecutionId));

		try {
			// Fire project execution (don't wait for completion)
			const response = await authFetch(api.startExecution(currentExecutionId), {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify(payload),
			});

			if (!response.ok) {
				const errorText = await response.text();
				console.error('[Execution] Backend error:', errorText);
				executionState = { ...executionState, isStarting: false };
				alert(`Failed to start project: ${errorText}`);
				return;
			}

			console.debug('[Execution] Project started on backend, running visual execution...');
			
			// Run visual execution that polls backend for real status
			await executeWithBackend(currentExecutionId);
			
		} catch (error) {
			console.error('[Execution] Failed to start project:', error);
			executionState = { ...executionState, isStarting: false };
			alert(`Failed to start project: ${error}`);
		}
	}

	async function executeWithBackend(executionId: string, preserveState: boolean = false) {
		if (!project || project.nodes.length === 0) return;

		if (isPollingActive) {
			console.debug('[Execution] Polling already active, skipping duplicate');
			return;
		}
		isPollingActive = true;

		if (!preserveState) {
			nodeOutputs = {};
			executionState = {
				isRunning: true,
				isStarting: false,
				isStopping: false,
				activeEdges: new Set(),
				nodeOutputs: {},
				nodeStatuses: {},
				nodeExecutions: {},
			};
		} else {
			executionState = {
				...executionState,
				isRunning: true,
				isStarting: false,
			};
		}

		const allNodeIds = new Set(project.nodes.map(n => n.id));
		let executionStatus = 'running';
		let lastNodeStatuses: Record<string, string> = {};

		// Poll backend for per-node statuses and outputs
		let consecutiveErrors = 0;

		let pollInterval = 1000;

		while ((executionStatus === 'running' || executionStatus === 'waiting_for_input') && !shouldStopPolling) {
			await new Promise(r => setTimeout(r, pollInterval));
			
			if (shouldStopPolling) {
				console.debug('[Execution] Polling stopped');
				break;
			}

			// Pause polling while tab is hidden
			if (typeof document !== 'undefined' && document.hidden) {
				continue;
			}

			try {
				const statusRes = await authFetch(api.getStatus(executionId));
				if (statusRes.ok) {
					executionStatus = (await statusRes.json()) as string;
					console.debug(`[Execution] Project status: ${executionStatus}`);
					consecutiveErrors = 0;
					pollInterval = 1000;
				} else if (statusRes.status === 404) {
					console.warn('[Execution] Execution not found (404), marking completed and stopping poll');
					// Update DB so checkForTriggerExecution doesn't pick this up again
					try {
						await authFetch(`/api/executions/${executionId}`, {
							method: 'POST',
							headers: { 'Content-Type': 'application/json' },
							body: JSON.stringify({ status: 'completed' }),
						});
					} catch (e) {
						console.warn('[Execution] Failed to update execution status in DB:', e);
					}
					executionStatus = 'completed';
					break;
				} else if (statusRes.status === 429) {
					console.warn('[Execution] Rate limited (429), backing off');
					pollInterval = Math.min(pollInterval * 2, 5000);
					consecutiveErrors++;
					continue;
				} else {
					console.warn(`[Execution] Failed to get status: ${statusRes.status} ${statusRes.statusText}`);
					consecutiveErrors++;
				}

				// Get per-node statuses (includes active edges computed from pulses)
				const nodeStatusRes = await authFetch(api.getNodeStatuses(executionId));
				if (!nodeStatusRes.ok) {
					console.warn(`[Execution] Failed to get node statuses: ${nodeStatusRes.status} ${nodeStatusRes.statusText}`);
				} else {
					const nodeStatusData = await nodeStatusRes.json();
					console.debug('[Execution] Node status response:', nodeStatusData);
					const nodeStatuses: Record<string, string> = nodeStatusData.statuses || {};
					const serverActiveEdges: string[] = nodeStatusData.activeEdges || [];

					// Log status changes
					for (const [nodeId, status] of Object.entries(nodeStatuses)) {
						if (lastNodeStatuses[nodeId] !== status) {
							console.debug(`[Execution] Node ${nodeId}: ${lastNodeStatuses[nodeId] || 'Pending'} -> ${status}`);
						}
					}
					lastNodeStatuses = nodeStatuses;

					executionState = {
						isRunning: true,
						isStarting: false,
						isStopping: executionState.isStopping,
						activeEdges: new Set(serverActiveEdges),
						nodeOutputs: executionState.nodeOutputs,
						nodeStatuses,
						nodeExecutions: executionState.nodeExecutions,
					};
				}

				// Get per-node outputs and executions in parallel
				const [outputsRes, execsRes] = await Promise.all([
					authFetch(api.getAllOutputs(executionId)),
					authFetch(api.getNodeExecutions(executionId)),
				]);
				if (outputsRes.ok) {
					const outputsData = await outputsRes.json();
					nodeOutputs = outputsData.outputs || {};
				}
				let nodeExecutions = executionState.nodeExecutions;
				if (execsRes.ok) {
					nodeExecutions = await execsRes.json();
				}
				executionState = {
					...executionState,
					nodeOutputs,
					nodeExecutions,
				};

			} catch (e) {
				console.error('[Execution] Poll failed:', e);
				consecutiveErrors++;
			}

			if (consecutiveErrors >= 10) {
				console.error('[Execution] Too many consecutive errors, stopping poll');
				executionStatus = 'failed';
				break;
			}
		}

		isPollingActive = false;
		console.debug('[Execution] Project finished. Status:', executionStatus);

		// Persist final state to DB (for history view and trigger poller)
		if (executionId && (executionStatus === 'completed' || executionStatus === 'failed')) {
			try {
				await authFetch(`/api/executions/${executionId}`, {
					method: 'POST',
					headers: { 'Content-Type': 'application/json' },
					body: JSON.stringify({
						status: executionStatus,
						nodeStatuses: executionState.nodeStatuses,
						nodeOutputs: nodeOutputs,
						nodeExecutions: executionState.nodeExecutions,
					}),
				});
			} catch (e) {
				console.warn('[Execution] Failed to update execution status in DB:', e);
			}
		}

		executionState = {
			isRunning: false,
			isStarting: false,
			isStopping: false,
			activeEdges: new Set(),
			nodeOutputs: executionStatus === 'cancelled' ? {} : nodeOutputs,
			nodeStatuses: executionStatus === 'cancelled' ? {} : executionState.nodeStatuses,
			nodeExecutions: executionStatus === 'cancelled' ? {} : executionState.nodeExecutions,
		};

		// Reset execution tracking
		currentExecutionId = null;
		shouldStopPolling = false;

		if (project) {
			clearRunningExecution(project.id);
			if (executionStatus !== 'cancelled') {
				saveExecutionStateUtil(project.id, {
					nodeOutputs,
					nodeExecutions: executionState.nodeExecutions,
				});
			}
		}
	}

	async function stopProject() {
		if (!project || !currentExecutionId) return;
		
		executionState = { ...executionState, isStopping: true };
		console.debug('[Execution] Stopping project:', currentExecutionId);
		
		// Cancel the backend project via ProjectExecutor cancel endpoint
		// Backend will update the dashboard DB via statusCallbackUrl
		try {
			const cancelRes = await authFetch(api.cancelExecution(currentExecutionId), {
				method: 'POST',
			});
			if (cancelRes.ok) {
				console.debug('[Execution] Backend project cancel request sent');
			} else {
				console.warn('[Execution] Failed to cancel backend project:', cancelRes.status);
				executionState = { ...executionState, isStopping: false };
			}
		} catch (e) {
			console.error('[Execution] Error cancelling backend project:', e);
			executionState = { ...executionState, isStopping: false };
		}
		// Polling loop will detect "cancelled" status and clean up
	}
</script>

{#if !project && loadingProjectById}
	<div class="flex flex-col items-center justify-center h-screen gap-3">
		<div class="w-6 h-6 border-2 border-zinc-300 border-t-zinc-800 rounded-full animate-spin"></div>
		<p class="text-sm text-zinc-500">Loading project…</p>
	</div>
{:else if !project}
	<div class="flex flex-col items-center justify-center h-screen">
		<h2 class="text-xl font-semibold">Project not found</h2>
		<Button class="mt-4" onclick={() => goto("/dashboard")}>Back to Dashboard</Button>
	</div>
{:else}
	<div class="h-screen w-screen relative flex flex-col">
		{#if viewMode === 'runner'}
			<!-- Runner: own 41px header to match ProjectEditorInner -->
			<div class="flex-shrink-0 flex items-center justify-between px-4 bg-white border-b border-zinc-200 relative z-40" style="height: 41px;">
				<div class="flex items-center gap-2">
					<a href="/dashboard" class="flex items-center justify-center w-6 h-6 rounded hover:bg-zinc-100 text-zinc-500 hover:text-zinc-900 transition-colors" title="Back to Dashboard">
						<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M18 6 6 18"/><path d="m6 6 12 12"/></svg>
					</a>
					<div class="h-4 w-px bg-zinc-200"></div>
					{#if scopeOptions.length > 1}
						<!-- Scope switcher dropdown. Lists the builder project
						     and every deployment cloned from it. Clicking
						     navigates to that scope's project page. Avoids
						     the Manage Deployments round-trip for every
						     flip between builder and deployment. -->
						<div class="relative">
							<button
								type="button"
								class="group text-sm font-semibold text-zinc-800 inline-flex items-center gap-2 px-2.5 py-1 rounded-md border border-zinc-200 hover:border-violet-400 hover:bg-violet-50 transition-colors"
								onclick={() => (scopeMenuOpen = !scopeMenuOpen)}
								title="Switch between the builder project and its deployments"
							>
								<!-- Pulsing dot: draws the eye to the switcher when
								     there are alternate scopes to jump to. Removed
								     once the menu is open so it doesn't keep
								     blinking in the user's face. -->
								{#if !scopeMenuOpen}
									<span class="relative flex h-2 w-2 flex-shrink-0">
										<span class="animate-ping absolute inline-flex h-full w-full rounded-full bg-violet-400 opacity-75"></span>
										<span class="relative inline-flex rounded-full h-2 w-2 bg-violet-500"></span>
									</span>
								{/if}
								<span class="truncate max-w-[18rem]">{project.name}</span>
								{#if project.isDeployment}
									<span class="text-[9px] font-semibold px-1.5 py-0.5 rounded bg-violet-100 text-violet-700 uppercase tracking-wider">Deployment</span>
								{/if}
								<svg class="text-zinc-500 group-hover:text-violet-600 transition-colors {scopeMenuOpen ? 'rotate-180' : ''}" xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><path d="m6 9 6 6 6-6"/></svg>
							</button>
							{#if scopeMenuOpen}
								<!-- svelte-ignore a11y_click_events_have_key_events -->
								<!-- svelte-ignore a11y_no_static_element_interactions -->
								<div class="fixed inset-0 z-40" onclick={() => (scopeMenuOpen = false)}></div>
								<div class="absolute left-0 top-full mt-1 w-80 bg-white border border-zinc-200 rounded-lg shadow-xl z-50 overflow-hidden">
									<div class="px-3 py-2 text-[10px] font-semibold uppercase tracking-wider text-zinc-500 bg-zinc-50 border-b border-zinc-200">
										Switch scope
									</div>
									{#each scopeOptions as opt (opt.projectId)}
										{@const isCurrent = opt.projectId === project.id}
										<button
											type="button"
											class="w-full text-left px-3 py-2.5 text-xs hover:bg-violet-50 transition-colors flex items-center justify-between gap-2 {isCurrent ? 'bg-zinc-50' : ''}"
											onclick={() => switchScope(opt.projectId)}
										>
											<div class="min-w-0 flex-1">
												<div class="font-medium text-zinc-800 truncate">{opt.label}</div>
												<div class="text-[10px] text-zinc-500 uppercase tracking-wider mt-0.5">
													{opt.kind === 'builder' ? 'Builder project' : 'Deployment'}
												</div>
											</div>
											{#if isCurrent}
												<span class="text-[9px] font-semibold px-1.5 py-0.5 rounded bg-violet-100 text-violet-700 uppercase tracking-wider flex-shrink-0">Current</span>
											{/if}
										</button>
									{/each}
								</div>
							{/if}
						</div>
					{:else}
						<span class="text-sm font-semibold text-zinc-800">{project.name}</span>
						{#if project.isDeployment}
							<span class="text-[9px] font-semibold px-1.5 py-0.5 rounded bg-violet-100 text-violet-700 uppercase tracking-wider">Deployment</span>
						{/if}
					{/if}
					{#if isProjectActive}
						<div class="flex items-center gap-1.5 px-2 py-0.5 bg-emerald-50 border border-emerald-200 rounded-full">
							<span class="flex h-1.5 w-1.5 relative">
								<span class="animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-75"></span>
								<span class="relative inline-flex rounded-full h-1.5 w-1.5 bg-emerald-500"></span>
							</span>
							<span class="text-[10px] font-medium text-emerald-700 uppercase tracking-wide">Active</span>
						</div>
					{/if}
					{#if executionState.isRunning}
						<div class="flex items-center gap-1.5 px-2 py-0.5 bg-orange-50 border border-orange-200 rounded-full">
							<span class="flex h-1.5 w-1.5 relative">
								<span class="animate-ping absolute inline-flex h-full w-full rounded-full bg-orange-400 opacity-75"></span>
								<span class="relative inline-flex rounded-full h-1.5 w-1.5 bg-orange-500"></span>
							</span>
							<span class="text-[10px] font-medium text-orange-700 uppercase tracking-wide">Running</span>
						</div>
					{/if}
				</div>
				{#if project.isDeployment}
					<!-- Deployment scope: builder view is locked. The toggle
					     is replaced with a static badge explaining why, so
					     the admin understands they can't edit code here.
					     To modify code, they go back to the origin builder
					     project and re-publish. -->
					<div class="inline-flex items-center gap-1.5 text-[11px] px-2.5 py-1 rounded border border-violet-200 bg-violet-50 text-violet-800" title="This is a deployed snapshot. To edit the code, open the origin builder project and re-publish.">
						<span class="flex h-1.5 w-1.5 rounded-full bg-violet-500"></span>
						Deployment: builder locked
					</div>
				{:else}
					<div class="inline-flex rounded border border-zinc-200 overflow-hidden">
						<button
							class="text-[11px] px-2.5 py-1 font-medium transition-colors bg-white text-zinc-500 hover:text-zinc-800 hover:bg-zinc-50"
							onclick={() => setViewMode('builder')}
						>Builder</button>
						<button
							class="text-[11px] px-2.5 py-1 font-medium border-l border-zinc-200 transition-colors bg-zinc-900 text-white"
							onclick={() => setViewMode('runner')}
						>Runner</button>
					</div>
				{/if}
			</div>
			<div class="flex-1 relative overflow-hidden">
				<RunnerView
					{project}
					onUpdateNodeConfig={handleRunnerUpdateNodeConfig}
					triggerState={{ hasTriggers, isActive: isProjectActive, isLoading: isCheckingStatus || isActivating, isStale: triggerStale }}
					onToggleTrigger={toggleProjectActivation}
					onResyncTrigger={resyncTrigger}
					infraState={{ hasInfrastructure, hasInfraInFrontend, hasInfraInBackend, status: infraStatus, nodes: infraNodes, isLoading: isInfraLoading }}
					onCheckInfraStatus={() => checkInfraStatus()}
					onStartInfra={requestStartInfra}
					onStopInfra={stopInfrastructure}
					onTerminateInfra={terminateInfrastructure}
					onForceRetry={forceRetryInfra}
					onRun={runProject}
					onStop={stopProject}
					{executionState}
					{infraLiveData}
					{testMode}
					onConfigureRunner={() => (showLoomEditor = true)}
					onPublish={() => (showPublishModalFromRunner = true)}
					{hasPublications}
					isLiveDeployment={!!project?.isDeployment}
					publicUrl={deploymentPublicUrl ?? undefined}
				/>
			</div>
		{/if}
			<!-- Always mount ProjectEditor so AI can edit weft code even in runner mode -->
			<div class="flex-1 relative overflow-hidden" class:hidden={viewMode === 'runner'}>
				{#key editorKey}
					<ProjectEditor
						bind:this={editorRef}
						{project}
						onSave={handleSave}
						onRun={runProject}
						onStop={stopProject}
						{executionState}
						triggerState={{ hasTriggers, hasTriggersInFrontend, hasTriggersInBackend, isActive: isProjectActive, isLoading: isCheckingStatus || isActivating, hasError: statusCheckFailed, isStale: triggerStale }}
						onToggleTrigger={toggleProjectActivation}
						onResyncTrigger={resyncTrigger}
						infraState={{ hasInfrastructure, hasInfraInFrontend, hasInfraInBackend, infraDiverged, status: infraStatus, nodes: infraNodes, isLoading: isInfraLoading }}
						onCheckInfraStatus={() => checkInfraStatus()}
						onStartInfra={requestStartInfra}
						onStopInfra={stopInfrastructure}
						onTerminateInfra={terminateInfrastructure}
						onForceRetry={forceRetryInfra}
						autoOrganizeOnMount={shouldAutoOrganize}
						{fitViewAfterOrganize}
						onExport={exportProject}
						onImport={importProject}
						{...(browser && window.parent !== window ? { onShare: shareProject } : {})}
						{viewMode}
						onSetViewMode={setViewMode}
						onPublish={() => (showPublishModalFromRunner = true)}
						{hasPublications}
						{infraLiveData}
						structuralLock={aiChatActive}
						{testMode}
						onOpenTestConfig={() => { showTestConfigModal = true; }}
					/>
				{/key}
			</div>
	</div>

	<ReconciliationDialog
		bind:open={showReconciliationDialog}
		plan={reconciliationPlan}
		onConfirm={confirmReconciliation}
		onCancel={cancelReconciliation}
	/>

	<LoomEditorModal
		bind:open={showLoomEditor}
		{project}
		onSave={handleSaveLoom}
	/>
	{#if showPublishModalFromRunner && project}
		<PublishProjectModal
			projectData={{ projectId: project.id, projectName: project.name, description: project.description }}
			sensitiveValueCount={countSensitiveValues(project.nodes)}
			onPublishNew={publishWithSanitize}
			onOverwrite={overwriteWithSanitize}
			onClose={(result) => {
				showPublishModalFromRunner = false;
				if (result?.deletedCurrentProject) {
					// The user just unpublished the deployment they were
					// viewing. The deployment's project row is gone, so
					// navigate to the origin builder project if we know it,
					// otherwise fall back to the dashboard.
					const origin = project?.originProjectId;
					if (origin) {
						window.location.href = `/projects/${origin}`;
					} else {
						goto('/dashboard');
					}
					return;
				}
				refreshPublications();
				if (result?.slug) {
					toast.success(`Published at /p/${result.slug}`);
				}
			}}
		/>
	{/if}
	{#if project}
		<TestConfigModal
			bind:open={showTestConfigModal}
			projectId={project.id}
			projectNodes={project.nodes.map(n => ({
				id: n.id,
				nodeType: n.nodeType,
				label: n.label,
				outputs: n.outputs ?? [],
				isTrigger: n.features?.isTrigger,
				isInfrastructure: n.features?.isInfrastructure,
			}))}
			selectedConfigId={selectedTestConfigId}
			onSelect={selectTestConfig}
		/>
	{/if}
{/if}
