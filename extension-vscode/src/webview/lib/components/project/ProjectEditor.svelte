<script lang="ts">
	import { SvelteFlowProvider } from "@xyflow/svelte";
	import ProjectEditorInner from "./ProjectEditorInner.svelte";
	import type { ValidationError } from "$lib/types";

	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	let inner: any = $state();

	/// Flush every pending debounced save. Called by App.svelte
	/// before posting runProject so the host sees the user's
	/// freshest edits in the build.
	export function flushAllPendingSaves(): void {
		inner?.flushAllPendingSaves();
	}

	export function applyExternalSource(weftCode: string, layoutCode: string): void {
		inner?.applyExternalSource(weftCode, layoutCode);
	}

	let {
		project,
		onSave,
		onRun,
		onStop,
		onDismissError,
		onActivate,
		onCancelActivate,
		onDeactivate,
		onReactivate,
		onCancelRunning,
		onResumeActive,
		onResync,
		onStartInfra,
		onStopInfra,
		onTerminateInfra,
		onUpgradeInfra,
		actionBarState,
		drift,
		infraNodes,
		hasInfraInGraph = false,
		hasTriggersInGraph = false,
		executionState,
		validationErrors,
		autoOrganizeOnMount = false,
		fitViewAfterOrganize = false,
		infraFeedByNode,
		signalFeedByNode,
		structuralLock = false,
	}: {
		project: ProjectDefinition;
		onSave: (data: { name?: string; description?: string; weftCode?: string }) => void;
		onRun?: () => void;
		onStop?: () => void;
		onDismissError?: () => void;
		onActivate?: () => void;
		onCancelActivate?: () => void;
		onDeactivate?: () => void;
		onReactivate?: () => void;
		onCancelRunning?: () => void;
		onResumeActive?: () => void;
		onResync?: () => void;
		onStartInfra?: () => void;
		onStopInfra?: () => void;
		onTerminateInfra?: () => void;
		onUpgradeInfra?: () => void;
		actionBarState: import('../../../../shared/protocol').ActionBarState;
		drift: import('../../../../shared/protocol').ActionAvailability | undefined;
		infraNodes?: Array<{ nodeId: string; nodeType: string; status: string }>;
		hasInfraInGraph?: boolean;
		hasTriggersInGraph?: boolean;
		executionState?: {
			isRunning: boolean;
			nodeOutputs: Record<string, unknown>;
			nodeStatuses: Record<string, string>;
			nodeExecutions: import('$lib/types').NodeExecutionTable;
		};
		validationErrors?: Map<string, ValidationError[]>;
		autoOrganizeOnMount?: boolean;
		fitViewAfterOrganize?: boolean;
		/// Per-node sidecar /live tick state. Only consumed for nodes
		/// with `requiresInfra: true`.
		infraFeedByNode?: Record<string, import('../../../../shared/protocol').NodeFeedState>;
		/// Per-node listener /display tick state. Only consumed for
		/// nodes with `features.isTrigger`.
		signalFeedByNode?: Record<string, import('../../../../shared/protocol').NodeFeedState>;
		structuralLock?: boolean;
	} = $props();
</script>

<SvelteFlowProvider>
	<ProjectEditorInner
		bind:this={inner}
		{project}
		{onSave}
		{onRun}
		{onStop}
		{onDismissError}
		{onActivate}
		{onCancelActivate}
		{onDeactivate}
		{onReactivate}
		{onCancelRunning}
		{onResumeActive}
		{onResync}
		{onStartInfra}
		{onStopInfra}
		{onTerminateInfra}
		{onUpgradeInfra}
		{actionBarState}
		{drift}
		{infraNodes}
		{hasInfraInGraph}
		{hasTriggersInGraph}
		{executionState}
		{validationErrors}
		{autoOrganizeOnMount}
		{fitViewAfterOrganize}
		{infraFeedByNode}
		{signalFeedByNode}
		{structuralLock}
	/>
</SvelteFlowProvider>
