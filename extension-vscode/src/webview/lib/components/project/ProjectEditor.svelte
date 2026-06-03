<script lang="ts">
	import { SvelteFlowProvider } from "@xyflow/svelte";
	import ProjectEditorInner from "./ProjectEditorInner.svelte";
	import type { ProjectDefinition } from "$lib/types";

	// The bound inner instance's public API this wrapper forwards. Typed (not
	// `any`) so a signature drift between wrapper and inner is a compile error,
	// the arity bug this interface was added to prevent.
	interface InnerApi {
		flushAllPendingSaves(): void;
		applyExternalSource(project: ProjectDefinition, weftCode: string, layoutCode: string): void;
	}
	let inner: InnerApi | undefined = $state();

	/// Flush every pending debounced save. Called by App.svelte
	/// before posting runProject so the host sees the user's
	/// freshest edits in the build.
	export function flushAllPendingSaves(): void {
		inner?.flushAllPendingSaves();
	}

	export function applyExternalSource(project: ProjectDefinition, weftCode: string, layoutCode: string): void {
		inner?.applyExternalSource(project, weftCode, layoutCode);
	}

	let {
		project,
		onSave,
		onApplyEdits,
		onApplyTextEdit,
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
		onInfraNodeStop,
		onInfraNodeTerminate,
		onUpgradeInfra,
		actionBarState,
		drift,
		infraNodes,
		hasInfraInGraph = false,
		hasTriggersInGraph = false,
		executionState,
		autoOrganizeOnMount = false,
		infraFeedByNode,
		signalFeedByNode,
		structuralLock = false,
		onOpenInclude = () => {},
		execPrefix = '',
		fileContents = {},
	}: {
		project: ProjectDefinition;
		onSave: (data: { layoutCode?: string; fileRef?: { path: string; content: string } }) => void;
		onApplyEdits: (ops: import('../../../../shared/protocol').EditOp[]) => Promise<import('../../../../shared/protocol').TextEdit | null>;
		onApplyTextEdit: (edit: import('../../../../shared/protocol').TextEdit) => Promise<import('../../../../shared/protocol').TextEdit | null>;
		onOpenInclude?: (path: string, alias: string) => void;
		execPrefix?: string;
		fileContents?: Record<string, import('../../../../shared/protocol').FileContent>;
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
		/// Per-node lifecycle, used by the graph's right-click menu
		/// on a single infra node. The parent dispatches the HTTP
		/// call through the extension host's CLI verb path.
		onInfraNodeStop?: (nodeId: string) => void;
		onInfraNodeTerminate?: (nodeId: string) => void;
		onUpgradeInfra?: () => void;
		actionBarState: import('../../../../shared/protocol').ActionBarState;
		drift: import('../../../../shared/protocol').ActionAvailability | undefined;
		infraNodes?: Array<{ nodeId: string; nodeType: string; status: string; failureStage?: string; failureMessage?: string }>;
		hasInfraInGraph?: boolean;
		hasTriggersInGraph?: boolean;
		executionState?: {
			isRunning: boolean;
			nodeOutputs: Record<string, unknown>;
			nodeStatuses: Record<string, string>;
			nodeExecutions: import('$lib/types').NodeExecutionTable;
		};
		autoOrganizeOnMount?: boolean;
		/// Per-node infra /live tick state. Only consumed for nodes
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
		{onApplyEdits}
		{onApplyTextEdit}
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
		{onInfraNodeStop}
		{onInfraNodeTerminate}
		{onUpgradeInfra}
		{actionBarState}
		{drift}
		{infraNodes}
		{hasInfraInGraph}
		{hasTriggersInGraph}
		{executionState}
		{autoOrganizeOnMount}
		{infraFeedByNode}
		{signalFeedByNode}
		{structuralLock}
		{onOpenInclude}
		{execPrefix}
		{fileContents}
	/>
</SvelteFlowProvider>
