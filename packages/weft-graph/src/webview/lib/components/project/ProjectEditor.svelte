<script lang="ts">
	import { SvelteFlowProvider } from "@xyflow/svelte";
	import ProjectEditorInner from "./ProjectEditorInner.svelte";
	import type { ProjectDefinition } from "../../types";
	import type { SourceLocation } from "../../../../protocol";

	// The bound inner instance's public API this wrapper forwards. Typed (not
	// `any`) so a signature drift between wrapper and inner is a compile error,
	// the arity bug this interface was added to prevent.
	interface InnerApi {
		flushAllPendingSaves(): void;
		applyExternalSource(project: ProjectDefinition, weftCode: string, layoutCode: string): void;
		setCodeEditTouched(): void;
		setGraphLogicLock(locked: boolean, reason?: string): void;
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

	/// An external change landed on the watched `.weft` doc: slide the editor's
	/// 1s auto-lock forward (source-mutating graph gestures pause).
	export function setCodeEditTouched(): void {
		inner?.setCodeEditTouched();
	}

	/// Engage/release the explicit graph-logic lock (AI assistant, UI toggle).
	export function setGraphLogicLock(locked: boolean, reason?: string): void {
		inner?.setGraphLogicLock(locked, reason);
	}

	let {
		project,
		onSave,
		onPersistLayout,
		onApplyEdits,
		onApplyTextEdit,
		onResyncSource,
		onRun,
		onStop,
		onDismissError,
		onOpenLocation,
		onActivate,
		onCancelActivate,
		onCancelBuild,
		onCancelInfra,
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
		onOpenInclude = () => {},
		execPrefix = '',
		fileContents = {},
	}: {
		project: ProjectDefinition;
		onSave: (data: { fileRef?: { path: string; content: string } }) => void;
		onPersistLayout: (layoutCode: string) => Promise<void>;
		onApplyEdits: (ops: import('../../../../protocol').EditOp[]) => Promise<import('../../projection/types').EditRpcResult>;
		onApplyTextEdit: (edit: import('../../../../protocol').TextEdit) => Promise<import('../../projection/types').EditRpcResult>;
		onResyncSource: () => Promise<{ project: ProjectDefinition; weftCode: string } | null>;
		onOpenInclude?: (path: string, alias: string) => void;
		execPrefix?: string;
		fileContents?: Record<string, import('../../../../protocol').FileContent>;
		onRun?: (targets: string[]) => void;
		onStop?: () => void;
		onDismissError?: () => void;
		onOpenLocation: (location: SourceLocation) => void;
		onActivate?: () => void;
		onCancelActivate?: () => void;
		onCancelBuild?: () => void;
		onCancelInfra?: () => void;
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
		actionBarState: import('../../../../protocol').ActionBarState;
		drift: import('../../../../protocol').ActionAvailability | undefined;
		infraNodes?: Array<{ nodeId: string; nodeType: string; status: string; failureStage?: string; failureMessage?: string }>;
		hasInfraInGraph?: boolean;
		hasTriggersInGraph?: boolean;
		executionState?: import('../../types').ExecutionState;
		autoOrganizeOnMount?: boolean;
		/// Per-node infra /live tick state. Only consumed for nodes
		/// with `requiresInfra: true`.
		infraFeedByNode?: Record<string, import('../../../../protocol').NodeFeedState>;
		/// Per-node listener /display tick state. Only consumed for
		/// nodes with `features.isTrigger`.
		signalFeedByNode?: Record<string, import('../../../../protocol').NodeFeedState>;
	} = $props();
</script>

<SvelteFlowProvider>
	<ProjectEditorInner
		bind:this={inner}
		{project}
		{onSave}
		{onPersistLayout}
		{onApplyEdits}
		{onApplyTextEdit}
		{onResyncSource}
		{onRun}
		{onStop}
		{onDismissError}
		{onOpenLocation}
		{onActivate}
		{onCancelActivate}
		{onCancelBuild}
		{onCancelInfra}
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
		{onOpenInclude}
		{execPrefix}
		{fileContents}
	/>
</SvelteFlowProvider>
