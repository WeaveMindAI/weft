<script lang="ts">
	import { SvelteFlowProvider } from "@xyflow/svelte";
	import ProjectEditorInner from "./ProjectEditorInner.svelte";
	import type { ProjectDefinition, ValidationError } from "$lib/types";
	import type { WeftParseError, WeftWarning, OpaqueBlock } from "$lib/ai/weft-parser";

	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	let inner: any = $state();

	export function patchFromProject(newProject: ProjectDefinition): Promise<void> {
		return inner?.patchFromProject(newProject) ?? Promise.resolve();
	}

	export function weftStreamStart(mode: 'weft' | 'weft-patch' | 'weft-continue') {
		inner?.weftStreamStart(mode);
	}

	export function weftStreamDelta(delta: string, mode: 'weft' | 'weft-patch' | 'weft-continue', at?: number) {
		inner?.weftStreamDelta(delta, mode, at);
	}


	export function weftStreamPatchSearch(searchText: string): { insertAt: number } | { error: string } {
		return inner?.weftStreamPatchSearch(searchText) ?? { error: 'Editor not ready' };
	}

	export async function weftStreamEnd(): Promise<{ errors: WeftParseError[]; warnings: WeftWarning[]; opaqueBlocks: OpaqueBlock[] }> {
		return (await inner?.weftStreamEnd()) ?? { errors: [], warnings: [], opaqueBlocks: [] };
	}

	export function getWeftCode(): string {
		return inner?.getWeftCode() ?? '';
	}

	export function getRawWeftCode(): string {
		return inner?.getRawWeftCode() ?? '';
	}

	export function isStreaming(): boolean {
		return inner?.isStreaming() ?? false;
	}

	export function updateNodeConfigs(configUpdates: Array<{ nodeId: string; fieldKey: string; value: unknown }>) {
		inner?.updateNodeConfigs(configUpdates);
	}

	let { project, onSave, onRun, onStop, executionState, triggerState, onToggleTrigger, onResyncTrigger, infraState, onCheckInfraStatus, onStartInfra, onStopInfra, onTerminateInfra, onForceRetry, validationErrors, autoOrganizeOnMount = false, fitViewAfterOrganize = false, onExport, onImport, onShare, viewMode, onSetViewMode, onPublish, hasPublications = false, infraLiveData, structuralLock = false, testMode = false, onOpenTestConfig, playground = false }: {
		project: ProjectDefinition; 
		onSave: (data: { name?: string; description?: string; weftCode?: string; loomCode?: string }) => void;
		onRun?: () => void;
		onStop?: () => void;
		executionState?: {
			isRunning: boolean;
			activeEdges: Set<string>;
			nodeOutputs: Record<string, unknown>;
			nodeStatuses: Record<string, string>;
			nodeExecutions: import('$lib/types').NodeExecutionTable;
		};
		triggerState?: {
			hasTriggers: boolean;
			hasTriggersInFrontend: boolean;
			hasTriggersInBackend: boolean;
			isActive: boolean;
			isLoading: boolean;
			hasError?: boolean;
			isStale?: boolean;
		};
		onToggleTrigger?: () => void;
		onResyncTrigger?: () => void;
		infraState?: {
			hasInfrastructure: boolean;
			hasInfraInFrontend: boolean;
			hasInfraInBackend: boolean;
			infraDiverged: boolean;
			status: string;
			nodes: Array<{ nodeId: string; nodeType: string; instanceId: string; status: string; backend?: string }>;
			isLoading: boolean;
		};
		onCheckInfraStatus?: () => void;
		onStartInfra?: () => void;
		onStopInfra?: () => void;
		onTerminateInfra?: () => void;
		onForceRetry?: () => void;
		validationErrors?: Map<string, ValidationError[]>;
		autoOrganizeOnMount?: boolean;
		fitViewAfterOrganize?: boolean;
		onExport?: (stripSensitive: boolean) => void;
		onImport?: () => void;
		onShare?: () => void;
		viewMode?: 'builder' | 'runner';
		onSetViewMode?: (mode: 'builder' | 'runner') => void;
		onPublish?: () => void;
		hasPublications?: boolean;
		infraLiveData?: Record<string, import('$lib/types').LiveDataItem[]>;
		structuralLock?: boolean;
		testMode?: boolean;
		onOpenTestConfig?: () => void;
		playground?: boolean;
	} = $props();
</script>

<SvelteFlowProvider>
	<ProjectEditorInner bind:this={inner} {project} {onSave} {onRun} {onStop} {executionState} {triggerState} {onToggleTrigger} {onResyncTrigger} {infraState} {onCheckInfraStatus} {onStartInfra} {onStopInfra} {onTerminateInfra} {onForceRetry} {validationErrors} {autoOrganizeOnMount} {fitViewAfterOrganize} {onExport} {...(onImport ? { onImport } : {})} {...(onShare ? { onShare } : {})} {...(onSetViewMode ? { viewMode, onSetViewMode } : {})} {...(onPublish ? { onPublish } : {})} {hasPublications} {infraLiveData} {structuralLock} {testMode} {...(onOpenTestConfig ? { onOpenTestConfig } : {})} {playground} />
</SvelteFlowProvider>
