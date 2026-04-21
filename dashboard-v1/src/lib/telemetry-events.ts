/**
 * Pre-defined telemetry event helpers.
 * Each function emits a structured event via the telemetry bridge.
 */

import { emit } from './telemetry';

// ── Navigation ──
export const nav = {
	pageView: (page: string) => emit('page_view', { page }),
	projectOpened: (projectId: string, nodeCount: number, edgeCount: number) =>
		emit('project_opened', { projectId, nodeCount, edgeCount }),
	projectClosed: (projectId: string, durationMs: number) =>
		emit('project_closed', { projectId, durationMs }),
};

// ── Project lifecycle ──
export const project = {
	created: (projectId: string, source: 'blank' | 'ai' | 'clone' | 'template' | 'import') =>
		emit('project_created', { projectId, source }),
	deleted: (projectId: string) =>
		emit('project_deleted', { projectId }),
	renamed: (projectId: string) =>
		emit('project_renamed', { projectId }),
	imported: (projectId: string) =>
		emit('project_imported', { projectId }),
	exported: (projectId: string, format: string) =>
		emit('project_exported', { projectId, format }),
	shared: (projectId: string) =>
		emit('project_shared', { projectId }),
};

// ── Graph editing ──
export const editor = {
	nodePlaced: (nodeType: string, source: 'palette' | 'context_menu' | 'ai' | 'paste') =>
		emit('node_placed', { nodeType, source }),
	nodeDeleted: (nodeType: string, count: number) =>
		emit('node_deleted', { nodeType, count }),
	nodeDuplicated: (nodeType: string) =>
		emit('node_duplicated', { nodeType }),
	nodeConfigChanged: (nodeType: string, field: string) =>
		emit('node_config_changed', { nodeType, field }),
	connectionCreated: (sourceType: string, targetType: string) =>
		emit('connection_created', { sourceType, targetType }),
	connectionDeleted: () =>
		emit('connection_deleted', {}),
	connectionFailed: (reason: string) =>
		emit('connection_failed', { reason }),
	groupCreated: () =>
		emit('group_created', {}),
	nodeMovedToGroup: (nodeType: string) =>
		emit('node_moved_to_group', { nodeType }),
	nodeMovedOutOfGroup: (nodeType: string) =>
		emit('node_moved_out_of_group', { nodeType }),
	portAdded: (side: 'input' | 'output', nodeType: string) =>
		emit('port_added', { side, nodeType }),
	portRemoved: (side: 'input' | 'output', nodeType: string) =>
		emit('port_removed', { side, nodeType }),
	undo: () => emit('undo', {}),
	redo: () => emit('redo', {}),
	autoOrganize: () => emit('auto_organize', {}),
};

// ── View controls ──
export const view = {
	codePanelToggled: (open: boolean) =>
		emit('code_panel_toggled', { open }),
	codePanelMaximized: (maximized: boolean) =>
		emit('code_panel_maximized', { maximized }),
	viewModeChanged: (mode: 'builder' | 'runner') =>
		emit('view_mode_changed', { mode }),
	testModeToggled: (enabled: boolean) =>
		emit('test_mode_toggled', { enabled }),
	sidebarTabChanged: (tab: string) =>
		emit('sidebar_tab_changed', { tab }),
	fitView: () => emit('fit_view', {}),
};

// ── Command palette ──
export const palette = {
	opened: () => emit('command_palette_opened', {}),
	searched: (query: string, resultCount: number) =>
		emit('command_palette_searched', { queryLength: query.length, resultCount }),
	nodeSelected: (nodeType: string) =>
		emit('command_palette_node_selected', { nodeType }),
	actionSelected: (actionId: string) =>
		emit('command_palette_action_selected', { actionId }),
};

// ── Execution ──
export const execution = {
	started: (projectId: string, nodeCount: number, hasInfra: boolean, hasTrigger: boolean) =>
		emit('execution_started', { projectId, nodeCount, hasInfra, hasTrigger }),
	succeeded: (projectId: string, durationMs: number, nodesRun: number) =>
		emit('execution_succeeded', { projectId, durationMs, nodesRun }),
	failed: (projectId: string, errorType: string) =>
		emit('execution_failed', { projectId, errorType }),
	cancelled: (projectId: string) =>
		emit('execution_cancelled', { projectId }),
};

// ── Infrastructure ──
export const infra = {
	started: (projectId: string) =>
		emit('infra_started', { projectId }),
	stopped: (projectId: string) =>
		emit('infra_stopped', { projectId }),
	terminated: (projectId: string) =>
		emit('infra_terminated', { projectId }),
};

// ── Triggers ──
export const trigger = {
	activated: (projectId: string, triggerType: string) =>
		emit('trigger_activated', { projectId, triggerType }),
	deactivated: (projectId: string) =>
		emit('trigger_deactivated', { projectId }),
	resynced: (projectId: string) =>
		emit('trigger_resynced', { projectId }),
};

// ── Weft code ──
export const code = {
	edited: (changeSize: number) =>
		emit('weft_code_edited', { changeSize }),
	syntaxError: (errorCount: number) =>
		emit('weft_syntax_error', { errorCount }),
};

// ── AI / Tangle ──
export const ai = {
	promptSent: (promptLength: number) =>
		emit('ai_prompt_sent', { promptLength }),
	suggestionApplied: (nodesAdded: number) =>
		emit('ai_suggestion_applied', { nodesAdded }),
	micStarted: () => emit('ai_mic_started', {}),
	micStopped: () => emit('ai_mic_stopped', {}),
};

// ── Tutorial ──
export const tutorial = {
	started: () => emit('tutorial_started', {}),
	stepReached: (step: number, totalSteps: number) =>
		emit('tutorial_step', { step, totalSteps }),
	completed: () => emit('tutorial_completed', {}),
	skipped: (atStep: number, totalSteps: number) =>
		emit('tutorial_skipped', { atStep, totalSteps }),
};

// ── Welcome ──
export const welcome = {
	shown: () => emit('welcome_shown', {}),
	dismissed: () => emit('welcome_dismissed', {}),
	discordClicked: () => emit('welcome_discord_clicked', {}),
};

// ── History / Versions ──
export const history = {
	versionSaved: (type: 'manual' | 'auto') =>
		emit('version_saved', { type }),
	versionRestored: () => emit('version_restored', {}),
	versionDeleted: () => emit('version_deleted', {}),
};

// ── Human-in-the-loop / Forms ──
export const form = {
	opened: (taskType: string) =>
		emit('form_opened', { taskType }),
	fieldFilled: (fieldType: string) =>
		emit('form_field_filled', { fieldType }),
	submitted: (taskType: string, fieldCount: number) =>
		emit('form_submitted', { taskType, fieldCount }),
};

// ── Files ──
export const files = {
	uploaded: (mimeType: string, sizeBytes: number) =>
		emit('file_uploaded', { mimeType, sizeBytes }),
	deleted: () => emit('file_deleted', {}),
};

// ── Community ──
export const community = {
	searched: (query: string) =>
		emit('community_searched', { queryLength: query.length }),
	projectViewed: (projectId: string) =>
		emit('community_project_viewed', { projectId }),
	projectCloned: (projectId: string) =>
		emit('community_project_cloned', { projectId }),
	liked: (projectId: string) =>
		emit('community_liked', { projectId }),
	commented: (projectId: string) =>
		emit('community_commented', { projectId }),
};
