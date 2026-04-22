import { clsx, type ClassValue } from "clsx";
import { twMerge } from "tailwind-merge";
import { browser } from "$app/environment";

export function cn(...inputs: ClassValue[]) {
	return twMerge(clsx(inputs));
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type WithoutChild<T> = T extends { child?: any } ? Omit<T, "child"> : T;
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export type WithoutChildren<T> = T extends { children?: any } ? Omit<T, "children"> : T;
export type WithoutChildrenOrChild<T> = WithoutChildren<WithoutChild<T>>;
export type WithElementRef<T, U extends HTMLElement = HTMLElement> = T & { ref?: U | null };

// =============================================================================
// localStorage Keys (centralized to avoid typos and enable easy refactoring)
// =============================================================================

export const STORAGE_KEYS = {
	userId: 'weavemind_user_id',
	/** Pseudonym shown on public publication URLs (/p/<username>/...).
	 *  Populated by the website auth handshake and read by the publish
	 *  modal so the deployer sees their URL preview before committing. */
	username: 'weavemind_username',
	authToken: 'weavemind_auth_token',
	apiUrl: 'weavemind_api_url',
	restateUrl: 'weavemind_restate_url',
	executorUrl: 'weavemind_executor_url',
	executionState: 'weavemind_execution_state',
	runningExecution: 'weavemind_running_execution',
	nodeFavorites: 'weavemind-node-favorites',
	nodeRecents: 'weavemind-node-recents',
} as const;

// =============================================================================
// User Identification
// =============================================================================

/**
 * Gets the current user ID from sessionStorage.
 * Returns 'local' if not in browser or no user ID is set.
 */
export function getUserId(): string {
	if (browser) {
		return sessionStorage.getItem(STORAGE_KEYS.userId) || 'local';
	}
	return 'local';
}


// =============================================================================
// Execution State Persistence
// =============================================================================

export function loadExecutionState(projectId: string): {
	nodeOutputs: Record<string, unknown>;
	nodeExecutions: Record<string, unknown>;
} | null {
	if (!browser) return null;
	try {
		const stored = localStorage.getItem(`${STORAGE_KEYS.executionState}_${projectId}`);
		if (stored) {
			const parsed = JSON.parse(stored);
			return {
				nodeOutputs: parsed.nodeOutputs || {},
				nodeExecutions: parsed.nodeExecutions || {},
			};
		}
	} catch (e) {
		console.error('Failed to load execution state:', e);
	}
	return null;
}

export function saveExecutionState(
	projectId: string,
	state: { nodeOutputs: Record<string, unknown>; nodeExecutions: Record<string, unknown> },
): void {
	if (!browser) return;
	try {
		localStorage.setItem(`${STORAGE_KEYS.executionState}_${projectId}`, JSON.stringify({
			nodeOutputs: state.nodeOutputs,
			nodeExecutions: state.nodeExecutions,
			savedAt: new Date().toISOString(),
		}));
	} catch (e) {
		console.error('Failed to save execution state:', e);
	}
}

export function saveRunningExecution(projectId: string, executionId: string): void {
	if (!browser) return;
	localStorage.setItem(`${STORAGE_KEYS.runningExecution}_${projectId}`, executionId);
}

export function loadRunningExecution(projectId: string): string | null {
	if (!browser) return null;
	return localStorage.getItem(`${STORAGE_KEYS.runningExecution}_${projectId}`);
}

export function clearRunningExecution(projectId: string): void {
	if (!browser) return;
	localStorage.removeItem(`${STORAGE_KEYS.runningExecution}_${projectId}`);
}


