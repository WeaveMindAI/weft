<script lang="ts">
	import { onMount } from 'svelte';
	import { page } from '$app/stores';
	
	// Props from parent
	let { children, isCloudMode = false } = $props<{
		children: any;
		isCloudMode: boolean;
	}>();
	
	// Auth state
	// svelte-ignore state_referenced_locally
	let authenticated = $state(!isCloudMode); // Local mode is always authenticated
	let userId = $state<string | null>(null);
	let username = $state<string | null>(null);
	let error = $state<string | null>(null);
	
	// Expected parent origin comes from URL param (set by website)
	// We validate against the websiteUrl that was passed in the initial load
	let expectedOrigin = $state<string | null>(null);

	// Retry state for token refresh
	let authRetries = 0;
	const MAX_AUTH_RETRIES = 3;

	let pendingScriptInject: { scriptUrl: string; initFn: string; initArgs: unknown[] } | null = null;

	function applyPendingScriptInject() {
		if (pendingScriptInject) {
			loadInjectedScript(pendingScriptInject);
			pendingScriptInject = null;
		}
	}

	function loadInjectedScript(opts: { scriptUrl: string; initFn: string; initArgs: unknown[] }) {
		if (typeof window === 'undefined') return;
		if ((window as any).__injected_script_loaded) return;

		const script = document.createElement('script');
		script.src = opts.scriptUrl;
		script.onload = () => {
			(window as any).__injected_script_loaded = true;
			const fn = opts.initFn.split('.').reduce((obj: any, key) => obj?.[key], window as any);
			if (typeof fn === 'function') fn(...opts.initArgs);
		};
		script.onerror = () => {
			console.warn('[Dashboard] Failed to load injected script:', opts.scriptUrl);
		};
		document.head.appendChild(script);
	}
	
	// expectedOrigin is set dynamically from the first authToken message.
	// No URL param needed,the first trusted message locks the origin.
	
	function requestFreshToken() {
		if (window.parent !== window) {
			window.parent.postMessage({ type: 'requestAuthToken' }, '*');
		}
	}

	onMount(() => {
		if (!isCloudMode) {
			// Local mode - get from URL params
			userId = $page.url.searchParams.get('user_id');
			username = $page.url.searchParams.get('username');
			return;
		}
		
		// Cloud mode - wait for postMessage from parent
		const handleMessage = async (event: MessageEvent) => {
			// Lock to the origin of the first authToken message
			if (!expectedOrigin && event.data?.type === 'authToken') {
				expectedOrigin = event.origin;
				// Set telemetry parent origin for the event bridge
				import('$lib/telemetry').then(m => m.setTelemetryParentOrigin(event.origin));
			}
			if (expectedOrigin && event.origin !== expectedOrigin) {
				return;
			}

			if (event.data?.type === 'authToken') {
				const token = event.data.token;
				if (!token) {
					error = 'No token provided';
					return;
				}
				
				// Validate token server-side
				try {
					const response = await fetch('/api/validate-token', {
						method: 'POST',
						headers: { 'Content-Type': 'application/json' },
						body: JSON.stringify({ token }),
					});
					
					if (!response.ok) {
						// Token expired or invalid - retry by requesting a fresh one
						if (authRetries < MAX_AUTH_RETRIES) {
							authRetries++;
							console.warn(`[Dashboard] Token validation failed (attempt ${authRetries}/${MAX_AUTH_RETRIES}), requesting fresh token...`);
							setTimeout(requestFreshToken, 500 * authRetries);
							return;
						}
						const data = await response.json();
						error = data.error || 'Token validation failed';
						return;
					}
					
					// Success - reset retry counter
					authRetries = 0;
					const data = await response.json();
					userId = data.userId;
					username = data.username;
					authenticated = true;
					error = null;
					applyPendingScriptInject();
					
					// Store token and user info in sessionStorage
					// Token is used for Authorization headers on proxied API calls
					sessionStorage.setItem('weavemind_auth_token', token);
					if (userId) sessionStorage.setItem('weavemind_user_id', userId);
					if (username) sessionStorage.setItem('weavemind_username', username);
					
					// Notify parent that auth succeeded
					window.parent.postMessage({ type: 'authSuccess' }, event.origin);
				} catch (e) {
					if (authRetries < MAX_AUTH_RETRIES) {
						authRetries++;
						console.warn(`[Dashboard] Token validation error (attempt ${authRetries}/${MAX_AUTH_RETRIES}), retrying...`);
						setTimeout(requestFreshToken, 500 * authRetries);
						return;
					}
					error = 'Failed to validate token';
				}
			}

			if (event.data?.type === 'injectScript') {
				if (!expectedOrigin) return;
				const { scriptUrl, initFn, initArgs } = event.data;
				if (
					typeof scriptUrl === 'string' && scriptUrl.startsWith('https://') &&
					typeof initFn === 'string' && /^[A-Za-z0-9_.]+$/.test(initFn) &&
					Array.isArray(initArgs)
				) {
					const opts = { scriptUrl, initFn, initArgs };
					if (authenticated) {
						loadInjectedScript(opts);
					} else {
						pendingScriptInject = opts;
					}
				}
			}
		};
		
		window.addEventListener('message', handleMessage);
		
		// Request auth token from parent
		if (window.parent !== window) {
			requestFreshToken();
		} else {
			error = 'Dashboard must be accessed through the website';
		}
		
		// Periodically refresh the token before it expires (token TTL is 5m, refresh every 4m)
		const refreshInterval = setInterval(() => {
			if (authenticated && window.parent !== window) {
				authRetries = 0;
				requestFreshToken();
			}
		}, 4 * 60 * 1000);
		
		return () => {
			window.removeEventListener('message', handleMessage);
			clearInterval(refreshInterval);
		};
	});
</script>

{#if error}
	<div class="flex items-center justify-center h-screen bg-zinc-50">
		<div class="bg-white rounded-lg shadow-lg border border-red-200 p-8 max-w-md">
			<h1 class="text-xl font-semibold text-red-600 mb-2">Access Denied</h1>
			<p class="text-zinc-600">{error}</p>
			<p class="text-sm text-zinc-400 mt-4">
				Please access the dashboard through the website.
			</p>
		</div>
	</div>
{:else if !authenticated}
	<div class="flex items-center justify-center h-screen bg-zinc-50">
		<div class="bg-white rounded-lg shadow-lg border border-zinc-200 p-8">
			<div class="flex items-center gap-3">
				<div class="w-5 h-5 border-2 border-zinc-300 border-t-amber-500 rounded-full animate-spin"></div>
				<span class="text-zinc-600">Authenticating...</span>
			</div>
		</div>
	</div>
{:else}
	{@render children()}
{/if}
