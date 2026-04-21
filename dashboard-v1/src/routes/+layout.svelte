<script lang="ts">
	import './layout.css';
	import favicon from '$lib/assets/favicon.png';
	import { Toaster } from '$lib/components/ui/sonner';
	import AuthGate from '$lib/auth-gate.svelte';
	import { onMount } from 'svelte';
	import { browser } from '$app/environment';
	import { page } from '$app/stores';
	import { goto } from '$app/navigation';
	import type { LayoutData } from './$types';

	let { children, data }: { children: any; data: LayoutData } = $props();
	
	// Check if we're in cloud mode (from server layout data, never changes)
	// svelte-ignore state_referenced_locally
	const isCloudMode = data.isCloudMode ?? false;
	
	// Track if we've already processed URL params (to avoid re-processing on navigation)
	let paramsProcessed = $state(false);

	// Store user_id, username, and api_url from load data (captured before navigation)
	// Only run once when we have valid params - don't overwrite on subsequent navigations
	$effect(() => {
		if (browser && !paramsProcessed) {
			const { userId, username, apiUrl, websiteUrl, initialPath } = data;
			
						
			// Only process if we have params (first load from iframe)
			if (userId || username || apiUrl) {
				paramsProcessed = true;
				
								
				if (userId) {
					sessionStorage.setItem('weavemind_user_id', userId);
									}
				if (username) {
					sessionStorage.setItem('weavemind_username', username);
				}
				if (apiUrl) {
					sessionStorage.setItem('weavemind_api_url', apiUrl);
				}
				if (websiteUrl) {
					sessionStorage.setItem('weavemind_website_url', websiteUrl);
				}
				
				// Navigate to initial path AFTER storing params
				// initialPath may include query params (e.g., /tasks/xxx?nodeId=yyy)
				// Extract just the pathname for comparison
				const initialPathname = initialPath?.split('?')[0];
				if (initialPath && initialPathname !== $page.url.pathname) {
					goto(initialPath);
				}
			} else {
							}
		}
	});
	
	onMount(() => {
		if (browser) {
			// Listen for navigation requests from parent
			window.addEventListener('message', handleParentMessage);
			return () => window.removeEventListener('message', handleParentMessage);
		}
	});
	
	function handleParentMessage(event: MessageEvent) {
		if (event.data?.type === 'navigate' && event.data?.path) {
			goto(event.data.path);
		}
	}
	
	// Notify parent of route changes (for iframe embedding)
	$effect(() => {
		if (browser && window.parent !== window) {
			const path = $page.url.pathname + $page.url.search;
			window.parent.postMessage({ type: 'routeChange', path }, '*');
		}
	});
</script>

<svelte:head><link rel="icon" href={favicon} /></svelte:head>
<Toaster richColors position="top-left" closeButton />
{#if $page.url.pathname.startsWith('/playground') || $page.url.pathname.startsWith('/p/')}
	{@render children()}
{:else}
	<AuthGate {isCloudMode}>
		{@render children()}
	</AuthGate>
{/if}
