<script lang="ts">
	import { page } from "$app/stores";
	import { afterNavigate } from "$app/navigation";
	import { onMount } from "svelte";
	import { browser } from "$app/environment";
	import { projects } from "$lib/stores/projects";
	import { authFetch } from "$lib/config";
	import { isCloudMode } from "$lib/utils/blob-upload";
	import WelcomeDialog from "$lib/components/WelcomeDialog.svelte";
	import TutorialOverlay from "$lib/components/TutorialOverlay.svelte";
	import { nav } from "$lib/telemetry-events";

	let { children } = $props();

	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	let tutorialRef: any = $state();

	const TUTORIAL_DONE_KEY = 'weavemind_tutorial_done';
	const TUTORIALS_SEEDED_KEY = 'weavemind_tutorials_seeded';

	function handleWelcomeClose() {
		if (browser && !localStorage.getItem(TUTORIAL_DONE_KEY)) {
			// Small delay so the welcome dialog fully closes before tutorial starts
			setTimeout(() => tutorialRef?.start(), 400);
		}
	}

	type NavItem = { href: string; label: string; position?: 'start' | 'end'; action?: string; variant?: string };
	
	const baseNavItems: NavItem[] = $derived.by(() => {
		const items: NavItem[] = [
			{ href: "/dashboard", label: "Dashboard" },
			{ href: "/executions", label: "Executions" },
			{ href: "/usage", label: "Usage" },
			{ href: "https://weavemind.ai/docs", label: "Docs" },
			{ href: "/extension", label: "Extension" },
			{ href: "https://github.com/WeaveMindAI/weft", label: "GitHub" },
		];
		if (isCloudMode()) {
			items.push({ href: "/files", label: "Files" });
		}
		return items;
	});
	
	// Injected nav items from parent (website)
	let injectedNavItems = $state<NavItem[]>([]);
	
	// User info from parent (website)
	let user = $state<{ name?: string; email?: string; image?: string; username?: string } | null>(null);
	
	// Combined nav items
	let navItems = $derived([
		...injectedNavItems.filter(i => i.position === 'start'),
		...baseNavItems,
		...injectedNavItems.filter(i => i.position !== 'start'),
	]);

	afterNavigate(({ to }) => {
		if (to?.url) nav.pageView(to.url.pathname);
		if (to?.url.pathname === '/' || to?.url.pathname === '/dashboard') {
			projects.init();
		}
	});

	onMount(() => {
		projects.init();
		
		if (browser) {
			window.addEventListener('message', handleParentMessage);
			
			// Request nav items from parent if in iframe
			if (window.parent !== window) {
				window.parent.postMessage({ type: 'requestNavItems' }, '*');
			}

			// Seed tutorial projects on first visit. The server
			// scopes the seed to the authenticated user from the JWT,
			// so we no longer pass a userId in the URL.
			if (!localStorage.getItem(TUTORIALS_SEEDED_KEY)) {
				authFetch('/api/projects/seed', { method: 'POST' })
					.then(() => {
						localStorage.setItem(TUTORIALS_SEEDED_KEY, 'true');
						projects.init();
					})
					.catch(() => {});
			}
			
			return () => window.removeEventListener('message', handleParentMessage);
		}
	});
	
	function handleParentMessage(event: MessageEvent) {
		if (event.data?.type === 'injectNavItems') {
			if (Array.isArray(event.data.items)) {
				injectedNavItems = event.data.items;
			}
			if (event.data.user) {
				user = event.data.user;
			}
		}
	}
	
	let mobileMenuOpen = $state(false);

	// Close mobile menu on navigation
	afterNavigate(() => { mobileMenuOpen = false; });

	function handleNavClick(e: MouseEvent, item: { href: string; label: string; action?: string }) {
		// If item has an action, prevent default navigation and notify parent
		if (item.action) {
			e.preventDefault();
			if (browser && window.parent !== window) {
				window.parent.postMessage({ type: 'navAction', action: item.action }, '*');
			}
			return;
		}

		// If in iframe, notify parent of nav click (parent may want to handle it)
		if (browser && window.parent !== window) {
			window.parent.postMessage({ type: 'navClick', href: item.href, label: item.label }, '*');
		}
	}
</script>

<WelcomeDialog onClose={handleWelcomeClose} />
<TutorialOverlay bind:this={tutorialRef} />

<div class="min-h-screen bg-background">
	<!-- Floating Navbar (desktop) -->
	{#if !$page.url.pathname.match(/^\/projects\/[^/]+$/) && !$page.url.pathname.match(/^\/tasks\//)}
		<nav class="fixed top-4 left-1/2 -translate-x-1/2 z-50 hidden md:block">
			<div
				class="flex items-center gap-1 px-1.5 py-1.5 rounded-2xl"
				style="background: rgba(255,255,255,0.55); backdrop-filter: blur(24px) saturate(1.8); -webkit-backdrop-filter: blur(24px) saturate(1.8); border: 1px solid rgba(255,255,255,0.7); box-shadow: 0 1px 3px rgba(0,0,0,0.04), 0 4px 24px rgba(0,0,0,0.06), inset 0 1px 0 rgba(255,255,255,0.6);"
			>
				<!-- Logo -->
				<a href="/dashboard" class="flex items-center gap-1.5 px-3 py-1 rounded-xl hover:bg-white/50 transition-colors">
					<img src="/web_logo.png" alt="WeaveMind" class="w-5 h-5" />
					<span class="text-[12px] font-semibold text-zinc-800">WeaveMind</span>
				</a>

				<!-- Nav Items -->
				{#each navItems.filter(i => i.variant !== 'danger') as item}
					{@const isActive = $page.url.pathname.startsWith(item.href)}
					<a
						href={item.href}
						target={item.href.startsWith('http') ? '_blank' : undefined}
						rel={item.href.startsWith('http') ? 'noopener noreferrer' : undefined}
						onclick={(e) => handleNavClick(e, item)}
						class="px-3 py-1 text-[11px] font-medium rounded-xl transition-all {isActive ? 'bg-zinc-800 text-white shadow-sm' : 'text-zinc-500 hover:text-zinc-800 hover:bg-white/60'}"
					>
						{item.label}
					</a>
				{/each}

				<!-- User + Sign Out -->
				{#if user || navItems.some(i => i.variant === 'danger')}
					<div class="w-px h-4 bg-zinc-200/60 mx-1"></div>

					{#if user}
						<div class="flex items-center px-1">
							{#if user.image}
								<img src={user.image} alt={user.name} class="w-5 h-5 rounded-full" title={user.username || user.name} />
							{:else}
								<div class="w-5 h-5 rounded-full bg-zinc-200 flex items-center justify-center text-[9px] font-semibold text-zinc-600" title={user.username || user.name}>
									{user.username?.charAt(0).toUpperCase() || user.name?.charAt(0).toUpperCase() || '?'}
								</div>
							{/if}
						</div>
					{/if}

					{#each navItems.filter(i => i.variant === 'danger') as item}
						<button
							onclick={(e) => handleNavClick(e, item)}
							class="px-3 py-1 text-[11px] font-medium rounded-xl transition-colors text-zinc-500 hover:text-red-600 hover:bg-red-50/60 whitespace-nowrap"
						>
							{item.label}
						</button>
					{/each}
				{/if}
			</div>
		</nav>

		<!-- Mobile navbar -->
		<nav class="fixed top-0 left-0 right-0 z-50 md:hidden">
			<div class="flex items-center justify-between px-4 py-3" style="background: rgba(255,255,255,0.85); backdrop-filter: blur(20px); -webkit-backdrop-filter: blur(20px); border-bottom: 1px solid rgba(0,0,0,0.06);">
				<a href="/dashboard" class="flex items-center gap-2">
					<img src="/web_logo.png" alt="WeaveMind" class="w-5 h-5" />
					<span class="text-sm font-semibold text-zinc-800">WeaveMind</span>
				</a>
				<div class="flex items-center gap-3">
					{#if user}
						{#if user.image}
							<img src={user.image} alt={user.name} class="w-6 h-6 rounded-full" />
						{:else}
							<div class="w-6 h-6 rounded-full bg-zinc-200 flex items-center justify-center text-[10px] font-semibold text-zinc-600">
								{user.username?.charAt(0).toUpperCase() || user.name?.charAt(0).toUpperCase() || '?'}
							</div>
						{/if}
					{/if}
					<button
						onclick={() => mobileMenuOpen = !mobileMenuOpen}
						class="p-1.5 rounded-lg hover:bg-zinc-100 transition-colors"
						aria-label="Toggle menu"
					>
						<svg class="w-5 h-5 text-zinc-600" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2">
							{#if mobileMenuOpen}
								<path stroke-linecap="round" stroke-linejoin="round" d="M6 18L18 6M6 6l12 12" />
							{:else}
								<path stroke-linecap="round" stroke-linejoin="round" d="M4 6h16M4 12h16M4 18h16" />
							{/if}
						</svg>
					</button>
				</div>
			</div>

			{#if mobileMenuOpen}
				<div class="border-b border-zinc-100 py-2 px-4 space-y-1" style="background: rgba(255,255,255,0.95); backdrop-filter: blur(20px); -webkit-backdrop-filter: blur(20px);">
					{#each navItems.filter(i => i.variant !== 'danger') as item}
						{@const isActive = $page.url.pathname.startsWith(item.href)}
						<a
							href={item.href}
							target={item.href.startsWith('http') ? '_blank' : undefined}
							rel={item.href.startsWith('http') ? 'noopener noreferrer' : undefined}
							onclick={(e) => handleNavClick(e, item)}
							class="block px-3 py-2 text-sm font-medium rounded-lg transition-colors {isActive ? 'bg-zinc-900 text-white' : 'text-zinc-600 hover:bg-zinc-50 hover:text-zinc-900'}"
						>
							{item.label}
						</a>
					{/each}

					{#each navItems.filter(i => i.variant === 'danger') as item}
						<button
							onclick={(e) => handleNavClick(e, item)}
							class="block w-full text-left px-3 py-2 text-sm font-medium rounded-lg text-red-500 hover:bg-red-50 transition-colors"
						>
							{item.label}
						</button>
					{/each}
				</div>
			{/if}
		</nav>
		<!-- Mobile spacer for fixed navbar -->
		<div class="h-14 md:hidden"></div>
	{/if}

	<!-- Main Content - Full height, content starts from top -->
	<main class="min-h-screen">
		{@render children()}
	</main>
</div>
