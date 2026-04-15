<script lang="ts">
	import { browser } from '$app/environment';
	import * as te from '$lib/telemetry-events';

	const TUTORIAL_DONE_KEY = 'weavemind_tutorial_done';
	const DISCORD_URL = 'https://discord.com/invite/FGwNu6mDkU';

	interface TutorialStep {
		title: string;
		description: string;
		targetSelector?: string;
		position?: 'top' | 'bottom' | 'left' | 'right' | 'center';
	}

	const steps: TutorialStep[] = [
		{
			title: 'Welcome to WeaveMind',
			description: "Let's take a quick tour so you know where everything is. You can skip at any time.",
			position: 'center',
		},
		{
			title: 'Your Projects',
			description: 'Projects are your automations. Each one is a pipeline of nodes that process data, call APIs, or run AI models.',
			targetSelector: 'a[href="/dashboard"]',
			position: 'bottom',
		},
		{
			title: 'Tangle, Your AI Builder',
			description: 'On any project page, Tangle appears in the sidebar. Describe what you want to build and it generates the project for you.',
			position: 'center',
		},
		{
			title: 'The Extension',
			description: 'Install the browser extension to enable human-in-the-loop tasks. Pause a project and wait for your input before continuing.',
			targetSelector: 'a[href="/extension"]',
			position: 'bottom',
		},
		{
			title: 'Pricing & credits',
			description: "Tangle uses Claude Sonnet and Opus under the hood, the only models that handle Weft well today. That burns credits fast, and I am paying full Anthropic prices. Open Pricing anytime to top up credits or subscribe. Every subscription in these early days directly funds the project.",
			targetSelector: 'a[href="/account"]',
			position: 'bottom',
		},
		{
			title: 'Want to talk to me?',
			description: "I am building Weft in the open and would love to hear what you are trying to build. Join the Discord and send me a message, we can hop on a call.",
			position: 'center',
		},
	];

	let open = $state(false);
	let currentStep = $state(0);
	let highlightRect = $state<DOMRect | null>(null);

	export function start() {
		currentStep = 0;
		open = true;
		te.tutorial.started();
		updateHighlight();
	}

	function updateHighlight() {
		const step = steps[currentStep];
		if (step?.targetSelector && browser) {
			const el = document.querySelector(step.targetSelector);
			if (el) {
				highlightRect = el.getBoundingClientRect();
				return;
			}
		}
		highlightRect = null;
	}

	function next() {
		if (currentStep < steps.length - 1) {
			currentStep++;
			te.tutorial.stepReached(currentStep, steps.length);
			updateHighlight();
		} else {
			finish();
		}
	}

	function finish() {
		if (browser) localStorage.setItem(TUTORIAL_DONE_KEY, 'true');
		if (currentStep >= steps.length - 1) {
			te.tutorial.completed();
		} else {
			te.tutorial.skipped(currentStep, steps.length);
		}
		open = false;
		highlightRect = null;
	}

	const step = $derived(steps[currentStep]);
	const isLast = $derived(currentStep === steps.length - 1);
	const isDiscordStep = $derived(currentStep === steps.length - 1);
</script>

{#if open}
	<!-- Backdrop with spotlight cutout -->
	<div
		class="fixed inset-0 z-[9000] pointer-events-none"
		aria-hidden="true"
	>
		{#if highlightRect}
			<!-- Dark overlay with hole -->
			<svg class="absolute inset-0 w-full h-full" xmlns="http://www.w3.org/2000/svg">
				<defs>
					<mask id="spotlight-mask">
						<rect width="100%" height="100%" fill="white" />
						<rect
							x={highlightRect.left - 6}
							y={highlightRect.top - 6}
							width={highlightRect.width + 12}
							height={highlightRect.height + 12}
							rx="8"
							fill="black"
						/>
					</mask>
				</defs>
				<rect width="100%" height="100%" fill="rgba(0,0,0,0.55)" mask="url(#spotlight-mask)" />
			</svg>
			<!-- Highlight ring -->
			<div
				class="absolute rounded-lg ring-2 ring-amber-400 ring-offset-0 transition-all duration-300"
				style="
					left: {highlightRect.left - 6}px;
					top: {highlightRect.top - 6}px;
					width: {highlightRect.width + 12}px;
					height: {highlightRect.height + 12}px;
				"
			></div>
		{:else}
			<div class="absolute inset-0 bg-black/50"></div>
		{/if}
	</div>

	<!-- Tooltip card -->
	<!-- svelte-ignore a11y_no_static_element_interactions -->
	<div
		class="fixed z-[9001] pointer-events-auto"
		style={highlightRect
			? `left: ${Math.min(highlightRect.left, (typeof window !== 'undefined' ? window.innerWidth : 1200) - 340)}px; top: ${highlightRect.bottom + 16}px;`
			: 'left: 50%; top: 50%; transform: translate(-50%, -50%);'}
	>
		<div class="bg-white rounded-xl shadow-2xl border border-zinc-200 w-80 overflow-hidden">
			<!-- Progress bar -->
			<div class="h-1 bg-zinc-100">
				<div
					class="h-full bg-amber-400 transition-all duration-300"
					style="width: {((currentStep + 1) / steps.length) * 100}%"
				></div>
			</div>

			<div class="p-5">
				<!-- Step counter -->
				<div class="flex items-center justify-between mb-3">
					<span class="text-xs font-medium text-zinc-400 uppercase tracking-wider">
						Step {currentStep + 1} of {steps.length}
					</span>
					<button
						onclick={finish}
						class="text-xs text-zinc-400 hover:text-zinc-600 transition-colors"
					>
						Skip tour
					</button>
				</div>

				<h3 class="font-semibold text-zinc-900 text-base mb-1.5">{step.title}</h3>
				<p class="text-sm text-zinc-500 leading-relaxed">{step.description}</p>

				{#if isDiscordStep}
					<a
						href={DISCORD_URL}
						target="_blank"
						rel="noopener noreferrer"
						class="mt-4 flex items-center justify-center gap-2 w-full py-2.5 px-4 rounded-lg bg-amber-500 hover:bg-amber-600 text-white font-medium text-sm transition-colors"
					>
						<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="currentColor">
							<path d="M20.317 4.37a19.791 19.791 0 0 0-4.885-1.515a.074.074 0 0 0-.079.037c-.21.375-.444.864-.608 1.25a18.27 18.27 0 0 0-5.487 0a12.64 12.64 0 0 0-.617-1.25a.077.077 0 0 0-.079-.037A19.736 19.736 0 0 0 3.677 4.37a.07.07 0 0 0-.032.027C.533 9.046-.32 13.58.099 18.057a.082.082 0 0 0 .031.057a19.9 19.9 0 0 0 5.993 3.03a.078.078 0 0 0 .084-.028a14.09 14.09 0 0 0 1.226-1.994a.076.076 0 0 0-.041-.106a13.107 13.107 0 0 1-1.872-.892a.077.077 0 0 1-.008-.128a10.2 10.2 0 0 0 .372-.292a.074.074 0 0 1 .077-.01c3.928 1.793 8.18 1.793 12.062 0a.074.074 0 0 1 .078.01c.12.098.246.198.373.292a.077.077 0 0 1-.006.127a12.299 12.299 0 0 1-1.873.892a.077.077 0 0 0-.041.107c.36.698.772 1.362 1.225 1.993a.076.076 0 0 0 .084.028a19.839 19.839 0 0 0 6.002-3.03a.077.077 0 0 0 .032-.054c.5-5.177-.838-9.674-3.549-13.66a.061.061 0 0 0-.031-.03zM8.02 15.33c-1.183 0-2.157-1.085-2.157-2.419c0-1.333.956-2.419 2.157-2.419c1.21 0 2.176 1.096 2.157 2.42c0 1.333-.956 2.418-2.157 2.418zm7.975 0c-1.183 0-2.157-1.085-2.157-2.419c0-1.333.955-2.419 2.157-2.419c1.21 0 2.176 1.096 2.157 2.42c0 1.333-.946 2.418-2.157 2.418z"/>
						</svg>
						Say hi on Discord
					</a>
					<p class="text-xs text-zinc-400 text-center mt-2">Tell me what you are building, we can hop on a call</p>
				{/if}

				<div class="flex items-center justify-between mt-4">
					{#if currentStep > 0}
						<button
							onclick={() => { currentStep--; updateHighlight(); }}
							class="text-sm text-zinc-400 hover:text-zinc-600 transition-colors"
						>
							← Back
						</button>
					{:else}
						<div></div>
					{/if}

					<button
						onclick={next}
						class="px-4 py-1.5 rounded-lg bg-zinc-900 hover:bg-zinc-700 text-white text-sm font-medium transition-colors"
					>
						{isLast ? 'Done' : 'Next →'}
					</button>
				</div>
			</div>
		</div>
	</div>
{/if}
