<script lang="ts">
	import * as Dialog from '$lib/components/ui/dialog';
	import { Button } from '$lib/components/ui/button';
	import { browser } from '$app/environment';
	import * as te from '$lib/telemetry-events';

	const WELCOME_SHOWN_KEY = 'weavemind_welcome_shown';

	let { onClose }: { onClose?: () => void } = $props();

	let open = $state(false);

	$effect(() => {
		if (browser) {
			const hasSeenWelcome = localStorage.getItem(WELCOME_SHOWN_KEY);
			if (!hasSeenWelcome) {
				open = true;
				te.welcome.shown();
			}
		}
	});

	function handleClose() {
		if (browser) {
			localStorage.setItem(WELCOME_SHOWN_KEY, 'true');
		}
		te.welcome.dismissed();
		open = false;
		onClose?.();
	}
</script>

<Dialog.Root bind:open onOpenChange={(isOpen) => { if (!isOpen) handleClose(); }}>
	<Dialog.Content class="max-w-md">
		<Dialog.Header class="text-center pb-2">
			<Dialog.Title class="text-2xl">Welcome to WeaveMind! 🎉</Dialog.Title>
			<p class="text-sm text-muted-foreground mt-1">This is a very early demo. Expect rough edges!</p>
		</Dialog.Header>

		<div class="space-y-3 py-3">
			<div class="flex items-start gap-3">
				<span class="text-lg">🧪</span>
				<div class="text-sm">
					<p><strong>Early demo.</strong> The node catalog is limited and things may break.</p>
					<p class="text-muted-foreground text-xs mt-0.5">I'm building fast based on feedback. Missing something? Tell me on Discord!</p>
				</div>
			</div>
			<div class="flex items-start gap-3">
				<span class="text-lg">💸</span>
				<div class="text-sm">
					<p><strong>You get $5 free credits.</strong> Tangle (the AI builder) uses Sonnet/Opus which burn through credits fast.</p>
					<p class="text-muted-foreground text-xs mt-0.5">I don't have preferential pricing with Anthropic yet. Working on fine-tuning smaller models to reduce costs.</p>
				</div>
			</div>
			<div class="flex items-start gap-3">
				<span class="text-lg">🔑</span>
				<div class="text-sm">
					<p><strong>Everything is encrypted at rest</strong> (even your Tangle conversations).</p>
					<p class="text-muted-foreground text-xs mt-0.5">We recommend using dedicated API keys for testing, or rotate your credentials.</p>
				</div>
			</div>
			<div class="flex items-start gap-3">
				<span class="text-lg">🧩</span>
				<div class="text-sm">
					<p>Check the <strong>Extension</strong> page for human in the loop</p>
				</div>
			</div>
		</div>

		<div class="flex flex-col gap-2 pt-2">
			<a 
				href="https://github.com/WeaveMindAI/weft" 
				target="_blank" 
				rel="noopener noreferrer"
				class="flex items-center justify-center gap-2 w-full py-2.5 px-4 rounded-md bg-zinc-900 hover:bg-zinc-800 text-white font-medium transition-colors"
			>
				<svg class="w-5 h-5" viewBox="0 0 24 24" fill="currentColor">
					<path d="M12 0C5.37 0 0 5.37 0 12c0 5.31 3.435 9.795 8.205 11.385.6.105.825-.255.825-.57 0-.285-.015-1.23-.015-2.235-3.015.555-3.795-.735-4.035-1.41-.135-.345-.72-1.41-1.23-1.695-.42-.225-1.02-.78-.015-.795.945-.015 1.62.87 1.845 1.23 1.08 1.815 2.805 1.305 3.495.99.105-.78.42-1.305.765-1.605-2.67-.3-5.46-1.335-5.46-5.925 0-1.305.465-2.385 1.23-3.225-.12-.3-.54-1.53.12-3.18 0 0 1.005-.315 3.3 1.23.96-.27 1.98-.405 3-.405s2.04.135 3 .405c2.295-1.56 3.3-1.23 3.3-1.23.66 1.65.24 2.88.12 3.18.765.84 1.23 1.905 1.23 3.225 0 4.605-2.805 5.625-5.475 5.925.435.375.81 1.095.81 2.22 0 1.605-.015 2.895-.015 3.3 0 .315.225.69.825.57A12.02 12.02 0 0 0 24 12c0-6.63-5.37-12-12-12z"/>
				</svg>
				Star us on GitHub
			</a>
			<a 
				href="https://discord.gg/FGwNu6mDkU" 
				target="_blank" 
				rel="noopener noreferrer"
				class="flex items-center justify-center gap-2 w-full py-2.5 px-4 rounded-md bg-[#5865F2] hover:bg-[#4752C4] text-white font-medium transition-colors"
			>
				<svg class="w-5 h-5" viewBox="0 0 24 24" fill="currentColor">
					<path d="M20.317 4.37a19.791 19.791 0 0 0-4.885-1.515.074.074 0 0 0-.079.037c-.21.375-.444.864-.608 1.25a18.27 18.27 0 0 0-5.487 0 12.64 12.64 0 0 0-.617-1.25.077.077 0 0 0-.079-.037A19.736 19.736 0 0 0 3.677 4.37a.07.07 0 0 0-.032.027C.533 9.046-.32 13.58.099 18.057a.082.082 0 0 0 .031.057 19.9 19.9 0 0 0 5.993 3.03.078.078 0 0 0 .084-.028 14.09 14.09 0 0 0 1.226-1.994.076.076 0 0 0-.041-.106 13.107 13.107 0 0 1-1.872-.892.077.077 0 0 1-.008-.128 10.2 10.2 0 0 0 .372-.292.074.074 0 0 1 .077-.01c3.928 1.793 8.18 1.793 12.062 0a.074.074 0 0 1 .078.01c.12.098.246.198.373.292a.077.077 0 0 1-.006.127 12.299 12.299 0 0 1-1.873.892.077.077 0 0 0-.041.107c.36.698.772 1.362 1.225 1.993a.076.076 0 0 0 .084.028 19.839 19.839 0 0 0 6.002-3.03.077.077 0 0 0 .032-.054c.5-5.177-.838-9.674-3.549-13.66a.061.061 0 0 0-.031-.03zM8.02 15.33c-1.183 0-2.157-1.085-2.157-2.419 0-1.333.956-2.419 2.157-2.419 1.21 0 2.176 1.096 2.157 2.42 0 1.333-.956 2.418-2.157 2.418zm7.975 0c-1.183 0-2.157-1.085-2.157-2.419 0-1.333.955-2.419 2.157-2.419 1.21 0 2.176 1.096 2.157 2.42 0 1.333-.946 2.418-2.157 2.418z"/>
				</svg>
				Join our Discord
			</a>
			<Button onclick={handleClose} variant="outline" class="w-full">
				Got it, let's go!
			</Button>
		</div>
	</Dialog.Content>
</Dialog.Root>
