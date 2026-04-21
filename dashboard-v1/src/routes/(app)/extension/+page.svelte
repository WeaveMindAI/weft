<!--
	Extension Token Management UI

	This page manages extension tokens for both local and cloud modes.
	All token logic is handled by weft-api (extension_tokens.rs, extension_api.rs).
	In cloud mode, cloud-api proxies requests to weft-api.

	Related code:
	- Backend: crates/weft-api/src/extension_tokens.rs
	- Extension client: extension/src/lib/api.ts
-->
<script lang="ts">
	import { onMount } from "svelte";
	import { browser } from "$app/environment";
	import { toast } from "svelte-sonner";
	import CopyButton from '$lib/components/ui/CopyButton.svelte';
	import { formatTimeAgo } from "$lib/utils/status";
	import { getApiUrl } from "$lib/config";

	function getWebsiteUrl(): string | null {
		if (browser) return sessionStorage.getItem('weavemind_website_url');
		return null;
	}

	function getTokenUrl(tokenId: string): string {
		const websiteUrl = getWebsiteUrl();
		const baseUrl = websiteUrl || (browser ? window.location.origin : '');
		return `${baseUrl}/api/ext/${tokenId}`;
	}

	interface ExtensionToken {
		id: string;
		userId: string;
		name: string | null;
		createdAt: string;
		lastUsedAt: string | null;
	}

	let tokens = $state<ExtensionToken[]>([]);
	let isLoading = $state(true);
	let newKeyName = $state('');
	let isCreatingToken = $state(false);
	let userId = $state<string | null>(null);
	let username = $state<string | null>(null);

	const adjectives = [
		'swift', 'bright', 'calm', 'bold', 'clever', 'cosmic', 'crystal', 'dancing',
		'daring', 'dreamy', 'eager', 'electric', 'emerald', 'endless', 'epic', 'eternal',
		'fierce', 'flying', 'frozen', 'gentle', 'glowing', 'golden', 'graceful', 'happy',
		'hidden', 'humble', 'icy', 'infinite', 'jade', 'jolly', 'keen', 'kind',
		'lively', 'lucky', 'lunar', 'magic', 'mellow', 'mighty', 'misty', 'noble',
		'ocean', 'peaceful', 'playful', 'polar', 'proud', 'quantum', 'quiet', 'radiant',
		'rapid', 'rising', 'roaming', 'royal', 'ruby', 'rustic', 'sacred', 'serene',
		'shiny', 'silent', 'silver', 'sleek', 'smooth', 'snowy', 'solar', 'sonic',
		'sparkling', 'speedy', 'stellar', 'stormy', 'sunny', 'swift', 'tender', 'thunder',
		'tidal', 'timeless', 'tranquil', 'twilight', 'urban', 'velvet', 'vibrant', 'vivid',
		'wandering', 'warm', 'wavy', 'wild', 'windy', 'winter', 'wise', 'witty',
		'wonder', 'wooden', 'young', 'zealous', 'zen', 'zesty', 'zippy', 'azure'
	];

	const nouns = [
		'anchor', 'arrow', 'aurora', 'beacon', 'bear', 'bird', 'bloom', 'breeze',
		'bridge', 'brook', 'canyon', 'castle', 'cedar', 'cloud', 'comet', 'coral',
		'crane', 'creek', 'crystal', 'dawn', 'delta', 'dolphin', 'dove', 'dragon',
		'dream', 'dune', 'eagle', 'echo', 'ember', 'falcon', 'fern', 'field',
		'flame', 'flower', 'forest', 'fountain', 'fox', 'frost', 'garden', 'glacier',
		'grove', 'harbor', 'hawk', 'heart', 'hill', 'horizon', 'island', 'jade',
		'jewel', 'jungle', 'lake', 'leaf', 'light', 'lion', 'lotus', 'maple',
		'meadow', 'meteor', 'moon', 'mountain', 'nebula', 'nest', 'night', 'oak',
		'ocean', 'orchid', 'otter', 'owl', 'palm', 'panda', 'path', 'peak',
		'pearl', 'phoenix', 'pine', 'planet', 'pond', 'prism', 'quartz', 'rain',
		'rainbow', 'raven', 'reef', 'ridge', 'river', 'robin', 'rose', 'sage',
		'salmon', 'sand', 'shadow', 'shore', 'sky', 'snow', 'spark', 'spring',
		'star', 'stone', 'storm', 'stream', 'summit', 'sun', 'swan', 'thunder',
		'tiger', 'trail', 'tree', 'valley', 'wave', 'willow', 'wind', 'wolf'
	];

	function generateRandomName(): string {
		const adj = adjectives[Math.floor(Math.random() * adjectives.length)];
		const noun = nouns[Math.floor(Math.random() * nouns.length)];
		const num = Math.floor(Math.random() * 100);
		return `${adj}-${noun}-${num}`;
	}

	function fillRandomName() { newKeyName = generateRandomName(); }

	type BrowserType = 'chrome' | 'firefox' | 'safari' | 'opera' | 'edge' | 'brave';
	let selectedBrowser = $state<BrowserType>('chrome');

	const browsers: { key: BrowserType; name: string; icon: string }[] = [
		{ key: 'chrome', name: 'Chrome', icon: '🌐' },
		{ key: 'firefox', name: 'Firefox', icon: '🦊' },
		{ key: 'safari', name: 'Safari', icon: '🧭' },
		{ key: 'opera', name: 'Opera', icon: '🔴' },
		{ key: 'edge', name: 'Edge', icon: '🌊' },
		{ key: 'brave', name: 'Brave', icon: '🦁' },
	];

	function getExtensionDownloadUrl(bt: BrowserType): string {
		const baseUrl = browser ? window.location.origin : '';
		if (bt === 'firefox') return `${baseUrl}/extensions/weavemind-firefox.xpi`;
		if (bt === 'brave') return `${baseUrl}/extensions/weavemind-chrome.zip`;
		return `${baseUrl}/extensions/weavemind-${bt}.zip`;
	}

	function getDownloadFilename(bt: BrowserType): string {
		if (bt === 'firefox') return 'weavemind-firefox.xpi';
		if (bt === 'brave') return 'weavemind-chrome.zip';
		return `weavemind-${bt}.zip`;
	}

	function getDownloadExt(bt: BrowserType): string {
		return bt === 'firefox' ? 'XPI file (signed)' : 'ZIP file';
	}

	onMount(async () => {
		if (browser) {
			const params = new URLSearchParams(window.location.search);
			userId = params.get('user_id') || sessionStorage.getItem('weavemind_user_id') || 'local';
			username = params.get('username') || sessionStorage.getItem('weavemind_username') || 'local';
			sessionStorage.setItem('weavemind_user_id', userId);
			sessionStorage.setItem('weavemind_username', username);
			await loadTokens();
			isLoading = false;
		}
	});

	async function loadTokens() {
		if (!userId) return;
		const apiUrl = getApiUrl();
		try {
			const response = await fetch(`${apiUrl}/api/extension/tokens/user/${userId}`);
			if (response.ok) tokens = (await response.json()).tokens || [];
		} catch (e) {
			console.error('Failed to load extension tokens:', e);
		}
	}

	async function createToken() {
		if (!userId || !username) { toast.error('User not identified'); return; }
		const keyName = newKeyName.trim();
		if (!keyName) { toast.error('Please enter a key name'); return; }
		const apiUrl = getApiUrl();

		isCreatingToken = true;
		try {
			const response = await fetch(`${apiUrl}/api/extension/tokens`, {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({ userId, username, keyName }),
			});
			if (response.ok) {
				tokens = [await response.json(), ...tokens];
				newKeyName = '';
				toast.success('Extension token created');
			} else {
				toast.error((await response.json()).error || 'Failed to create token');
			}
		} catch {
			toast.error('Failed to create token');
		} finally {
			isCreatingToken = false;
		}
	}

	async function deleteToken(tokenId: string) {
		const apiUrl = getApiUrl();
		try {
			const response = await fetch(`${apiUrl}/api/extension/tokens/${tokenId}`, { method: 'DELETE' });
			if (response.ok) { tokens = tokens.filter(t => t.id !== tokenId); toast.success('Token deleted'); }
			else toast.error('Failed to delete token');
		} catch { toast.error('Failed to delete token'); }
	}
</script>

<div class="min-h-screen pt-20 px-6 pb-12" style="background: #f8f9fa; background-image: radial-gradient(circle, #d4d4d8 1px, transparent 1px); background-size: 24px 24px;">
	<div class="max-w-3xl mx-auto">

		<div class="mb-5">
			<h2 class="text-[15px] font-semibold text-zinc-800">Browser Extension</h2>
			<p class="text-[12px] text-zinc-400 mt-0.5">Install the extension to receive human-in-the-loop tasks from your projects</p>
		</div>

		<!-- Step 1: Install -->
		<div class="bg-white rounded-xl border border-zinc-200 overflow-hidden mb-4" style="box-shadow: 0 1px 3px rgba(0,0,0,0.06);">
			<div class="px-5 py-3 border-b border-zinc-100 flex items-center gap-2">
				<span class="flex items-center justify-center w-5 h-5 rounded-full bg-zinc-800 text-white text-[10px] font-bold">1</span>
				<span class="text-[12px] font-semibold text-zinc-700">Install the Extension</span>
			</div>

			<div class="px-5 py-4 space-y-4">
				<!-- Browser selector -->
				<div class="flex gap-1.5 flex-wrap">
					{#each browsers as b}
						<button
							onclick={() => selectedBrowser = b.key}
							class="flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-[11px] font-medium border transition-all {selectedBrowser === b.key ? 'border-zinc-800 bg-zinc-800 text-white' : 'border-zinc-200 bg-white text-zinc-600 hover:bg-zinc-50'}"
						>
							<span class="text-[14px]">{b.icon}</span>
							{b.name}
						</button>
					{/each}
				</div>

				<!-- Download -->
				<div class="flex items-center gap-3">
					{#if selectedBrowser === 'safari'}
						<a href={getExtensionDownloadUrl(selectedBrowser)} download={getDownloadFilename(selectedBrowser)}
							class="inline-flex items-center gap-2 px-3.5 py-2 text-[11px] font-medium rounded-lg bg-zinc-100 text-zinc-600 hover:bg-zinc-200 transition-colors"
						>
							<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg>
							Download Safari Build
						</a>
						<span class="text-[10px] text-zinc-400">Requires macOS + Xcode</span>
					{:else}
						<a href={getExtensionDownloadUrl(selectedBrowser)} download={getDownloadFilename(selectedBrowser)}
							class="inline-flex items-center gap-2 px-3.5 py-2 text-[11px] font-medium rounded-lg bg-zinc-800 text-white hover:bg-zinc-700 transition-colors"
						>
							<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg>
							Download for {browsers.find(b => b.key === selectedBrowser)?.name}
						</a>
						<span class="text-[10px] text-zinc-400">~140 KB · {getDownloadExt(selectedBrowser)}</span>
					{/if}
				</div>

				<!-- Installation steps -->
				<div class="border-t border-zinc-100 pt-4">
					<div class="text-[11px] font-semibold text-zinc-500 uppercase tracking-wider mb-3">
						Installation Steps
					</div>

					{#if selectedBrowser === 'chrome' || selectedBrowser === 'brave'}
						{@const browserName = browsers.find(b => b.key === selectedBrowser)?.name}
						{@const extUrl = selectedBrowser === 'chrome' ? 'chrome://extensions' : 'brave://extensions'}
						<ol class="space-y-2.5">
							{#each [
								{ title: 'Extract the ZIP file', desc: `Unzip to a permanent location (e.g., Documents/WeaveMind-Extension). Do not delete this folder, ${browserName} needs it.` },
								{ title: 'Open Extensions Page', desc: extUrl, isCode: true },
								{ title: 'Enable Developer Mode', desc: 'Toggle the "Developer mode" switch in the top-right corner' },
								{ title: 'Load the Extension', desc: 'Click "Load unpacked" and select the extracted folder containing manifest.json' },
								{ title: 'Done', desc: 'The extension persists after restarts. Dismiss the "Disable developer mode extensions" popup if it appears.' },
							] as step, i}
								<li class="flex gap-2.5">
									<span class="flex-shrink-0 w-5 h-5 rounded-full bg-zinc-100 text-zinc-500 text-[10px] font-bold flex items-center justify-center mt-0.5">{i + 1}</span>
									<div>
										<p class="text-[11px] font-medium text-zinc-700">{step.title}</p>
										{#if step.isCode}
											<p class="text-[11px] text-zinc-400 mt-0.5">Go to <code class="px-1 py-0.5 bg-zinc-100 rounded text-[10px] text-zinc-600">{step.desc}</code></p>
										{:else}
											<p class="text-[11px] text-zinc-400 mt-0.5">{step.desc}</p>
										{/if}
									</div>
								</li>
							{/each}
						</ol>
					{:else if selectedBrowser === 'firefox'}
						<ol class="space-y-2.5">
							<li class="flex gap-2.5">
								<span class="flex-shrink-0 w-5 h-5 rounded-full bg-zinc-100 text-zinc-500 text-[10px] font-bold flex items-center justify-center mt-0.5">1</span>
								<div>
									<p class="text-[11px] font-medium text-zinc-700">Download the Extension</p>
									<p class="text-[11px] text-zinc-400 mt-0.5">Click the download button above to get the signed .xpi file</p>
								</div>
							</li>
							<li class="flex gap-2.5">
								<span class="flex-shrink-0 w-5 h-5 rounded-full bg-zinc-100 text-zinc-500 text-[10px] font-bold flex items-center justify-center mt-0.5">2</span>
								<div>
									<p class="text-[11px] font-medium text-zinc-700">Install the Extension</p>
									<p class="text-[11px] text-zinc-400 mt-0.5">Firefox will prompt you to install. Click "Add" to confirm.</p>
								</div>
							</li>
							<li class="flex gap-2.5">
								<span class="flex-shrink-0 w-5 h-5 rounded-full bg-zinc-100 text-zinc-500 text-[10px] font-bold flex items-center justify-center mt-0.5">3</span>
								<div>
									<p class="text-[11px] font-medium text-zinc-700">Done</p>
									<p class="text-[11px] text-zinc-400 mt-0.5">Permanently installed (signed by Mozilla), persists after restarts.</p>
								</div>
							</li>
						</ol>
						<div class="mt-3 px-3 py-2 bg-zinc-50 border border-zinc-200 rounded-lg">
							<p class="text-[10px] text-zinc-500">
								<span class="font-medium">Alternative:</span> Go to <code class="px-1 py-0.5 bg-zinc-100 rounded text-[10px]">about:addons</code>, click the gear icon, select "Install Add-on From File..."
							</p>
						</div>
					{:else if selectedBrowser === 'safari'}
						<ol class="space-y-2.5">
							{#each [
								{ title: 'Requirements', desc: 'You need macOS with Xcode and Command Line Tools installed.' },
								{ title: 'Extract and Convert', desc: 'Unzip, then run: xcrun safari-web-extension-converter /path/to/folder' },
								{ title: 'Build in Xcode', desc: 'Click Run (Cmd+R) to build and install.' },
								{ title: 'Enable in Safari', desc: 'Settings > Extensions > enable WeaveMind. May need "Allow Unsigned Extensions" from Develop menu.' },
							] as step, i}
								<li class="flex gap-2.5">
									<span class="flex-shrink-0 w-5 h-5 rounded-full bg-zinc-100 text-zinc-500 text-[10px] font-bold flex items-center justify-center mt-0.5">{i + 1}</span>
									<div>
										<p class="text-[11px] font-medium text-zinc-700">{step.title}</p>
										<p class="text-[11px] text-zinc-400 mt-0.5">{step.desc}</p>
									</div>
								</li>
							{/each}
						</ol>
						<div class="mt-3 px-3 py-2 bg-amber-50 border border-amber-200 rounded-lg">
							<p class="text-[10px] text-amber-700">Safari extensions require a native app wrapper built with Xcode. This is an Apple requirement.</p>
						</div>
					{:else}
						{@const extUrl = selectedBrowser === 'opera' ? 'opera://extensions' : 'edge://extensions'}
						{@const devModePos = selectedBrowser === 'edge' ? 'bottom-left' : 'top-right'}
						<ol class="space-y-2.5">
							{#each [
								{ title: 'Extract the ZIP file', desc: 'Unzip to a permanent location. Do not delete this folder.' },
								{ title: 'Open Extensions Page', desc: extUrl, isCode: true },
								{ title: 'Enable Developer Mode', desc: `Toggle "Developer mode" in the ${devModePos} corner` },
								{ title: 'Load the Extension', desc: 'Click "Load unpacked" and select the extracted folder containing manifest.json' },
								{ title: 'Done', desc: 'The extension persists after restarts.' },
							] as step, i}
								<li class="flex gap-2.5">
									<span class="flex-shrink-0 w-5 h-5 rounded-full bg-zinc-100 text-zinc-500 text-[10px] font-bold flex items-center justify-center mt-0.5">{i + 1}</span>
									<div>
										<p class="text-[11px] font-medium text-zinc-700">{step.title}</p>
										{#if step.isCode}
											<p class="text-[11px] text-zinc-400 mt-0.5">Go to <code class="px-1 py-0.5 bg-zinc-100 rounded text-[10px] text-zinc-600">{step.desc}</code></p>
										{:else}
											<p class="text-[11px] text-zinc-400 mt-0.5">{step.desc}</p>
										{/if}
									</div>
								</li>
							{/each}
						</ol>
					{/if}
				</div>
			</div>
		</div>

		<!-- Step 2: Token -->
		{#if isLoading}
			<div class="flex items-center justify-center py-16">
				<div class="h-5 w-5 border-2 border-zinc-300 border-t-zinc-600 rounded-full animate-spin"></div>
			</div>
		{:else if !userId}
			<div class="bg-white rounded-xl border border-zinc-200 px-5 py-12 text-center" style="box-shadow: 0 1px 3px rgba(0,0,0,0.06);">
				<p class="text-[13px] text-zinc-600">User not identified</p>
				<p class="text-[11px] text-zinc-400 mt-1">Please reload the application</p>
			</div>
		{:else}
			<div class="bg-white rounded-xl border border-zinc-200 overflow-hidden mb-4" style="box-shadow: 0 1px 3px rgba(0,0,0,0.06);">
				<div class="px-5 py-3 border-b border-zinc-100 flex items-center gap-2">
					<span class="flex items-center justify-center w-5 h-5 rounded-full bg-zinc-800 text-white text-[10px] font-bold">2</span>
					<span class="text-[12px] font-semibold text-zinc-700">Configure Extension Token</span>
				</div>

				<div class="px-5 py-4 space-y-4">
					<p class="text-[11px] text-zinc-400">
						Generate a token to connect the extension to your account. Each token allows the extension to receive and respond to human-in-the-loop tasks.
					</p>

					<!-- Security tip -->
					<div class="px-3 py-2 bg-amber-50 border border-amber-200 rounded-lg">
						<p class="text-[10px] text-amber-700">
							<span class="font-semibold">Security tip:</span> Your token name becomes part of the URL. Avoid guessable names. Use the random generator for better security.
						</p>
					</div>

					<!-- Create token -->
					<div class="space-y-2">
						<div class="flex items-center gap-2">
							<span class="text-[11px] text-zinc-400 font-mono shrink-0">{username}_</span>
							<input
								type="text"
								bind:value={newKeyName}
								placeholder="my-extension-key"
								class="flex-1 px-2.5 py-1.5 text-[11px] border border-zinc-200 rounded-lg bg-white font-mono text-zinc-700 focus:outline-none focus:border-zinc-400 transition-colors"
								disabled={isCreatingToken}
							/>
							<button
								onclick={fillRandomName}
								class="px-2.5 py-1.5 text-[10px] font-medium bg-zinc-100 text-zinc-600 rounded-lg hover:bg-zinc-200 transition-colors whitespace-nowrap"
							>Random</button>
						</div>
						<button
							onclick={createToken}
							disabled={isCreatingToken || !newKeyName.trim()}
							class="px-3 py-1.5 text-[11px] font-medium rounded-lg bg-zinc-800 text-white hover:bg-zinc-700 transition-colors disabled:opacity-40 disabled:cursor-default"
						>
							{isCreatingToken ? 'Creating...' : 'Generate Token'}
						</button>
					</div>

					<!-- Token list -->
					{#if tokens.length === 0}
						<div class="border border-dashed border-zinc-200 rounded-lg px-4 py-6 text-center">
							<p class="text-[12px] text-zinc-500">No extension tokens yet</p>
							<p class="text-[10px] text-zinc-400 mt-0.5">Generate a token above</p>
						</div>
					{:else}
						<div class="space-y-1.5">
							{#each tokens as token}
								<div class="border border-zinc-200 rounded-lg px-3 py-2.5">
									<div class="flex items-start justify-between gap-3">
										<div class="min-w-0 flex-1">
											<p class="text-[12px] font-medium text-zinc-700">{token.name || 'Unnamed Token'}</p>
											<p class="text-[10px] text-zinc-400 font-mono mt-0.5 truncate">{token.id}</p>
											<div class="flex gap-3 mt-1">
												<span class="text-[10px] text-zinc-400">Created {formatTimeAgo(token.createdAt)}</span>
												<span class="text-[10px] text-zinc-400">Used {formatTimeAgo(token.lastUsedAt)}</span>
											</div>
										</div>
										<div class="flex gap-1.5 shrink-0">
											<CopyButton text={getTokenUrl(token.id)} />
											<button
												onclick={() => deleteToken(token.id)}
												class="px-2 py-1 text-[10px] font-medium rounded-md border border-red-200 text-red-600 hover:bg-red-50 transition-colors"
											>Delete</button>
										</div>
									</div>
								</div>
							{/each}
						</div>
					{/if}
				</div>
			</div>

			<!-- Step 3: Connect -->
			<div class="bg-white rounded-xl border border-zinc-200 overflow-hidden" style="box-shadow: 0 1px 3px rgba(0,0,0,0.06);">
				<div class="px-5 py-3 border-b border-zinc-100 flex items-center gap-2">
					<span class="flex items-center justify-center w-5 h-5 rounded-full bg-zinc-800 text-white text-[10px] font-bold">3</span>
					<span class="text-[12px] font-semibold text-zinc-700">Connect the Extension</span>
				</div>
				<div class="px-5 py-4">
					<ol class="space-y-1.5">
						{#each [
							'Generate a token above and click "Copy URL"',
							'Click the WeaveMind extension icon in your browser toolbar',
							'Paste the URL in the extension settings',
							'The extension will now receive tasks from your projects',
						] as step, i}
							<li class="flex items-start gap-2">
								<span class="text-[10px] text-zinc-400 font-mono mt-0.5 shrink-0">{i + 1}.</span>
								<span class="text-[11px] text-zinc-500">{step}</span>
							</li>
						{/each}
					</ol>
				</div>
			</div>
		{/if}
	</div>
</div>
