<script lang="ts">
	import type { Brick, RunnerMode, ProjectDefinition, LiveDataItem } from '$lib/types';
	import BlockList from './BlockList.svelte';
	import TabsBrick from './TabsBrick.svelte';
	import {
		parseSize,
		paddingValue,
		gapValue,
		parseColumnRatio,
		radiusValue,
		shadowValue,
		blurValue,
	} from './sizing';

	let {
		brick,
		mode,
		project,
		renderMarkdown,
		onUpdateNodeConfig,
		executionState,
		infraLiveData,
		infraStatus,
	}: {
		brick: Brick;
		mode: RunnerMode;
		project: ProjectDefinition;
		renderMarkdown: (s: string) => string;
		onUpdateNodeConfig: (nodeId: string, config: Record<string, unknown>) => void;
		executionState: { isRunning: boolean; nodeOutputs?: Record<string, unknown> };
		infraLiveData?: Record<string, LiveDataItem[]>;
		infraStatus: string;
	} = $props();

	const p = $derived(brick.props);
	function str(key: string, fallback = ''): string {
		const v = p[key];
		return typeof v === 'string' ? v : fallback;
	}
	function bool(key: string): boolean {
		const v = p[key];
		return v === true || v === 'true';
	}

	// Hero sizing → font size.
	function heroTitleSize(size: string): string {
		switch (size) {
			case 'sm': return 'text-3xl sm:text-4xl';
			case 'md': return 'text-4xl sm:text-5xl';
			case 'lg': return 'text-5xl sm:text-6xl';
			case 'xl': return 'text-6xl sm:text-7xl';
			case '2xl': return 'text-7xl sm:text-8xl';
			default: return 'text-5xl sm:text-6xl';
		}
	}
	function heroPadding(size: string): string {
		switch (size) {
			case 'sm': return 'py-8';
			case 'md': return 'py-12';
			case 'lg': return 'py-20';
			case 'xl': return 'py-28';
			case '2xl': return 'py-32';
			default: return 'py-16';
		}
	}
	function heroAlign(align: string): string {
		if (align === 'left') return 'text-left items-start';
		if (align === 'right') return 'text-right items-end';
		return 'text-center items-center';
	}
</script>

{#if brick.kind === 'hero'}
	{@const size = str('size', 'md')}
	{@const align = str('align', 'center')}
	{@const bg = str('background', 'none')}
	{@const bgStyle = bg.startsWith('image:') ? `background-image: url(${bg.slice('image:'.length)}); background-size: cover; background-position: center` : bg.startsWith('solid:') ? `background: ${bg.slice('solid:'.length)}` : bg === 'gradient' ? 'background: linear-gradient(135deg, color-mix(in srgb, var(--runner-primary) 20%, var(--runner-bg)), var(--runner-bg))' : ''}
	<section class="flex flex-col gap-4 rounded-3xl {heroPadding(size)} {heroAlign(align)}" style={bgStyle}>
		{#if str('eyebrow')}
			<div class="text-xs uppercase tracking-widest font-semibold" style="color: var(--runner-primary)">{str('eyebrow')}</div>
		{/if}
		<h1 class="{heroTitleSize(size)} font-bold tracking-tight leading-[1.05]" style="color: var(--runner-fg)">{str('title')}</h1>
		{#if str('subtitle')}
			<p class="text-lg sm:text-xl max-w-2xl leading-relaxed" style="color: var(--runner-muted)">{str('subtitle')}</p>
		{/if}
		{#if str('image')}
			<img src={str('image')} alt={str('title')} class="mt-6 max-w-xl w-full rounded-2xl shadow-lg" />
		{/if}
	</section>

{:else if brick.kind === 'navbar'}
	{@const sticky = bool('sticky')}
	<nav
		class="flex flex-wrap items-center justify-between gap-x-4 gap-y-2 rounded-2xl px-5 py-3 {sticky ? 'sticky top-2 z-10 backdrop-blur' : ''}"
		style="background: var(--runner-card-bg); border: var(--runner-card-border); box-shadow: var(--runner-card-shadow)"
	>
		{#if brick.children}
			<div class="flex flex-wrap items-center gap-x-6 gap-y-2 flex-1 min-w-0">
				<BlockList blocks={brick.children} {mode} {project} {renderMarkdown} {onUpdateNodeConfig} {executionState} {infraLiveData} {infraStatus} />
			</div>
		{:else if str('title')}
			<span class="font-semibold truncate" style="color: var(--runner-fg)">{str('title')}</span>
		{/if}
	</nav>

{:else if brick.kind === 'navlink'}
	<a
		href={str('href', '#')}
		class="text-sm font-medium transition-opacity hover:opacity-70"
		style="color: var(--runner-fg)"
	>{str('label') || str('content')}</a>

{:else if brick.kind === 'logo'}
	{#if str('src')}
		<img src={str('src')} alt={str('alt', 'Logo')} class="h-8 w-auto" />
	{:else}
		<div class="flex items-center gap-2">
			<div class="w-8 h-8 rounded-lg" style="background: linear-gradient(135deg, var(--runner-primary), var(--runner-accent))"></div>
			<span class="font-bold text-lg" style="color: var(--runner-fg)">{str('name') || str('content')}</span>
		</div>
	{/if}

{:else if brick.kind === 'banner'}
	{@const variant = str('variant', 'info')}
	{@const palette = variant === 'warning' ? 'bg-amber-50 border-amber-200 text-amber-900' : variant === 'error' ? 'bg-red-50 border-red-200 text-red-900' : variant === 'success' ? 'bg-emerald-50 border-emerald-200 text-emerald-900' : 'bg-blue-50 border-blue-200 text-blue-900'}
	<div class="rounded-xl border px-4 py-3 text-sm {palette}">{str('text') || str('content')}</div>

{:else if brick.kind === 'text'}
	<div class="max-w-none runner-markdown" style="color: var(--runner-fg)">{@html renderMarkdown(str('content'))}</div>

{:else if brick.kind === 'heading'}
	{@const level = Number(str('level', '2'))}
	{#if level === 1}
		<h1 class="text-4xl font-bold tracking-tight" style="color: var(--runner-fg)">{str('content')}</h1>
	{:else if level === 2}
		<h2 class="text-2xl font-semibold tracking-tight" style="color: var(--runner-fg)">{str('content')}</h2>
	{:else if level === 3}
		<h3 class="text-xl font-semibold" style="color: var(--runner-fg)">{str('content')}</h3>
	{:else}
		<h4 class="text-lg font-semibold" style="color: var(--runner-fg)">{str('content')}</h4>
	{/if}

{:else if brick.kind === 'divider'}
	<hr class="border-0 h-px" style="background: var(--runner-card-border)" />

{:else if brick.kind === 'image'}
	{@const h = parseSize(str('height'))}
	<img
		src={str('src')}
		alt={str('alt', '')}
		class="w-full rounded-2xl"
		style={[
			'object-fit: ' + (str('fit', 'cover')),
			h ? 'height: ' + h : '',
			'border: var(--runner-card-border)',
		].filter(Boolean).join('; ')}
	/>

{:else if brick.kind === 'video'}
	{@const h = parseSize(str('height'))}
	<!-- svelte-ignore a11y_media_has_caption -->
	<video
		src={str('src')}
		controls
		class="w-full rounded-2xl"
		style={[h ? 'height: ' + h : '', 'border: var(--runner-card-border)'].filter(Boolean).join('; ')}
	></video>

{:else if brick.kind === 'embed'}
	{@const h = parseSize(str('height')) ?? '480px'}
	<div class="w-full rounded-2xl overflow-hidden" style="height: {h}; border: var(--runner-card-border)">
		<iframe src={str('url')} title={str('title', 'Embedded content')} class="w-full h-full" allowfullscreen></iframe>
	</div>

{:else if brick.kind === 'quote'}
	<blockquote class="border-l-4 pl-6 italic text-lg" style="border-color: var(--runner-primary); color: var(--runner-muted)">
		<p>"{str('content')}"</p>
		{#if str('author')}
			<footer class="text-sm mt-3 not-italic font-medium" style="color: var(--runner-fg)">{str('author')}</footer>
		{/if}
	</blockquote>

{:else if brick.kind === 'stat'}
	<div class="text-center">
		<div class="text-4xl font-bold tracking-tight" style="color: var(--runner-fg)">{str('value')}</div>
		<div class="text-xs uppercase tracking-widest mt-2" style="color: var(--runner-muted)">{str('label')}</div>
	</div>

{:else if brick.kind === 'stats'}
	<div class="grid grid-cols-2 sm:grid-cols-3 gap-6 py-6">
		{#if brick.children}
			<BlockList blocks={brick.children} {mode} {project} {renderMarkdown} {onUpdateNodeConfig} {executionState} {infraLiveData} {infraStatus} />
		{/if}
	</div>

{:else if brick.kind === 'feature'}
	<div class="space-y-3">
		{#if str('icon')}
			<div class="w-12 h-12 rounded-xl flex items-center justify-center text-xl" style="background: color-mix(in srgb, var(--runner-primary) 15%, transparent); color: var(--runner-primary)">●</div>
		{/if}
		<h3 class="font-semibold text-lg" style="color: var(--runner-fg)">{str('title')}</h3>
		<p class="text-sm leading-relaxed" style="color: var(--runner-muted)">{str('content')}</p>
	</div>

{:else if brick.kind === 'feature-grid'}
	<div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-8 py-6">
		{#if brick.children}
			<BlockList blocks={brick.children} {mode} {project} {renderMarkdown} {onUpdateNodeConfig} {executionState} {infraLiveData} {infraStatus} />
		{/if}
	</div>

{:else if brick.kind === 'faq'}
	<div class="space-y-3">
		{#if brick.children}
			<BlockList blocks={brick.children} {mode} {project} {renderMarkdown} {onUpdateNodeConfig} {executionState} {infraLiveData} {infraStatus} />
		{/if}
	</div>

{:else if brick.kind === 'qa'}
	<details class="rounded-xl p-4 group" style="background: var(--runner-card-bg); border: var(--runner-card-border)">
		<summary class="font-medium cursor-pointer list-none flex items-center justify-between" style="color: var(--runner-fg)">
			{str('q')}
			<span style="color: var(--runner-muted)" class="group-open:rotate-180 transition-transform">▾</span>
		</summary>
		<p class="mt-3 text-sm leading-relaxed" style="color: var(--runner-muted)">{str('a')}</p>
	</details>

{:else if brick.kind === 'testimonial'}
	<figure class="rounded-2xl p-6" style="background: var(--runner-card-bg); border: var(--runner-card-border); box-shadow: var(--runner-card-shadow)">
		<blockquote class="text-base leading-relaxed" style="color: var(--runner-fg)">"{str('content')}"</blockquote>
		<figcaption class="mt-4 flex items-center gap-3">
			{#if str('avatar')}
				<img src={str('avatar')} alt={str('author')} class="w-10 h-10 rounded-full" />
			{/if}
			<div>
				<div class="font-medium" style="color: var(--runner-fg)">{str('author')}</div>
				{#if str('role')}
					<div class="text-xs" style="color: var(--runner-muted)">{str('role')}</div>
				{/if}
			</div>
		</figcaption>
	</figure>

{:else if brick.kind === 'badge'}
	{@const variant = str('variant', 'default')}
	{@const palette = variant === 'success' ? 'bg-emerald-100 text-emerald-900' : variant === 'warning' ? 'bg-amber-100 text-amber-900' : variant === 'error' ? 'bg-red-100 text-red-900' : variant === 'primary' ? '' : 'bg-zinc-100 text-zinc-900'}
	{#if variant === 'primary'}
		<span class="inline-block text-xs font-semibold px-3 py-1 rounded-full max-w-full truncate align-middle" style="background: color-mix(in srgb, var(--runner-primary) 15%, transparent); color: var(--runner-primary)">{str('text') || str('content')}</span>
	{:else}
		<span class="inline-block text-xs font-semibold px-3 py-1 rounded-full max-w-full truncate align-middle {palette}">{str('text') || str('content')}</span>
	{/if}

{:else if brick.kind === 'spacer'}
	{@const size = str('size', 'md')}
	<div class={size === 'xs' ? 'h-2' : size === 'sm' ? 'h-4' : size === 'lg' ? 'h-16' : size === 'xl' ? 'h-24' : size === '2xl' ? 'h-32' : 'h-8'}></div>

{:else if brick.kind === 'section'}
	{@const pad = paddingValue(str('padding'), 'lg')}
	{@const rad = radiusValue(str('radius'), 'xl')}
	{@const bg = str('background', 'transparent')}
	{@const gap = gapValue(str('gap'), 'lg')}
	{@const bgCss = bg === 'card' ? 'var(--runner-card-bg)' : bg === 'primary' ? 'color-mix(in srgb, var(--runner-primary) 10%, transparent)' : bg === 'subtle' ? 'color-mix(in srgb, var(--runner-fg) 4%, transparent)' : bg}
	<section class="w-full flex flex-col" style="padding: {pad}; border-radius: {rad}; background: {bgCss}; gap: {gap}">
		{#if str('title')}
			<h2 class="text-2xl font-bold" style="color: var(--runner-fg)">{str('title')}</h2>
		{/if}
		{#if brick.children}
			<BlockList blocks={brick.children} {mode} {project} {renderMarkdown} {onUpdateNodeConfig} {executionState} {infraLiveData} {infraStatus} />
		{/if}
	</section>

{:else if brick.kind === 'columns'}
	{@const cols = Number(str('cols', '2'))}
	{@const ratio = parseColumnRatio(str('ratio'), cols)}
	{@const gap = gapValue(str('gap'), 'md')}
	{@const align = str('align', 'stretch')}
	{@const alignClass = align === 'start' ? 'items-start' : align === 'center' ? 'items-center' : align === 'end' ? 'items-end' : 'items-stretch'}
	{@const responsive = str('responsive', 'collapse')}
	{@const gridCols = ratio ?? `repeat(${cols}, minmax(0, 1fr))`}
	<div
		class="grid {alignClass} {responsive === 'keep' ? '' : 'max-sm:!grid-cols-1'}"
		style="grid-template-columns: {gridCols}; gap: {gap}"
	>
		{#if brick.children}
			<BlockList blocks={brick.children} {mode} {project} {renderMarkdown} {onUpdateNodeConfig} {executionState} {infraLiveData} {infraStatus} />
		{/if}
	</div>

{:else if brick.kind === 'card'}
	{@const pad = paddingValue(str('padding'), 'lg')}
	{@const rad = radiusValue(str('radius'), 'xl')}
	{@const sh = shadowValue(str('shadow'), str('elevated') === 'true' ? 'lg' : 'sm')}
	{@const blur = blurValue(str('blur'), str('glass') === 'true' ? 'md' : 'none')}
	{@const glass = bool('glass') || bool('elevated')}
	{@const borderStrength = str('border', 'subtle')}
	{@const borderCss = borderStrength === 'none' ? 'none' : borderStrength === 'strong' ? '2px solid var(--runner-card-border)' : '1px solid var(--runner-card-border)'}
	{@const bgCss = glass ? 'color-mix(in srgb, var(--runner-card-bg) 92%, transparent)' : 'var(--runner-card-bg)'}
	{@const h = parseSize(str('height'))}
	<div
		class="flex flex-col gap-4"
		style={[
			`padding: ${pad}`,
			`border-radius: ${rad}`,
			`box-shadow: ${sh}`,
			`background: ${bgCss}`,
			`border: ${borderCss}`,
			glass ? `backdrop-filter: blur(${blur})` : '',
			glass ? `-webkit-backdrop-filter: blur(${blur})` : '',
			h ? `height: ${h}` : '',
		].filter(Boolean).join('; ')}
	>
		{#if str('title')}
			<h3 class="text-lg font-semibold tracking-tight" style="color: var(--runner-fg)">{str('title')}</h3>
		{/if}
		{#if brick.children}
			<BlockList blocks={brick.children} {mode} {project} {renderMarkdown} {onUpdateNodeConfig} {executionState} {infraLiveData} {infraStatus} />
		{/if}
	</div>

{:else if brick.kind === 'tabs'}
	<TabsBrick {brick} {mode} {project} {renderMarkdown} {onUpdateNodeConfig} {executionState} {infraLiveData} {infraStatus} />

{:else if brick.kind === 'cta'}
	{@const action = str('action', 'run')}
	{@const size = str('size', 'md')}
	{@const sizeClass = size === 'sm' ? 'px-4 py-2 text-sm' : size === 'lg' ? 'px-8 py-4 text-base' : size === 'xl' ? 'px-10 py-5 text-lg' : 'px-6 py-3 text-sm'}
	{@const align = str('align', 'center')}
	{@const alignClass = align === 'left' ? 'justify-start' : align === 'right' ? 'justify-end' : 'justify-center'}
	<div class="flex {alignClass} py-4 w-full">
		{#if action.startsWith('link:')}
			<a
				href={action.slice('link:'.length)}
				class="inline-flex items-center justify-center text-center rounded-xl font-semibold transition-all hover:scale-[1.02] hover:shadow-lg max-w-full break-words leading-tight {sizeClass}"
				style="background: linear-gradient(135deg, var(--runner-primary), color-mix(in srgb, var(--runner-primary) 70%, var(--runner-accent))); color: white; box-shadow: 0 8px 24px -6px color-mix(in srgb, var(--runner-primary) 50%, transparent)"
			>{str('label', 'Learn more')}</a>
		{:else}
			<button
				type="button"
				class="inline-flex items-center justify-center text-center rounded-xl font-semibold transition-all hover:scale-[1.02] hover:shadow-lg disabled:opacity-50 disabled:cursor-not-allowed max-w-full break-words leading-tight {sizeClass}"
				style="background: linear-gradient(135deg, var(--runner-primary), color-mix(in srgb, var(--runner-primary) 70%, var(--runner-accent))); color: white; box-shadow: 0 8px 24px -6px color-mix(in srgb, var(--runner-primary) 50%, transparent)"
				onclick={() => window.dispatchEvent(new CustomEvent('runner:cta-run'))}
				disabled={executionState.isRunning}
			>
				{#if executionState.isRunning}
					<span class="inline-block w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin mr-2 flex-shrink-0"></span>
					Running…
				{:else}
					{str('label', 'Run')}
				{/if}
			</button>
		{/if}
	</div>

{:else if brick.kind === 'footer'}
	<footer class="mt-16 pt-8 text-center text-xs space-y-2" style="color: var(--runner-muted); border-top: var(--runner-card-border)">
		{#if str('note')}
			<div>{str('note')}</div>
		{/if}
		<div>
			Built with
			<a href="https://weavemind.ai" target="_blank" rel="noopener" class="font-semibold hover:underline" style="color: var(--runner-primary)">WeaveMind</a>
		</div>
	</footer>
{/if}
