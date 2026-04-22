<script lang="ts">
	import type { SetupItem, ProjectDefinition, FileRef } from '$lib/types';
	import { NODE_TYPE_CONFIG } from '$lib/nodes';
	import { isApiKeyReady } from '$lib/validation';
	import { createFieldEditor } from '$lib/utils/field-editor.svelte';
	import { HelpCircle } from '@lucide/svelte';
	import BlobField from '../project/BlobField.svelte';
	import { sizingStyle, parseSize } from './sizing';
	import {
		decodeBool, encodeBool,
		decodeList, encodeList,
		resolveOptions,
	} from './field-adapters';

	let {
		item,
		project,
		renderMarkdown,
		onUpdateNodeConfig,
	}: {
		item: SetupItem;
		project: ProjectDefinition;
		renderMarkdown: (s: string) => string;
		onUpdateNodeConfig: (nodeId: string, config: Record<string, unknown>) => void;
	} = $props();

	const fieldEditor = createFieldEditor();
	let helpOpen = $state(false);

	function getFieldDef() {
		const node = project.nodes.find(n => n.id === item.nodeId);
		if (!node) return null;
		const template = NODE_TYPE_CONFIG[node.nodeType];
		if (!template) return null;
		return template.fields.find(f => f.key === item.fieldKey) ?? null;
	}

	function getSetupGuide(): string[] {
		const node = project.nodes.find(n => n.id === item.nodeId);
		if (!node) return [];
		const template = NODE_TYPE_CONFIG[node.nodeType];
		return template?.setupGuide ?? [];
	}

	function getValue(): unknown {
		const node = project.nodes.find(n => n.id === item.nodeId);
		if (!node) return '';
		const val = node.config[item.fieldKey];
		if (val === undefined || val === null) return '';
		return val;
	}

	function setValue(value: unknown) {
		const node = project.nodes.find(n => n.id === item.nodeId);
		if (!node) return;
		onUpdateNodeConfig(item.nodeId, { ...node.config, [item.fieldKey]: value });
	}

	function stringifyValue(val: unknown): string {
		if (val === undefined || val === null) return '';
		if (typeof val === 'string') return val;
		return JSON.stringify(val, null, 2);
	}

	function saveString(v: string) { setValue(v); }
	function saveNumber(v: string) { setValue(v === '' ? '' : Number(v)); }

	const field = $derived(getFieldDef());
	const value = $derived(getValue());
	const guide = $derived(getSetupGuide());
	const label = $derived(item.label ?? field?.label ?? item.fieldKey);
	const description = $derived(item.description ?? field?.description);
	const variant = $derived(item.as);

	// Cross-type adapter helpers. Each derives the UI value from the raw
	// stored value through an adapter so that variants work on compatible
	// field types, not just their native one.
	const boolValue = $derived(decodeBool(value));
	const listValue = $derived(decodeList(value));
	const options = $derived(resolveOptions(item, field));

	// Runtime validation: a picker variant with no resolved options can't
	// render anything useful. Surface an inline error so the AI (on retry)
	// and the human see what's wrong, instead of silently falling through.
	const PICKER_VARIANTS = ['select', 'radio', 'cards', 'multiselect', 'multicards'];
	const missingOptions = $derived(
		variant !== undefined && PICKER_VARIANTS.includes(variant) && options.length === 0,
	);

	function saveBool(v: boolean) {
		if (!field) return;
		setValue(encodeBool(v, field.type));
	}
	function saveList(v: string[]) {
		if (!field) return;
		setValue(encodeList(v, field.type));
	}
	function toggleListValue(opt: string) {
		if (listValue.includes(opt)) {
			saveList(listValue.filter(x => x !== opt));
		} else {
			saveList([...listValue, opt]);
		}
	}

	// Default maxLength for textarea-style fields when the node doesn't
	// declare one. High enough to never cap real usage, just surfaces a live
	// counter so visitors know they have headroom.
	const DEFAULT_TEXTAREA_MAX_LENGTH = 10000;

	// Character count for text/textarea fields. Read straight from the current
	// value so it stays live as the user types.
	const charCount = $derived.by(() => {
		if (typeof value === 'string') return value.length;
		if (value === null || value === undefined || value === '') return 0;
		return String(value).length;
	});

	// Effective maxLength: explicit field value, or the textarea default, or
	// none (single-line text inputs keep no cap unless declared).
	const effectiveMaxLength = $derived.by(() => {
		if (typeof field?.maxLength === 'number') return field.maxLength;
		if (field?.type === 'textarea' || item.as === 'textarea') return DEFAULT_TEXTAREA_MAX_LENGTH;
		return undefined;
	});
	const hasMaxLength = $derived(typeof effectiveMaxLength === 'number');
	const overLimit = $derived(hasMaxLength && charCount > (effectiveMaxLength as number));

	function errorFor(): string | null {
		if (!field) return null;
		const v = getValue();
		if (field.type === 'api_key') {
			if (!isApiKeyReady(item.fieldKey, { [item.fieldKey]: v })) return 'Own key selected but not entered';
			return null;
		}
		if (field.type === 'password') {
			if (!v || (typeof v === 'string' && v.trim() === '')) return 'This field is required';
			return null;
		}
		// Length constraints on text fields. Use the effective max so the
		// textarea default cap also gets enforced, not just shown. Only
		// apply when the item is actually rendered as a text input variant
		// (not a toggle, radio, slider, etc. on a Text-type node).
		const isTextVariant = item.as === undefined
			? (field.type === 'text' || field.type === 'textarea')
			: (item.as === 'text' || item.as === 'textarea' || item.as === 'password' || item.as === 'email' || item.as === 'url');
		if (isTextVariant && typeof v === 'string') {
			const max = typeof field.maxLength === 'number'
				? field.maxLength
				: (field.type === 'textarea' || item.as === 'textarea')
					? DEFAULT_TEXTAREA_MAX_LENGTH
					: undefined;
			if (typeof max === 'number' && v.length > max) {
				return `Too long (${v.length}/${max})`;
			}
			if (typeof field.minLength === 'number' && v.length > 0 && v.length < field.minLength) {
				return `Too short (${v.length}/${field.minLength} minimum)`;
			}
			if (typeof field.pattern === 'string' && v.length > 0) {
				try {
					const re = new RegExp(`^(?:${field.pattern})$`);
					if (!re.test(v)) return 'Invalid format';
				} catch {
					// Malformed pattern in node definition, ignore.
				}
			}
		}
		// Range constraints on number fields.
		if (field.type === 'number' && typeof v === 'number') {
			if (typeof field.min === 'number' && v < field.min) return `Must be at least ${field.min}`;
			if (typeof field.max === 'number' && v > field.max) return `Must be at most ${field.max}`;
		}
		return null;
	}
	const error = $derived(errorFor());
</script>

{#if field}
	{@const chrome = item.chrome ?? 'none'}
	{@const isStretching = variant === 'textarea' || (!variant && field.type === 'textarea') || parseSize(item.height) !== undefined}
	{@const stretchClass = isStretching ? 'h-full flex flex-col' : ''}
	{@const wrapperClass = chrome === 'card' ? `rounded-xl p-4 space-y-2 ${stretchClass}` : chrome === 'subtle' ? 'py-3 space-y-2 border-b' : `space-y-2 ${stretchClass}`}
	{@const wrapperStyle = chrome === 'card' ? 'background: var(--runner-card-bg); border: var(--runner-card-border)' : chrome === 'subtle' ? 'border-color: var(--runner-card-border)' : ''}
	{@const textareaHeight = parseSize(item.height) ?? ((variant === 'textarea' || (!variant && field.type === 'textarea')) ? '200px' : undefined)}
	{@const inputBaseStyle = 'background: var(--runner-input-bg); border: 1px solid var(--runner-input-border); color: var(--runner-fg)'}
	{@const inputBaseClass = 'w-full text-sm px-4 py-2.5 rounded-xl outline-none transition-all focus:ring-2'}
	{@const ringStyle = error || overLimit ? '--tw-ring-color: rgb(248 113 113 / 0.5)' : '--tw-ring-color: color-mix(in srgb, var(--runner-primary) 50%, transparent)'}
	<div class={wrapperClass} style={wrapperStyle}>
		<div class="flex items-start justify-between gap-2">
			<div class="flex-1 min-w-0">
				<div class="flex items-center gap-2">
					<span class="text-[11px] font-semibold uppercase tracking-wider select-none runner-markdown break-words" style="color: var(--runner-muted)">{@html renderMarkdown(label)}</span>
					{#if guide.length > 0}
						<button
							type="button"
							class="transition-opacity hover:opacity-60"
							style="color: var(--runner-muted)"
							onclick={() => (helpOpen = !helpOpen)}
							title="Setup guide"
						>
							<HelpCircle class="w-3.5 h-3.5" />
						</button>
					{/if}
				</div>
				{#if description}
					<div class="text-xs mt-0.5 runner-markdown" style="color: var(--runner-muted)">{@html renderMarkdown(description)}</div>
				{/if}
			</div>
		</div>

		{#if helpOpen && guide.length > 0}
			<div class="rounded-lg p-3 space-y-1" style="background: color-mix(in srgb, var(--runner-fg) 4%, transparent); border: 1px solid var(--runner-card-border)">
				{#each guide as line}
					{#if line === ''}
						<div class="h-1"></div>
					{:else if line.startsWith('---') && line.endsWith('---')}
						<p class="text-xs font-semibold" style="color: var(--runner-fg)">{line.replace(/---/g, '').trim()}</p>
					{:else}
						<div class="text-xs runner-markdown" style="color: var(--runner-muted)">{@html renderMarkdown(line)}</div>
					{/if}
				{/each}
			</div>
		{/if}

		{#if missingOptions}
			<div class="rounded-lg p-3 text-xs font-medium flex items-start gap-2" style="background: color-mix(in srgb, rgb(239 68 68) 10%, transparent); border: 1px solid color-mix(in srgb, rgb(239 68 68) 30%, transparent); color: rgb(239 68 68)">
				<span class="text-sm">⚠</span>
				<div class="flex-1">
					<div class="font-semibold">Missing options for as:{variant}</div>
					<div class="mt-0.5 opacity-80">Add <code class="font-mono">options:"a, b, c"</code> to this field in Loom, or declare <code class="font-mono">options</code> on the <code class="font-mono">{field.key}</code> field in the node catalog.</div>
				</div>
			</div>
		{:else if variant === 'textarea' || (!variant && field.type === 'textarea')}
			<div class="relative flex-1 flex flex-col">
				<textarea
					class="{inputBaseClass} resize-y flex-1"
					style="{inputBaseStyle}; {ringStyle}; {textareaHeight ? `height: ${textareaHeight}; min-height: ${textareaHeight};` : 'min-height: 200px;'}"
					placeholder={field.placeholder ?? ''}
					maxlength={effectiveMaxLength}
					minlength={field.minLength}
					value={fieldEditor.display(item.id, stringifyValue(value))}
					onfocus={() => fieldEditor.focus(item.id, stringifyValue(value))}
					oninput={(e) => fieldEditor.input(e.currentTarget.value, item.id, saveString)}
					onblur={() => fieldEditor.blur(item.id, saveString)}
				></textarea>
				{#if hasMaxLength}
					<div class="absolute bottom-3 right-4 text-[10px] font-mono pointer-events-none select-none" style="color: {overLimit ? 'rgb(239 68 68)' : 'var(--runner-muted)'}">
						{charCount}/{effectiveMaxLength}
					</div>
				{/if}
			</div>
		{:else if variant === 'slider'}
			{@const rawNum = Number(value ?? 0)}
			{@const displayNum = Number.isFinite(rawNum) ? (Math.abs(rawNum) >= 1000 || (rawNum !== 0 && Math.abs(rawNum) < 0.01) ? rawNum.toPrecision(4) : rawNum.toString().slice(0, 10)) : '0'}
			<div class="flex items-center gap-3 pt-1 min-w-0">
				<input
					type="range"
					min={field.min ?? 0}
					max={field.max ?? 1}
					step={field.step ?? 0.01}
					value={rawNum}
					oninput={(e) => setValue(field.type === 'number' ? Number(e.currentTarget.value) : String(e.currentTarget.value))}
					class="flex-1 min-w-0 accent-[color:var(--runner-primary)]"
				/>
				<span class="text-sm font-mono tabular-nums truncate max-w-[80px] flex-shrink-0" style="color: var(--runner-fg)" title={String(rawNum)}>{displayNum}</span>
			</div>
		{:else if variant === 'toggle'}
			<!-- Pill toggle. The field label already shows above, so we only
			     show a short on/off caption next to the switch to avoid
			     duplicating the label. -->
			<button
				type="button"
				role="switch"
				aria-checked={boolValue}
				class="inline-flex items-center gap-2.5 rounded-full px-1 py-1 transition-colors focus:outline-none focus:ring-2 focus:ring-offset-2"
				style="--tw-ring-color: color-mix(in srgb, var(--runner-primary) 50%, transparent)"
				onclick={() => saveBool(!boolValue)}
			>
				<span
					class="relative inline-flex h-6 w-11 items-center rounded-full transition-colors"
					style="background: {boolValue ? 'var(--runner-primary)' : 'color-mix(in srgb, var(--runner-fg) 18%, transparent)'}"
				>
					<span
						class="inline-block h-5 w-5 rounded-full bg-white shadow-sm transform transition-transform"
						style="transform: translateX({boolValue ? '22px' : '2px'})"
					></span>
				</span>
				<span class="text-xs font-medium uppercase tracking-wider" style="color: var(--runner-muted)">
					{boolValue ? 'On' : 'Off'}
				</span>
			</button>
		{:else if variant === 'checkbox'}
			<label class="flex items-center gap-2 cursor-pointer">
				<input
					type="checkbox"
					class="w-4 h-4 rounded accent-[color:var(--runner-primary)]"
					checked={boolValue}
					onchange={(e) => saveBool(e.currentTarget.checked)}
				/>
				<span class="text-sm runner-markdown" style="color: var(--runner-muted)">{@html renderMarkdown(description ?? label)}</span>
			</label>
		{:else if variant === 'select' && options.length > 0}
			<select
				class={inputBaseClass}
				style="{inputBaseStyle}; {ringStyle}"
				value={String(value ?? '')}
				onchange={(e) => setValue(e.currentTarget.value)}
			>
				{#each options as opt}
					<option value={opt}>{opt}</option>
				{/each}
			</select>
		{:else if variant === 'radio' && options.length > 0}
			<!-- Segmented pill control. Chips grow to share the full row
			     width. If the column is too narrow to fit them at a minimum
			     comfortable size, they wrap to a second row instead of
			     clipping. The container always fills the field width. -->
			<div class="flex flex-wrap gap-1.5 p-1 rounded-xl w-full" style="background: var(--runner-input-bg); border: 1px solid var(--runner-input-border)">
				{#each options as opt}
					{@const selected = String(value ?? '') === opt}
					<button
						type="button"
						class="flex-1 text-sm font-medium px-3 py-1.5 rounded-lg transition-all whitespace-nowrap text-center"
						style="min-width: max-content; {selected
							? 'background: var(--runner-primary); color: white; box-shadow: 0 1px 2px 0 rgba(0,0,0,0.08)'
							: 'background: transparent; color: var(--runner-muted)'}"
						onclick={() => setValue(opt)}
					>{opt}</button>
				{/each}
			</div>
		{:else if variant === 'cards' && options.length > 0}
			<div class="grid gap-2" style="grid-template-columns: repeat(auto-fit, minmax(min(140px, 100%), 1fr))">
				{#each options as opt}
					{@const selected = String(value ?? '') === opt}
					<button
						type="button"
						class="rounded-xl text-left p-3 text-sm transition-all truncate"
						style="background: {selected ? 'color-mix(in srgb, var(--runner-primary) 10%, transparent)' : 'var(--runner-input-bg)'}; border: 1px solid {selected ? 'var(--runner-primary)' : 'var(--runner-input-border)'}; color: var(--runner-fg)"
						onclick={() => setValue(opt)}
					>{opt}</button>
				{/each}
			</div>
		{:else if variant === 'multiselect' && options.length > 0}
			<div class="flex flex-col gap-2">
				{#each options as opt}
					{@const selected = listValue.includes(opt)}
					<label class="flex items-start gap-2 cursor-pointer text-sm" style="color: var(--runner-fg)">
						<input type="checkbox" checked={selected} onchange={() => toggleListValue(opt)} class="accent-[color:var(--runner-primary)] mt-0.5 flex-shrink-0" />
						<span class="break-words min-w-0">{opt}</span>
					</label>
				{/each}
			</div>
		{:else if variant === 'multicards' && options.length > 0}
			<div class="grid gap-2" style="grid-template-columns: repeat(auto-fit, minmax(min(140px, 100%), 1fr))">
				{#each options as opt}
					{@const selected = listValue.includes(opt)}
					<button
						type="button"
						class="rounded-xl text-left p-3 text-sm transition-all truncate"
						style="background: {selected ? 'color-mix(in srgb, var(--runner-primary) 10%, transparent)' : 'var(--runner-input-bg)'}; border: 1px solid {selected ? 'var(--runner-primary)' : 'var(--runner-input-border)'}; color: var(--runner-fg)"
						onclick={() => toggleListValue(opt)}
					>{opt}</button>
				{/each}
			</div>
		{:else if variant === 'tags'}
			{@const tagsText = listValue.join(', ')}
			<input
				type="text"
				class={inputBaseClass}
				style="{inputBaseStyle}; {ringStyle}"
				placeholder="tag1, tag2, tag3"
				value={tagsText}
				oninput={(e) => {
					const parts = e.currentTarget.value.split(',').map(s => s.trim()).filter(s => s.length > 0);
					saveList(parts);
				}}
			/>
			{#if listValue.length > 0}
				<div class="flex flex-wrap gap-1.5 pt-1 max-w-full">
					{#each listValue as tag}
						<span class="inline-flex items-center gap-1 text-xs px-2 py-1 rounded-full max-w-[220px] min-w-0" style="background: color-mix(in srgb, var(--runner-primary) 12%, transparent); color: var(--runner-primary)">
							<span class="truncate">{tag}</span>
							<button type="button" class="hover:opacity-60 flex-shrink-0" onclick={() => toggleListValue(tag)}>×</button>
						</span>
					{/each}
				</div>
			{/if}
		{:else if variant === 'date'}
			<input
				type="date"
				class={inputBaseClass}
				style="{inputBaseStyle}; {ringStyle}"
				value={String(value ?? '')}
				onchange={(e) => setValue(e.currentTarget.value)}
			/>
		{:else if variant === 'time'}
			<input
				type="time"
				class={inputBaseClass}
				style="{inputBaseStyle}; {ringStyle}"
				value={String(value ?? '')}
				onchange={(e) => setValue(e.currentTarget.value)}
			/>
		{:else if variant === 'datetime'}
			<input
				type="datetime-local"
				class={inputBaseClass}
				style="{inputBaseStyle}; {ringStyle}"
				value={String(value ?? '')}
				onchange={(e) => setValue(e.currentTarget.value)}
			/>
		{:else if variant === 'color'}
			<div class="flex items-center gap-3">
				<input
					type="color"
					class="w-12 h-10 rounded-lg cursor-pointer border-0"
					value={String(value ?? '#000000')}
					oninput={(e) => setValue(e.currentTarget.value)}
				/>
				<input
					type="text"
					class="{inputBaseClass} font-mono"
					style="{inputBaseStyle}; {ringStyle}"
					placeholder="#000000"
					value={String(value ?? '')}
					oninput={(e) => setValue(e.currentTarget.value)}
				/>
			</div>
		{:else if variant === 'email'}
			<input
				type="email"
				class={inputBaseClass}
				style="{inputBaseStyle}; {ringStyle}"
				placeholder={field.placeholder ?? 'you@example.com'}
				value={fieldEditor.display(item.id, stringifyValue(value))}
				onfocus={() => fieldEditor.focus(item.id, stringifyValue(value))}
				oninput={(e) => fieldEditor.input(e.currentTarget.value, item.id, saveString)}
				onblur={() => fieldEditor.blur(item.id, saveString)}
			/>
		{:else if variant === 'url'}
			<input
				type="url"
				class={inputBaseClass}
				style="{inputBaseStyle}; {ringStyle}"
				placeholder={field.placeholder ?? 'https://…'}
				value={fieldEditor.display(item.id, stringifyValue(value))}
				onfocus={() => fieldEditor.focus(item.id, stringifyValue(value))}
				oninput={(e) => fieldEditor.input(e.currentTarget.value, item.id, saveString)}
				onblur={() => fieldEditor.blur(item.id, saveString)}
			/>
		{:else if variant === 'text'}
			<input
				type="text"
				class={inputBaseClass}
				style="{inputBaseStyle}; {ringStyle}"
				placeholder={field.placeholder ?? ''}
				maxlength={field.maxLength}
				minlength={field.minLength}
				value={fieldEditor.display(item.id, stringifyValue(value))}
				onfocus={() => fieldEditor.focus(item.id, stringifyValue(value))}
				oninput={(e) => fieldEditor.input(e.currentTarget.value, item.id, saveString)}
				onblur={() => fieldEditor.blur(item.id, saveString)}
			/>
		{:else if field.type === 'text'}
			<div class="relative">
				<textarea
					class="{inputBaseClass} resize-y min-h-[44px]"
					style="{inputBaseStyle}; {ringStyle}"
					placeholder={field.placeholder ?? ''}
					maxlength={effectiveMaxLength}
					minlength={field.minLength}
					value={fieldEditor.display(item.id, stringifyValue(value))}
					onfocus={() => fieldEditor.focus(item.id, stringifyValue(value))}
					oninput={(e) => fieldEditor.input(e.currentTarget.value, item.id, saveString)}
					onblur={() => fieldEditor.blur(item.id, saveString)}
					rows={1}
				></textarea>
				{#if hasMaxLength}
					<div class="absolute bottom-3 right-4 text-[10px] font-mono pointer-events-none select-none" style="color: {overLimit ? 'rgb(239 68 68)' : 'var(--runner-muted)'}">
						{charCount}/{effectiveMaxLength}
					</div>
				{/if}
			</div>
		{:else if field.type === 'password'}
			<input
				type="password"
				class="{inputBaseClass} font-mono"
				style="{inputBaseStyle}; {ringStyle}"
				placeholder={field.placeholder ?? ''}
				maxlength={effectiveMaxLength}
				minlength={field.minLength}
				value={fieldEditor.display(item.id, stringifyValue(value))}
				onfocus={() => fieldEditor.focus(item.id, stringifyValue(value))}
				oninput={(e) => fieldEditor.input(e.currentTarget.value, item.id, saveString)}
				onblur={() => fieldEditor.blur(item.id, saveString)}
			/>
		{:else if field.type === 'number'}
			<input
				type="number"
				class={inputBaseClass}
				style="{inputBaseStyle}; {ringStyle}"
				placeholder={field.placeholder ?? ''}
				min={field.min}
				max={field.max}
				step={field.step}
				value={fieldEditor.display(item.id, String(value ?? ''))}
				onfocus={() => fieldEditor.focus(item.id, String(value ?? ''))}
				oninput={(e) => fieldEditor.input(e.currentTarget.value, item.id, saveNumber)}
				onblur={() => fieldEditor.blur(item.id, saveNumber)}
			/>
		{:else if field.type === 'select' && field.options}
			{#if variant === 'radio'}
				<div class="flex flex-col gap-2">
					{#each field.options as opt}
						<label class="flex items-center gap-2 cursor-pointer text-sm" style="color: var(--runner-fg)">
							<input type="radio" name={item.id} value={opt} checked={stringifyValue(value) === opt} onchange={() => setValue(opt)} class="accent-[color:var(--runner-primary)]" />
							<span>{opt}</span>
						</label>
					{/each}
				</div>
			{:else if variant === 'cards'}
				<div class="grid grid-cols-2 gap-2">
					{#each field.options as opt}
						{@const selected = stringifyValue(value) === opt}
						<button
							type="button"
							class="rounded-xl text-left p-3 text-sm transition-all"
							style="background: {selected ? 'color-mix(in srgb, var(--runner-primary) 10%, transparent)' : 'var(--runner-input-bg)'}; border: 1px solid {selected ? 'var(--runner-primary)' : 'var(--runner-input-border)'}; color: var(--runner-fg)"
							onclick={() => setValue(opt)}
						>{opt}</button>
					{/each}
				</div>
			{:else}
				<select
					class={inputBaseClass}
					style="{inputBaseStyle}; {ringStyle}"
					value={stringifyValue(value)}
					onchange={(e) => setValue(e.currentTarget.value)}
				>
					{#each field.options as opt}
						<option value={opt}>{opt}</option>
					{/each}
				</select>
			{/if}
		{:else if field.type === 'checkbox'}
			<label class="flex items-center gap-2 cursor-pointer">
				<input
					type="checkbox"
					class="w-4 h-4 rounded accent-[color:var(--runner-primary)]"
					checked={value === true}
					onchange={(e) => setValue(e.currentTarget.checked)}
				/>
				<span class="text-sm runner-markdown" style="color: var(--runner-muted)">{@html renderMarkdown(field.description ?? field.label)}</span>
			</label>
		{:else if field.type === 'api_key'}
			{@const strVal = String(value ?? '')}
			{@const isByok = strVal !== '' && strVal !== '__PLATFORM__'}
			<div class="space-y-2">
				<div class="flex">
					<div class="inline-flex rounded-lg overflow-hidden" style="border: 1px solid var(--runner-input-border)">
						<button type="button" class="text-xs px-4 py-1.5 font-medium transition-colors" style="background: {!isByok ? 'rgb(16 185 129)' : 'transparent'}; color: {!isByok ? 'white' : 'var(--runner-muted)'}" onclick={() => setValue('')}>Credits</button>
						<button type="button" class="text-xs px-4 py-1.5 font-medium transition-colors" style="border-left: 1px solid var(--runner-input-border); background: {isByok ? 'rgb(59 130 246)' : 'transparent'}; color: {isByok ? 'white' : 'var(--runner-muted)'}" onclick={() => { if (!isByok) setValue('__BYOK__'); }}>Own key</button>
					</div>
				</div>
				{#if isByok}
					<input
						type="password"
						class="{inputBaseClass} font-mono"
						style="{inputBaseStyle}; {ringStyle}"
						placeholder="sk-or-v1-..."
						value={fieldEditor.display(item.id, strVal === '__BYOK__' ? '' : strVal)}
						onfocus={() => fieldEditor.focus(item.id, strVal === '__BYOK__' ? '' : strVal)}
						oninput={(e) => fieldEditor.input(e.currentTarget.value, item.id, (v) => setValue(v || '__BYOK__'))}
						onblur={() => fieldEditor.blur(item.id, (v) => setValue(v || '__BYOK__'))}
					/>
				{/if}
			</div>
		{:else if field.type === 'blob'}
			<BlobField
				fileRef={value as FileRef | undefined}
				accept={field.accept}
				id={`runner-${item.id}`}
				placeholder={field.placeholder}
				onUpdate={(ref) => setValue(ref)}
			/>
		{/if}

		{#if error}
			<p class="text-xs" style="color: rgb(239 68 68)">{error}</p>
		{/if}
	</div>
{/if}
