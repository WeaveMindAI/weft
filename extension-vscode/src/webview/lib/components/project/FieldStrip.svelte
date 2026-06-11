<script lang="ts">
	/// Renders a list of FieldDefinition entries as inline form controls
	/// against a `config` record. Handles the primitive field types:
	/// text, textarea, select, multiselect, checkbox, number, password.
	/// Exotic types (api_key, form_builder, code) are left to the
	/// parent: pass a `customFieldKeys` set so the strip skips those
	/// keys, and supply a `renderCustom` snippet that draws them inline
	/// at the right position (the strip iterates the field list once,
	/// in order, so custom fields stay interleaved with primitives).
	/// File-backed primitives render through the strip itself via the
	/// `displayValueOf` / `readonlyKeys` / `headerBadge` capabilities.
	import type { Snippet } from 'svelte';
	import type { FieldDefinition } from '$lib/types';
	import { createFieldEditor } from '$lib/utils/field-editor.svelte';
	import { useFieldEditorRegistry } from './field-editor-registry';

	let {
		fields,
		config,
		idPrefix,
		onUpdate,
		customFieldKeys,
		renderCustom,
		heights,
		onHeightChange,
		displayValueOf,
		readonlyKeys,
		headerBadge,
	}: {
		fields: FieldDefinition[];
		config: Record<string, unknown>;
		/// Used to build unique DOM ids per field. Pass the owning
		/// node's id; the strip prefixes `${idPrefix}-field-${key}`.
		idPrefix: string;
		/// Called when a field's value changes. The parent decides how
		/// to plumb it (typically to data.onUpdate so the round-trip
		/// turns into setConfig EditOps).
		onUpdate: (key: string, value: unknown) => void;
		/// Keys the parent renders itself (api_key / form_builder /
		/// code / file-backed). The strip skips them as primitives and
		/// hands each to `renderCustom` instead.
		customFieldKeys?: Set<string>;
		/// Parent-supplied renderer for the keys in `customFieldKeys`.
		/// Invoked once per custom field, in list order, so exotic
		/// fields render at their authored position. Required only when
		/// `customFieldKeys` is non-empty.
		renderCustom?: Snippet<[FieldDefinition]>;
		/// Optional per-field persisted textarea heights (px), keyed by
		/// field key. When supplied, a resized textarea reports its new
		/// height through `onHeightChange` so the parent can persist it.
		heights?: Record<string, number>;
		onHeightChange?: (key: string, height: number) => void;
		/// Per-key display override. When it returns a string for a key,
		/// that string is the field's store value (still routed through
		/// the debounced field editor). Used for file-backed fields whose
		/// displayed value is the resolved file content (or a read
		/// status), never what `config` holds (the `@file` marker).
		displayValueOf?: (key: string) => string | undefined;
		/// Keys whose value is a status string, not editable content
		/// (e.g. a file-backed field still loading or in read error).
		/// Rendered read-only/disabled with an error tint so edits can't
		/// clobber the backing store with the status text.
		readonlyKeys?: Set<string>;
		/// Optional badge rendered next to a field's label (e.g. the
		/// file path chip on file-backed fields).
		headerBadge?: Snippet<[FieldDefinition]>;
	} = $props();

	const fieldEditor = createFieldEditor();
	const fieldEditorRegistry = useFieldEditorRegistry();
	$effect(() => fieldEditorRegistry?.register(fieldEditor.flush));

	function getDisplayValue(key: string): string {
		const override = displayValueOf?.(key);
		if (override !== undefined) return fieldEditor.display(key, override);
		const v = config?.[key];
		const storeStr = (v === undefined || v === null)
			? ''
			: (typeof v === 'string' ? v : JSON.stringify(v, null, 2));
		return fieldEditor.display(key, storeStr);
	}

	function saveFn(key: string): (value: string) => void {
		return (value: string) => onUpdate(key, value);
	}

	function saveNumber(key: string, raw: string) {
		if (raw.trim() === '') {
			onUpdate(key, null);
			return;
		}
		const n = Number(raw);
		if (!Number.isFinite(n)) return;
		onUpdate(key, n);
	}

	/// Observe a textarea's manual resize and report the new height so the
	/// parent can persist it. Attached unconditionally; no-ops when
	/// `onHeightChange` is absent (the callback is optional-chained).
	function observeTextareaResize(node: HTMLTextAreaElement, key: string) {
		let lastHeight = node.clientHeight;
		const observer = new ResizeObserver(() => {
			const newHeight = node.clientHeight;
			if (newHeight !== lastHeight && newHeight >= 60) {
				lastHeight = newHeight;
				onHeightChange?.(key, newHeight);
			}
		});
		observer.observe(node);
		return {
			destroy() {
				observer.disconnect();
			},
		};
	}
</script>

{#each fields as field}
	{#if customFieldKeys?.has(field.key)}
		{@render renderCustom?.(field)}
	{:else}
		{@const ro = readonlyKeys?.has(field.key) ?? false}
		<div class="space-y-1">
			<div class="flex items-center justify-between">
				<label for={`${idPrefix}-field-${field.key}`} class="text-[10px] text-muted-foreground font-medium block">
					{field.label}
				</label>
				{@render headerBadge?.(field)}
			</div>

			{#if field.type === 'textarea'}
				<textarea
					id={`${idPrefix}-field-${field.key}`}
					readonly={ro}
					class="text-xs px-2 py-1.5 rounded border-none outline-none font-mono nodrag nopan box-border block w-full {ro ? 'bg-rose-50 text-rose-700' : 'bg-muted'}"
					style="resize: vertical; min-height: 60px; {heights?.[field.key] ? `height: ${heights[field.key]}px;` : ''}"
					placeholder={field.placeholder}
					value={getDisplayValue(field.key)}
					onfocusin={(e) => e.currentTarget.classList.add('nowheel')}
					onfocusout={(e) => e.currentTarget.classList.remove('nowheel')}
					onfocus={() => fieldEditor.focus(field.key, getDisplayValue(field.key))}
					oninput={(e) => fieldEditor.input(e.currentTarget.value, field.key, saveFn(field.key))}
					onblur={() => fieldEditor.blur(field.key, saveFn(field.key))}
					onclick={(e) => e.stopPropagation()}
					use:observeTextareaResize={field.key}
				></textarea>
			{:else if field.type === 'select' && field.options}
				<select
					id={`${idPrefix}-field-${field.key}`}
					disabled={ro}
					class="w-full text-xs bg-muted px-2 py-1.5 rounded border-none outline-none"
					value={(config[field.key] as string) ?? field.options[0]}
					onchange={(e) => onUpdate(field.key, e.currentTarget.value)}
					onclick={(e) => e.stopPropagation()}
				>
					{#each field.options as option}
						<option value={option}>{option}</option>
					{/each}
				</select>
			{:else if field.type === 'multiselect' && field.options}
				{#if field.options.length === 0}
					<div class="text-[10px] text-muted-foreground/60 italic px-1.5 py-1">
						(no options)
					</div>
				{:else}
					<div class="flex flex-wrap gap-1 p-1.5 bg-muted rounded">
						{#each field.options as option}
							{@const current = (config[field.key] as string[] | undefined) ?? []}
							{@const isSelected = current.includes(option)}
							<button
								type="button"
								disabled={ro}
								class="text-[10px] px-1.5 py-0.5 rounded transition-colors whitespace-nowrap nodrag {isSelected ? 'bg-primary text-primary-foreground' : 'bg-background text-muted-foreground hover:bg-accent'}"
								onclick={(e) => {
									e.stopPropagation();
									const next = isSelected
										? current.filter((v) => v !== option)
										: [...current, option];
									onUpdate(field.key, next);
								}}
							>
								{option}
							</button>
						{/each}
					</div>
				{/if}
			{:else if field.type === 'checkbox'}
				<label class="flex items-center gap-2 cursor-pointer">
					<input
						type="checkbox"
						disabled={ro}
						class="w-4 h-4 rounded border-muted-foreground/30 nodrag"
						checked={config[field.key] === true}
						onchange={(e) => onUpdate(field.key, e.currentTarget.checked)}
						onclick={(e) => e.stopPropagation()}
					/>
					<span class="text-xs text-muted-foreground">{field.description || field.label}</span>
				</label>
			{:else if field.type === 'number'}
				<input
					id={`${idPrefix}-field-${field.key}`}
					type="number"
					readonly={ro}
					class="w-full text-xs bg-muted px-2 py-1.5 rounded border-none outline-none nodrag"
					placeholder={field.placeholder}
					min={field.min}
					max={field.max}
					step={field.step}
					value={getDisplayValue(field.key)}
					onfocus={() => fieldEditor.focus(field.key, getDisplayValue(field.key))}
					oninput={(e) => fieldEditor.input(e.currentTarget.value, field.key, (v) => saveNumber(field.key, v))}
					onblur={() => fieldEditor.blur(field.key, (v) => saveNumber(field.key, v))}
					onclick={(e) => e.stopPropagation()}
				/>
			{:else if field.type === 'password'}
				<input
					id={`${idPrefix}-field-${field.key}`}
					type="password"
					readonly={ro}
					class="w-full text-xs bg-muted px-2 py-1.5 rounded border-none outline-none font-mono nodrag"
					placeholder={field.placeholder}
					value={getDisplayValue(field.key)}
					onfocus={() => fieldEditor.focus(field.key, getDisplayValue(field.key))}
					oninput={(e) => fieldEditor.input(e.currentTarget.value, field.key, saveFn(field.key))}
					onblur={() => fieldEditor.blur(field.key, saveFn(field.key))}
					onclick={(e) => e.stopPropagation()}
				/>
			{:else if field.type === 'text'}
				<input
					id={`${idPrefix}-field-${field.key}`}
					type="text"
					readonly={ro}
					class="w-full text-xs {ro ? 'bg-rose-50 text-rose-700' : 'bg-muted'} px-2 py-1.5 rounded border-none outline-none nodrag"
					placeholder={field.placeholder}
					value={getDisplayValue(field.key)}
					onfocus={() => fieldEditor.focus(field.key, getDisplayValue(field.key))}
					oninput={(e) => fieldEditor.input(e.currentTarget.value, field.key, saveFn(field.key))}
					onblur={() => fieldEditor.blur(field.key, saveFn(field.key))}
					onclick={(e) => e.stopPropagation()}
				/>
			{:else}
				<!-- code / api_key / form_builder MUST be in customFieldKeys
				     and rendered by the parent's renderCustom snippet.
				     Reaching this branch means the parent forgot to claim
				     this key; surface loud rather than silently rendering a
				     text input. -->
				<div class="text-[10px] px-1.5 py-1 rounded bg-destructive/10 text-destructive">
					FieldStrip: field type "{field.type}" must be handled by the parent (add "{field.key}" to customFieldKeys).
				</div>
			{/if}
		</div>
	{/if}
{/each}
