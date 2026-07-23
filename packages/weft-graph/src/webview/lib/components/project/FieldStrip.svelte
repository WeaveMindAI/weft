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
	import type { FieldDefinition } from '../../types';
	import { createFieldEditor } from '../../utils/field-editor.svelte';
	import { useFieldEditorRegistry } from './field-editor-registry';
	import { clampToRange } from '../../utils/input-field';

	let {
		fields,
		config,
		portValues,
		idPrefix,
		onUpdate,
		customFieldKeys,
		renderCustom,
		heights,
		onHeightChange,
		displayValueOf,
		readonlyKeys,
		headerBadge,
		onReadonlyEdit,
	}: {
		fields: FieldDefinition[];
		config: Record<string, unknown>;
		/// Values for PORT-DRIVEN fields (`field.portDriven`): the node's
		/// portLiterals map. Separate from `config` because a wired-only
		/// port's literal may legally coexist with a same-named config
		/// field, each with its own value.
		portValues?: Record<string, unknown>;
		/// Used to build unique DOM ids per field. Pass the owning
		/// node's id; the strip prefixes `${idPrefix}-field-${key}`.
		idPrefix: string;
		/// Called when a field's value changes. `portDriven` says which
		/// home the key lives in so the parent routes the write (a port
		/// literal vs a config field). The parent decides how to plumb it
		/// (typically to data.onUpdate so the round-trip turns into
		/// setConfig EditOps).
		onUpdate: (key: string, value: unknown, portDriven?: boolean) => void;
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
		/// Called when the user tries to TYPE into a readonly field (a
		/// readonly input swallows keystrokes silently). The parent decides
		/// how to surface it (e.g. a throttled toast explaining why).
		onReadonlyEdit?: (key: string) => void;
	} = $props();

	/// Keydown handler for readonly text controls: an editing keystroke
	/// (printable char, deletion, enter) on a readonly field means the user
	/// is trying to type; report it so the parent can explain the lock.
	function readonlyKeydown(e: KeyboardEvent, key: string, ro: boolean) {
		if (!ro) return;
		if (e.key.length === 1 || e.key === 'Backspace' || e.key === 'Delete' || e.key === 'Enter') {
			onReadonlyEdit?.(key);
		}
	}

	/// Paste/drop attempt on a readonly control: the DOM already rejects the
	/// edit (`readonly`), this only reports it so the lock gets explained for
	/// input methods that never produce an editing keydown.
	function readonlyPasteDrop(key: string, ro: boolean) {
		if (ro) onReadonlyEdit?.(key);
	}

	const fieldEditor = createFieldEditor();
	const fieldEditorRegistry = useFieldEditorRegistry();
	$effect(() => fieldEditorRegistry?.register(fieldEditor.flush));

	/// The field's stored value, routed by its home: a port-driven field
	/// reads `portValues`, a config field reads `config`. This is the ONE
	/// value lookup; every control below goes through it.
	function fieldValue(field: FieldDefinition): unknown {
		return field.portDriven ? portValues?.[field.key] : config?.[field.key];
	}

	/// The field's identity for DOM ids and the debounced field editor.
	/// Port-driven fields are prefixed so the legal same-name duplicate (a
	/// wired-only port's literal next to a same-named config field) gets
	/// two independent editors and two DOM ids.
	function fieldKey(field: FieldDefinition): string {
		return field.portDriven ? `port:${field.key}` : field.key;
	}

	function domId(field: FieldDefinition): string {
		return `${idPrefix}-field-${field.portDriven ? 'port-' : ''}${field.key}`;
	}

	/// The field's EFFECTIVE stored value: the set value, else the
	/// input's declared default (what the runtime would supply). One
	/// fallback rule for every control, so the field always shows what
	/// the node will actually read.
	function effectiveValue(field: FieldDefinition): unknown {
		const v = fieldValue(field);
		return v === undefined || v === null ? field.defaultValue : v;
	}

	function getDisplayValue(field: FieldDefinition): string {
		const k = fieldKey(field);
		// The override reads the CONFIG home (file-backed field content).
		// A port-driven row shows its own literal, even when a same-named
		// file-backed config field coexists.
		const override = field.portDriven ? undefined : displayValueOf?.(field.key);
		if (override !== undefined) return fieldEditor.display(k, override);
		const v = effectiveValue(field);
		const storeStr = (v === undefined || v === null)
			? ''
			: (typeof v === 'string' ? v : JSON.stringify(v, null, 2));
		return fieldEditor.display(k, storeStr);
	}

	function saveFn(field: FieldDefinition): (value: string) => void {
		return (value: string) => onUpdate(field.key, value, field.portDriven);
	}

	/// Save a number field, CLAMPED to its declared min/max: the widget's
	/// range is a contract (the compiler rejects out-of-range literals),
	/// so the editor never writes a value outside it.
	function saveNumber(field: FieldDefinition, raw: string) {
		if (raw.trim() === '') {
			onUpdate(field.key, null, field.portDriven);
			return;
		}
		const n = Number(raw);
		if (!Number.isFinite(n)) return;
		onUpdate(field.key, clampToRange(n, field.min, field.max), field.portDriven);
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
		<!-- readonlyKeys comes from the CONFIG home (file-backed field
		     states); a port-driven row is never file-backed, so it never
		     inherits a same-named config sibling's lock. -->
		{@const ro = !field.portDriven && (readonlyKeys?.has(field.key) ?? false)}
		<div class="space-y-1">
			<div class="flex items-center justify-between">
				<label for={domId(field)} class="text-[10px] text-muted-foreground font-medium block">
					{field.label}
				</label>
				{@render headerBadge?.(field)}
			</div>

			{#if field.type === 'textarea'}
				<textarea
					id={domId(field)}
					readonly={ro}
					class="text-xs px-2 py-1.5 rounded border-none outline-none font-mono nodrag nopan box-border block w-full {ro ? 'bg-rose-50 text-rose-700' : 'bg-muted'}"
					style="resize: vertical; min-height: 60px; {heights?.[field.key] ? `height: ${heights[field.key]}px;` : ''}"
					placeholder={field.placeholder}
					value={getDisplayValue(field)}
					onfocusin={(e) => e.currentTarget.classList.add('nowheel')}
					onfocusout={(e) => e.currentTarget.classList.remove('nowheel')}
					onfocus={() => fieldEditor.focus(fieldKey(field), getDisplayValue(field))}
					oninput={(e) => fieldEditor.input(e.currentTarget.value, fieldKey(field), saveFn(field))}
					onblur={() => fieldEditor.blur(fieldKey(field), saveFn(field))}
					onclick={(e) => e.stopPropagation()}
					onkeydown={(e) => readonlyKeydown(e, field.key, ro)}
					onpaste={() => readonlyPasteDrop(field.key, ro)}
					ondrop={() => readonlyPasteDrop(field.key, ro)}
					use:observeTextareaResize={field.key}
				></textarea>
			{:else if field.type === 'select' && field.options}
				<!-- The shown value is the EFFECTIVE one (set value, else the
				     input's declared default). With neither, an explicit
				     "(unset)" placeholder is selected: the control never lies
				     by displaying an option the node would not receive. -->
				{@const selected = effectiveValue(field) as string | undefined}
				<select
					id={domId(field)}
					disabled={ro}
					class="w-full text-xs bg-muted px-2 py-1.5 rounded border-none outline-none"
					value={selected ?? ''}
					onchange={(e) => onUpdate(field.key, e.currentTarget.value, field.portDriven)}
					onclick={(e) => e.stopPropagation()}
				>
					{#if selected === undefined}
						<option value="" disabled>(unset)</option>
					{/if}
					{#each field.options as option}
						<option value={option}>{option}</option>
					{/each}
				</select>
			{:else if field.type === 'multiselect' && field.options}
				<!-- Empty options cannot reach here: an optionless
				     select/multiselect widget fails the node's metadata load. -->
				<div class="flex flex-wrap gap-1 p-1.5 bg-muted rounded">
					{#each field.options as option}
						{@const current = (effectiveValue(field) as string[] | undefined) ?? []}
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
								onUpdate(field.key, next, field.portDriven);
							}}
						>
							{option}
						</button>
					{/each}
				</div>
			{:else if field.type === 'checkbox'}
				<label class="flex items-center gap-2 cursor-pointer">
					<input
						type="checkbox"
						disabled={ro}
						class="w-4 h-4 rounded border-muted-foreground/30 nodrag"
						checked={effectiveValue(field) === true}
						onchange={(e) => onUpdate(field.key, e.currentTarget.checked, field.portDriven)}
						onclick={(e) => e.stopPropagation()}
					/>
					<span class="text-xs text-muted-foreground">{field.description || field.label}</span>
				</label>
			{:else if field.type === 'number'}
				<input
					id={domId(field)}
					type="number"
					readonly={ro}
					class="w-full text-xs bg-muted px-2 py-1.5 rounded border-none outline-none nodrag"
					placeholder={field.placeholder}
					min={field.min}
					max={field.max}
					step={field.step}
					value={getDisplayValue(field)}
					onfocus={() => fieldEditor.focus(fieldKey(field), getDisplayValue(field))}
					oninput={(e) => fieldEditor.input(e.currentTarget.value, fieldKey(field), (v) => saveNumber(field, v))}
					onblur={() => fieldEditor.blur(fieldKey(field), (v) => saveNumber(field, v))}
					onclick={(e) => e.stopPropagation()}
					onkeydown={(e) => readonlyKeydown(e, field.key, ro)}
					onpaste={() => readonlyPasteDrop(field.key, ro)}
					ondrop={() => readonlyPasteDrop(field.key, ro)}
				/>
			{:else if field.type === 'password'}
				<input
					id={domId(field)}
					type="password"
					readonly={ro}
					class="w-full text-xs bg-muted px-2 py-1.5 rounded border-none outline-none font-mono nodrag"
					placeholder={field.placeholder}
					value={getDisplayValue(field)}
					onfocus={() => fieldEditor.focus(fieldKey(field), getDisplayValue(field))}
					oninput={(e) => fieldEditor.input(e.currentTarget.value, fieldKey(field), saveFn(field))}
					onblur={() => fieldEditor.blur(fieldKey(field), saveFn(field))}
					onclick={(e) => e.stopPropagation()}
					onkeydown={(e) => readonlyKeydown(e, field.key, ro)}
					onpaste={() => readonlyPasteDrop(field.key, ro)}
					ondrop={() => readonlyPasteDrop(field.key, ro)}
				/>
			{:else if field.type === 'text'}
				<input
					id={domId(field)}
					type="text"
					readonly={ro}
					class="w-full text-xs {ro ? 'bg-rose-50 text-rose-700' : 'bg-muted'} px-2 py-1.5 rounded border-none outline-none nodrag"
					placeholder={field.placeholder}
					value={getDisplayValue(field)}
					onfocus={() => fieldEditor.focus(fieldKey(field), getDisplayValue(field))}
					oninput={(e) => fieldEditor.input(e.currentTarget.value, fieldKey(field), saveFn(field))}
					onblur={() => fieldEditor.blur(fieldKey(field), saveFn(field))}
					onclick={(e) => e.stopPropagation()}
					onkeydown={(e) => readonlyKeydown(e, field.key, ro)}
					onpaste={() => readonlyPasteDrop(field.key, ro)}
					ondrop={() => readonlyPasteDrop(field.key, ro)}
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
