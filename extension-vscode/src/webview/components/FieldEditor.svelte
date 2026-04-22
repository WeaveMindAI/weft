<script lang="ts">
  import type { FieldDef } from '../../shared/protocol';
  import { cn } from '../utils/cn';
  import { createFieldEditor } from '../utils/field-editor.svelte';

  interface Props {
    field: FieldDef;
    value: unknown;
    onChange: (newValue: unknown) => void;
    wired?: boolean;
    // Persisted textarea height (ResizeObserver pushes here).
    textareaHeight?: number;
    onResize?: (height: number) => void;
  }

  let {
    field,
    value,
    onChange,
    wired = false,
    textareaHeight,
    onResize,
  }: Props = $props();

  // v1 toggles the `.nowheel` class on focus so scrolling inside the
  // textarea doesn't pan the canvas. It's removed on blur so scroll
  // events propagate to xyflow again when the cursor leaves.
  function addNoWheel(e: FocusEvent) {
    const t = e.currentTarget as HTMLElement | null;
    t?.classList.add('nowheel');
  }
  function removeNoWheel(e: FocusEvent) {
    const t = e.currentTarget as HTMLElement | null;
    t?.classList.remove('nowheel');
  }

  // ResizeObserver to persist manual textarea resizes.
  function observeResize(node: HTMLTextAreaElement) {
    const obs = new ResizeObserver((entries) => {
      for (const entry of entries) {
        const h = entry.contentRect.height;
        onResize?.(h);
      }
    });
    obs.observe(node);
    return { destroy: () => obs.disconnect() };
  }

  // Debounced text editor for string-valued fields. Keeps the user's
  // typing independent of store updates until they pause or blur.
  const editor = createFieldEditor(2000);

  function apply(next: unknown) {
    if (wired) return;
    onChange(next);
  }

  const kind = $derived(field.field_type.kind);

  // Display values for different render paths.
  const stringValue = $derived(
    typeof value === 'string' ? value : value == null ? '' : JSON.stringify(value),
  );
  const numberValue = $derived(typeof value === 'number' ? value : Number(value) || 0);
  const boolValue = $derived(Boolean(value));

  // Options: supports string[] or { value, label }[] schemas.
  const selectOptions: Array<{ value: string; label: string }> = $derived.by(() => {
    const opts = (field.field_type as any).options;
    if (!Array.isArray(opts)) return [];
    return opts.map((o) =>
      typeof o === 'string'
        ? { value: o, label: o }
        : { value: String(o.value), label: String(o.label ?? o.value) },
    );
  });

  // Multiselect state: current value as Set for toggle lookups.
  const multiValues: Set<string> = $derived(
    Array.isArray(value) ? new Set((value as unknown[]).map(String)) : new Set(),
  );

  const numberMin: number | undefined = $derived(
    typeof (field.field_type as any).min === 'number' ? (field.field_type as any).min : undefined,
  );
  const numberMax: number | undefined = $derived(
    typeof (field.field_type as any).max === 'number' ? (field.field_type as any).max : undefined,
  );

  // Event handlers. Each one extracts the DOM value and calls apply.
  function onInputChange(e: Event) {
    apply((e.currentTarget as HTMLInputElement).value);
  }
  function onInputFocus(e: Event) {
    const t = e.currentTarget as HTMLInputElement;
    editor.focus(field.key, t.value);
  }
  function onInputTypeDebounced(e: Event) {
    const t = e.currentTarget as HTMLInputElement;
    editor.input(t.value, field.key, (v) => apply(v));
  }
  function onInputBlur() {
    editor.blur(field.key, (v) => apply(v));
  }

  function onNumberChange(e: Event) {
    apply(Number((e.currentTarget as HTMLInputElement).value));
  }
  function onBoolChange(e: Event) {
    apply((e.currentTarget as HTMLInputElement).checked);
  }
  function onSelectChange(e: Event) {
    apply((e.currentTarget as HTMLSelectElement).value);
  }
  function onTextareaFocus(e: Event) {
    const t = e.currentTarget as HTMLTextAreaElement;
    editor.focus(field.key, t.value);
  }
  function onTextareaInput(e: Event) {
    const t = e.currentTarget as HTMLTextAreaElement;
    editor.input(t.value, field.key, (v) => apply(v));
  }
  function onTextareaBlur() {
    editor.blur(field.key, (v) => apply(v));
  }

  function toggleMulti(optionValue: string) {
    if (wired) return;
    const next = new Set(multiValues);
    if (next.has(optionValue)) next.delete(optionValue);
    else next.add(optionValue);
    onChange(Array.from(next));
  }

  // api_key state: "__BYOK__" sentinel means "use own key"; empty
  // string means "use platform credits".
  const byokActive = $derived(typeof value === 'string' && value.length > 0);
  function setByok(on: boolean) {
    if (on) apply('__BYOK__');
    else apply('');
  }

  // display(): keep user typing local until debounce flushes
  const display = $derived(editor.display(field.key, stringValue));
</script>

<div class="flex flex-col gap-1">
  {#if field.label}
    <!-- svelte-ignore a11y_label_has_associated_control -->
    <label class="text-[10px] text-zinc-500 font-medium flex items-center gap-1">
      {field.label}
      {#if field.required}
        <span class="text-red-500">*</span>
      {/if}
    </label>
  {/if}

  {#if wired}
    <div class="text-[11px] italic text-zinc-400 border border-dashed border-zinc-300 rounded px-2 py-1 bg-zinc-50">
      wired from upstream
    </div>
  {:else if kind === 'textarea' || kind === 'code'}
    <textarea
      class={cn(
        'w-full text-xs bg-zinc-100 px-2 py-1.5 rounded border-none outline-none font-mono box-border block',
        'min-h-[60px] resize-y',
      )}
      value={display}
      rows={4}
      style={textareaHeight ? `height: ${textareaHeight}px;` : ''}
      onfocus={(e) => {
        addNoWheel(e);
        onTextareaFocus(e);
      }}
      oninput={onTextareaInput}
      onblur={(e) => {
        removeNoWheel(e);
        onTextareaBlur();
      }}
      onclick={(e) => e.stopPropagation()}
      use:observeResize
    ></textarea>
  {:else if kind === 'number'}
    <input
      type="number"
      class="w-full text-xs bg-zinc-100 px-2 py-1.5 rounded border-none outline-none"
      value={numberValue}
      min={numberMin}
      max={numberMax}
      onchange={onNumberChange}
      onclick={(e) => e.stopPropagation()}
    />
  {:else if kind === 'checkbox' || kind === 'toggle' || kind === 'boolean'}
    <label class="flex items-center gap-2 text-xs text-zinc-700">
      <input
        type="checkbox"
        class="w-4 h-4 rounded border border-zinc-400"
        checked={boolValue}
        onchange={onBoolChange}
      />
      <span>{boolValue ? 'on' : 'off'}</span>
    </label>
  {:else if kind === 'select'}
    <select
      class="w-full text-xs bg-zinc-100 px-2 py-1.5 rounded border-none outline-none"
      value={stringValue}
      onchange={onSelectChange}
      onclick={(e) => e.stopPropagation()}
    >
      {#each selectOptions as opt}
        <option value={opt.value}>{opt.label}</option>
      {/each}
    </select>
  {:else if kind === 'multiselect'}
    <div class="flex flex-wrap gap-1 p-1.5 bg-zinc-100 rounded">
      {#each selectOptions as opt}
        {@const on = multiValues.has(opt.value)}
        <button
          type="button"
          class={cn(
            'text-[10px] px-1.5 py-0.5 rounded transition-colors',
            on ? 'bg-zinc-900 text-white' : 'bg-white text-zinc-700 hover:bg-zinc-50',
          )}
          onclick={(e) => {
            e.stopPropagation();
            toggleMulti(opt.value);
          }}
        >
          {opt.label}
        </button>
      {/each}
    </div>
  {:else if kind === 'password'}
    <input
      type="password"
      class="w-full text-xs bg-zinc-100 px-2 py-1.5 rounded border-none outline-none font-mono"
      value={stringValue}
      placeholder="********"
      onchange={onInputChange}
      onclick={(e) => e.stopPropagation()}
    />
  {:else if kind === 'api_key'}
    <div class="flex gap-1">
      <button
        type="button"
        class={cn(
          'flex-1 text-[10px] py-1 rounded transition-colors',
          !byokActive ? 'bg-emerald-500 text-white' : 'bg-zinc-100 text-zinc-600 hover:bg-zinc-200',
        )}
        onclick={(e) => {
          e.stopPropagation();
          setByok(false);
        }}
      >Credits</button>
      <button
        type="button"
        class={cn(
          'flex-1 text-[10px] py-1 rounded transition-colors',
          byokActive ? 'bg-blue-500 text-white' : 'bg-zinc-100 text-zinc-600 hover:bg-zinc-200',
        )}
        onclick={(e) => {
          e.stopPropagation();
          setByok(true);
        }}
      >Own Key</button>
    </div>
    {#if byokActive}
      <input
        type="password"
        class="w-full text-xs bg-zinc-100 px-2 py-1.5 rounded border-none outline-none font-mono mt-1"
        value={stringValue === '__BYOK__' ? '' : stringValue}
        placeholder="sk-..."
        onchange={onInputChange}
        onclick={(e) => e.stopPropagation()}
      />
    {/if}
  {:else if kind === 'form_builder'}
    <div class="text-[11px] text-zinc-400 border border-dashed border-zinc-300 rounded px-2 py-1">
      form-builder editor (edit .weft source directly for now)
    </div>
  {:else if kind === 'blob'}
    <!-- Minimal blob placeholder: paste URL. File upload lands once
         the dispatcher exposes an upload endpoint. -->
    <input
      type="url"
      class="w-full text-xs bg-zinc-100 px-2 py-1.5 rounded border-none outline-none font-mono"
      value={stringValue}
      placeholder="https://... (file upload lands later)"
      onchange={onInputChange}
      onclick={(e) => e.stopPropagation()}
    />
  {:else}
    <input
      type="text"
      class="w-full text-xs bg-zinc-100 px-2 py-1.5 rounded border-none outline-none font-mono"
      value={display}
      onfocus={onInputFocus}
      oninput={onInputTypeDebounced}
      onblur={onInputBlur}
      onclick={(e) => e.stopPropagation()}
    />
  {/if}

  {#if field.description}
    <div class="text-[10px] text-zinc-400">{field.description}</div>
  {/if}
</div>
