<script lang="ts">
  import type { FieldDef } from '../../shared/protocol';
  import { cn } from '../utils/cn';

  interface Props {
    field: FieldDef;
    value: unknown;
    onChange: (newValue: unknown) => void;
    wired?: boolean;
  }

  let { field, value, onChange, wired = false }: Props = $props();

  function apply(next: unknown) {
    if (wired) return;
    onChange(next);
  }

  const kind = $derived(field.field_type.kind);
  const stringValue = $derived(
    typeof value === 'string' ? value : value == null ? '' : JSON.stringify(value),
  );
  const numberValue = $derived(typeof value === 'number' ? value : Number(value) || 0);
  const boolValue = $derived(Boolean(value));
  const selectOptions: string[] = $derived(
    Array.isArray(field.field_type.options) ? (field.field_type.options as string[]) : [],
  );
  const numberMin: number | undefined = $derived(
    typeof field.field_type.min === 'number' ? field.field_type.min : undefined,
  );
  const numberMax: number | undefined = $derived(
    typeof field.field_type.max === 'number' ? field.field_type.max : undefined,
  );

  // Event handlers with DOM-typed parameters so templates don't
  // need `as` casts inside attributes.
  function onInputChange(e: Event) {
    const t = e.currentTarget as HTMLInputElement;
    apply(t.value);
  }
  function onNumberChange(e: Event) {
    const t = e.currentTarget as HTMLInputElement;
    apply(Number(t.value));
  }
  function onBoolChange(e: Event) {
    const t = e.currentTarget as HTMLInputElement;
    apply(t.checked);
  }
  function onSelectChange(e: Event) {
    const t = e.currentTarget as HTMLSelectElement;
    apply(t.value);
  }
  function onTextareaChange(e: Event) {
    const t = e.currentTarget as HTMLTextAreaElement;
    apply(t.value);
  }
</script>

<div class="flex flex-col gap-1">
  {#if field.label}
    <!-- svelte-ignore a11y_label_has_associated_control -->
    <label class="text-[10px] uppercase tracking-wide text-muted-foreground">
      {field.label}
      {#if field.required}
        <span class="text-destructive">*</span>
      {/if}
    </label>
  {/if}

  {#if wired}
    <div
      class="text-[11px] italic text-muted-foreground border border-dashed border-border/50 rounded px-2 py-1"
    >
      wired from upstream
    </div>
  {:else if kind === 'textarea' || kind === 'code'}
    <textarea
      class={cn(
        'w-full min-h-[60px] resize-y rounded border border-border/60 bg-input px-2 py-1',
        'font-mono text-[11px]',
        'focus:outline-none focus:ring-1 focus:ring-ring',
      )}
      value={stringValue}
      rows={4}
      onchange={onTextareaChange}
    ></textarea>
  {:else if kind === 'number'}
    <input
      type="number"
      class="w-full rounded border border-border/60 bg-input px-2 py-1 text-[11px] focus:outline-none focus:ring-1 focus:ring-ring"
      value={numberValue}
      min={numberMin}
      max={numberMax}
      onchange={onNumberChange}
    />
  {:else if kind === 'toggle' || kind === 'boolean'}
    <label class="flex items-center gap-2">
      <input type="checkbox" checked={boolValue} onchange={onBoolChange} />
      <span class="text-[11px]">{boolValue ? 'on' : 'off'}</span>
    </label>
  {:else if kind === 'select'}
    <select
      class="w-full rounded border border-border/60 bg-input px-2 py-1 text-[11px] focus:outline-none focus:ring-1 focus:ring-ring"
      value={stringValue}
      onchange={onSelectChange}
    >
      {#each selectOptions as option}
        <option value={option}>{option}</option>
      {/each}
    </select>
  {:else if kind === 'password' || kind === 'api_key'}
    <input
      type="password"
      class="w-full rounded border border-border/60 bg-input px-2 py-1 text-[11px] font-mono focus:outline-none focus:ring-1 focus:ring-ring"
      value={stringValue}
      placeholder="********"
      onchange={onInputChange}
    />
  {:else if kind === 'form_builder'}
    <div
      class="text-[11px] text-muted-foreground border border-dashed border-border/50 rounded px-2 py-1"
    >
      form-builder editor (open .weft to edit fields directly)
    </div>
  {:else}
    <input
      type="text"
      class="w-full rounded border border-border/60 bg-input px-2 py-1 text-[11px] font-mono focus:outline-none focus:ring-1 focus:ring-ring"
      value={stringValue}
      onchange={onInputChange}
    />
  {/if}

  {#if field.description}
    <div class="text-[10px] text-muted-foreground/80">{field.description}</div>
  {/if}
</div>
