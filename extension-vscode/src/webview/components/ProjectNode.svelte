<script lang="ts">
  // Custom xyflow node type. One of these renders per NodeDefinition
  // in the project (except Passthrough, which stays hidden). Owns:
  //  - header (icon + label + optional collapse toggle)
  //  - input port rows (left handles, type badge, wired/config dots)
  //  - inline form-field editors (catalog fields + synthesized config-filled ports)
  //  - output port rows (right handles)
  //  - optional execution inspector (last exec input/output/error)
  //
  // Data comes from the `data` prop xyflow passes: the NodeDefinition
  // plus the catalog entry for the node type plus (optional) execution
  // state plus an onConfigChange callback back to the host.

  import { cn } from '../utils/cn';
  import { resolveIcon } from '../utils/icon';
  import PortRow from './PortRow.svelte';
  import FieldEditor from './FieldEditor.svelte';
  import ExecutionInspector, { type NodeExecStatus } from './ExecutionInspector.svelte';
  import type {
    CatalogEntry,
    FieldDef,
    NodeDefinition,
    PortDefinition,
  } from '../../shared/protocol';
  import { ChevronDown, ChevronRight } from 'lucide-svelte';

  interface NodeViewData {
    node: NodeDefinition;
    catalog: CatalogEntry | null;
    wiredInputs: Set<string>;
    exec: {
      status: NodeExecStatus;
      input?: unknown;
      output?: unknown;
      error?: string;
    };
    onConfigChange: (nodeId: string, key: string, value: unknown) => void;
  }

  interface Props {
    data: NodeViewData;
    id: string;
    selected?: boolean;
  }

  let { data, id, selected }: Props = $props();

  let expanded = $state(true);

  const catalog = $derived(data.catalog);
  const node = $derived(data.node);
  const wired = $derived(data.wiredInputs);
  const exec = $derived(data.exec);
  const color = $derived(catalog?.color ?? 'var(--vscode-focusBorder)');
  const Icon = $derived(resolveIcon(catalog?.icon));

  const label = $derived(
    node.label ?? catalog?.label ?? node.nodeType,
  );
  const description = $derived(catalog?.description ?? '');

  /** Inputs from the enriched NodeDefinition. Falls back to catalog
   *  defaults for lenient /parse where enrich skipped an unknown type. */
  const inputs: PortDefinition[] = $derived(
    (node.inputs?.length ? node.inputs : (catalog?.inputs ?? [])) as PortDefinition[],
  );
  const outputs: PortDefinition[] = $derived(
    (node.outputs?.length ? node.outputs : (catalog?.outputs ?? [])) as PortDefinition[],
  );

  /** Fields rendered: catalog-declared fields plus synthesized fields
   *  for configurable inputs that have a config value and no edge
   *  (matches v1's displayedFields). */
  const displayedFields: FieldDef[] = $derived.by(() => {
    const catalogFields: FieldDef[] = catalog?.fields ?? [];
    const result: FieldDef[] = [...catalogFields];
    const knownKeys = new Set(catalogFields.map((f) => f.key));
    for (const p of inputs) {
      if (knownKeys.has(p.name)) continue;
      if (p.configurable === false) continue;
      if (wired.has(p.name)) continue;
      const cfg = node.config as Record<string, unknown>;
      const v = cfg[p.name];
      if (v === undefined || v === null) continue;
      const multiline = typeof v === 'string' && v.includes('\n');
      result.push({
        key: p.name,
        label: p.name,
        field_type: { kind: multiline ? 'textarea' : 'text' },
      });
    }
    return result;
  });

  function onFieldChange(key: string, next: unknown) {
    data.onConfigChange(node.id, key, next);
  }

  const config: Record<string, unknown> = $derived(
    (node.config ?? {}) as Record<string, unknown>,
  );
</script>

<div
  class={cn(
    'rounded-md border bg-card text-card-foreground shadow-sm min-w-[220px] max-w-[320px]',
    selected ? 'border-ring ring-1 ring-ring' : 'border-border/60',
    exec.status === 'started' && 'animate-pulse',
  )}
  style={`border-top: 3px solid ${color};`}
>
  <div class="flex items-center gap-2 px-2 py-1">
    <Icon class="size-3.5 shrink-0" style={`color: ${color}`} />
    <span class="text-[11px] uppercase tracking-wide truncate flex-1" title={description}
      >{label}</span
    >
    <button
      type="button"
      class="text-muted-foreground hover:text-foreground"
      onclick={() => (expanded = !expanded)}
      aria-label={expanded ? 'collapse' : 'expand'}
    >
      {#if expanded}
        <ChevronDown class="size-3" />
      {:else}
        <ChevronRight class="size-3" />
      {/if}
    </button>
  </div>

  {#if id && node.id && id !== node.id}
    <div class="px-2 text-[10px] text-muted-foreground">{node.id}</div>
  {/if}

  <div class="border-t border-border/40"></div>

  <!-- Input ports -->
  {#if inputs.length}
    <div class="px-2 py-1 flex flex-col gap-0.5">
      {#each inputs as port}
        <PortRow
          name={port.name}
          portType={port.portType}
          side="input"
          required={port.required}
          wired={wired.has(port.name)}
          configFilled={Boolean(
            !wired.has(port.name) &&
              config[port.name] != null,
          )}
          description={port.description ?? ''}
        />
      {/each}
    </div>
  {/if}

  <!-- Body: fields + exec inspector -->
  {#if expanded}
    {#if displayedFields.length}
      <div class="px-2 py-1 flex flex-col gap-2 border-t border-border/40">
        {#each displayedFields as field}
          <FieldEditor
            field={field}
            value={config[field.key]}
            wired={wired.has(field.key)}
            onChange={(v) => onFieldChange(field.key, v)}
          />
        {/each}
      </div>
    {/if}

    {#if exec.status !== 'idle' || exec.error}
      <div class="px-2 py-1 border-t border-border/40">
        <ExecutionInspector
          status={exec.status}
          input={exec.input}
          output={exec.output}
          error={exec.error}
        />
      </div>
    {/if}
  {/if}

  <!-- Output ports -->
  {#if outputs.length}
    <div class="px-2 py-1 flex flex-col gap-0.5 border-t border-border/40">
      {#each outputs as port}
        <PortRow name={port.name} portType={port.portType} side="output" />
      {/each}
    </div>
  {/if}
</div>
