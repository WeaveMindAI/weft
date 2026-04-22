<script lang="ts">
  // Ported from dashboard-v1/src/lib/components/project/ProjectNode.svelte.
  //
  // Layout top-down:
  //   accent bar → header (status icon + type label + exec controls
  //                        + expand toggle)
  //              → label row (editable)
  //              → port rows (two columns)
  //              → live data (always visible)
  //              → expanded block (Setup Guide + field editors + debug preview)
  //              → _raw handle (absolute top-right; Handle wraps the SVG)
  //
  // The glow class on the outer element is driven by the latest
  // NodeExecution status (`node-running` / `node-completed` /
  // `node-failed`). Xyflow sets `.svelte-flow__node-weft` outer
  // wrapper; we apply the glow to the inner `.project-node` div so
  // the `:global(.node-running) .project-node` rules in the stylesheet
  // match without needing the xyflow wrapper class.

  import { Handle, NodeResizer, Position, useSvelteFlow } from '@xyflow/svelte';
  import { Maximize2, Minimize2 } from 'lucide-svelte';
  import { tick } from 'svelte';
  import { cn } from '../utils/cn';
  import { resolveIcon } from '../utils/icon';
  import { portMarkerStyle } from '../utils/port-marker';
  import { getPortTypeColor } from '../utils/colors';
  import { getStatusIcon } from '../utils/status';
  import { buildPortMenuItems, createPortContextMenu } from '../utils/port-context-menu';
  import { computeNodeMinResizeHeight } from '../utils/node-geometry';
  import FieldEditor from './FieldEditor.svelte';
  import ExecutionInspector from './ExecutionInspector.svelte';
  import type {
    FieldDef,
    PortDefinition,
  } from '../../shared/protocol';
  import type { NodeExecution } from './exec-types';
  import type { NodeViewData } from './node-view-data';

  interface Props {
    data: NodeViewData;
    id: string;
    selected?: boolean;
  }

  let { data, id: _id, selected }: Props = $props();

  const { getViewport, setViewport } = useSvelteFlow();

  const node = $derived(data.node);
  const catalog = $derived(data.catalog);
  const wired = $derived(data.wiredInputs);
  const executions: NodeExecution[] = $derived(data.executions ?? []);
  const latestExec = $derived(executions[executions.length - 1] ?? null);
  const config: Record<string, unknown> = $derived(
    (node.config ?? {}) as Record<string, unknown>,
  );
  const color = $derived(catalog?.color ?? '#52525b');
  const Icon = $derived(resolveIcon(catalog?.icon));

  const typeLabel = $derived(catalog?.label ?? node.nodeType);
  const userLabel = $derived(node.label ?? '');
  const expanded = $derived(Boolean(config.expanded));

  const glowClass = $derived.by(() => {
    switch (latestExec?.status) {
      case 'running':
      case 'waiting_for_input':
        return 'node-running';
      case 'completed':
      case 'skipped':
        return 'node-completed';
      case 'failed':
        return 'node-failed';
      default:
        return '';
    }
  });

  const inputs: PortDefinition[] = $derived(
    node.inputs?.length ? node.inputs : ((catalog?.inputs ?? []) as PortDefinition[]),
  );
  const outputs: PortDefinition[] = $derived(
    node.outputs?.length ? node.outputs : ((catalog?.outputs ?? []) as PortDefinition[]),
  );
  const catalogInputNames: Set<string> = $derived(
    new Set(((catalog?.inputs ?? []) as PortDefinition[]).map((p) => p.name)),
  );
  const catalogOutputNames: Set<string> = $derived(
    new Set(((catalog?.outputs ?? []) as PortDefinition[]).map((p) => p.name)),
  );
  const canAddInputs = $derived(Boolean(catalog?.features?.canAddInputPorts));
  const canAddOutputs = $derived(Boolean(catalog?.features?.canAddOutputPorts));
  const showDebugPreview = $derived(Boolean(catalog?.features?.showDebugPreview));

  const oneOfRequiredPorts: Set<string> = $derived.by(() => {
    const s = new Set<string>();
    for (const grp of catalog?.features?.oneOfRequired ?? []) for (const p of grp) s.add(p);
    return s;
  });
  const configFilledPorts: Set<string> = $derived.by(() => {
    const filled = new Set<string>();
    for (const p of inputs) {
      if (!p.configurable) continue;
      if (wired.has(p.name)) continue;
      const v = config[p.name];
      if (v !== undefined && v !== null && v !== '') filled.add(p.name);
    }
    return filled;
  });

  const displayedFields: FieldDef[] = $derived.by(() => {
    const catalogFields: FieldDef[] = catalog?.fields ?? [];
    const result: FieldDef[] = [...catalogFields];
    const known = new Set(catalogFields.map((f) => f.key));
    for (const p of inputs) {
      if (known.has(p.name)) continue;
      if (!p.configurable) continue;
      if (wired.has(p.name)) continue;
      const v = config[p.name];
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

  const hasExpandableContent = $derived(displayedFields.length > 0 || showDebugPreview);

  const setupGuide = $derived(
    (catalog as unknown as { setupGuide?: string })?.setupGuide ?? null,
  );
  let setupGuideOpen = $state(false);

  // textareaHeights: Record<key, number> persisted in config. We
  // mutate via onConfigChange so the value round-trips.
  const textareaHeights = $derived(
    (config.textareaHeights as Record<string, number> | undefined) ?? {},
  );
  function handleTextareaResize(key: string, height: number) {
    if (height < 60) return;
    const cur = textareaHeights[key];
    if (cur === height) return;
    data.onConfigChange(node.id, 'textareaHeights', {
      ...textareaHeights,
      [key]: height,
    });
  }

  // Label editing.
  let editingLabel = $state(false);
  let labelInput = $state('');
  function startEditLabel(e: MouseEvent) {
    e.stopPropagation();
    labelInput = userLabel;
    editingLabel = true;
  }
  function saveLabel() {
    editingLabel = false;
    data.onLabelChange(node.id, labelInput.trim() || null);
  }
  function handleLabelKeydown(e: KeyboardEvent) {
    if (e.key === 'Enter') saveLabel();
    else if (e.key === 'Escape') {
      editingLabel = false;
      labelInput = userLabel;
    }
  }

  // Expand / collapse with viewport anchoring.
  let nodeEl: HTMLDivElement | undefined = $state();
  async function toggleExpand(e: MouseEvent) {
    e.stopPropagation();
    const before = nodeEl?.getBoundingClientRect();
    data.onConfigChange(node.id, 'expanded', !expanded);
    if (!before) return;
    await tick();
    await new Promise((r) => requestAnimationFrame(() => r(undefined)));
    await new Promise((r) => requestAnimationFrame(() => r(undefined)));
    const after = nodeEl?.getBoundingClientRect();
    if (!after) return;
    const dx = after.right - before.right;
    const dy = after.top - before.top;
    if (Math.abs(dx) > 0.5 || Math.abs(dy) > 0.5) {
      const vp = getViewport();
      setViewport({ x: vp.x - dx, y: vp.y - dy, zoom: vp.zoom });
    }
  }

  function onFieldChange(key: string, next: unknown) {
    data.onConfigChange(node.id, key, next);
  }

  // Port context menu.
  let menuCleanup: (() => void) | undefined;
  function openPortMenu(
    e: MouseEvent,
    port: PortDefinition,
    side: 'input' | 'output',
  ) {
    e.preventDefault();
    e.stopPropagation();
    menuCleanup?.();
    const isCustom =
      side === 'input' ? !catalogInputNames.has(port.name) : !catalogOutputNames.has(port.name);
    const canAddPorts = side === 'input' ? canAddInputs : canAddOutputs;
    const items = buildPortMenuItems({
      port,
      side,
      isCustom,
      canAddPorts,
      onToggleRequired: () => {
        const list = (side === 'input' ? inputs : outputs).map((p) =>
          p.name === port.name ? { ...p, required: !p.required } : p,
        );
        data.onPortsChange(node.id, side === 'input' ? { inputs: list } : { outputs: list });
      },
      onSetType: (newType) => {
        const list = (side === 'input' ? inputs : outputs).map((p) =>
          p.name === port.name ? { ...p, portType: newType } : p,
        );
        data.onPortsChange(node.id, side === 'input' ? { inputs: list } : { outputs: list });
      },
      onRemove: () => {
        const list = (side === 'input' ? inputs : outputs).filter((p) => p.name !== port.name);
        data.onPortsChange(node.id, side === 'input' ? { inputs: list } : { outputs: list });
      },
    });
    menuCleanup = createPortContextMenu(e.clientX, e.clientY, items, () => {
      menuCleanup?.();
      menuCleanup = undefined;
    });
  }

  // Add-port UI state.
  let addingInput = $state(false);
  let addingOutput = $state(false);
  let newPortName = $state('');
  function addPort(side: 'input' | 'output') {
    const name = newPortName.trim();
    if (!name) {
      if (side === 'input') addingInput = false;
      else addingOutput = false;
      newPortName = '';
      return;
    }
    const existing = (side === 'input' ? inputs : outputs).map((p) => p.name);
    if (existing.includes(name) || name === '_raw') {
      newPortName = '';
      if (side === 'input') addingInput = false;
      else addingOutput = false;
      return;
    }
    const fresh: PortDefinition = {
      name,
      portType: 'MustOverride',
      required: false,
      laneMode: 'Single',
      laneDepth: 1,
      configurable: side === 'input',
    };
    const next = [...(side === 'input' ? inputs : outputs), fresh];
    data.onPortsChange(node.id, side === 'input' ? { inputs: next } : { outputs: next });
    newPortName = '';
    if (side === 'input') addingInput = false;
    else addingOutput = false;
  }
  function handlePortAddKey(e: KeyboardEvent, side: 'input' | 'output') {
    if (e.key === 'Enter') addPort(side);
    else if (e.key === 'Escape') {
      newPortName = '';
      if (side === 'input') addingInput = false;
      else addingOutput = false;
    }
  }

  // Debug preview — v1 strips `_raw` from the object before rendering
  // so the pretty view matches what downstream nodes see.
  const debugData = $derived(showDebugPreview ? latestExec?.output : undefined);
  function stripRawKeys(v: unknown): unknown {
    if (v === null || typeof v !== 'object') return v;
    if (Array.isArray(v)) return v.map(stripRawKeys);
    const out: Record<string, unknown> = {};
    for (const [k, val] of Object.entries(v as Record<string, unknown>)) {
      if (k === '_raw') continue;
      out[k] = stripRawKeys(val);
    }
    return out;
  }
  const debugDataJson = $derived(
    debugData !== undefined ? JSON.stringify(stripRawKeys(debugData), null, 2) : '',
  );

  const liveDataItems = $derived(data.liveData ?? []);
  const rawConnected = $derived(wired.has('_raw'));
  const minResizeHeight = $derived(
    computeNodeMinResizeHeight(inputs.length, outputs.length),
  );
</script>

<NodeResizer
  isVisible={selected && expanded}
  minWidth={200}
  minHeight={minResizeHeight}
  lineStyle="border-color: #a1a1aa;"
  handleStyle="background-color: #71717a; width: 8px; height: 8px; border-radius: 2px;"
/>

<div
  bind:this={nodeEl}
  class={cn(
    'project-node relative flex flex-col rounded-md bg-white overflow-hidden select-none transition-all duration-200',
    'min-w-[200px]',
    glowClass,
  )}
  style={`width: 100%; height: 100%; background: rgba(255, 255, 255, 0.95); border: 1px solid ${
    selected ? color : 'rgba(0,0,0,0.08)'
  }; box-shadow: 0 1px 3px rgba(0,0,0,0.08), 0 4px 12px rgba(0,0,0,0.05)${
    selected ? `, 0 0 0 1px ${color}20` : ''
  }; backdrop-filter: blur(8px);`}
>
  <!-- Accent bar -->
  <div class="h-[2px] w-full rounded-t" style={`background: ${color};`}></div>

  <!-- Header -->
  <div class="flex items-center justify-between px-3 py-2 border-b border-black/5">
    <div class="flex items-center gap-1.5 min-w-0">
      <span
        class={cn(
          'text-[11px]',
          (latestExec?.status === 'running' || latestExec?.status === 'waiting_for_input') && 'animate-pulse',
        )}
        style={`color: ${color};`}
      >
        {getStatusIcon(latestExec?.status ?? '')}
      </span>
      <Icon class="size-3" style={`color: ${color};`} />
      <span
        class="text-[10px] font-semibold uppercase tracking-wider truncate"
        style={`color: ${color};`}
        title={catalog?.description ?? ''}
      >
        {typeLabel}
      </span>
    </div>
    <div class="flex items-center gap-0.5 shrink-0 nodrag">
      <ExecutionInspector executions={executions} label={userLabel || typeLabel} />
      {#if hasExpandableContent}
        <button
          type="button"
          class="w-5 h-5 flex items-center justify-center rounded hover:bg-black/5 text-zinc-400 hover:text-zinc-600 transition-colors"
          onclick={toggleExpand}
          aria-label={expanded ? 'Collapse' : 'Expand'}
          title={expanded ? 'Collapse' : 'Expand'}
        >
          {#if expanded}
            <Minimize2 class="w-3 h-3" />
          {:else}
            <Maximize2 class="w-3 h-3" />
          {/if}
        </button>
      {/if}
    </div>
  </div>

  <!-- Label row -->
  <div class="px-3 pt-2">
    {#if editingLabel}
      <!-- svelte-ignore a11y_autofocus -->
      <input
        type="text"
        class="w-full text-sm font-medium bg-zinc-100 text-zinc-900 px-2 py-1 rounded border border-zinc-200 outline-none focus:border-zinc-400 nodrag"
        bind:value={labelInput}
        onblur={saveLabel}
        onkeydown={handleLabelKeydown}
        onclick={(e) => e.stopPropagation()}
        autofocus
      />
    {:else}
      <!-- svelte-ignore a11y_no_static_element_interactions -->
      <!-- svelte-ignore a11y_click_events_have_key_events -->
      <p
        class="text-sm font-medium text-zinc-800 cursor-text hover:bg-black/5 px-1 py-0.5 rounded -mx-1 truncate"
        ondblclick={startEditLabel}
        title="Double-click to edit"
      >
        {userLabel || `${typeLabel} Node`}
      </p>
    {/if}
  </div>

  <!-- Port rows -->
  <div class="flex justify-between gap-4 px-3 py-2 text-[10px] text-zinc-500">
    <!-- Inputs -->
    <div class="space-y-1 min-w-0 flex-1">
      {#each inputs as port (port.name)}
        {@const pm = portMarkerStyle(port, oneOfRequiredPorts, configFilledPorts, getPortTypeColor(port.portType), 'input')}
        <!-- svelte-ignore a11y_no_static_element_interactions -->
        <div
          class="relative flex items-center gap-1.5 group pl-3"
          title={`${port.name}: ${port.portType}${port.required ? ' (required)' : ''}`}
          oncontextmenu={(e) => openPortMenu(e, port, 'input')}
        >
          <Handle
            type="target"
            position={Position.Left}
            id={port.name}
            class={pm.class}
            style={`top: 50%; ${pm.style}`}
          />
          <span class="truncate">{port.name}</span>
        </div>
      {/each}
      {#if canAddInputs}
        {#if addingInput}
          <!-- svelte-ignore a11y_autofocus -->
          <input
            class="w-full text-[10px] bg-zinc-100 px-2 py-0.5 rounded outline-none border border-zinc-200 nodrag"
            placeholder="port name"
            bind:value={newPortName}
            onkeydown={(e) => handlePortAddKey(e, 'input')}
            onblur={() => {
              addingInput = false;
              newPortName = '';
            }}
            onclick={(e) => e.stopPropagation()}
            autofocus
          />
        {:else}
          <button
            class="flex items-center gap-0.5 text-zinc-400 hover:text-zinc-600 transition-colors nodrag"
            onclick={(e) => {
              e.stopPropagation();
              addingInput = true;
            }}
          >
            <span class="text-xs">+</span>
            <span>input</span>
          </button>
        {/if}
      {/if}
    </div>

    <!-- Outputs -->
    <div class="space-y-1 text-right flex flex-col items-end min-w-0 flex-1">
      {#each outputs as port (port.name)}
        {@const pm = portMarkerStyle(port, oneOfRequiredPorts, configFilledPorts, getPortTypeColor(port.portType), 'output')}
        <!-- svelte-ignore a11y_no_static_element_interactions -->
        <div
          class="relative flex items-center gap-1.5 group pr-3"
          title={`${port.name}: ${port.portType}`}
          oncontextmenu={(e) => openPortMenu(e, port, 'output')}
        >
          <span class="truncate">{port.name}</span>
          <Handle
            type="source"
            position={Position.Right}
            id={port.name}
            class={pm.class}
            style={`top: 50%; ${pm.style}`}
          />
        </div>
      {/each}
      {#if canAddOutputs}
        {#if addingOutput}
          <!-- svelte-ignore a11y_autofocus -->
          <input
            class="w-full text-[10px] bg-zinc-100 px-2 py-0.5 rounded outline-none border border-zinc-200 nodrag"
            placeholder="port name"
            bind:value={newPortName}
            onkeydown={(e) => handlePortAddKey(e, 'output')}
            onblur={() => {
              addingOutput = false;
              newPortName = '';
            }}
            onclick={(e) => e.stopPropagation()}
            autofocus
          />
        {:else}
          <button
            class="flex items-center gap-0.5 text-zinc-400 hover:text-zinc-600 transition-colors nodrag"
            onclick={(e) => {
              e.stopPropagation();
              addingOutput = true;
            }}
          >
            <span>output</span>
            <span class="text-xs">+</span>
          </button>
        {/if}
      {/if}
    </div>
  </div>

  <!-- Live data (always visible) -->
  {#if liveDataItems.length > 0}
    <div class="px-3 py-2 border-t border-black/5 space-y-2">
      {#each liveDataItems as item}
        {#if item.type === 'image' && typeof item.data === 'string'}
          <div>
            <span class="text-[10px] text-zinc-500 font-medium">{item.label}</span>
            <img src={item.data} alt={item.label} class="w-full rounded border border-zinc-200 mt-1" />
          </div>
        {:else if item.type === 'text'}
          <div>
            <span class="text-[10px] text-zinc-500 font-medium block mb-1">{item.label}</span>
            <div class="w-full text-[10px] font-mono bg-zinc-100 rounded px-2 py-1.5 break-all border border-zinc-200 select-text cursor-text">
              {String(item.data)}
            </div>
          </div>
        {:else if item.type === 'progress' && typeof item.data === 'number'}
          <div>
            <span class="text-[10px] text-zinc-500 font-medium">{item.label}</span>
            <div class="w-full h-1.5 bg-zinc-200 rounded-full mt-1 overflow-hidden">
              <div class="h-full bg-emerald-500 rounded-full transition-all" style={`width: ${Math.round(item.data * 100)}%`}></div>
            </div>
          </div>
        {/if}
      {/each}
    </div>
  {/if}

  <!-- Expanded block -->
  {#if expanded}
    <div class="px-3 pt-2 pb-3 border-t border-black/5 space-y-2 nodrag">
      {#if setupGuide}
        <div class="border border-zinc-200 rounded">
          <button
            type="button"
            class="w-full flex items-center justify-between px-2 py-1 text-[10px] uppercase tracking-wider text-zinc-500 hover:bg-zinc-50"
            onclick={(e) => {
              e.stopPropagation();
              setupGuideOpen = !setupGuideOpen;
            }}
          >
            <span>Setup Guide</span>
            <span>{setupGuideOpen ? '−' : '+'}</span>
          </button>
          {#if setupGuideOpen}
            <div class="p-2 text-[11px] text-zinc-600 whitespace-pre-wrap">{setupGuide}</div>
          {/if}
        </div>
      {/if}

      {#each displayedFields as field (field.key)}
        <FieldEditor
          field={field}
          value={config[field.key]}
          wired={wired.has(field.key)}
          textareaHeight={textareaHeights[field.key]}
          onResize={(h) => handleTextareaResize(field.key, h)}
          onChange={(v) => onFieldChange(field.key, v)}
        />
      {/each}

      {#if showDebugPreview}
        {#if debugData !== undefined}
          <pre class="debug-data-container nodrag nopan nowheel select-text cursor-text">{debugDataJson}</pre>
        {:else if latestExec?.status === 'completed'}
          <div class="debug-placeholder completed">✓ Execution complete</div>
        {:else if latestExec?.status === 'failed'}
          <div class="debug-placeholder failed">✗ Execution failed{latestExec.error ? `: ${latestExec.error}` : ''}</div>
        {:else if latestExec?.status === 'running' || latestExec?.status === 'waiting_for_input'}
          <div class="debug-placeholder running">
            <span class="debug-spinner"></span>
            <span>Processing...</span>
          </div>
        {:else}
          <div class="debug-placeholder waiting">📥 Waiting for data...</div>
        {/if}
      {/if}
    </div>
  {/if}

  <!-- _raw handle. v1 wraps Handle around the SVG so the handle IS
       the square; we do the same: Handle is positioned absolutely at
       top-right and its content is the svg. -->
  <div class="absolute" style="top: 18px; right: -5px; z-index: 10;">
    <Handle
      type="source"
      position={Position.Right}
      id="_raw"
      class="!w-[10px] !h-[10px] !bg-transparent !border-none !relative !inset-auto !transform-none"
    >
      <svg width="10" height="10" viewBox="0 0 10 10" style="pointer-events: none;">
        <rect x="0.75" y="0.75" width="8.5" height="8.5" fill={rawConnected ? '#18181b' : 'white'} stroke="#18181b" stroke-width="1.5" />
      </svg>
    </Handle>
  </div>
</div>

<style>
  :global(.node-running) .project-node,
  :global(.project-node.node-running) {
    box-shadow:
      0 1px 3px rgba(0, 0, 0, 0.08),
      0 4px 12px rgba(0, 0, 0, 0.05),
      0 0 0 2px rgba(245, 158, 11, 0.4) !important;
  }
  :global(.node-completed) .project-node,
  :global(.project-node.node-completed) {
    box-shadow:
      0 1px 3px rgba(0, 0, 0, 0.08),
      0 4px 12px rgba(0, 0, 0, 0.05),
      0 0 0 2px rgba(16, 185, 129, 0.3) !important;
  }
  :global(.node-failed) .project-node,
  :global(.project-node.node-failed) {
    box-shadow:
      0 1px 3px rgba(0, 0, 0, 0.08),
      0 4px 12px rgba(0, 0, 0, 0.05),
      0 0 0 2px rgba(239, 68, 68, 0.4) !important;
  }
  .debug-data-container {
    margin: 0;
    background: #f8fafc;
    border: 1px solid #e2e8f0;
    border-radius: 6px;
    padding: 8px;
    min-height: 60px;
    max-height: 400px;
    overflow: auto;
    font-family: ui-monospace, 'SF Mono', Monaco, monospace;
    font-size: 10px;
    line-height: 1.4;
    white-space: pre-wrap;
    word-break: break-word;
    resize: vertical;
    color: #334155;
  }
  .debug-placeholder {
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: 4px;
    padding: 16px 8px;
    border: 1px dashed #e2e8f0;
    border-radius: 6px;
    color: #94a3b8;
    font-size: 11px;
  }
  .debug-placeholder.completed {
    background: #f0fdf4;
    border-color: #bbf7d0;
    color: #22c55e;
  }
  .debug-placeholder.failed {
    background: #fef2f2;
    border-color: #fecaca;
    color: #ef4444;
  }
  .debug-placeholder.running {
    background: #fffbeb;
    border-color: #fde68a;
    color: #f59e0b;
  }
  .debug-placeholder.waiting {
    background: #f8fafc;
    border-color: #e2e8f0;
  }
  .debug-spinner {
    width: 14px;
    height: 14px;
    border: 2px solid #fde68a;
    border-top-color: #f59e0b;
    border-radius: 50%;
    animation: debug-spin 0.8s linear infinite;
    display: inline-block;
  }
  @keyframes debug-spin {
    to {
      transform: rotate(360deg);
    }
  }
</style>
