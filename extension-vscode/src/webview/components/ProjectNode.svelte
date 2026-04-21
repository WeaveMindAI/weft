<script lang="ts">
  import { Handle, Position, NodeResizer, useSvelteFlow } from '@xyflow/svelte';
  import { tick } from 'svelte';
  import { cn } from '../utils/cn';
  import { resolveIcon } from '../utils/icon';
  import { portMarkerStyle } from '../utils/port-marker';
  import { getPortTypeColor } from '../utils/colors';
  import { getStatusIcon } from '../utils/status';
  import { buildPortMenuItems, createPortContextMenu } from '../utils/port-context-menu';
  import FieldEditor from './FieldEditor.svelte';
  import ExecutionInspector from './ExecutionInspector.svelte';
  import type {
    FieldDef,
    PortDefinition,
  } from '../../shared/protocol';
  import type { NodeViewData } from './node-view-data';

  interface Props {
    data: NodeViewData;
    id: string;
    selected?: boolean;
  }

  let { data, id, selected }: Props = $props();

  const { getViewport, setViewport } = useSvelteFlow();

  const node = $derived(data.node);
  const catalog = $derived(data.catalog);
  const wired = $derived(data.wiredInputs);
  const exec = $derived(data.exec);
  const config: Record<string, unknown> = $derived(
    (node.config ?? {}) as Record<string, unknown>,
  );
  const color = $derived(catalog?.color ?? '#52525b');
  const Icon = $derived(resolveIcon(catalog?.icon));

  const typeLabel = $derived(catalog?.label ?? node.nodeType);
  const userLabel = $derived(node.label ?? '');
  const expanded = $derived(Boolean(config.expanded));

  const statusClass = $derived.by(() => {
    switch (exec.status) {
      case 'started':
        return 'node-running-glow';
      case 'completed':
        return 'node-completed-glow';
      case 'failed':
        return 'node-failed-glow';
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

  // A catalog input port's name set, to know which ports are
  // user-added (custom) vs declared in the catalog.
  const catalogInputNames: Set<string> = $derived(
    new Set(((catalog?.inputs ?? []) as PortDefinition[]).map((p) => p.name)),
  );
  const catalogOutputNames: Set<string> = $derived(
    new Set(((catalog?.outputs ?? []) as PortDefinition[]).map((p) => p.name)),
  );

  const canAddInputs: boolean = $derived(
    Boolean(catalog?.features?.canAddInputPorts),
  );
  const canAddOutputs: boolean = $derived(
    Boolean(catalog?.features?.canAddOutputPorts),
  );

  const oneOfRequiredPorts: Set<string> = $derived.by(() => {
    const s = new Set<string>();
    const groups = (catalog?.features?.oneOfRequired ?? []) as string[][];
    for (const grp of groups) for (const p of grp) s.add(p);
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

  const hasExpandableContent = $derived(displayedFields.length > 0);

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
    if (e.key === 'Enter') {
      saveLabel();
    } else if (e.key === 'Escape') {
      editingLabel = false;
      labelInput = userLabel;
    }
  }

  // Collapse/expand with viewport anchoring. Pin the node's screen
  // position between before/after layout so the cursor stays over the
  // expand button, matching v1.
  let nodeEl: HTMLDivElement | undefined = $state();
  async function toggleExpand(e: MouseEvent) {
    e.stopPropagation();
    const before = nodeEl?.getBoundingClientRect();
    data.onConfigChange(node.id, 'expanded', !expanded);
    if (!before) return;
    await tick();
    await new Promise((r) => requestAnimationFrame(() => r(undefined)));
    const after = nodeEl?.getBoundingClientRect();
    if (!after) return;
    // Pin the top-right corner (where the expand button sits). If
    // the node moved in screen-space after the layout/size change,
    // offset the viewport to put it back.
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

  // Port context menu. The right-click handler captures the client
  // coords and spawns the shared menu on document.body. The cleanup
  // callback nukes the menu on close.
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
        const nextList = (side === 'input' ? inputs : outputs).map((p) =>
          p.name === port.name ? { ...p, required: !p.required } : p,
        );
        data.onPortsChange(node.id, side === 'input' ? { inputs: nextList } : { outputs: nextList });
      },
      onSetType: (newType) => {
        const nextList = (side === 'input' ? inputs : outputs).map((p) =>
          p.name === port.name ? { ...p, portType: newType } : p,
        );
        data.onPortsChange(node.id, side === 'input' ? { inputs: nextList } : { outputs: nextList });
      },
      onRemove: () => {
        const nextList = (side === 'input' ? inputs : outputs).filter((p) => p.name !== port.name);
        data.onPortsChange(node.id, side === 'input' ? { inputs: nextList } : { outputs: nextList });
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
      // Reject dup or reserved name; just reset.
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
      laneDepth: 0,
      configurable: side === 'input',
    };
    const next = [...(side === 'input' ? inputs : outputs), fresh];
    data.onPortsChange(node.id, side === 'input' ? { inputs: next } : { outputs: next });
    newPortName = '';
    if (side === 'input') addingInput = false;
    else addingOutput = false;
  }
  function handlePortAddKey(e: KeyboardEvent, side: 'input' | 'output') {
    if (e.key === 'Enter') {
      addPort(side);
    } else if (e.key === 'Escape') {
      newPortName = '';
      if (side === 'input') addingInput = false;
      else addingOutput = false;
    }
  }

  const liveData = $derived(data.liveData ?? []);
  const rawConnected = $derived(wired.has('_raw'));
</script>

<NodeResizer
  isVisible={selected && expanded}
  minWidth={200}
  minHeight={120}
  lineClass="!border-zinc-400"
  handleClass="!bg-zinc-500 !w-2 !h-2 !rounded-[2px]"
/>

<div
  bind:this={nodeEl}
  class={cn(
    'project-node rounded-md bg-white relative flex flex-col overflow-hidden select-none transition-all duration-200',
    'min-w-[200px]',
    statusClass,
  )}
  style={`border: 1px solid ${selected ? color : 'rgba(0,0,0,0.08)'}; ${selected ? `box-shadow: 0 1px 3px rgba(0,0,0,0.08), 0 4px 12px rgba(0,0,0,0.05), 0 0 0 1px ${color}20;` : 'box-shadow: 0 1px 3px rgba(0,0,0,0.08), 0 4px 12px rgba(0,0,0,0.05);'}`}
>
  <div class="h-[2px] w-full" style={`background: ${color};`}></div>

  <!-- Header -->
  <div class="flex items-center justify-between px-3 py-2 border-b border-black/5">
    <div class="flex items-center gap-1.5 min-w-0">
      <span class={cn('text-[11px]', exec.status === 'started' && 'animate-pulse')} style={`color: ${color};`}>
        {getStatusIcon(exec.status === 'idle' ? '' : exec.status === 'started' ? 'running' : exec.status)}
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
      <ExecutionInspector status={exec.status} input={exec.input} output={exec.output} error={exec.error} />
      {#if hasExpandableContent}
        <button
          type="button"
          class="w-5 h-5 flex items-center justify-center rounded hover:bg-black/5 text-zinc-400 hover:text-zinc-600 transition-colors"
          onclick={toggleExpand}
          aria-label={expanded ? 'collapse' : 'expand'}
          title={expanded ? 'Collapse' : 'Expand'}
        >
          {#if expanded}
            <svg viewBox="0 0 24 24" width="12" height="12" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
              <polyline points="4 14 10 14 10 20" />
              <polyline points="20 10 14 10 14 4" />
              <line x1="14" y1="10" x2="21" y2="3" />
              <line x1="3" y1="21" x2="10" y2="14" />
            </svg>
          {:else}
            <svg viewBox="0 0 24 24" width="12" height="12" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
              <polyline points="15 3 21 3 21 9" />
              <polyline points="9 21 3 21 3 15" />
              <line x1="21" y1="3" x2="14" y2="10" />
              <line x1="3" y1="21" x2="10" y2="14" />
            </svg>
          {/if}
        </button>
      {/if}
    </div>
  </div>

  <!-- Label -->
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
      <p
        class="text-sm font-medium text-zinc-800 cursor-text hover:bg-black/5 px-1 py-0.5 rounded -mx-1 truncate"
        ondblclick={startEditLabel}
        title="Double-click to edit"
      >
        {userLabel || `${typeLabel} Node`}
      </p>
    {/if}
  </div>

  <!-- Port rows. Two columns, always visible. -->
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
          <Handle type="target" position={Position.Left} id={port.name} class={pm.class} style={`top: 50%; ${pm.style}`} />
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

    <!-- Outputs (right-aligned) -->
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
          <Handle type="source" position={Position.Right} id={port.name} class={pm.class} style={`top: 50%; ${pm.style}`} />
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

  {#if expanded && displayedFields.length > 0}
    <div class="px-3 pt-2 pb-3 border-t border-black/5 space-y-2 nodrag">
      {#each displayedFields as field (field.key)}
        <FieldEditor
          field={field}
          value={config[field.key]}
          wired={wired.has(field.key)}
          onChange={(v) => onFieldChange(field.key, v)}
        />
      {/each}
    </div>
  {/if}

  {#if liveData.length > 0}
    <div class="px-3 py-2 border-t border-black/5 space-y-2">
      {#each liveData as item}
        {#if item.type === 'image' && typeof item.data === 'string'}
          <div>
            <span class="text-[10px] text-zinc-500 font-medium">{item.label}</span>
            <img src={item.data} alt={item.label} class="w-full rounded border border-zinc-200 mt-1" />
          </div>
        {:else if item.type === 'text'}
          <div>
            <span class="text-[10px] text-zinc-500 font-medium block mb-1">{item.label}</span>
            <div class="w-full text-[10px] font-mono bg-zinc-100 rounded px-2 py-1.5 break-all border border-zinc-200 select-text">
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

  <div class="absolute" style="top: 18px; right: -5px; pointer-events: none;">
    <svg width="10" height="10" viewBox="0 0 10 10" style="pointer-events: none;">
      <rect x="0.75" y="0.75" width="8.5" height="8.5" fill={rawConnected ? '#18181b' : 'white'} stroke="#18181b" stroke-width="1.5" />
    </svg>
    <Handle
      type="source"
      position={Position.Right}
      id="_raw"
      class="!w-[10px] !h-[10px] !bg-transparent !border-none"
      style="top: 0; right: 0; opacity: 0; pointer-events: auto;"
    />
  </div>
</div>
