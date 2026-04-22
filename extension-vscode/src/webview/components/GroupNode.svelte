<script lang="ts">
  // Ported from dashboard-v1/src/lib/components/project/GroupNode.svelte.
  // Two modes: expanded frame (dashed border, dual-handle side ports,
  // resizer, label editing, add-port UI) and collapsed pill (looks
  // like a regular ProjectNode with only the external handles).
  //
  // The dual-handle scheme is the core: each interface port renders
  // BOTH an external Handle (bare id = port name) and an internal
  // Handle (id = `{port}__inner`). Outside connections end at the
  // external; child connections end at the internal. This is how v1
  // routes edges through a group boundary while keeping the Group a
  // single xyflow node.

  import { Handle, NodeResizer, Position, useSvelteFlow } from '@xyflow/svelte';
  import { Maximize2, Minimize2, Layers } from 'lucide-svelte';
  import { tick } from 'svelte';
  import { cn } from '../utils/cn';
  import { portMarkerStyle } from '../utils/port-marker';
  import { getPortTypeColor } from '../utils/colors';
  import { buildPortMenuItems, createPortContextMenu } from '../utils/port-context-menu';
  import { computeGroupMinHeight, computeMinNodeWidth } from '../utils/node-geometry';
  import ExecutionInspector from './ExecutionInspector.svelte';
  import type { PortDefinition } from '../../shared/protocol';
  import type { NodeViewData } from './node-view-data';

  interface Props {
    data: NodeViewData;
    id: string;
    selected?: boolean;
  }

  let { data, id: _id, selected }: Props = $props();

  const { updateNodeInternals } = useSvelteFlow();

  const node = $derived(data.node);
  const config = $derived((node.config ?? {}) as Record<string, unknown>);
  const expanded = $derived(Boolean(config.expanded ?? true));
  const userLabel = $derived(node.label ?? '');
  const inputs: PortDefinition[] = $derived(node.inputs ?? []);
  const outputs: PortDefinition[] = $derived(node.outputs ?? []);
  const executions = $derived(data.executions ?? []);

  const oneOfRequiredPorts: Set<string> = $derived.by(() => {
    const s = new Set<string>();
    for (const grp of node.features?.oneOfRequired ?? []) for (const p of grp) s.add(p);
    return s;
  });

  const description = $derived((config.description as string | undefined) ?? '');
  let showFullDescription = $state(false);
  const descriptionLong = $derived(
    Boolean(description) && (description.length > 80 || description.includes('\n')),
  );

  // Label editing. Group labels are sanitized to identifier form.
  let editingLabel = $state(false);
  let labelInput = $state('');
  function sanitizeLabel(raw: string): string {
    const cleaned = raw.replace(/\s+/g, '_').replace(/[^a-zA-Z0-9_]/g, '');
    return cleaned.replace(/^[0-9]+/, '');
  }
  function startEditLabel(e: MouseEvent) {
    e.stopPropagation();
    labelInput = userLabel;
    editingLabel = true;
  }
  function saveLabel() {
    editingLabel = false;
    const clean = sanitizeLabel(labelInput);
    if (clean && clean !== userLabel) data.onLabelChange(node.id, clean);
  }
  function handleLabelKeydown(e: KeyboardEvent) {
    if (e.key === 'Enter') saveLabel();
    else if (e.key === 'Escape') {
      editingLabel = false;
      labelInput = userLabel;
    }
  }

  // Resize → push width/height into config (which routes to layout
  // sidecar on the other side of onConfigChange).
  function handleResizeEnd(_event: unknown, params: { width: number; height: number }) {
    data.onConfigChange(node.id, 'width', params.width);
    data.onConfigChange(node.id, 'height', params.height);
    data.onConfigChange(node.id, 'expanded', true);
  }

  // Min-height enforcement when port count grows.
  let lastEnforcedMinH = $state(0);
  $effect(() => {
    const want = computeGroupMinHeight(inputs.length, outputs.length);
    const cur = (config.height as number | undefined) ?? 0;
    if (want > cur && want !== lastEnforcedMinH) {
      lastEnforcedMinH = want;
      data.onConfigChange(node.id, 'height', want);
    }
  });

  async function toggleExpand(e: MouseEvent) {
    e.stopPropagation();
    data.onConfigChange(node.id, 'expanded', !expanded);
    await tick();
    updateNodeInternals(node.id);
  }

  // Port context menu on right-click.
  let menuCleanup: (() => void) | undefined;
  function openPortMenu(
    e: MouseEvent,
    port: PortDefinition,
    side: 'input' | 'output',
  ) {
    e.preventDefault();
    e.stopPropagation();
    menuCleanup?.();
    const items = buildPortMenuItems({
      port,
      side,
      isCustom: true,
      canAddPorts: true,
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

  // Add-port UI.
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
    if (existing.includes(name)) {
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
      configurable: true,
    };
    const list = [...(side === 'input' ? inputs : outputs), fresh];
    data.onPortsChange(node.id, side === 'input' ? { inputs: list } : { outputs: list });
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

  // Collapsed width sized to the widest port-row.
  const collapsedWidth = $derived(computeMinNodeWidth(inputs, outputs));

  function execStatus() {
    const latest = executions[executions.length - 1];
    if (!latest) return '';
    if (latest.status === 'failed') return 'node-failed';
    if (latest.status === 'completed' || latest.status === 'skipped') return 'node-completed';
    if (latest.status === 'running' || latest.status === 'waiting_for_input') return 'node-running';
    return '';
  }
  const glowClass = $derived(execStatus());
</script>

{#if expanded}
  <NodeResizer
    minWidth={250}
    minHeight={Math.max(200, computeGroupMinHeight(inputs.length, outputs.length))}
    isVisible={selected}
    lineStyle="border-color: #4f46e5; border-width: 2px;"
    handleStyle="background-color: #4f46e5; width: 10px; height: 10px; border-radius: 2px;"
    onResizeEnd={handleResizeEnd}
  />

  <div
    class={cn(
      'expanded-container relative w-full h-full rounded-xl',
      selected && 'selected',
      glowClass,
    )}
  >
    <!-- Header ribbon -->
    <div class="absolute top-0 left-0 right-0 h-10 flex items-center justify-between px-3 bg-white/80 border-b border-black/10 rounded-t-xl z-[2]">
      <div class="flex items-center gap-2 min-w-0">
        <Layers class="size-3.5 text-zinc-600" />
        {#if editingLabel}
          <!-- svelte-ignore a11y_autofocus -->
          <input
            class="text-[11px] font-semibold text-zinc-700 bg-zinc-100 px-1.5 py-0.5 rounded border border-zinc-200 outline-none focus:border-zinc-400 nodrag"
            bind:value={labelInput}
            onblur={saveLabel}
            onkeydown={handleLabelKeydown}
            onclick={(e) => e.stopPropagation()}
            autofocus
          />
        {:else}
          <!-- svelte-ignore a11y_no_static_element_interactions -->
          <!-- svelte-ignore a11y_click_events_have_key_events -->
          <span
            class="text-[11px] font-semibold text-zinc-700 cursor-text hover:bg-black/5 px-1.5 py-0.5 rounded truncate"
            role="button"
            tabindex={0}
            ondblclick={startEditLabel}
            title="Double-click to edit"
          >
            {userLabel || node.id}
          </span>
        {/if}
      </div>
      <div class="flex items-center gap-0.5 shrink-0 nodrag">
        <ExecutionInspector executions={executions} label={userLabel || 'Group'} />
        <button
          type="button"
          class="w-5 h-5 flex items-center justify-center rounded hover:bg-black/5 text-zinc-400 hover:text-zinc-600 transition-colors"
          onclick={toggleExpand}
          title="Collapse"
          aria-label="Collapse"
        >
          <Minimize2 class="w-3 h-3" />
        </button>
      </div>
    </div>

    <!-- Input side ports (absolute, left) -->
    <div class="expanded-side-ports expanded-side-left">
      {#each inputs as port (port.name)}
        {@const pm = portMarkerStyle(port, oneOfRequiredPorts, new Set(), getPortTypeColor(port.portType), 'input')}
        <!-- svelte-ignore a11y_no_static_element_interactions -->
        <div
          class="expanded-port-row flex items-center gap-2"
          oncontextmenu={(e) => openPortMenu(e, port, 'input')}
          title={`${port.name}: ${port.portType}${port.required ? ' (required)' : ''}`}
        >
          <Handle
            type="target"
            position={Position.Left}
            id={port.name}
            style={pm.style}
            class={pm.class}
          />
          <span class="text-[10px] text-zinc-600 truncate">{port.name}</span>
          <Handle
            type="source"
            position={Position.Right}
            id={`${port.name}__inner`}
            style={`background-color: ${getPortTypeColor(port.portType)}; border-color: white;`}
            class="!w-2.5 !h-2.5 !border !border-white !rounded-full !relative !inset-auto !transform-none"
          />
        </div>
      {/each}
      {#if addingInput}
        <!-- svelte-ignore a11y_autofocus -->
        <input
          class="text-[10px] bg-zinc-100 px-2 py-0.5 rounded outline-none border border-zinc-200 nodrag"
          placeholder="port name"
          bind:value={newPortName}
          onkeydown={(e) => handlePortAddKey(e, 'input')}
          onblur={() => {
            addingInput = false;
            newPortName = '';
          }}
          autofocus
        />
      {:else}
        <button
          class="flex items-center gap-0.5 text-zinc-400 hover:text-zinc-600 text-[10px] nodrag"
          onclick={(e) => {
            e.stopPropagation();
            addingInput = true;
          }}
        >
          <span class="text-xs">+</span>
          <span>input</span>
        </button>
      {/if}
    </div>

    <!-- Output side ports (absolute, right) -->
    <div class="expanded-side-ports expanded-side-right">
      {#each outputs as port (port.name)}
        {@const pm = portMarkerStyle(port, oneOfRequiredPorts, new Set(), getPortTypeColor(port.portType), 'output')}
        <!-- svelte-ignore a11y_no_static_element_interactions -->
        <div
          class="expanded-port-row flex items-center gap-2 justify-end"
          oncontextmenu={(e) => openPortMenu(e, port, 'output')}
          title={`${port.name}: ${port.portType}`}
        >
          <Handle
            type="target"
            position={Position.Left}
            id={`${port.name}__inner`}
            style={`background-color: ${getPortTypeColor(port.portType)}; border-color: white;`}
            class="!w-2.5 !h-2.5 !border !border-white !rounded-full !relative !inset-auto !transform-none"
          />
          <span class="text-[10px] text-zinc-600 truncate">{port.name}</span>
          <Handle
            type="source"
            position={Position.Right}
            id={port.name}
            style={pm.style}
            class={pm.class}
          />
        </div>
      {/each}
      {#if addingOutput}
        <!-- svelte-ignore a11y_autofocus -->
        <input
          class="text-[10px] bg-zinc-100 px-2 py-0.5 rounded outline-none border border-zinc-200 nodrag"
          placeholder="port name"
          bind:value={newPortName}
          onkeydown={(e) => handlePortAddKey(e, 'output')}
          onblur={() => {
            addingOutput = false;
            newPortName = '';
          }}
          autofocus
        />
      {:else}
        <button
          class="flex items-center gap-0.5 text-zinc-400 hover:text-zinc-600 text-[10px] nodrag"
          onclick={(e) => {
            e.stopPropagation();
            addingOutput = true;
          }}
        >
          <span>output</span>
          <span class="text-xs">+</span>
        </button>
      {/if}
    </div>
  </div>
{:else}
  <!-- Collapsed pill. Uses the same shape as ProjectNode in collapsed. -->
  <div
    class={cn(
      'collapsed-node relative rounded-lg bg-white overflow-hidden',
      selected && 'selected',
      glowClass,
    )}
    style={`width: ${collapsedWidth}px;`}
  >
    <div class="h-[3px] w-full" style="background: #52525b;"></div>
    <div class="flex items-center justify-between px-3 py-2 border-b border-black/5">
      <div class="flex items-center gap-1.5 min-w-0">
        <Layers class="size-3 text-zinc-600" />
        <span class="text-[10px] font-semibold uppercase tracking-wider text-zinc-700">Group</span>
      </div>
      <div class="flex items-center gap-0.5 shrink-0 nodrag">
        <ExecutionInspector executions={executions} label={userLabel || 'Group'} />
        <button
          type="button"
          class="w-5 h-5 flex items-center justify-center rounded hover:bg-black/5 text-zinc-400 hover:text-zinc-600 transition-colors"
          onclick={toggleExpand}
          title="Expand"
          aria-label="Expand"
        >
          <Maximize2 class="w-3 h-3" />
        </button>
      </div>
    </div>

    <div class="px-3 pt-2">
      {#if editingLabel}
        <!-- svelte-ignore a11y_autofocus -->
        <input
          class="w-full text-sm font-medium bg-zinc-100 text-zinc-900 px-2 py-1 rounded border border-zinc-200 outline-none focus:border-zinc-400 nodrag"
          bind:value={labelInput}
          onblur={saveLabel}
          onkeydown={handleLabelKeydown}
          autofocus
        />
      {:else}
        <p
          class="text-sm font-medium text-zinc-800 cursor-text hover:bg-black/5 px-1 py-0.5 rounded -mx-1 truncate"
          ondblclick={startEditLabel}
          title="Double-click to edit"
        >
          {userLabel || node.id}
        </p>
      {/if}
    </div>

    {#if description}
      <div class="px-3 pt-1 text-[10px] text-zinc-500">
        <span class={showFullDescription ? '' : 'line-clamp-2 block'}>{description}</span>
        {#if descriptionLong}
          <button
            class="text-blue-500 hover:underline mt-0.5 text-[10px] nodrag"
            onclick={(e) => {
              e.stopPropagation();
              showFullDescription = !showFullDescription;
            }}
          >{showFullDescription ? 'Show less' : 'Show more'}</button>
        {/if}
      </div>
    {/if}

    <div class="flex justify-between gap-4 px-3 py-2 text-[10px] text-zinc-500">
      <div class="space-y-1 min-w-0 flex-1">
        {#each inputs as port (port.name)}
          {@const pm = portMarkerStyle(port, oneOfRequiredPorts, new Set(), getPortTypeColor(port.portType), 'input')}
          <!-- svelte-ignore a11y_no_static_element_interactions -->
          <div
            class="relative flex items-center gap-1.5 pl-3"
            oncontextmenu={(e) => openPortMenu(e, port, 'input')}
            title={`${port.name}: ${port.portType}${port.required ? ' (required)' : ''}`}
          >
            <Handle
              type="target"
              position={Position.Left}
              id={port.name}
              style={`top: 50%; ${pm.style}`}
              class={pm.class}
            />
            <span class="truncate">{port.name}</span>
          </div>
        {/each}
      </div>
      <div class="space-y-1 text-right flex flex-col items-end min-w-0 flex-1">
        {#each outputs as port (port.name)}
          {@const pm = portMarkerStyle(port, oneOfRequiredPorts, new Set(), getPortTypeColor(port.portType), 'output')}
          <!-- svelte-ignore a11y_no_static_element_interactions -->
          <div
            class="relative flex items-center gap-1.5 pr-3"
            oncontextmenu={(e) => openPortMenu(e, port, 'output')}
            title={`${port.name}: ${port.portType}`}
          >
            <span class="truncate">{port.name}</span>
            <Handle
              type="source"
              position={Position.Right}
              id={port.name}
              style={`top: 50%; ${pm.style}`}
              class={pm.class}
            />
          </div>
        {/each}
      </div>
    </div>
  </div>
{/if}

<style>
  .expanded-container {
    background: rgba(148, 163, 184, 0.06);
    border: 2px dashed rgba(148, 163, 184, 0.4);
    min-width: 250px;
    min-height: 200px;
  }
  .expanded-container.selected {
    border-color: #4f46e5;
    border-style: solid;
    background: rgba(79, 70, 229, 0.04);
  }
  .expanded-side-ports {
    position: absolute;
    top: 48px;
    display: flex;
    flex-direction: column;
    gap: 6px;
    z-index: 1;
    padding: 4px 0;
  }
  .expanded-side-left {
    left: 6px;
  }
  .expanded-side-right {
    right: 6px;
  }
  .expanded-port-row {
    min-height: 18px;
  }
  .collapsed-node {
    border: 1px solid #e4e4e7;
    box-shadow: 0 1px 3px rgba(0, 0, 0, 0.06);
    min-width: 160px;
  }
  .collapsed-node.selected {
    border-color: #4f46e5;
    box-shadow: 0 0 0 2px rgba(79, 70, 229, 0.15);
  }
  :global(.node-running .collapsed-node),
  :global(.node-running) .expanded-container {
    box-shadow:
      0 1px 3px rgba(0, 0, 0, 0.08),
      0 0 0 2px rgba(245, 158, 11, 0.4) !important;
  }
  :global(.node-completed .collapsed-node),
  :global(.node-completed) .expanded-container {
    box-shadow:
      0 1px 3px rgba(0, 0, 0, 0.08),
      0 0 0 2px rgba(16, 185, 129, 0.3) !important;
  }
  :global(.node-failed .collapsed-node),
  :global(.node-failed) .expanded-container {
    box-shadow:
      0 1px 3px rgba(0, 0, 0, 0.08),
      0 0 0 2px rgba(239, 68, 68, 0.4) !important;
  }
</style>
