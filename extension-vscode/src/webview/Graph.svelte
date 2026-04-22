<script lang="ts">
  // Thin wrapper: <SvelteFlowProvider> establishes the xyflow store
  // context BEFORE any descendant calls useSvelteFlow(). GraphCanvas
  // holds all the real logic (composition, interactions, overlay).
  //
  // Why not put this inside GraphCanvas? `useSvelteFlow()` runs at
  // script-evaluation time; by then GraphCanvas's template hasn't
  // mounted yet, so a provider INSIDE its template wouldn't exist
  // when the hook fires. Wrapping one level up is the idiomatic
  // pattern from xyflow's docs.

  import { SvelteFlowProvider } from '@xyflow/svelte';
  import GraphCanvas from './GraphCanvas.svelte';
  import type { CatalogEntry, ProjectDefinition } from '../shared/protocol';

  interface Props {
    project: ProjectDefinition;
    catalog: Record<string, CatalogEntry>;
  }

  let { project, catalog }: Props = $props();
</script>

<SvelteFlowProvider>
  <GraphCanvas {project} {catalog} />
</SvelteFlowProvider>
