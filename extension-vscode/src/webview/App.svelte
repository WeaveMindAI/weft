<script lang="ts">
  import { onMount } from 'svelte';
  import Graph from './Graph.svelte';
  import { send, onMessage } from './vscode';
  import type { ParseResponse } from '../shared/protocol';

  let response: ParseResponse | null = $state(null);
  let error: string | null = $state(null);

  onMount(() => {
    const unsub = onMessage((msg) => {
      if (msg.kind === 'parseResult') {
        response = msg.response;
        error = null;
      } else if (msg.kind === 'parseError') {
        error = msg.error;
      }
    });
    send({ kind: 'ready' });
    return unsub;
  });
</script>

<div class="absolute inset-0">
  {#if error}
    <div class="p-4 text-destructive">parse error: {error}</div>
  {:else if response}
    <Graph project={response.project} catalog={response.catalog} />
  {:else}
    <div class="p-4 text-muted-foreground">loading graph...</div>
  {/if}
</div>
