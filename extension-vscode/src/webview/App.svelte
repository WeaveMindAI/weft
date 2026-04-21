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

<div class="root">
  {#if error}
    <div class="error">parse error: {error}</div>
  {:else if response}
    <Graph project={response.project} />
  {:else}
    <div class="loading">loading graph...</div>
  {/if}
</div>

<style>
  .root {
    position: absolute;
    inset: 0;
    background: var(--vscode-editor-background);
    color: var(--vscode-editor-foreground);
    font-family: var(--vscode-font-family);
  }

  .loading, .error {
    padding: 1rem;
    color: var(--vscode-descriptionForeground);
  }

  .error {
    color: var(--vscode-errorForeground);
  }
</style>
