<script lang="ts">
  import { onMount } from 'svelte';
  import ProjectEditor from './lib/components/project/ProjectEditor.svelte';
  import { send, onMessage } from './vscode';
  import {
    setCachedParseResponse,
    setRemoteParseTrigger,
    type WeftParseError,
  } from './lib/ai/weft-parser';
  import { translateProject } from './host-bridge';
  import type { ProjectDefinition as V1Project } from './lib/types';

  let project: V1Project | null = $state(null);
  let error: string | null = $state(null);
  let weftCode = $state('');
  let layoutCode = $state('');

  onMount(() => {
    setRemoteParseTrigger((_source) => {
      // Webview text surgery has produced a new source; the host
      // writes it to the document, which triggers a new parse.
      // Nothing to do here — the resulting parseResult message
      // lands through onMessage below.
    });

    const unsub = onMessage((msg) => {
      if (msg.kind === 'parseResult') {
        const errs: WeftParseError[] = msg.response.diagnostics
          .filter((d) => d.severity === 'error')
          .map((d) => ({ line: d.line, message: d.message }));
        weftCode = msg.source;
        layoutCode = msg.layoutCode;
        setCachedParseResponse(msg.response.project, msg.source, msg.layoutCode, errs);
        project = translateProject(msg.response.project, msg.source, msg.layoutCode);
        error = null;
      } else if (msg.kind === 'parseError') {
        error = msg.error;
      }
    });
    send({ kind: 'ready' });
    return unsub;
  });

  function onSave(data: {
    name?: string;
    description?: string;
    weftCode?: string;
    loomCode?: string;
    layoutCode?: string;
  }) {
    // v1 sends the whole new weft source after every edit. We
    // forward that to the host which range-replaces the VS Code
    // document; the reparse round-trips through onMessage above.
    if (data.weftCode !== undefined && data.weftCode !== weftCode) {
      send({ kind: 'saveWeft', source: data.weftCode });
    }
    if (data.layoutCode !== undefined && data.layoutCode !== layoutCode) {
      send({ kind: 'saveLayout', layoutCode: data.layoutCode });
    }
  }

  // Preserve the host-supplied weftCode / layoutCode on the project
  // we hand ProjectEditor, which otherwise falls back to prop
  // defaults that are empty. v1's editor also patches the prop on
  // parse updates so this matches the contract it expects.
  const projectForEditor = $derived<V1Project | null>(
    project ? { ...project, weftCode, layoutCode } : null,
  );
</script>

<div class="absolute inset-0">
  {#if error}
    <div class="p-4 text-destructive">parse error: {error}</div>
  {:else if projectForEditor}
    <ProjectEditor project={projectForEditor} {onSave} playground={true} />
  {:else}
    <div class="p-4 text-muted-foreground">loading graph...</div>
  {/if}
</div>
