<script lang="ts">
  import { onMount } from 'svelte';
  import ProjectEditor from './lib/components/project/ProjectEditor.svelte';
  import { send, onMessage } from './vscode';
  import {
    setCachedParseResponse,
    setRemoteParseTrigger,
    type WeftParseError,
  } from './lib/ai/weft-parser';
  import { registerCatalog, type CatalogEntry } from './lib/nodes';
  import { translateProject } from './host-bridge';
  import type { ProjectDefinition as V1Project } from './lib/types';

  let project: V1Project | null = $state(null);
  let error: string | null = $state(null);
  let weftCode = $state('');
  let layoutCode = $state('');
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  let editorRef: any = $state();

  onMount(() => {
    setRemoteParseTrigger((_source) => {
      // Webview text surgery produced a new source; the host writes
      // it to the document and re-parses. The resulting parseResult
      // message lands through onMessage below.
    });

    const unsub = onMessage((msg) => {
      if (msg.kind === 'catalogAll') {
        registerCatalog(msg.catalog as unknown as Record<string, CatalogEntry>);
        return;
      }
      if (msg.kind === 'parseResult') {
        const errs: WeftParseError[] = msg.response.diagnostics
          .filter((d) => d.severity === 'error')
          .map((d) => ({ line: d.line, message: d.message }));
        // IMPORTANT: populate NODE_TYPE_CONFIG BEFORE setting
        // `project`. ProjectEditorInner reads the registry during
        // its first render via $derived, and we don't want those
        // reads to see an empty registry on mount.
        registerCatalog(msg.response.catalog as unknown as Record<string, CatalogEntry>);
        const firstMount = project === null;
        setCachedParseResponse(msg.response.project, msg.source, msg.layoutCode, errs);
        weftCode = msg.source;
        layoutCode = msg.layoutCode;
        if (firstMount) {
          // Initial mount: hand a fully-populated project to the
          // editor so it grabs weftCode/layoutCode as its initial
          // $state snapshot.
          project = translateProject(msg.response.project, msg.source, msg.layoutCode);
        } else if (editorRef) {
          // Subsequent updates: push into the already-mounted editor
          // so its local weftCode stays in sync with the document.
          editorRef.applyExternalSource?.(msg.source, msg.layoutCode);
        }
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
    if (data.weftCode !== undefined && data.weftCode !== weftCode) {
      weftCode = data.weftCode;
      send({ kind: 'saveWeft', source: data.weftCode });
    }
    if (data.layoutCode !== undefined && data.layoutCode !== layoutCode) {
      layoutCode = data.layoutCode;
      send({ kind: 'saveLayout', layoutCode: data.layoutCode });
    }
  }
</script>

<div class="absolute inset-0">
  {#if error}
    <div class="p-4 text-destructive">parse error: {error}</div>
  {:else if project}
    <ProjectEditor bind:this={editorRef} {project} {onSave} playground={true} />
  {:else}
    <div class="p-4 text-muted-foreground">loading graph...</div>
  {/if}
</div>
