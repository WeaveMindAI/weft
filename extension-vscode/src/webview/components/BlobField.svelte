<script lang="ts">
  // Stub BlobField for file-style config fields. v1 did drag-drop
  // upload to R2 / object storage; v2 defers the upload pipeline. For
  // now we render a simple URL input so the user can paste a blob ref.

  import { Upload } from 'lucide-svelte';

  interface Props {
    value: string | null;
    onChange: (next: string | null) => void;
    accept?: string;
  }

  let { value, onChange, accept }: Props = $props();
</script>

<div class="flex items-center gap-2">
  <Upload class="size-3 text-muted-foreground" />
  <input
    type="url"
    class="flex-1 rounded border border-border/60 bg-input px-2 py-1 text-[11px] font-mono focus:outline-none focus:ring-1 focus:ring-ring"
    value={value ?? ''}
    placeholder="https://... or file://..."
    onchange={(e) => onChange((e.currentTarget as HTMLInputElement).value || null)}
  />
  {#if accept}
    <span class="text-[10px] text-muted-foreground">{accept}</span>
  {/if}
</div>
