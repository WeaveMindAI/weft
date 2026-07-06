<script lang="ts">
  // Edit a node's tags as chips. Tags are free-form labels on a node
  // (`config._tags`), used to scope which signals a listener may see and
  // answer. Charset matches the parser's rule ([A-Za-z0-9_-], 1..64), enforced
  // here so an invalid tag is rejected before it reaches config.
  //
  // Pure + host-agnostic: it takes the current tags and an `onChange`, and never
  // touches config directly. The caller (a node context menu, a host sidebar)
  // wires `onChange` to whatever edit path it uses.

  import { X } from '@lucide/svelte';

  let {
    tags,
    onChange,
    note
  }: {
    tags: string[];
    onChange: (next: string[]) => void;
    /// Optional caption under the input (e.g. the account-wide-tag warning).
    note?: string;
  } = $props();

  let draft = $state('');
  let error = $state<string | null>(null);

  const TAG_RE = /^[A-Za-z0-9_-]{1,64}$/;

  function addTag() {
    const t = draft.trim();
    if (t === '') return;
    if (!TAG_RE.test(t)) {
      error = 'Tags allow letters, numbers, _ and -, up to 64 characters.';
      return;
    }
    if (tags.includes(t)) {
      error = `Already tagged "${t}".`;
      return;
    }
    error = null;
    onChange([...tags, t]);
    draft = '';
  }

  function removeTag(t: string) {
    onChange(tags.filter((x) => x !== t));
  }

  function onKeydown(e: KeyboardEvent) {
    if (e.key === 'Enter' || e.key === ',') {
      e.preventDefault();
      addTag();
    } else if (e.key === 'Backspace' && draft === '' && tags.length > 0) {
      removeTag(tags[tags.length - 1]);
    }
  }

  /// Blur is not an explicit submit (Enter/comma are). Committing the raw draft on
  /// blur committed partial/unintended text when the user just clicked away, and it
  /// raced the click-away close. Only commit on blur when the draft is ALREADY a
  /// valid, complete, non-duplicate tag; otherwise drop it silently (no error
  /// banner, since the user didn't ask to add it).
  function onBlur() {
    const t = draft.trim();
    if (t !== '' && TAG_RE.test(t) && !tags.includes(t)) {
      error = null;
      onChange([...tags, t]);
    }
    draft = '';
  }
</script>

<div class="flex flex-col gap-2">
  <div class="flex flex-wrap items-center gap-1.5">
    {#each tags as t (t)}
      <span class="flex items-center gap-1 rounded-full bg-indigo-50 px-2 py-0.5 text-[11px] font-medium text-indigo-700">
        {t}
        <button
          class="text-indigo-400 hover:text-indigo-700"
          title="Remove tag"
          aria-label="Remove tag {t}"
          onclick={() => removeTag(t)}
        >
          <X class="h-3 w-3" />
        </button>
      </span>
    {/each}
    <input
      class="min-w-[6rem] flex-1 rounded border border-zinc-200 bg-white px-2 py-1 text-xs text-zinc-700 placeholder-zinc-400 focus:border-indigo-400 focus:outline-none focus:ring-1 focus:ring-indigo-400"
      placeholder={tags.length === 0 ? 'Add a tag…' : 'Add another…'}
      bind:value={draft}
      onkeydown={onKeydown}
      onblur={onBlur}
    />
  </div>
  {#if error}
    <p class="text-[10px] text-red-500">{error}</p>
  {:else if note}
    <p class="text-[10px] leading-tight text-zinc-400">{note}</p>
  {/if}
</div>
