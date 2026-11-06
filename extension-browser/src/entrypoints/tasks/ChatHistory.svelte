<script lang="ts">
  import { tick } from 'svelte';
  import type { FormField, PendingTask } from '../../lib/api';
  import { continues, isLongText, loadConversation, type ChatMessageView, type ChatPart, type LoadedConversation } from '../../lib/chat-history';

  /// A `chat_history` field: the conversation its file holds, message by
  /// message, scrolled to the latest. Read again each time the task or
  /// field changes, so a stored file's link is always fresh.
  let { task, field }: { task: PendingTask; field: FormField } = $props();

  /// How many of the latest messages the view opens on, and how many
  /// more each "show earlier" adds. A long agent session holds
  /// thousands, and drawing them all at once stalls the page.
  const PAGE = 50;

  /// The messages on screen. They stay drawn while a refresh reads the
  /// file again (the task list replaces its task objects every few
  /// seconds): only the first read of a conversation shows "Loading", so
  /// the view is never torn down and rebuilt, which would throw away the
  /// reader's place and every fold they opened. Raw: thousands of
  /// messages are replaced whole, never edited in place.
  let messages: ChatMessageView[] | undefined = $state.raw(undefined);
  let failure: string | undefined = $state(undefined);
  /// The index of the first message drawn. It moves only when the reader
  /// is at the end (then new messages slide it on, so at most `PAGE` plus
  /// what they revealed is drawn), never while they read further up, so
  /// messages arriving at the end never push the ones they are looking
  /// at out of the top.
  let start = $state(0);
  /// How many earlier messages "show earlier" revealed, on top of `PAGE`.
  let revealed = 0;
  /// The folds the reader opened, by message index (`<i>`) and tool call
  /// (`<i>.<j>`): kept across refreshes, as a message keeps its index
  /// while the conversation grows at its end.
  let opened: Set<string> = $state(new Set());
  /// The last conversation read, so a read of the same text hands its
  /// messages back instead of parsing them again. Plain, not state:
  /// nothing draws from it.
  let last: LoadedConversation | undefined;
  /// Which read is the latest: an older one that lands after it is dropped.
  let reads = 0;
  let chat: HTMLElement | undefined = $state(undefined);
  /// Whether the reader is at the end: the view follows new messages and
  /// pictures loading in only then, and otherwise stays where they read.
  let atEnd = true;

  /// Which conversation is on screen: everything starts over only when
  /// it changes, never on a refresh that re-reads the same one.
  const which = $derived(`${task.token}\u0000${field.key}`);
  $effect(() => {
    void which;
    messages = undefined;
    failure = undefined;
    last = undefined;
    opened = new Set();
    atEnd = true;
  });

  $effect(() => {
    const read = ++reads;
    loadConversation(task, field, last).then(
      (loaded) => {
        if (read !== reads) return;
        last = loaded;
        failure = undefined;
        if (loaded.messages === messages) return;
        const next = loaded.messages;
        if (messages === undefined || !continues(messages, next, start)) {
          // The first read, or a conversation started over (it no longer
          // holds the messages on screen, whatever its length): nothing
          // of the old one's place or folds applies to it.
          start = Math.max(0, next.length - PAGE);
          revealed = 0;
          opened = new Set();
          atEnd = true;
        } else if (atEnd) {
          start = Math.max(start, next.length - PAGE - revealed);
        }
        messages = next;
      },
      (e) => {
        if (read !== reads) return;
        failure = e instanceof Error ? e.message : String(e);
      },
    );
  });

  function onScroll() {
    if (chat) atEnd = chat.scrollHeight - chat.scrollTop - chat.clientHeight < 24;
  }
  function toEnd() {
    if (chat && atEnd) chat.scrollTop = chat.scrollHeight;
  }
  // After each new set of messages is drawn.
  $effect(() => {
    void messages;
    void chat;
    toEnd();
  });

  /// Draw `PAGE` earlier messages above, keeping the message the reader
  /// was looking at in place: the scroll moves down by the height added.
  /// The view's own scroll anchoring is off (`overflow-anchor: none`), so
  /// the browser does not move it a second time.
  async function showEarlier() {
    const from = chat?.scrollHeight ?? 0;
    revealed += start - Math.max(0, start - PAGE);
    start = Math.max(0, start - PAGE);
    await tick();
    if (chat) chat.scrollTop += chat.scrollHeight - from;
  }

  function toggled(key: string, e: Event) {
    const isOpen = (e.currentTarget as HTMLDetailsElement).open;
    if (isOpen === opened.has(key)) return;
    const next = new Set(opened);
    if (isOpen) next.add(key);
    else next.delete(key);
    opened = next;
  }

  function partsText(parts: ChatPart[]): string {
    return parts.map((p) => (p.kind === 'text' ? p.text : '')).join('\n');
  }
</script>

{#if failure !== undefined}
  <div class="error-box">{failure}</div>
{/if}
{#if messages === undefined}
  {#if failure === undefined}
    <p class="field-empty">Loading conversation...</p>
  {/if}
{:else if messages.length === 0}
  <p class="field-empty">(no messages)</p>
{:else}
  <!-- `load` does not bubble; capture catches each picture as it lands. -->
  <div class="chat" bind:this={chat} onscroll={onScroll} onloadcapture={toEnd}>
    {#if start > 0}
      <button class="earlier" onclick={showEarlier}>
        Show earlier messages ({start} more)
      </button>
    {/if}
    {#each messages.slice(start) as message, i (start + i)}
      {@const at = start + i}
      <div class="msg msg-{message.role}">
        <p class="role">
          {#if message.answers}
            Result of {message.answers.toolName ?? 'a tool call'}
          {:else}
            {message.role}{message.name ? ` (${message.name})` : ''}
          {/if}
        </p>
        {#if message.answers || message.role === 'system'}
          <!-- A tool's result and a system prompt are context, often
               long: shown whole when short, folded when long. -->
          {@const text = partsText(message.parts)}
          {#if isLongText(text)}
            <details class="fold" open={opened.has(`${at}`)} ontoggle={(e) => toggled(`${at}`, e)}>
              <summary>{text.slice(0, 120).replace(/\s+/g, ' ')}...</summary>
              {@render parts(message.parts)}
            </details>
          {:else}
            {@render parts(message.parts)}
          {/if}
        {:else}
          {@render parts(message.parts)}
        {/if}
        {#each message.toolCalls as call, j}
          <details class="fold tool-call" open={opened.has(`${at}.${j}`)} ontoggle={(e) => toggled(`${at}.${j}`, e)}>
            <summary>Calls <span class="tool-name">{call.name}</span></summary>
            <pre class="args">{call.arguments}</pre>
          </details>
        {/each}
      </div>
    {/each}
  </div>
{/if}

{#snippet parts(list: ChatPart[])}
  {#each list as part}
    {#if part.kind === 'text'}
      <p class="text">{part.text}</p>
    {:else if part.url}
      {#if part.kind === 'image'}
        <img src={part.url} alt={part.filename ?? 'image'} class="media" />
      {:else if part.kind === 'audio'}
        <audio src={part.url} controls class="media"></audio>
      {:else}
        <video src={part.url} controls class="media"><track kind="captions" /></video>
      {/if}
    {:else}
      <!-- A file weft stored inside the conversation: only weft opens it,
           so the page names it rather than drawing a broken picture. -->
      <p class="field-empty">({part.kind}{part.filename ? `: ${part.filename}` : ''}, stored by weft, not viewable here)</p>
    {/if}
  {/each}
{/snippet}

<style>
  /* The page's own notice styles, restated: Svelte scopes each
     component's CSS, so App.svelte's do not reach in here. */
  .field-empty {
    font-size: 12px;
    color: #a1a1aa;
    font-style: italic;
    margin: 0;
  }
  .error-box {
    background: #fef2f2;
    border: 1px solid #fecaca;
    color: #dc2626;
    border-radius: 6px;
    padding: 8px 12px;
    font-size: 12px;
  }

  .chat {
    display: flex;
    flex-direction: column;
    gap: 8px;
    max-height: 420px;
    overflow-y: auto;
    padding: 10px;
    background: #fafafa;
    border: 1px solid #e4e4e7;
    border-radius: 6px;
    /* "Show earlier" keeps the reader's place itself (`showEarlier`). */
    overflow-anchor: none;
  }
  .earlier {
    align-self: center;
    padding: 4px 10px;
    font-size: 12px;
    color: #52525b;
    background: white;
    border: 1px solid #e4e4e7;
    border-radius: 6px;
    cursor: pointer;
    font-family: inherit;
  }
  .earlier:hover { background: #f4f4f5; }
  .msg {
    padding: 8px 10px;
    border-radius: 6px;
    border: 1px solid #e4e4e7;
    background: white;
    max-width: 88%;
    align-self: flex-start;
  }
  /* The person's side of the talk sits right, as in any chat. */
  .msg-user { align-self: flex-end; background: #f4f4f5; }
  .msg-system { max-width: 100%; align-self: stretch; background: #fffbeb; border-color: #fde68a; }
  .msg-tool { background: #fafafa; border-style: dashed; }
  .role {
    font-size: 10px;
    font-weight: 600;
    color: #71717a;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    margin: 0 0 4px;
  }
  .text {
    font-size: 13px;
    color: #18181b;
    white-space: pre-wrap;
    word-break: break-word;
    margin: 0;
    line-height: 1.45;
  }
  .text + .text, .text + .media, .media + .text { margin-top: 6px; }
  .media {
    display: block;
    max-width: 100%;
    max-height: 240px;
    border-radius: 6px;
    border: 1px solid #e4e4e7;
    object-fit: contain;
  }
  .fold summary {
    cursor: pointer;
    font-size: 12px;
    color: #52525b;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .fold[open] summary { margin-bottom: 6px; }
  .tool-call { margin-top: 6px; }
  .tool-name {
    font-family: ui-monospace, SFMono-Regular, monospace;
    font-weight: 600;
    color: #18181b;
  }
  .args {
    font-size: 12px;
    background: #fafafa;
    border: 1px solid #e4e4e7;
    border-radius: 6px;
    padding: 8px 10px;
    font-family: ui-monospace, SFMono-Regular, monospace;
    white-space: pre-wrap;
    word-break: break-word;
    max-height: 200px;
    overflow: auto;
    margin: 0;
  }
</style>
