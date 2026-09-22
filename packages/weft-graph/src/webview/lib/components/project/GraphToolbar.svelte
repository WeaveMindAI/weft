<script lang="ts">
  import { ArrowLeft, Eye, EyeOff, Info, Lock } from '@lucide/svelte';
  import type { Snippet } from 'svelte';
  import type { FollowMode } from '../../../../protocol';

  let {
    mode,
    color,
    pendingCount,
    notPainted = undefined,
    onSetMode,
    navDepth = 0,
    navFileName = '',
    interactive = true,
    onNavigateBack,
    leading,
    trailing,
    below,
  }: {
    mode: FollowMode;
    color: string | undefined;
    pendingCount: number;
    /// Set when the run on screen cannot be painted from its journal,
    /// with the reason. Shown as its own pill: the canvas is empty or
    /// half-empty and the person deserves to be told why rather than
    /// left to guess at the viewer.
    notPainted?: string;
    /// The person picked a mode on the toggle, or clicked the "new
    /// runs" chip (which picks following).
    onSetMode: (mode: FollowMode) => void;
    /// Include-navigation depth: > 0 means viewing an @include'd file, so a
    /// Return button shows. navFileName labels the current file.
    navDepth?: number;
    navFileName?: string;
    /// False when the file on screen was opened on its own rather than
    /// reached from the entry file: the graph is drawn and edited, but
    /// no run is shown, and a pill says where to go for one.
    interactive?: boolean;
    onNavigateBack?: () => void;
    /// Host-injected controls placed at the START of the toolbar, before the
    /// editor's own buttons. A web host can put a "back to projects" button
    /// here so it sits inline with Live instead of floating separately;
    /// VS Code injects nothing (its native chrome owns navigation). Absent =
    /// nothing rendered.
    leading?: Snippet;
    /// Host-injected controls placed at the END of the toolbar, after the
    /// editor's own buttons. Symmetric with `leading`. Absent = nothing.
    trailing?: Snippet;
    /// Controls for the RUN itself, on their own row under the toolbar.
    ///
    /// The row above is about the view (which run is followed, whether the
    /// source is open, where in an include you are). This one is about
    /// what happened inside the run, and those are not the same question,
    /// so they do not share a line. A run's caller conversation goes here.
    ///
    /// The row is not rendered at all when the snippet is absent or draws
    /// nothing, so a run with nothing to say about it costs no space.
    below?: Snippet;
  } = $props();

  const shortColor = $derived(color ? color.slice(0, 8) : '');

  /// The three choices of the follow toggle, in the order drawn. Only
  /// the active one shows its word; the others show their icon, and
  /// every one says on hover what picking it does.
  const choices = $derived([
    {
      mode: 'following' as const,
      icon: Eye,
      label: 'Following',
      hint: 'Following: every run that starts takes over the graph.',
      disabled: false,
    },
    {
      mode: 'locked' as const,
      icon: Lock,
      label: 'Locked',
      hint: color || mode === 'locked'
        ? 'Locked: the graph stays on this run. Runs that start are counted, not shown.'
        : 'Locked: keeps the graph on one run. There is no run on the graph to lock onto yet.',
      disabled: !color && mode !== 'locked',
    },
    {
      mode: 'off' as const,
      icon: EyeOff,
      label: 'Off',
      hint: 'Off: no run on the graph, just the program. Runs that start are counted, not shown.',
      disabled: false,
    },
  ]);
</script>

<div class="absolute top-3 left-3 z-30 flex flex-col items-start gap-2 pointer-events-auto">
<div class="flex items-center gap-2">
  {#if leading}{@render leading()}{/if}
  {#if navDepth > 0}
    <button
      type="button"
      onclick={() => onNavigateBack?.()}
      class="flex items-center gap-1.5 px-2.5 py-1.5 rounded-md border border-violet-300 bg-white text-violet-700 shadow-sm text-xs font-medium hover:bg-violet-50 transition"
      title="Return to the file you came from"
    >
      <ArrowLeft class="w-3 h-3" />
      Return{navFileName ? ` · ${navFileName}` : ''}
    </button>
  {/if}

  <!-- A view that is no place in the program shows no run, so none of
       the run controls below (the follow toggle, the new-runs chip) mean anything
       here: the pill takes their spot and says where a run is seen. -->
  {#if !interactive}
    <div
      class="flex items-center gap-1.5 px-3 py-1.5 rounded-md bg-slate-100 text-slate-700 border border-slate-300 shadow-sm text-xs font-medium"
      title="This file is included by the program, maybe from more than one place. To see what a run did here, open src/main.weft and walk in through the include that reaches it."
    >
      <Info class="w-3 h-3" />
      Opened on its own: no runs shown. Walk in from src/main.weft for those.
    </div>
  {:else}

  {#if notPainted}
    <div
      class="flex items-center gap-1.5 px-3 py-1.5 rounded-md bg-amber-50 text-amber-800 border border-amber-300 shadow-sm text-xs font-medium max-w-lg"
      title={notPainted}
    >
      <span class="w-1.5 h-1.5 rounded-full bg-amber-500"></span>
      <span class="truncate">{notPainted}</span>
    </div>
  {/if}

  <div
    role="radiogroup"
    aria-label="Which run the graph shows"
    class="flex items-center gap-0.5 p-0.5 rounded-md border border-zinc-200 bg-white shadow-sm text-xs font-medium"
  >
    {#each choices as choice (choice.mode)}
      {@const active = mode === choice.mode}
      <button
        type="button"
        role="radio"
        aria-checked={active}
        aria-label={choice.label}
        title={choice.hint}
        aria-disabled={choice.disabled}
        onclick={() => { if (!choice.disabled) onSetMode(choice.mode); }}
        class="flex items-center gap-1.5 h-6 rounded transition
          {active ? 'px-2 bg-zinc-900 text-white' : 'w-6 justify-center text-zinc-500'}
          {choice.disabled ? 'opacity-40 cursor-default' : active ? '' : 'hover:bg-zinc-100 hover:text-zinc-900'}"
      >
        <!-- aria-disabled rather than disabled: a disabled button shows no
             tooltip, and the hover text is what says why Locked is greyed. -->
        <choice.icon class="w-3 h-3" />
        {#if active}
          {choice.label}{choice.mode !== 'off' && shortColor ? ` · ${shortColor}` : ''}
        {/if}
      </button>
    {/each}
  </div>

  <!-- Runs that started while not following. Clicking follows again,
       starting from the newest of them. Louder while locked (the person
       is looking at a run and newer ones exist) than while off. -->
  {#if mode !== 'following' && pendingCount > 0}
    <button
      type="button"
      onclick={() => onSetMode('following')}
      class="flex items-center gap-1.5 px-3 py-1.5 rounded-md border shadow-sm text-xs font-medium transition
        {mode === 'locked'
          ? 'bg-amber-100 text-amber-800 border-amber-300 hover:bg-amber-200'
          : 'bg-white text-zinc-600 border-zinc-200 hover:bg-zinc-50'}"
      title="Show the newest of them and follow runs again"
    >
      {#if mode === 'locked'}
        <span class="w-1.5 h-1.5 rounded-full bg-amber-500 animate-pulse"></span>
      {/if}
      {pendingCount} new {pendingCount === 1 ? 'run' : 'runs'} &middot; Follow
    </button>
  {/if}
  {/if}

  {#if trailing}{@render trailing()}{/if}
</div>
{#if below}
  <div class="flex items-center gap-2">{@render below()}</div>
{/if}
</div>
