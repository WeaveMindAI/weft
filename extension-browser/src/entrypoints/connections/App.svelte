<script lang="ts">
  // A member's settings page: for every member token saved in the
  // extension, the fields that program asks the member to fill (their
  // connections among them), each with its own control (the connect
  // library's, the same ones the weft editor uses). Everything goes
  // through the member door with that token, so it reaches that member's
  // values and connections and nothing else. A token that is not a member
  // token is left out: it acts as nobody, so it has nothing to fill. A member token whose
  // door fails (its runtime unreachable, the token expired) stays, with
  // what went wrong.
  import { MemberDoor, MemberDoorError } from '@weft/connect';
  import { MemberSettings } from '@weft/connect/svelte';
  import { getTokens, type ApiToken } from '../../lib/api';

  type Entry = { token: ApiToken; door: MemberDoor; failed: string | null };

  let entries = $state<Entry[]>([]);
  let loading = $state(true);
  let error = $state<string | null>(null);

  async function load() {
    loading = true;
    error = null;
    try {
      const found: Entry[] = [];
      for (const token of await getTokens()) {
        const door = new MemberDoor(token.token, {
          base: token.dispatcherUrl,
          opener: (url) => {
            void browser.tabs.create({ url });
          },
        });
        // A member token answers its fields; any other token is refused
        // at the member door as not a member token, and is not listed.
        try {
          await door.fields();
          found.push({ token, door, failed: null });
        } catch (e) {
          if (e instanceof MemberDoorError && e.notAMemberToken) continue;
          found.push({ token, door, failed: e instanceof Error ? e.message : String(e) });
        }
      }
      entries = found;
    } catch (e) {
      error = e instanceof Error ? e.message : String(e);
    } finally {
      loading = false;
    }
  }

  $effect(() => {
    void load();
  });
</script>

<main class="page">
  <h1>Your settings</h1>
  {#if loading}
    <p class="muted">Loading...</p>
  {:else if error}
    <p class="error">{error}</p>
  {:else if entries.length === 0}
    <p class="muted">
      None of the tokens saved here is a member token. A program you use gives you one (it acts as you, in that
      program); add it from the extension's settings to fill in what that program asks you for here.
    </p>
  {:else}
    {#each entries as entry (entry.token.token)}
      <section class="program">
        <h2>{entry.token.name}</h2>
        {#if entry.failed}
          <p class="error">{entry.failed}</p>
        {:else}
          <MemberSettings door={entry.door} />
        {/if}
      </section>
    {/each}
  {/if}
</main>

<style>
  :global(html),
  :global(body) {
    margin: 0;
    font-family: system-ui, sans-serif;
    background: #fafafa;
  }
  .page {
    max-width: 40rem;
    margin: 2rem auto;
    padding: 0 1rem;
    --wc-font-size: 13px;
  }
  h1 {
    font-size: 1.4rem;
  }
  h2 {
    font-size: 1.05rem;
    margin: 0 0 0.5rem;
  }
  .program {
    background: #fff;
    border: 1px solid #e4e4e7;
    border-radius: 0.5rem;
    padding: 1rem;
    margin-bottom: 1rem;
  }
  .muted {
    color: #71717a;
  }
  .error {
    color: #ef4444;
  }
</style>
