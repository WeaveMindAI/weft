import { fetchPendingTasks, isTrigger, type PendingTask } from '../lib/api';
import { getSettings } from '../lib/settings';
import { singleFlight } from '../lib/single-flight';

const POLL_INTERVAL_MS = 30000;

// Notification ids carry the signal token after this prefix, so the
// click handler can rebuild the task URL with no state held in the
// (restartable) background worker.
const NOTIFICATION_ID_PREFIX = 'weft-task::';

// Signal tokens the user has already been notified about, grouped by the
// api token that surfaced them, PERSISTED in extension storage: on MV3
// targets the browser kills this worker between polls, so anything held
// in a module variable would forget every notified task and re-pop the
// same notification on every poll. The grouping matters too: a poll only
// refreshes the entries of api tokens whose dispatcher was actually
// REACHED that round. An unreached dispatcher's tasks are absent from
// the response without being gone; a single flat set rebuilt from each
// response would forget them on a transient blip and re-notify every
// live task when the dispatcher recovers.
type SeenByToken = Record<string, string[]>;

/// `null` on a failed read, NEVER `{}`: unknown state reported as
/// known-empty would re-notify every pending task at once on one
/// transient storage failure. The caller skips the notify phase for
/// that round instead.
async function readSeen(): Promise<SeenByToken | null> {
  try {
    const result = await browser.storage.local.get('seenByToken');
    return (result.seenByToken as SeenByToken) ?? {};
  } catch (error) {
    console.error('[weft] Could not read the notified-task state:', error);
    return null;
  }
}

async function writeSeen(seen: SeenByToken): Promise<void> {
  try {
    await browser.storage.local.set({ seenByToken: seen });
  } catch (error) {
    console.warn('[weft] Could not persist the notified-task state:', error);
  }
}

export default defineBackground(() => {
  console.log('[weft] Background service started', { id: browser.runtime.id });

  // Checked on EVERY worker wake, armed only when absent: creating an
  // alarm that already exists would reset its countdown (a wake for a
  // notification click would keep deferring the next poll), while
  // never re-checking would leave polling dead for the whole session
  // any time the browser dropped the alarm without firing onInstalled
  // or onStartup (a disable/enable cycle from the extensions page).
  // The immediate poll runs only when the alarm was actually armed,
  // so a wake CAUSED by the alarm never double-polls beside it.
  ensurePolling();
  browser.runtime.onInstalled.addListener(ensurePolling);
  browser.runtime.onStartup.addListener(ensurePolling);

  browser.alarms.onAlarm.addListener(async (alarm) => {
    if (alarm.name === 'poll-tasks') {
      await pollForTasks();
    }
  });

  // A clicked notification opens the extension-hosted task runner,
  // focused on the task whose signal token rides in the notification id
  // (the id survives a background-worker restart; a captured URL would
  // not).
  browser.notifications.onClicked.addListener((id) => {
    if (!id.startsWith(NOTIFICATION_ID_PREFIX)) return;
    const token = id.slice(NOTIFICATION_ID_PREFIX.length);
    browser.notifications.clear(id);
    browser.tabs.create({
      url: `${browser.runtime.getURL('/tasks.html')}#/${encodeURIComponent(token)}`,
    });
  });
});

// Overlapping polls collapse into one: the seen-state below is a
// read-modify-write across awaits, and two interleaved polls would
// have the earlier write win and re-notify tasks the later one
// recorded.
const pollFlight = singleFlight(runPoll);
const pollForTasks = pollFlight.join;

// Single-flight too: the module body, onInstalled and onStartup can
// all fire on one wake, and a second concurrent `alarms.get` seeing
// "absent" would re-create the alarm and reset its countdown.
const ensurePolling = singleFlight(async () => {
  try {
    if (await browser.alarms.get('poll-tasks')) return;
    browser.alarms.create('poll-tasks', { periodInMinutes: POLL_INTERVAL_MS / 60000 });
    await pollForTasks();
  } catch (error) {
    console.error('[weft] Could not arm the task poll:', error);
  }
}).join;

async function runPoll() {
  try {
    // Single round-trip: fetch tasks AND infer connectivity. Bounded,
    // so a hung dispatcher cannot keep this poll alive into the next
    // alarm tick.
    const result = await fetchPendingTasks({ timeoutMs: 20000 });
    const seenByToken = await readSeen();

    // Only RESUME tasks (a paused run awaiting an answer) notify and
    // count on the badge; triggers just stay listed in the popup (see
    // `isTrigger` for the distinction).
    const actionable = result.tasks.filter(t => !isTrigger(t));
    // The badge is zero when nothing is reachable: the count is
    // unknown then, and a stale number would contradict the popup's
    // Offline card. (With SOME tokens failed the count is partial;
    // the popup's per-token banner is where that is said.)
    const badgeCount = result.anyReachable ? actionable.length : 0;

    if (seenByToken === null) {
      // Unknown notified-state: skip the notify/clear/persist phase
      // (missing one round of alerts is recoverable, a false burst of
      // twelve is not) but keep the badge truthful.
      await updateBadge(badgeCount);
      return;
    }

    // Drop state of tokens the user deleted FIRST, reachable or not: a
    // removed token's entries would otherwise linger forever.
    const configured = new Set(result.configured.map(t => t.token));
    for (const apiToken of Object.keys(seenByToken)) {
      if (!configured.has(apiToken)) delete seenByToken[apiToken];
    }

    if (!result.anyReachable) {
      await writeSeen(seenByToken);
      await updateBadge(badgeCount);
      return;
    }

    // One notification PER TASK: each id carries its own signal token,
    // so the clearing loop below (which reasons per task) is exactly
    // right, and answering one task never silently tears down or
    // falsifies a digest that spoke for its siblings. Each successful
    // create is recorded and PERSISTED before the next, so a worker
    // teardown mid-loop re-alerts nothing already shown; a FAILED
    // create is deliberately not recorded, so it retries next poll.
    // The settings read happens once, not per task.
    const notificationsEnabled = (await getSettings()).notificationsEnabled;
    const seen = new Set(Object.values(seenByToken).flat());
    const alerted = new Set<string>();
    for (const task of actionable) {
      if (seen.has(task.token)) {
        alerted.add(task.token);
        continue;
      }
      if (await showNotification(task, notificationsEnabled)) {
        alerted.add(task.token);
        (seenByToken[task._tokenConfig!.token] ??= []).push(task.token);
        await writeSeen(seenByToken);
      }
    }

    // Rebuild the seen-sets of the api tokens that were REACHED this
    // poll (their absent tasks are genuinely gone); carry every failed
    // token's set forward untouched (its dispatcher just didn't
    // answer). A task that LEFT a reached set was answered somewhere
    // (popup, task page, another consumer): take its notification out
    // of the OS tray too, or a click on the leftover would open a
    // different task an hour later. The clears are awaited BEFORE the
    // seen-state persists: a worker teardown between the two would
    // otherwise leave a stale notification the state says is handled.
    const failed = new Set(result.failures.map(f => f.token.token));
    const reached = result.configured.filter(t => !failed.has(t.token)).map(t => t.token);
    const live = new Set(result.tasks.map(t => t.token));
    const gone: string[] = [];
    for (const apiToken of reached) {
      for (const signalToken of seenByToken[apiToken] ?? []) {
        if (!live.has(signalToken)) {
          gone.push(signalToken);
        }
      }
      seenByToken[apiToken] = [];
    }
    await Promise.all(
      gone.map(token => browser.notifications.clear(`${NOTIFICATION_ID_PREFIX}${token}`)),
    );
    // Only ALERTED tasks are recorded: the state means "the user has
    // been notified about this", so triggers (which never notify) and
    // a resume task whose alert FAILED this round (retried next poll)
    // stay out. `_tokenConfig` is stamped on every fetched task; a
    // hedge here would silently drop the task from `seen` and
    // re-notify it on every poll forever.
    for (const t of actionable) {
      if (!alerted.has(t.token)) continue;
      (seenByToken[t._tokenConfig!.token] ??= []).push(t.token);
    }
    await writeSeen(seenByToken);

    await updateBadge(badgeCount);
  } catch (error) {
    console.error('[weft] Poll error:', error);
  }
}

/// Whether the task now counts as alerted: created, or deliberately
/// discarded (notifications off; recording it keeps a later re-enable
/// from replaying a backlog of stale alerts). A CREATE FAILURE answers
/// false, so the caller retries it next poll.
async function showNotification(task: PendingTask, enabled: boolean): Promise<boolean> {
  if (!enabled) {
    return true;
  }
  try {
    await browser.notifications.create(`${NOTIFICATION_ID_PREFIX}${task.token}`, {
      type: 'basic',
      iconUrl: browser.runtime.getURL('/icon/128.png'),
      title: 'WeaveMind Task',
      message: `New task: ${task.title}`,
    });
    return true;
  } catch (error) {
    console.error('[weft] Notification error:', error);
    return false;
  }
}

async function updateBadge(count: number) {
  try {
    // MV3 exposes `action`, MV2 (Firefox, Safari) exposes
    // `browserAction`; WXT's `browser` is a plain namespace pick, not
    // a polyfill, so both spellings are read here.
    const badgeApi = browser.action ?? browser.browserAction;
    if (!badgeApi) {
      console.error('[weft] No badge API on this browser; the count cannot be shown');
      return;
    }

    if (count > 0) {
      await badgeApi.setBadgeText({ text: count.toString() });
      await badgeApi.setBadgeBackgroundColor({ color: '#6366f1' });
    } else {
      await badgeApi.setBadgeText({ text: '' });
    }
  } catch (error) {
    console.error('[weft] Badge error:', error);
  }
}
