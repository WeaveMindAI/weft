import { fetchPendingTasks, type PendingTask, type ApiToken } from '../lib/api';

const POLL_INTERVAL_MS = 30000; // 30 seconds

// Signal tokens the user has already been notified about, grouped by the
// api token that surfaced them. The grouping matters: a poll only refreshes
// the entries of api tokens whose dispatcher was actually REACHED that round.
// An unreached dispatcher's tasks are absent from the response without being
// gone; a single flat set rebuilt from each response would forget them on a
// transient blip and re-notify every live task when the dispatcher recovers.
let seenByToken = new Map<string, Set<string>>();

// Settings interface
interface ExtensionSettings {
  notificationsEnabled: boolean;
}

const DEFAULT_SETTINGS: ExtensionSettings = {
  notificationsEnabled: true,
};

async function getSettings(): Promise<ExtensionSettings> {
  try {
    const result = await browser.storage.local.get('settings');
    if (result.settings && typeof result.settings === 'object') {
      return { ...DEFAULT_SETTINGS, ...result.settings };
    }
    return DEFAULT_SETTINGS;
  } catch {
    return DEFAULT_SETTINGS;
  }
}

export async function saveSettings(settings: Partial<ExtensionSettings>): Promise<void> {
  const current = await getSettings();
  await browser.storage.local.set({ settings: { ...current, ...settings } });
}

export default defineBackground(() => {
  console.log('[weft] Background service started', { id: browser.runtime.id });

  // Set up polling alarm
  browser.alarms.create('poll-tasks', { periodInMinutes: 0.5 });

  browser.alarms.onAlarm.addListener(async (alarm) => {
    if (alarm.name === 'poll-tasks') {
      await pollForTasks();
    }
  });

  // Listen for messages from content scripts (toast clicks).
  browser.runtime.onMessage.addListener(
    (message: { type: string; url?: string }) => {
      if (message.type === 'OPEN_TASK_RUNNER' && message.url) {
        // Task toast: open the extension-hosted runner. Content
        // scripts can't navigate to chrome-extension:// URLs with
        // `window.open` from a web-origin so the toast delegates
        // here.
        browser.tabs.create({ url: message.url });
      }
    },
  );

  // Initial poll
  pollForTasks();
});

async function pollForTasks() {
  try {
    // Single round-trip: fetch tasks AND infer connectivity. The
    // previous version did checkConnection() first which doubled
    // the per-poll cost.
    const result = await fetchPendingTasks();
    if (!result.anyReachable) {
      console.log('[weft] No reachable dispatcher, skipping poll');
      return;
    }
    const tasks = result.tasks;

    // Find tasks the user hasn't been notified about yet. Signal
    // token uniquely identifies a task; we use it as the seen-key
    // so re-fetches don't re-notify on the same task.
    const seen = new Set([...seenByToken.values()].flatMap(s => [...s]));
    const newTasks = tasks.filter(t => !seen.has(t.token));

    if (newTasks.length > 0) {
      await showNotification(newTasks.length, newTasks[0]);
    }

    // Rebuild the seen-sets of the api tokens that were REACHED this poll
    // (their absent tasks are genuinely gone); carry every other token's
    // set forward untouched (its dispatcher just didn't answer).
    const next = new Map<string, Set<string>>(
      result.reachedTokens.map(apiToken => [apiToken, new Set<string>()]),
    );
    for (const t of tasks) {
      const owner = (t as PendingTask & { _tokenConfig?: ApiToken })._tokenConfig?.token;
      if (owner) next.get(owner)?.add(t.token);
    }
    // Carry forward only tokens the user still has configured: a deleted
    // token's set would otherwise linger in this long-lived worker forever.
    const configured = new Set(result.configuredTokens);
    for (const [apiToken, set] of seenByToken) {
      if (!next.has(apiToken) && configured.has(apiToken)) next.set(apiToken, set);
    }
    seenByToken = next;

    // Update badge
    await updateBadge(tasks.length);
  } catch (error) {
    console.error('[weft] Poll error:', error);
  }
}

async function showNotification(count: number, task: PendingTask & { _tokenConfig?: ApiToken }) {
  try {
    const settings = await getSettings();
    if (!settings.notificationsEnabled) {
      console.log('[weft] Notifications disabled, skipping');
      return;
    }

    const notificationId = `task-${task.token}-${Date.now()}`;
    // Hash fragment carries the signal token; the extension-hosted
    // runner reads it to focus the right task.
    const taskUrl = `${browser.runtime.getURL('/tasks.html')}#/${encodeURIComponent(task.token)}`;

    const toastData = {
      id: notificationId,
      title: 'WeaveMind Task',
      message: count === 1
        ? `New task: ${task.title}`
        : `${count} new tasks waiting for your approval`,
      taskUrl,
    };

    // Get all tabs and send message to each
    const tabs = await browser.tabs.query({});
    for (const tab of tabs) {
      if (tab.id) {
        try {
          await browser.tabs.sendMessage(tab.id, { type: 'SHOW_TOAST', toast: toastData });
        } catch {
          // Tab might not have content script loaded, ignore
        }
      }
    }
    
    console.log('[weft] Toast notification sent to tabs');
  } catch (error) {
    console.error('[weft] Notification error:', error);
  }
}

async function updateBadge(count: number) {
  try {
    // WXT polyfills browser.action for MV2 targets (Firefox, Safari)
    // Fallback to browserAction for edge cases where polyfill isn't loaded
    const badgeApi = browser.action ?? (browser as any).browserAction;
    if (!badgeApi) return;
    
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
