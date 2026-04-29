import { fetchPendingTasks, type PendingTask, type ExtensionToken } from '../lib/api';

const POLL_INTERVAL_MS = 30000; // 30 seconds

let seenTaskIds = new Set<string>();

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
  console.log('[WeaveMind] Background service started', { id: browser.runtime.id });

  // Set up polling alarm
  browser.alarms.create('poll-tasks', { periodInMinutes: 0.5 });

  browser.alarms.onAlarm.addListener(async (alarm) => {
    if (alarm.name === 'poll-tasks') {
      await pollForTasks();
    }
  });

  // Listen for messages from content scripts (toast clicks).
  browser.runtime.onMessage.addListener(
    (message: { type: string; actionUrl?: string; url?: string }) => {
      if (message.type === 'OPEN_AND_DISMISS_ACTION' && message.actionUrl) {
        // Action toast: open the user's URL in a new tab. The
        // dispatcher auto-expires the action from the task list.
        browser.tabs.create({ url: message.actionUrl });
      } else if (message.type === 'OPEN_TASK_RUNNER' && message.url) {
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
      console.log('[WeaveMind] No reachable dispatcher, skipping poll');
      return;
    }
    const tasks = result.tasks;

    // Find tasks the user hasn't been notified about yet
    const newTasks = tasks.filter(t => !seenTaskIds.has(t.executionId));

    if (newTasks.length > 0) {
      await showNotification(newTasks.length, newTasks[0]);
    }

    // Update seen IDs to current task list only (prune completed tasks automatically)
    seenTaskIds = new Set(tasks.map(t => t.executionId));

    // Update badge
    await updateBadge(tasks.length);
  } catch (error) {
    console.error('[WeaveMind] Poll error:', error);
  }
}

async function showNotification(count: number, task: PendingTask & { _tokenConfig?: ExtensionToken }) {
  try {
    // Check if notifications are enabled
    const settings = await getSettings();
    if (!settings.notificationsEnabled) {
      console.log('[WeaveMind] Notifications disabled, skipping');
      return;
    }

    // Generate unique notification ID
    const notificationId = `task-${task.executionId}-${Date.now()}`;
    
    // Build the runner URL. The extension-hosted full-page runner
    // (entrypoints/tasks/*) lives at tasks.html in the bundle. The
    // hash fragment carries the executionId so clicking the toast
    // lands on the right task.
    const taskUrl = `${browser.runtime.getURL('/tasks.html')}#/${encodeURIComponent(task.executionId)}`;

    // Send toast message to all tabs via content script
    const isAction = task.taskType === 'Action';
    const toastData = {
      id: notificationId,
      title: isAction ? 'WeaveMind' : 'WeaveMind Task',
      message: count === 1 
        ? (isAction ? `${task.title}` : `New task: ${task.title}`)
        : `${count} new tasks waiting for your approval`,
      taskUrl: isAction ? undefined : taskUrl,
      actionUrl: isAction ? task.actionUrl : undefined,
      taskType: task.taskType || 'Task',
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
    
    console.log('[WeaveMind] Toast notification sent to tabs');
  } catch (error) {
    console.error('[WeaveMind] Notification error:', error);
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
    console.error('[WeaveMind] Badge error:', error);
  }
}
