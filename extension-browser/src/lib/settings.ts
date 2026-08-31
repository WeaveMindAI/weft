/// The extension's user settings, one storage object read and written
/// through here by every entry (popup toggle, background gate), so a
/// new setting can never be wiped by a partial write elsewhere.

export interface ExtensionSettings {
  notificationsEnabled: boolean;
}

export const DEFAULT_SETTINGS: ExtensionSettings = {
  notificationsEnabled: true,
};

export async function getSettings(): Promise<ExtensionSettings> {
  try {
    const result = await browser.storage.local.get('settings');
    if (result.settings && typeof result.settings === 'object') {
      return { ...DEFAULT_SETTINGS, ...result.settings };
    }
    return DEFAULT_SETTINGS;
  } catch (error) {
    // A failed read is worth a line (it means EVERY preference is
    // silently on defaults), but the page still has to render.
    console.warn('[weft] Could not read settings; using defaults:', error);
    return DEFAULT_SETTINGS;
  }
}

/// Merge-write: only the given keys change, so two settings written
/// from two places can never wipe each other.
export async function saveSettings(settings: Partial<ExtensionSettings>): Promise<void> {
  const current = await getSettings();
  await browser.storage.local.set({ settings: { ...current, ...settings } });
}
