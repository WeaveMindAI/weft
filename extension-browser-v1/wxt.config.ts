import { defineConfig } from 'wxt';

// See https://wxt.dev/api/config.html
export default defineConfig({
  srcDir: 'src',
  modules: ['@wxt-dev/module-svelte'],
  runner: {
    startUrls: [],
    openDevtools: false,
  },
  manifest: ({ browser }) => ({
    name: 'WeaveMind',
    description: 'Human-in-the-loop task manager for WeaveMind projects',
    permissions: ['storage', 'notifications', 'alarms'],
    host_permissions: [
      'http://localhost:*/*',
      'http://127.0.0.1:*/*',
      'https://*.weavemind.ai/*',
      'https://weavemind.ai/*',
    ],
    // Firefox: gecko settings for permanent installation
    ...(browser === 'firefox' && {
      browser_specific_settings: {
        gecko: {
          id: 'extension@weavemind.ai',
          strict_min_version: '109.0',
        },
      },
    }),
    // Opera: minimum version for Opera addons store
    ...(browser === 'opera' && {
      minimum_opera_version: '91',
    }),
  }),
});
