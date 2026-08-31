import { defineConfig } from 'wxt';

// See https://wxt.dev/api/config.html
export default defineConfig({
  srcDir: 'src',
  modules: ['@wxt-dev/module-svelte'],
  // Single build dir under the extension folder so the project tree
  // stays self-contained: unpacked output lives under build/<browser>/,
  // zipped artifacts alongside them in build/. The zip filenames are
  // unversioned (`{{name}}-{{browser}}.zip`) so each rebuild
  // overwrites the previous output; git tracks the latest zip
  // and the version bump lives in package.json.
  outDir: 'build',
  // SYNC: the zip filename templates (with package.json "name") <->
  //       .github/workflows/release.yml (publish-browser: the per-store
  //       zip paths in the submit step),
  //       setup.sh (the browser build block: the per-target zip names
  //       and the sources zip)
  zip: {
    artifactTemplate: '{{name}}-{{browser}}.zip',
    sourcesTemplate: '{{name}}-sources.zip',
  },
  runner: {
    startUrls: [],
    openDevtools: false,
  },
  manifest: ({ browser, manifestVersion }) => ({
    name: 'WeaveMind',
    description: 'Human-in-the-loop task manager for WeaveMind projects',
    permissions: ['storage', 'notifications', 'alarms'],
    // No fixed host access. Each runtime's host is requested from the
    // user at the moment they add its token (popup, `permissions.request`)
    // and handed back when the last token on that host is removed, so the
    // extension can only ever reach addresses the user granted one by one,
    // and any host works, not just a hardcoded list.
    // MV2 (Firefox, Safari) declares optional origins in
    // `optional_permissions`; MV3 split them into
    // `optional_host_permissions`. Keyed on the manifest version, not the
    // browser name, so every target lands in the right field.
    ...(manifestVersion === 2
      ? { optional_permissions: ['http://*/*', 'https://*/*'] }
      : { optional_host_permissions: ['http://*/*', 'https://*/*'] }),
    // Firefox: gecko settings for permanent installation. AMO refuses a
    // new submission without a data-collection declaration; `none` states
    // that nothing is collected or transmitted to the developer (answers
    // go only to the runtime hosts the user granted, never to us).
    ...(browser === 'firefox' && {
      browser_specific_settings: {
        gecko: {
          // The AMO listing identity: the submit targets this listing.
          // SYNC: gecko.id <-> .github/workflows/release.yml
          //       (publish-browser: FIREFOX_EXTENSION_ID)
          id: 'extension@weavemind.ai',
          // The floor is set by `data_collection_permissions`: Firefox
          // understands it from 140 (desktop) / 142 (Android), and AMO
          // warns on any floor below that.
          strict_min_version: '140.0',
          data_collection_permissions: {
            required: ['none'],
          },
        },
        gecko_android: {
          strict_min_version: '142.0',
        },
      },
    }),
    // Opera: minimum version for Opera addons store
    ...(browser === 'opera' && {
      minimum_opera_version: '91',
    }),
  }),
});
