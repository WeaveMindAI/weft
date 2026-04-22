// SvelteKit `$app/environment` shim. The v1 code uses `browser` in
// two ways: gate DOM-only work (localStorage/sessionStorage) and
// gate `<SvelteFlow>` mounting. Webviews always run in the browser,
// so hard-code `true`.

export const browser = true;
export const dev = false;
export const building = false;
export const version = '0.0.0';
