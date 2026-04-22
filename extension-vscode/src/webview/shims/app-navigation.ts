// SvelteKit `$app/navigation` shim. Only `goto` is used by a
// component we don't render (ExecutionsPanel). No-op so the module
// resolves if anything slips through.

export function goto(_url: string): Promise<void> {
  return Promise.resolve();
}
