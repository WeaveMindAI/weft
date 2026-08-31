import { mount, type Component } from 'svelte';
import './reset.css';

/// Mount an entry page's root component onto `#app` once the DOM is
/// ready. The one mounting path for the popup and the tasks page, so
/// the two cannot drift on readiness handling or the shared reset css.
export function mountWhenReady(App: Component) {
  const init = () => {
    const target = document.getElementById('app');
    if (!target) {
      // Only a broken build reaches this; say so ON the page, where
      // the user is looking, not just in a devtools console nobody
      // has open.
      document.body.textContent =
        '[weft] This extension page failed to load (its #app element is missing). '
        + 'Reinstall or rebuild the extension.';
      console.error('[weft] Target element not found');
      return;
    }
    mount(App, { target });
  };
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
}
