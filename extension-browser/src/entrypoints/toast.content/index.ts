import './style.css';
import Toast from './Toast.svelte';
import { mount, unmount } from 'svelte';

export default defineContentScript({
  matches: ['<all_urls>'],
  cssInjectionMode: 'ui',

  async main(ctx) {
    const ui = await createShadowRootUi(ctx, {
      name: 'weavemind-toast',
      position: 'overlay',
      onMount: (container) => {
        return mount(Toast, { target: container });
      },
      onRemove: (app) => {
        if (app) unmount(app);
      },
    });

    ui.mount();
  },
});
