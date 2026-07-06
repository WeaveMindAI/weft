import { mount } from 'svelte';
import App from './App.svelte';
import '../popup/app.css';

function initApp() {
  const target = document.getElementById('app');
  if (!target) {
    console.error('[weft] Target element not found');
    return;
  }
  return mount(App, { target });
}

if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', initApp);
} else {
  initApp();
}
