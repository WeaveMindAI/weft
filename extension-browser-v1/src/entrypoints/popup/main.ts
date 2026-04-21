import { mount } from 'svelte';
import App from './App.svelte';
import './app.css';

// Ensure DOM is ready before mounting
function initApp() {
  const target = document.getElementById('app');
  if (!target) {
    console.error('[WeaveMind] Target element not found');
    return;
  }
  
  const app = mount(App, { target });
  return app;
}

// Use DOMContentLoaded to ensure the DOM is ready
if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', initApp);
} else {
  initApp();
}
