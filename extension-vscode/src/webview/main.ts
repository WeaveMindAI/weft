// Webview entry. Svelte 5 mounts here, reads messages from the
// extension host, and forwards mutation intents back.

import { mount } from 'svelte';
import App from './App.svelte';

const target = document.getElementById('app');
if (!target) throw new Error('missing #app root in webview html');

mount(App, { target });
