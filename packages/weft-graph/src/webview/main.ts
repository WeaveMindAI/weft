// Webview entry. Svelte 5 mounts here, reads messages from the
// extension host, and forwards mutation intents back.

import './app.css';
import { mount } from 'svelte';
import { Toaster } from 'svelte-sonner';
import App from './App.svelte';

const target = document.getElementById('app');
if (!target) throw new Error('missing #app root in webview html');

mount(App, { target });
// The Toaster is HOST chrome, mounted once per host next to the editor (a
// browser host mounts its own styled one in its layout). Without a mounted
// Toaster every `toast.*` call in the editor is a silent no-op.
mount(Toaster, { target, props: { position: 'top-right', duration: 4000, closeButton: true, richColors: true } });
