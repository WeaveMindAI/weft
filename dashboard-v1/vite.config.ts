import tailwindcss from '@tailwindcss/vite';
import { sveltekit } from '@sveltejs/kit/vite';
import { defineConfig } from 'vitest/config';

export default defineConfig({
	plugins: [tailwindcss(), sveltekit()],
	test: {
		include: ['src/**/*.test.ts'],
	},
	resolve: {
		preserveSymlinks: true,
	},
	optimizeDeps: {
		include: ['svelte-sonner']
	},
	ssr: {
		noExternal: ['svelte-sonner']
	}
});
