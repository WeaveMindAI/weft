import adapterNode from '@sveltejs/adapter-node';
import adapterStatic from '@sveltejs/adapter-static';

const useStatic = process.env.BUILD_STATIC === 'true';

/** @type {import('@sveltejs/kit').Config} */
const config = {
	kit: {
		adapter: useStatic
			? adapterStatic({
					pages: 'build',
					assets: 'build',
					fallback: 'index.html',
					precompress: false,
					strict: false
				})
			: adapterNode({
					out: 'build'
				}),
		paths: {
			base: process.env.BASE_PATH || ''
		}
	}
};

export default config;
