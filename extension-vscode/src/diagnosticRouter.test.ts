import { describe, expect, it } from 'vitest';
import { DiagnosticRouter, type DiagSink } from './diagnosticRouter';

// The Problems-panel bookkeeping against a hand-rolled sink: which URI
// each file's merged findings publish at, through opens, closes, and
// two spellings of the same real file.

function rig() {
	const published = new Map<string, string[]>();
	const sink: DiagSink<string> = {
		set: (uri, diags) => published.set(uri, diags),
		delete: (uri) => published.delete(uri),
	};
	const router = new DiagnosticRouter<string>(sink, (file) => `file://${file}`);
	return { published, router };
}

describe('DiagnosticRouter', () => {
	it('two spellings of the same real file each get the merged findings', () => {
		const { published, router } = rig();
		router.register('file:///link/a.weft', '/real/a.weft');
		router.register('file:///real/a.weft', '/real/a.weft');
		router.publish('file:///link/a.weft', new Map([['/real/a.weft', ['from-link']]]));
		router.publish('file:///real/a.weft', new Map([['/real/a.weft', ['from-real']]]));
		expect(published.get('file:///link/a.weft')).toEqual(['from-link', 'from-real']);
		expect(published.get('file:///real/a.weft')).toEqual(['from-link', 'from-real']);
	});

	it('closing one spelling keeps the survivor publishing at its own URI', () => {
		const { published, router } = rig();
		router.register('file:///link/a.weft', '/real/a.weft');
		router.register('file:///real/a.weft', '/real/a.weft');
		router.publish('file:///link/a.weft', new Map([['/real/a.weft', ['from-link']]]));
		router.publish('file:///real/a.weft', new Map([['/real/a.weft', ['from-real']]]));
		router.close('file:///link/a.weft', '/real/a.weft');
		expect(published.has('file:///link/a.weft')).toBe(false);
		expect(published.get('file:///real/a.weft')).toEqual(['from-real']);
	});

	it('closing the last document clears everything for the file', () => {
		const { published, router } = rig();
		router.register('file:///a.weft', '/a.weft');
		router.publish('file:///a.weft', new Map([['/a.weft', ['finding']]]));
		router.close('file:///a.weft', '/a.weft');
		expect(published.size).toBe(0);
	});

	it('a closed include publishes at the fallback URI; opening it moves the findings and clears the fallback', () => {
		const { published, router } = rig();
		router.register('file:///main.weft', '/main.weft');
		// main's run lands a finding on the (closed) include.
		router.publish('file:///main.weft', new Map([['/sub.weft', ['in-sub']]]));
		expect(published.get('file:///sub.weft')).toEqual(['in-sub']);
		// The include opens through a symlinked spelling: its editor gets
		// the squiggles, the closed-file entry goes.
		router.register('file:///link/sub.weft', '/sub.weft');
		expect(published.has('file:///sub.weft')).toBe(false);
		expect(published.get('file:///link/sub.weft')).toEqual(['in-sub']);
	});

	it('two owners landing on one include contribute a union, and one closing keeps the other half', () => {
		const { published, router } = rig();
		router.register('file:///a.weft', '/a.weft');
		router.register('file:///b.weft', '/b.weft');
		router.publish('file:///a.weft', new Map([['/sub.weft', ['from-a']]]));
		router.publish('file:///b.weft', new Map([['/sub.weft', ['from-b']]]));
		expect(published.get('file:///sub.weft')).toEqual(['from-a', 'from-b']);
		router.close('file:///a.weft', '/a.weft');
		expect(published.get('file:///sub.weft')).toEqual(['from-b']);
	});

	it('an emptied slice deletes the panel entry', () => {
		const { published, router } = rig();
		router.register('file:///a.weft', '/a.weft');
		router.publish('file:///a.weft', new Map([['/a.weft', ['finding']]]));
		router.publish('file:///a.weft', new Map());
		expect(published.has('file:///a.weft')).toBe(false);
	});
});
