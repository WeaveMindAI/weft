import { describe, it, expect } from 'vitest';
import { parseLoom } from './loom-parser';
import type { Brick } from '$lib/types';

/** Wrap raw loom in a 4-backtick fence so parseLoom can extract it. */
function loom(code: string) {
	return '````loom\n' + code.trim() + '\n````';
}

/** Parse loom and return the first brick of the given kind. */
function firstBrick(code: string, kind: string): Brick {
	const { manifest } = parseLoom(loom(code));
	expect(manifest).not.toBeNull();
	const found = (manifest!.blocks ?? [])
		.filter(b => b.kind === 'brick')
		.map(b => (b as { kind: 'brick'; brick: Brick }).brick)
		.find(b => b.kind === kind);
	expect(found, `no ${kind} brick parsed`).toBeDefined();
	return found!;
}

function errorsFor(code: string) {
	return parseLoom(loom(code)).errors.map(e => e.message);
}

describe('attribute vs positional classification', () => {
	it('keeps a positional string that contains a colon', () => {
		// `includes(':')` misread this as an attribute and split it at the first
		// colon, so `content` came back undefined and the text vanished.
		const brick = firstBrick('text "Runs 9:00 to 17:00"', 'text');
		expect(brick.props.content).toBe('Runs 9:00 to 17:00');
	});

	it('keeps a positional string containing a URL', () => {
		const brick = firstBrick('text "Docs: https://example.com"', 'text');
		expect(brick.props.content).toBe('Docs: https://example.com');
	});

	it('keeps positional content alongside real attributes', () => {
		const brick = firstBrick(
			'feature icon:"clock" title:"Scheduled" "Fires at 06:30 UTC"',
			'feature'
		);
		expect(brick.props.icon).toBe('clock');
		expect(brick.props.title).toBe('Scheduled');
		expect(brick.props.content).toBe('Fires at 06:30 UTC');
	});

	it('still parses ordinary attributes and content', () => {
		const brick = firstBrick(
			'feature icon:"shield" title:"Verify evidence" "Every finding is checked."',
			'feature'
		);
		expect(brick.props.title).toBe('Verify evidence');
		expect(brick.props.content).toBe('Every finding is checked.');
	});

	it('keeps an attribute value that contains a colon', () => {
		const brick = firstBrick('hero title:"Ships fast" subtitle:"Note: it is fast"', 'hero');
		expect(brick.props.subtitle).toBe('Note: it is fast');
	});

	it('preserves a phase description containing a colon', () => {
		const { manifest } = parseLoom(loom('phase "Configure" "Runs at 9:00" {\n}'));
		expect(manifest!.phases[0].description).toBe('Runs at 9:00');
	});
});

describe('key= instead of key:', () => {
	it('reports a bare key= rather than absorbing it as body text', () => {
		const src = 'feature icon:"shield" title="Verify evidence" "Every finding is checked."';
		const messages = errorsFor(src);
		expect(messages).toContainEqual(expect.stringContaining("Did you mean 'title:'?"));

		// The malformed token must not leak into the rendered copy.
		const brick = firstBrick(src, 'feature');
		expect(brick.props.content).toBe('Every finding is checked.');
	});

	it('reports key= on a phase line', () => {
		expect(errorsFor('phase "Configure" description="Set up" {\n}')).toContainEqual(
			expect.stringContaining("Did you mean 'description:'?")
		);
	});

	it('does not flag an equals sign inside a quoted value', () => {
		expect(errorsFor('text "a = b"')).toEqual([]);
	});
});
