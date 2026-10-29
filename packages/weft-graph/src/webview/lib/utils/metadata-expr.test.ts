import { describe, expect, it } from 'vitest';
import { metadataExprNamesInside, metadataExprSegments, metadataExprSource, metadataRefLabel, type ExprSegment } from './metadata-expr';
import type { MetadataExpr } from '../../../protocol';

const port = (source: string, name: string, path?: string[]): MetadataExpr =>
	({ kind: 'port', source, port: name, ...(path ? { path } : {}) });
const lit = (value: unknown): MetadataExpr => ({ kind: 'literal', value });

/** The segments as one string: a ref reads `<label>` (`...<label>` when
 *  spread), a literal `{json}`, so the shape under test shows at a glance. */
const read = (segments: ExprSegment[]) =>
	segments.map((s) => (s.kind === 'text' ? s.text : s.kind === 'ref' ? `${s.spread ? '...' : ''}<${s.label}>` : `{${s.text}}`)).join('');

describe('metadataRefLabel', () => {
	it('reads the owner relative to the scope it is written in', () => {
		expect(metadataRefLabel({ kind: 'port', source: 'a.calls.act', port: 'tools' }, 'a.calls')).toBe('act.tools');
		expect(metadataRefLabel({ kind: 'port', source: 'a.calls', port: 'tools', path: ['anthropic'] }, 'a')).toBe('calls.tools.anthropic');
	});

	it('reads a name reaching out of the scope relative to the closest group holding it', () => {
		// From `p.g`'s body, its sibling `p.other` is written `other`.
		expect(metadataRefLabel({ kind: 'port', source: 'p.other', port: 'tool' }, 'p.g')).toBe('other.tool');
		expect(metadataRefLabel({ kind: 'port', source: 'top', port: 'tool' }, 'p.g')).toBe('top.tool');
	});

	it('keeps the whole id outside every scope around, and brackets a key that is no plain name', () => {
		expect(metadataRefLabel({ kind: 'port', source: 'other.dog', port: 'tool' }, 'a')).toBe('other.dog.tool');
		expect(metadataRefLabel({ kind: 'port', source: 'dog', port: 'tool', path: ['a b', 'c'] }, undefined)).toBe('dog.tool["a b"].c');
	});
});

describe('metadataExprSegments', () => {
	it('reads a single name', () => {
		expect(read(metadataExprSegments(port('g.act', 'tools'), 'g'))).toBe('<act.tools>');
	});

	it('reads a list mixing spreads, single names and inline values', () => {
		const expr: MetadataExpr = {
			kind: 'list',
			items: [
				{ spread: true, value: port('g.screen', 'tools') },
				{ value: port('g.pause', 'tool') },
				{ value: lit({ name: 'wait' }) },
				{ spread: true, value: lit([1, 2]) },
			],
		};
		expect(read(metadataExprSegments(expr, 'g'))).toBe('[...<screen.tools>, <pause.tool>, {{"name":"wait"}}, ...{[1,2]}]');
	});

	it('reads an object whose keys each hold a list of spreads', () => {
		const expr: MetadataExpr = {
			kind: 'object',
			fields: [
				{ key: 'anthropic', value: { kind: 'list', items: [
					{ spread: true, value: port('p.screen.shot', 'tools', ['anthropic']) },
					{ spread: true, value: port('p.screen.zoom', 'tools', ['anthropic']) },
				] } },
				{ key: 'openai', value: { kind: 'list', items: [{ spread: true, value: port('p.screen.shot', 'tools', ['openai']) }] } },
			],
		};
		expect(read(metadataExprSegments(expr, 'p.screen'))).toBe(
			'{ "anthropic": [...<shot.tools.anthropic>, ...<zoom.tools.anthropic>], "openai": [...<shot.tools.openai>] }',
		);
	});

	it('reads nested lists and objects, and a name reading several keys', () => {
		const expr: MetadataExpr = {
			kind: 'list',
			items: [
				{ value: { kind: 'list', items: [{ value: port('g.a', 'm', ['x', 'y']) }] } },
				{ value: { kind: 'object', fields: [{ key: 'k', value: { kind: 'object', fields: [] } }] } },
				{ value: { kind: 'list', items: [] } },
			],
		};
		expect(read(metadataExprSegments(expr, 'g'))).toBe('[[<a.m.x.y>], { "k": {} }, []]');
	});

	it('joins the punctuation between two names into one text run', () => {
		const segments = metadataExprSegments({ kind: 'list', items: [{ value: port('g.a', 'p') }, { value: port('g.b', 'p') }] }, 'g');
		expect(segments.map((s) => s.kind)).toEqual(['text', 'ref', 'text', 'ref', 'text']);
		expect(segments[2]).toEqual({ kind: 'text', text: ', ' });
	});

	it('keeps the named port on each chip, so a click follows it', () => {
		const segments = metadataExprSegments({ kind: 'list', items: [{ spread: true, value: port('g.s', 'tools', ['openai']) }] }, 'g');
		expect(segments[1]).toEqual({ kind: 'ref', ref: { kind: 'port', source: 'g.s', port: 'tools', path: ['openai'] }, label: 's.tools.openai', spread: true });
	});
});

describe('metadataExprSource', () => {
	it('writes a single name, keys read included', () => {
		expect(metadataExprSource(port('g.calls', 'tools', ['anthropic']), 'g')).toBe('calls.tools.anthropic');
	});

	it('writes a list mixing spreads, single names and inline values', () => {
		const expr: MetadataExpr = {
			kind: 'list',
			items: [
				{ spread: true, value: port('g.screen', 'tools') },
				{ value: port('g.pause', 'tool') },
				{ value: lit({ name: 'wait', 'max tries': 3 }) },
				{ spread: true, value: lit(['a']) },
			],
		};
		expect(metadataExprSource(expr, 'g')).toBe('[...screen.tools, pause.tool, {"name":"wait","max tries":3}, ...["a"]]');
	});

	it('writes an object whose keys each hold a list of spreads, keys quoted', () => {
		const expr: MetadataExpr = {
			kind: 'object',
			fields: [
				{ key: 'anthropic', value: { kind: 'list', items: [
					{ spread: true, value: port('p.screen.shot', 'tools', ['anthropic']) },
					{ spread: true, value: port('p.screen.zoom', 'tools', ['anthropic']) },
				] } },
				{ key: 'openai', value: { kind: 'list', items: [{ spread: true, value: port('p.screen.shot', 'tools', ['openai']) }] } },
			],
		};
		expect(metadataExprSource(expr, 'p.screen')).toBe(
			'{ "anthropic": [...shot.tools.anthropic, ...zoom.tools.anthropic], "openai": [...shot.tools.openai] }',
		);
	});

	it('writes nested lists and objects, a name reading several keys, and empty ones', () => {
		const expr: MetadataExpr = {
			kind: 'list',
			items: [
				{ value: { kind: 'list', items: [{ value: port('g.a', 'm', ['x', 'y']) }] } },
				{ value: { kind: 'object', fields: [{ key: 'k', value: { kind: 'object', fields: [] } }] } },
				{ value: { kind: 'list', items: [] } },
				{ value: lit(null) },
			],
		};
		expect(metadataExprSource(expr, 'g')).toBe('[[a.m.x.y], { "k": {} }, [], null]');
	});

	it('writes a string literal with its escapes, as JSON reads it', () => {
		expect(metadataExprSource({ kind: 'list', items: [{ value: lit('say "hi"\n') }] }, 'g')).toBe('["say \\"hi\\"\\n"]');
	});
});

describe('metadataExprNamesInside', () => {
	const port = (source: string): MetadataExpr => ({ kind: 'port', source, port: 'tools' });
	it('finds a member at any depth, in lists and objects', () => {
		const written: MetadataExpr = {
			kind: 'object',
			fields: [{ key: 'a', value: { kind: 'list', items: [{ spread: true, value: port('g.screen.shot') }] } }],
		};
		expect(metadataExprNamesInside(written, 'g')).toBe(true);
		expect(metadataExprNamesInside(written, 'g.screen')).toBe(true);
	});
	it('reads a port outside the group, or a group sharing a prefix, as outside', () => {
		expect(metadataExprNamesInside(port('other'), 'g')).toBe(false);
		expect(metadataExprNamesInside(port('gg.x'), 'g')).toBe(false);
		expect(metadataExprNamesInside({ kind: 'literal', value: [1] }, 'g')).toBe(false);
	});
});
