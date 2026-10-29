// A gathered metadata value as the editor shows it: the expression the
// group's body writes (`self.tools = [...screen.tools, pause.tool]`), read
// back as compact weft-like text in which every named port is something a
// person can click to follow. Pure, so every shape is tested on its own.

import type { MetadataExpr } from '../../../protocol';
import type { MetadataRef } from '../types';

/** One run of the rendered expression: plain punctuation and keys, a named
 *  port (`spread` when it is written `...name` in a list), or a value
 *  written out inline, as compact JSON. */
export type ExprSegment =
	| { kind: 'text'; text: string }
	| { kind: 'ref'; ref: MetadataRef; label: string; spread: boolean }
	| { kind: 'literal'; text: string };

const IDENT = /^[A-Za-z_][A-Za-z0-9_]*$/;

/** How a named port reads where it is written: its owner's id relative to
 *  `scope` (the group whose body names it, or whose surroundings a wire
 *  is written in), the port, then each key read off it. A name reaching
 *  out of `scope` reads relative to the closest group around it that
 *  holds the owner, the way the source names it (a group's body names a
 *  sibling of the group by its own name). A key that is no plain name
 *  reads in brackets. */
export function metadataRefLabel(ref: MetadataRef, scope: string | undefined): string {
	let owner = ref.source;
	for (let s = scope; s; s = s.includes('.') ? s.slice(0, s.lastIndexOf('.')) : undefined) {
		if (ref.source.startsWith(`${s}.`)) {
			owner = ref.source.slice(s.length + 1);
			break;
		}
	}
	const keys = (ref.path ?? []).map((k) => (IDENT.test(k) ? `.${k}` : `[${JSON.stringify(k)}]`)).join('');
	return `${owner}.${ref.port}${keys}`;
}

/** The expression as segments, in written order, adjacent text joined. */
export function metadataExprSegments(expr: MetadataExpr, scope: string | undefined): ExprSegment[] {
	const out: ExprSegment[] = [];
	const text = (t: string) => {
		const last = out[out.length - 1];
		if (last?.kind === 'text') last.text += t;
		else out.push({ kind: 'text', text: t });
	};
	const walk = (e: MetadataExpr, spread: boolean) => {
		switch (e.kind) {
			case 'literal':
				if (spread) text('...');
				out.push({ kind: 'literal', text: JSON.stringify(e.value) });
				return;
			case 'port':
				out.push({ kind: 'ref', ref: e, label: metadataRefLabel(e, scope), spread });
				return;
			case 'list':
				if (spread) text('...');
				text('[');
				e.items.forEach((item, i) => {
					if (i > 0) text(', ');
					walk(item.value, item.spread ?? false);
				});
				text(']');
				return;
			case 'object':
				if (spread) text('...');
				if (e.fields.length === 0) {
					text('{}');
					return;
				}
				text('{ ');
				e.fields.forEach((field, i) => {
					if (i > 0) text(', ');
					text(`${JSON.stringify(field.key)}: `);
					walk(field.value, false);
				});
				text(' }');
				return;
		}
	};
	walk(expr, false);
	return out;
}

/** The expression as weft source text, the way a group's body writes it
 *  (`[...screen.tools, pause.tool]`): each name relative to `scope`, a
 *  spread with its `...`, keys read after the port, and every value
 *  written out inline as the JSON weft reads, keys quoted. What
 *  `set_metadata_value` takes to write the gathered value back. */
export function metadataExprSource(expr: MetadataExpr, scope: string | undefined): string {
	return metadataExprSegments(expr, scope)
		.map((s) => (s.kind === 'ref' ? `${s.spread ? '...' : ''}${s.label}` : s.text))
		.join('');
}

/** Whether the expression names a port of one of `groupId`'s own members
 *  (anything inside it, at any depth). Duplicating a group copies an
 *  empty shell, so such a value would name members the copy lacks. */
export function metadataExprNamesInside(expr: MetadataExpr, groupId: string): boolean {
	switch (expr.kind) {
		case 'literal':
			return false;
		case 'port':
			return expr.source.startsWith(`${groupId}.`);
		case 'list':
			return expr.items.some((item) => metadataExprNamesInside(item.value, groupId));
		case 'object':
			return expr.fields.some((field) => metadataExprNamesInside(field.value, groupId));
	}
}

/// Above this many characters a value written out inline shows only its
/// start, until clicked.
export const SHORT_LITERAL = 40;

/** The expression's text as the strip shows it before anything is
 *  clicked open (`MetadataExprView`): what its box wraps, so the box's
 *  height is known before it draws. */
export function metadataExprDisplayText(expr: MetadataExpr, scope: string | undefined): string {
	return metadataExprSegments(expr, scope)
		.map((s) => s.kind === 'text' ? s.text
			: s.kind === 'ref' ? `${s.spread ? '...' : ''}${s.label}`
			: s.text.length > SHORT_LITERAL ? `${s.text.slice(0, SHORT_LITERAL)}...` : s.text)
		.join('');
}
