// The layout corpus: real projects run through the real `autoOrganize`, with
// assertions on the SHAPE of the result rather than on pixel positions. A
// layout option that helps one project and wrecks another has to fail here,
// not on somebody's screen.
//
// A fixture is a `ProjectDefinition` in the shape `weft parse` emits
// (`weft parse --file main.weft < main.weft`, the `project` key), translated
// through the same `translateProject` the webview uses, so a fixture is the
// webview's own input and not a hand-built approximation of it. The two
// checked-in JSON files are the repo's own example projects, parsed; to add
// one, parse the project and drop the JSON here, with connection ids in
// config replaced by zeros (they carry nothing the layout reads). The big
// one is generated in code below, because the real project it stands in for
// is private.
//
// Set `WEFT_LAYOUT_SVG_DIR` to also get one SVG per fixture, drawn from the
// same positions the assertions see, for looking at a candidate layout.

import { describe, it, expect } from 'vitest';
import fs from 'node:fs';
import path from 'node:path';
import { layoutToSvg } from './render-svg';
import { autoOrganize } from '../auto-organize';
import { translateProject } from '../../host-bridge';
import type { ProjectDefinition } from '../../../protocol';
import type { NodeInstance } from '../types';
import whatsappSupportBot from './whatsapp-support-bot.project.json';
import telegramImageBot from './telegram-image-bot.project.json';

// The same estimate `autoOrganize` uses for a leaf it was given no measured
// size for (see `leafSize` there), so the boxes asserted on are the boxes
// ELK laid out.
const NODE_WIDTH = 280;
const NODE_BASE_HEIGHT = 90;
const PORT_ROW_HEIGHT = 22;
function estimatedLeafSize(n: NodeInstance): { width: number; height: number } {
	const portCount = Math.max(n.inputs?.length ?? 0, n.outputs?.length ?? 0, 1);
	return { width: NODE_WIDTH, height: NODE_BASE_HEIGHT + portCount * PORT_ROW_HEIGHT };
}

// A big project, generated rather than checked in: two programs sharing a
// database, a provider and a few params nodes across the whole file, each
// program a long chain with a seven-node group in the middle. This is the
// shape that crashed elkjs 0.12 when the model-order options were set (a
// group of ordinary nodes under `INCLUDE_CHILDREN`), and the shape where
// forcing source order doubled the crossings; a private project surfaced
// both and this stands in for it.
function bigPipeline(): ProjectDefinition {
	const nodes: Record<string, unknown>[] = [];
	const edges: Record<string, unknown>[] = [];
	const groups: Record<string, unknown>[] = [];
	const str = (name: string, required = true) => ({ name, portType: 'String', required });
	const access = (name: string) => ({ name, portType: 'Access', required: true });
	const flow = { name: '_should_flow', portType: 'T__should_flow', required: false, fromSpec: true };
	const node = (id: string, nodeType: string, inputs: unknown[], outputs: unknown[], scope: string[] = [], extra: Record<string, unknown> = {}) => {
		nodes.push({ id, nodeType, label: null, config: {}, position: { x: 0, y: 0 }, scope, groupBoundary: null, inputs: [flow, ...inputs], outputs, ...extra });
	};
	const edge = (source: string, sourceHandle: string, target: string, targetHandle: string) => {
		edges.push({ id: `${source}.${sourceHandle}->${target}.${targetHandle}`, source, target, sourceHandle, targetHandle });
	};
	node('db', 'PostgresDatabase', [], [{ name: 'access', portType: 'Access', required: false }], [], { features: { requiresInfra: true } });
	node('llm', 'OpenRouterProvider', [], [{ name: 'provider', portType: 'LlmProvider', required: false }]);
	node('params', 'LlmParams', [], [{ name: 'params', portType: 'Dict[String, String]', required: false }]);
	node('bot_name', 'Text', [], [str('value', false)]);
	for (const program of ['msg', 'tick']) {
		const trigger = program === 'msg' ? 'BaileyReceive' : 'Cron';
		node(`${program}_in`, trigger, [], [str('content', false), str('chatId', false)], [], { features: { isTrigger: true } });
		let prev = `${program}_in`;
		let prevPort = 'content';
		for (let i = 0; i < 5; i++) {
			const id = `${program}_pre${i}`;
			node(id, i % 2 ? 'PostgresExecuteQuery' : 'ExecPython', i % 2 ? [access('account'), str('query')] : [str('text')], [str('rows', false)]);
			if (i % 2) edge('db', 'access', id, 'account');
			edge(prev, prevPort, id, i % 2 ? 'query' : 'text');
			prev = id; prevPort = 'rows';
		}
		const g = `${program}_think`;
		groups.push({ id: g, kind: 'group', label: null, anonymous: false, oneOfRequired: [], inPorts: [str('text'), access('db'), { name: 'provider', portType: 'LlmProvider', required: true }, { name: 'params', portType: 'Dict[String, String]', required: true }, str('botName')], outPorts: [str('answer', false)], parentGroupId: null, childGroupIds: [], nodeIds: ['history', 'interrupted', 'rendered', 'analyst_prompt', 'analyst', 'persona_prompt', 'persona'].map(l => `${g}.${l}`) });
		node(`${g}__in`, 'Passthrough', [str('text'), access('db'), { name: 'provider', portType: 'LlmProvider', required: true }, { name: 'params', portType: 'Dict[String, String]', required: true }, str('botName')], [str('text', false), { name: 'db', portType: 'Access', required: false }, { name: 'provider', portType: 'LlmProvider', required: false }, { name: 'params', portType: 'Dict[String, String]', required: false }, str('botName', false)], [], { groupBoundary: { groupId: g, role: 'In' } });
		node(`${g}__out`, 'Passthrough', [str('answer', false)], [str('answer', false)], [], { groupBoundary: { groupId: g, role: 'Out' } });
		edge(prev, prevPort, `${g}__in`, 'text');
		edge('db', 'access', `${g}__in`, 'db');
		edge('llm', 'provider', `${g}__in`, 'provider');
		edge('params', 'params', `${g}__in`, 'params');
		edge('bot_name', 'value', `${g}__in`, 'botName');
		const inner = (local: string) => `${g}.${local}`;
		node(inner('history'), 'PostgresExecuteQuery', [access('account'), str('query')], [str('rows', false)], [g]);
		node(inner('interrupted'), 'PostgresExecuteQuery', [access('account'), str('query')], [str('rows', false)], [g]);
		node(inner('rendered'), 'ExecPython', [str('rows'), str('interrupted')], [str('transcript', false)], [g]);
		node(inner('analyst_prompt'), 'ExecPython', [str('transcript'), str('bot_name')], [str('prompt', false)], [g]);
		node(inner('analyst'), 'LlmInference', [str('prompt'), { name: 'provider', portType: 'LlmProvider', required: true }, { name: 'params', portType: 'Dict[String, String]', required: true }], [str('guidance', false)], [g]);
		node(inner('persona_prompt'), 'ExecPython', [str('transcript'), str('guidance'), str('bot_name')], [str('prompt', false)], [g]);
		node(inner('persona'), 'LlmInference', [str('prompt'), { name: 'provider', portType: 'LlmProvider', required: true }, { name: 'params', portType: 'Dict[String, String]', required: true }], [str('answer', false)], [g]);
		for (const q of ['history', 'interrupted']) {
			edge(`${g}__in`, 'db', inner(q), 'account');
			edge(`${g}__in`, 'text', inner(q), 'query');
			edge(`${g}__in`, '_should_flow', inner(q), '_should_flow');
		}
		edge(inner('history'), 'rows', inner('rendered'), 'rows');
		edge(inner('interrupted'), 'rows', inner('rendered'), 'interrupted');
		edge(inner('rendered'), 'transcript', inner('analyst_prompt'), 'transcript');
		edge(`${g}__in`, 'botName', inner('analyst_prompt'), 'bot_name');
		edge(inner('analyst_prompt'), 'prompt', inner('analyst'), 'prompt');
		edge(`${g}__in`, 'provider', inner('analyst'), 'provider');
		edge(`${g}__in`, 'params', inner('analyst'), 'params');
		edge(inner('rendered'), 'transcript', inner('persona_prompt'), 'transcript');
		edge(inner('analyst'), 'guidance', inner('persona_prompt'), 'guidance');
		edge(`${g}__in`, 'botName', inner('persona_prompt'), 'bot_name');
		edge(inner('persona_prompt'), 'prompt', inner('persona'), 'prompt');
		edge(`${g}__in`, 'provider', inner('persona'), 'provider');
		edge(`${g}__in`, 'params', inner('persona'), 'params');
		edge(inner('persona'), 'answer', `${g}__out`, 'answer');
		prev = `${g}__out`; prevPort = 'answer';
		for (let i = 0; i < 4; i++) {
			const id = `${program}_post${i}`;
			node(id, i % 2 ? 'PostgresExecuteQuery' : 'ExecPython', i % 2 ? [access('account'), str('query')] : [str('text')], [str('rows', false)]);
			if (i % 2) edge('db', 'access', id, 'account');
			edge(prev, prevPort, id, i % 2 ? 'query' : 'text');
			prev = id; prevPort = 'rows';
		}
		node(`${program}_out`, 'Debug', [{ name: 'data', portType: 'T', required: true }], [], [], { features: { isOutputDefault: true } });
		edge(prev, prevPort, `${program}_out`, 'data');
	}
	return { id: 'big-pipeline', nodes, edges, groups } as unknown as ProjectDefinition;
}

const FIXTURES: Array<{ name: string; def: unknown; describe: string }> = [
	{ name: 'big-pipeline', def: bigPipeline(), describe: 'generated: two programs sharing infra across the file, each with a seven-node group' },
	{ name: 'whatsapp-support-bot', def: whatsappSupportBot, describe: '9 nodes, one Switch fork rejoined by FirstInOrder' },
	{ name: 'telegram-image-bot', def: telegramImageBot, describe: '21 nodes, two groups, a maintenance branch off its own trigger' },
];

type Box = { id: string; x: number; y: number; w: number; h: number };

function overlaps(a: Box, b: Box): boolean {
	return a.x < b.x + b.w && b.x < a.x + a.w && a.y < b.y + b.h && b.y < a.y + a.h;
}

async function layOut(name: string, def: unknown) {
	const v1 = translateProject(def as ProjectDefinition, '', '');
	const nodes = v1.nodes;
	const edges = v1.edges;
	// `autoOrganize` throws when ELK refuses the graph, which fails the
	// test with ELK's own message.
	const { positions, groupSizes } = await autoOrganize(nodes, edges);
	const leafSize = (id: string) => {
		const n = nodes.find(x => x.id === id);
		return n ? estimatedLeafSize(n) : undefined;
	};
	const svgDir = process.env.WEFT_LAYOUT_SVG_DIR;
	if (svgDir) {
		fs.mkdirSync(svgDir, { recursive: true });
		fs.writeFileSync(path.join(svgDir, `${name}.svg`), layoutToSvg({ nodes, edges, positions, groupSizes, leafSize }));
	}
	const boxes: Box[] = [];
	for (const n of nodes) {
		const p = positions.get(n.id);
		if (!p) continue;
		const g = groupSizes.get(n.id);
		const s = g ? { width: g.width, height: g.height } : estimatedLeafSize(n);
		boxes.push({ id: n.id, x: p.x, y: p.y, w: s.width, h: s.height });
	}
	return { nodes, edges, positions, groupSizes, boxes };
}

describe.each(FIXTURES)('layout corpus: $name ($describe)', ({ name, def }) => {
	it('lays every node out without ELK throwing', async () => {
		const { nodes, positions } = await layOut(name, def);
		const missing = nodes.filter(n => !positions.has(n.id)).map(n => n.id);
		expect(missing).toEqual([]);
	});

	it('no two siblings overlap', async () => {
		const { nodes, boxes } = await layOut(name, def);
		const parentOf = new Map(nodes.map(n => [n.id, n.parentId]));
		const byParent = new Map<string | undefined, Box[]>();
		for (const b of boxes) {
			const k = parentOf.get(b.id);
			if (!byParent.has(k)) byParent.set(k, []);
			byParent.get(k)!.push(b);
		}
		const collisions: string[] = [];
		for (const [, sibs] of byParent) {
			for (let i = 0; i < sibs.length; i++) {
				for (let j = i + 1; j < sibs.length; j++) {
					if (overlaps(sibs[i], sibs[j])) collisions.push(`${sibs[i].id} x ${sibs[j].id}`);
				}
			}
		}
		expect(collisions).toEqual([]);
	});

	it('every child sits inside its group', async () => {
		const { nodes, boxes, groupSizes } = await layOut(name, def);
		const byId = new Map(boxes.map(b => [b.id, b]));
		const outside: string[] = [];
		for (const n of nodes) {
			if (!n.parentId) continue;
			const child = byId.get(n.id);
			const g = groupSizes.get(n.parentId);
			if (!child || !g) continue;
			// Child positions are relative to the group.
			if (child.x < 0 || child.y < 0 || child.x + child.w > g.width || child.y + child.h > g.height) {
				outside.push(`${n.id} in ${n.parentId}`);
			}
		}
		expect(outside).toEqual([]);
	});
});
