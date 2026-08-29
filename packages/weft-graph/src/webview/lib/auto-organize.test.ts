import { describe, it, expect } from 'vitest';
import { autoOrganize } from './auto-organize';

const node = (id: string, extra: Record<string, unknown> = {}) => ({
  id,
  nodeType: 'Text',
  label: null,
  config: {},
  position: { x: 0, y: 0 },
  inputs: [{ name: 'value', type: 'String' }],
  outputs: [{ name: 'value', type: 'String' }],
  features: {},
  ...extra,
});

describe('autoOrganize', () => {
  it('a _should_flow wire is a real dependency: the gated node lands downstream', async () => {
    const nodes = [node('gate'), node('gated')] as any;
    const edges = [
      { id: 'e1', source: 'gate', target: 'gated', sourceHandle: 'value', targetHandle: '_should_flow' },
    ] as any;
    const { positions } = await autoOrganize(nodes, edges);
    const gate = positions.get('gate')!;
    const gated = positions.get('gated')!;
    // Right of the gate's whole box, not beside or above it (the default
    // node width is 280): the wire was fed to ELK, so it layered them.
    expect(gated.x).toBeGreaterThan(gate.x + 280);
  });

  it('without the wire the two nodes are unrelated components, stacked in bands', async () => {
    const nodes = [node('gate'), node('gated')] as any;
    const { positions } = await autoOrganize(nodes, []);
    const gate = positions.get('gate')!;
    const gated = positions.get('gated')!;
    // Each root component gets its own horizontal band, sharing the left edge.
    expect(Math.abs(gated.x - gate.x)).toBeLessThan(5);
    expect(gated.y).toBeGreaterThan(gate.y);
  });
});

describe('satellites and side paths', () => {
	it('a single-consumer feeder parks under its consumer instead of layer zero', async () => {
		// provider -> llm <- upstream ; provider has no inputs and feeds only
		// the llm's wire-exposure hookup input.
		const llmNode = {
			...node('llm'),
			inputs: [
				{ name: 'value', type: 'String' },
				{ name: 'provider', portType: 'LlmProvider', exposure: 'wire' },
			],
		};
		const nodes = [node('upstream'), llmNode, node('provider')] as any;
		const edges = [
			{ id: 'e1', source: 'upstream', target: 'llm', sourceHandle: 'value', targetHandle: 'value' },
			{ id: 'e2', source: 'provider', target: 'llm', sourceHandle: 'value', targetHandle: 'provider' },
		] as any;
		const { positions } = await autoOrganize(nodes, edges);
		const llm = positions.get('llm')!;
		const provider = positions.get('provider')!;
		// Boxed with its consumer: directly to its left, not in the
		// graph-wide first layer with a wire across the canvas.
		expect(provider.x).toBeLessThan(llm.x);
		expect(Math.abs(provider.y - llm.y)).toBeLessThan(300);
	});

	it('a path joined to another only by plumbing gets its own band below', async () => {
		// access feeds main1 and side1 through Access ports; the two chains
		// share nothing else, so they stack instead of interleaving.
		const withAccess = (id: string) => ({
			...node(id),
			inputs: [{ name: 'value', type: 'String' }, { name: 'account', portType: 'Access' }],
		});
		const nodes = [
			node('access'), withAccess('main1'), node('main2'),
			withAccess('side1'), node('side2'),
		] as any;
		const edges = [
			{ id: 'p1', source: 'access', target: 'main1', sourceHandle: 'value', targetHandle: 'account' },
			{ id: 'p2', source: 'access', target: 'side1', sourceHandle: 'value', targetHandle: 'account' },
			{ id: 'm', source: 'main1', target: 'main2', sourceHandle: 'value', targetHandle: 'value' },
			{ id: 's', source: 'side1', target: 'side2', sourceHandle: 'value', targetHandle: 'value' },
		] as any;
		const { positions } = await autoOrganize(nodes, edges);
		const mainBottom = Math.max(positions.get('main1')!.y, positions.get('main2')!.y);
		expect(positions.get('side1')!.y).toBeGreaterThan(mainBottom);
		expect(positions.get('side2')!.y).toBeGreaterThan(mainBottom);
		// The shared access node is boxed beside its earliest consumer,
		// in the main band, above the side band.
		expect(positions.get('access')!.x).toBeLessThan(positions.get('main1')!.x);
		expect(positions.get('access')!.y).toBeLessThan(positions.get('side1')!.y);
	});
});

describe('moon parking picks the majority path', () => {
	const withAccess = (id: string) => ({
		...node(id),
		inputs: [{ name: 'value', type: 'String' }, { name: 'account', portType: 'Access' }],
	});
	it('a source shared by one main user and two side users parks with the side path', async () => {
		const nodes = [
			node('access'), withAccess('main1'), node('main2'),
			withAccess('side1'), withAccess('side2'),
		] as any;
		const edges = [
			{ id: 'p1', source: 'access', target: 'main1', sourceHandle: 'value', targetHandle: 'account' },
			{ id: 'p2', source: 'access', target: 'side1', sourceHandle: 'value', targetHandle: 'account' },
			{ id: 'p3', source: 'access', target: 'side2', sourceHandle: 'value', targetHandle: 'account' },
			{ id: 'm', source: 'main1', target: 'main2', sourceHandle: 'value', targetHandle: 'value' },
			{ id: 's', source: 'side1', target: 'side2', sourceHandle: 'value', targetHandle: 'value' },
		] as any;
		const { positions } = await autoOrganize(nodes, edges);
		// Boxed beside side1 (earliest consumer of the two-consumer path),
		// down in the side band rather than up with the main flow.
		expect(positions.get('access')!.x).toBeLessThan(positions.get('side1')!.x);
		const mainBottom = Math.max(positions.get('main1')!.y, positions.get('main2')!.y);
		expect(positions.get('access')!.y).toBeGreaterThan(mainBottom);
	});
});

describe('simplified view', () => {
	it('group padding shrinks to the slim simplified chrome', async () => {
		// One group holding one child. Builder groups reserve room for the
		// header plus per-port label rows; simplified groups draw a slim
		// header and bare dots, so the box must come out tighter.
		const group = { ...node('g'), nodeType: 'Group', config: { expanded: true } };
		const child = { ...node('g.child'), parentId: 'g' };
		const sizes = new Map([['g.child', { width: 96, height: 96 }]]);
		const builder = await autoOrganize([group, child] as any, [], sizes);
		const simplified = await autoOrganize([group, child] as any, [], sizes, undefined, true);
		const b = builder.groupSizes.get('g')!;
		const s = simplified.groupSizes.get('g')!;
		expect(s.width).toBeLessThan(b.width);
		expect(s.height).toBeLessThan(b.height);
	});
});
