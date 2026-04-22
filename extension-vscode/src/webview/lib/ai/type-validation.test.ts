/**
 * Frontend type validation tests, mirrors the backend enrich.rs tests.
 * Tests the resolveAndValidateTypes pipeline: TypeVar resolution, stack depth,
 * edge type compatibility, MustOverride checks.
 */
import { describe, it, expect } from 'vitest';
import {
	parseWeftType,
	weftTypeToString,
	isWeftTypeCompatible,
	type PortDefinition,
	type NodeInstance,
	type Edge,
	type LaneMode,
} from '$lib/types';
import { resolveAndValidateTypes, type WeftParseError } from '$lib/ai/weft-parser';

// ── Helpers ─────────────────────────────────────────────────────────────────

function sp(name: string, portType: string): PortDefinition {
	return { name, portType, required: false };
}

function ep(name: string, portType: string): PortDefinition {
	return { name, portType, required: false, laneMode: 'Expand' as LaneMode };
}

function gp(name: string, portType: string): PortDefinition {
	return { name, portType, required: false, laneMode: 'Gather' as LaneMode };
}

/** Compute target expected wire type (mirrors backend target_expected_wire_type) */
function targetWireType(port: PortDefinition): string {
	if (port.laneMode === 'Expand') {
		return `List[${port.portType}]`;
	}
	if (port.laneMode === 'Gather') {
		const parsed = parseWeftType(port.portType);
		if (parsed && parsed.kind === 'list') {
			return weftTypeToString(parsed.inner);
		}
	}
	return port.portType;
}

/** Check edge type compatibility accounting for lane modes */
function checkEdgeCompat(sourcePort: PortDefinition, targetPort: PortDefinition): boolean {
	const srcWire = sourcePort.portType; // source wire = declared
	const tgtWire = targetWireType(targetPort);
	return isWeftTypeCompatible(srcWire, tgtWire);
}

// ── Edge type compatibility ─────────────────────────────────────────────────

describe('edge type compatibility', () => {
	it('same type compatible', () => {
		expect(checkEdgeCompat(sp('out', 'String'), sp('in', 'String'))).toBe(true);
	});

	it('different types incompatible', () => {
		expect(checkEdgeCompat(sp('out', 'String'), sp('in', 'Number'))).toBe(false);
	});

	it('single into union compatible', () => {
		expect(checkEdgeCompat(sp('out', 'String'), sp('in', 'String | Number'))).toBe(true);
	});

	it('union into narrow incompatible', () => {
		expect(checkEdgeCompat(sp('out', 'String | Number'), sp('in', 'String'))).toBe(false);
	});

	it('same list compatible', () => {
		expect(checkEdgeCompat(sp('out', 'List[String]'), sp('in', 'List[String]'))).toBe(true);
	});

	it('different list inner incompatible', () => {
		expect(checkEdgeCompat(sp('out', 'List[String]'), sp('in', 'List[Number]'))).toBe(false);
	});
});

// ── Expand/Gather wire types ────────────────────────────────────────────────

describe('expand/gather wire types', () => {
	it('expand input receives list compatible', () => {
		// Source: List[String], Target expand declares String → wire expects List[String]
		expect(checkEdgeCompat(sp('out', 'List[String]'), ep('in', 'String'))).toBe(true);
	});

	it('expand input non-list incompatible', () => {
		// Source: String, Target expand declares String → wire expects List[String]
		expect(checkEdgeCompat(sp('out', 'String'), ep('in', 'String'))).toBe(false);
	});

	it('expand input wrong inner type incompatible', () => {
		// Source: List[Number], Target expand declares String → wire expects List[String]
		expect(checkEdgeCompat(sp('out', 'List[Number]'), ep('in', 'String'))).toBe(false);
	});

	it('gather input element compatible', () => {
		// Source: String, Target gather declares List[String] → wire expects String
		expect(checkEdgeCompat(sp('out', 'String'), gp('in', 'List[String]'))).toBe(true);
	});

	it('gather input wrong element incompatible', () => {
		// Source: Number, Target gather declares List[String] → wire expects String
		expect(checkEdgeCompat(sp('out', 'Number'), gp('in', 'List[String]'))).toBe(false);
	});

	it('expand output into list expecting node fails', () => {
		// Expand output: declared String (per lane), Single input expects List[String]
		expect(checkEdgeCompat(sp('out', 'String'), sp('in', 'List[String]'))).toBe(false);
	});

	it('expand output into same element type works', () => {
		// Expand output: String. Next Single input: String. Compatible.
		expect(checkEdgeCompat(sp('out', 'String'), sp('in', 'String'))).toBe(true);
	});
});

// ── Union edge cases ────────────────────────────────────────────────────────

describe('union edge cases', () => {
	it('overlapping non-subset unions incompatible', () => {
		expect(isWeftTypeCompatible('String | Number', 'String | Boolean')).toBe(false);
	});

	it('subset union compatible', () => {
		expect(isWeftTypeCompatible('String | Number', 'String | Number | Boolean')).toBe(true);
	});

	it('superset union incompatible', () => {
		expect(isWeftTypeCompatible('String | Number | Boolean', 'String | Number')).toBe(false);
	});

	it('union through expand', () => {
		// List[String | Number] → expand(String | Number). Wire: List[S|N] → List[S|N]. OK.
		expect(checkEdgeCompat(sp('out', 'List[String | Number]'), ep('in', 'String | Number'))).toBe(true);
	});

	it('union narrowing in expand fails', () => {
		// List[String | Number] → expand(String). Wire: List[S|N] → List[S]. Fail.
		expect(checkEdgeCompat(sp('out', 'List[String | Number]'), ep('in', 'String'))).toBe(false);
	});

	it('union through gather', () => {
		// Source: String | Number → gather(List[String | Number]). Wire: S|N → S|N. OK.
		expect(checkEdgeCompat(sp('out', 'String | Number'), gp('in', 'List[String | Number]'))).toBe(true);
	});
});

// ── Expand/Gather with complex types ────────────────────────────────────────

describe('expand/gather with complex types', () => {
	it('expand with dict types', () => {
		expect(checkEdgeCompat(
			sp('out', 'List[Dict[String, Number]]'),
			ep('in', 'Dict[String, Number]'),
		)).toBe(true);
	});

	it('expand type change then gather correct new type', () => {
		// Inside expand: String→Dict. Gather expects List[Dict]. Wire: Dict → Dict. OK.
		expect(checkEdgeCompat(
			sp('out', 'Dict[String, Number]'),
			gp('in', 'List[Dict[String, Number]]'),
		)).toBe(true);
	});

	it('expand type change then gather wrong type', () => {
		// Inside expand: produces Number. Gather expects List[String]. Wire: Number → String. Fail.
		expect(checkEdgeCompat(sp('out', 'Number'), gp('in', 'List[String]'))).toBe(false);
	});

	it('expand wrong inner gather correct fails', () => {
		// List[String] → expand(Number). Wire: List[String] → List[Number]. Fail.
		expect(checkEdgeCompat(sp('out', 'List[String]'), ep('in', 'Number'))).toBe(false);
	});

	it('nested list expand', () => {
		// List[List[String]] → expand(List[String]). Wire: List[List[String]] → List[List[String]]. OK.
		expect(checkEdgeCompat(
			sp('out', 'List[List[String]]'),
			ep('in', 'List[String]'),
		)).toBe(true);
	});

	it('nested list expand wrong inner', () => {
		// List[List[String]] → expand(List[Number]). Fail.
		expect(checkEdgeCompat(
			sp('out', 'List[List[String]]'),
			ep('in', 'List[Number]'),
		)).toBe(false);
	});

	it('gather output into expand input re-expand', () => {
		// Gather produces List[String] → expand(String). Wire: List[String] → List[String]. OK.
		expect(checkEdgeCompat(sp('out', 'List[String]'), ep('in', 'String'))).toBe(true);
	});

	it('expand gather expand different type fails', () => {
		// After gather: List[String]. Second expand(Number). Wire: List[String] → List[Number]. Fail.
		expect(checkEdgeCompat(sp('out', 'List[String]'), ep('in', 'Number'))).toBe(false);
	});
});

// ── TypeVar / MustOverride ──────────────────────────────────────────────────

describe('TypeVar and MustOverride', () => {
	it('TypeVar compatible with anything', () => {
		expect(isWeftTypeCompatible('T', 'String')).toBe(true);
		expect(isWeftTypeCompatible('String', 'T')).toBe(true);
		expect(isWeftTypeCompatible('T', 'T')).toBe(true);
		expect(isWeftTypeCompatible('T1', 'T2')).toBe(true);
	});

	it('MustOverride compatible with anything', () => {
		expect(isWeftTypeCompatible('MustOverride', 'String')).toBe(true);
		expect(isWeftTypeCompatible('String', 'MustOverride')).toBe(true);
	});

	it('TypeVar in List compatible', () => {
		expect(isWeftTypeCompatible('List[T]', 'List[String]')).toBe(true);
		expect(isWeftTypeCompatible('List[String]', 'List[T]')).toBe(true);
	});
});

// ── Wire type computation ───────────────────────────────────────────────────

describe('targetWireType', () => {
	it('Single: wire = declared', () => {
		expect(targetWireType(sp('x', 'String'))).toBe('String');
	});

	it('Expand: wire = List[declared]', () => {
		expect(targetWireType(ep('x', 'String'))).toBe('List[String]');
	});

	it('Expand with complex type', () => {
		expect(targetWireType(ep('x', 'Dict[String, Number]'))).toBe('List[Dict[String, Number]]');
	});

	it('Gather: wire = inner(declared List)', () => {
		expect(targetWireType(gp('x', 'List[String]'))).toBe('String');
	});

	it('Gather with complex inner', () => {
		expect(targetWireType(gp('x', 'List[Dict[String, Number]]'))).toBe('Dict[String, Number]');
	});

	it('Gather with non-list declared falls through', () => {
		// Edge case: gather declares String instead of List[String]. Wire = String.
		expect(targetWireType(gp('x', 'String'))).toBe('String');
	});
});

// ── Pack/Unpack dynamic resolution (testing logic directly) ─────────────────

describe('Pack/Unpack resolveTypes logic', () => {
	// Replicate the Pack resolveTypes logic without importing heavy node modules
	function packResolve(inputs: PortDefinition[]): Record<string, string> {
		if (inputs.length === 0) return {};
		const valueTypes = [...new Set(inputs.map(p => p.portType))];
		const valueType = valueTypes.length === 1 ? valueTypes[0] : valueTypes.join(' | ');
		return { out: `Dict[String, ${valueType}]` };
	}

	function unpackResolve(outputs: PortDefinition[]): Record<string, string> {
		if (outputs.length === 0) return {};
		const valueTypes = [...new Set(outputs.map(p => p.portType))];
		const valueType = valueTypes.length === 1 ? valueTypes[0] : valueTypes.join(' | ');
		return { in: `Dict[String, ${valueType}]` };
	}

	it('pack basic: String inputs → Dict[String, String]', () => {
		const result = packResolve([sp('name', 'String'), sp('city', 'String')]);
		expect(result.out).toBe('Dict[String, String]');
	});

	it('pack mixed: String + Number → Dict[String, String | Number]', () => {
		const result = packResolve([sp('name', 'String'), sp('age', 'Number')]);
		expect(result.out).toBe('Dict[String, String | Number]');
	});

	it('pack empty: no inputs → no overrides', () => {
		const result = packResolve([]);
		expect(Object.keys(result)).toHaveLength(0);
	});

	it('pack dedup: String + String → Dict[String, String]', () => {
		const result = packResolve([sp('a', 'String'), sp('b', 'String')]);
		expect(result.out).toBe('Dict[String, String]');
	});

	it('pack three types', () => {
		const result = packResolve([sp('a', 'String'), sp('b', 'Number'), sp('c', 'Boolean')]);
		expect(result.out).toBe('Dict[String, String | Number | Boolean]');
	});

	it('unpack basic: String outputs → Dict[String, String]', () => {
		const result = unpackResolve([sp('name', 'String'), sp('city', 'String')]);
		expect(result.in).toBe('Dict[String, String]');
	});

	it('unpack mixed: String + Number → Dict[String, String | Number]', () => {
		const result = unpackResolve([sp('name', 'String'), sp('score', 'Number')]);
		expect(result.in).toBe('Dict[String, String | Number]');
	});

	it('unpack empty: no outputs → no overrides', () => {
		const result = unpackResolve([]);
		expect(Object.keys(result)).toHaveLength(0);
	});

	it('pack output compatible with matching downstream', () => {
		const packOut = packResolve([sp('a', 'String'), sp('b', 'Number')]);
		// Pack output: Dict[String, String | Number] → consumer expecting same. OK.
		expect(isWeftTypeCompatible(packOut.out, 'Dict[String, String | Number]')).toBe(true);
	});

	it('pack output incompatible with narrow downstream', () => {
		const packOut = packResolve([sp('a', 'String'), sp('b', 'Number')]);
		// Pack output: Dict[String, String | Number] → consumer expecting Dict[String, String]. Fail.
		expect(isWeftTypeCompatible(packOut.out, 'Dict[String, String]')).toBe(false);
	});
});

// ── Complex compatibility chains (simulating full pipelines) ────────────────

describe('full pipeline type chains', () => {
	it('simple String chain', () => {
		// Text(String) → LLM(String→String) → Debug(String)
		expect(checkEdgeCompat(sp('value', 'String'), sp('prompt', 'String'))).toBe(true);
		expect(checkEdgeCompat(sp('response', 'String'), sp('data', 'String'))).toBe(true);
	});

	it('expand process gather chain', () => {
		// List[String] → expand(String) → process(String→String) → gather(List[String])
		expect(checkEdgeCompat(sp('out', 'List[String]'), ep('in', 'String'))).toBe(true);
		expect(checkEdgeCompat(sp('out', 'String'), sp('in', 'String'))).toBe(true);
		expect(checkEdgeCompat(sp('out', 'String'), gp('in', 'List[String]'))).toBe(true);
	});

	it('type mismatch after expand', () => {
		// expand(String) → process(String→Number) → gather(List[String]). Number → String wire: fail.
		expect(checkEdgeCompat(sp('out', 'Number'), gp('in', 'List[String]'))).toBe(false);
	});

	it('nested expand-expand-gather-gather', () => {
		// List[List[String]] → expand(List[String]) → expand(String) → gather(List[String]) → gather(List[List[String]])
		expect(checkEdgeCompat(sp('out', 'List[List[String]]'), ep('in', 'List[String]'))).toBe(true);
		expect(checkEdgeCompat(sp('out', 'List[String]'), ep('in', 'String'))).toBe(true);
		expect(checkEdgeCompat(sp('out', 'String'), gp('in', 'List[String]'))).toBe(true);
		expect(checkEdgeCompat(sp('out', 'List[String]'), gp('in', 'List[List[String]]'))).toBe(true);
	});

	it('expand gather expand gather sequential', () => {
		expect(checkEdgeCompat(sp('out', 'List[String]'), ep('in', 'String'))).toBe(true);
		expect(checkEdgeCompat(sp('out', 'String'), gp('in', 'List[String]'))).toBe(true);
		expect(checkEdgeCompat(sp('out', 'List[String]'), ep('in', 'String'))).toBe(true);
		expect(checkEdgeCompat(sp('out', 'String'), gp('in', 'List[String]'))).toBe(true);
	});

	it('pack inside expand-gather', () => {
		// expand(String) → Pack(String→Dict[String,String]) → gather(List[Dict[String,String]])
		expect(checkEdgeCompat(sp('out', 'String'), sp('a', 'String'))).toBe(true);
		expect(checkEdgeCompat(sp('out', 'Dict[String, String]'), gp('in', 'List[Dict[String, String]]'))).toBe(true);
	});

	it('pack wrong downstream type fails', () => {
		expect(checkEdgeCompat(sp('out', 'Dict[String, String]'), sp('in', 'Dict[String, Number]'))).toBe(false);
	});

	it('triple nested expand-gather', () => {
		expect(checkEdgeCompat(sp('out', 'List[List[List[String]]]'), ep('in', 'List[List[String]]'))).toBe(true);
		expect(checkEdgeCompat(sp('out', 'List[List[String]]'), ep('in', 'List[String]'))).toBe(true);
		expect(checkEdgeCompat(sp('out', 'List[String]'), ep('in', 'String'))).toBe(true);
		expect(checkEdgeCompat(sp('out', 'String'), gp('in', 'List[String]'))).toBe(true);
		expect(checkEdgeCompat(sp('out', 'List[String]'), gp('in', 'List[List[String]]'))).toBe(true);
		expect(checkEdgeCompat(sp('out', 'List[List[String]]'), gp('in', 'List[List[List[String]]]'))).toBe(true);
	});

	it('type conversion inside expand-gather', () => {
		// expand(String) → xform(String→Number) → gather(List[Number])
		expect(checkEdgeCompat(sp('out', 'List[String]'), ep('in', 'String'))).toBe(true);
		expect(checkEdgeCompat(sp('out', 'String'), sp('in', 'String'))).toBe(true);
		expect(checkEdgeCompat(sp('out', 'Number'), gp('in', 'List[Number]'))).toBe(true);
	});

	it('expand with two outputs different types to separate gathers', () => {
		// splitter(String→String+Number). gatherS(List[String]), gatherN(List[Number])
		expect(checkEdgeCompat(sp('text', 'String'), gp('in', 'List[String]'))).toBe(true);
		expect(checkEdgeCompat(sp('num', 'Number'), gp('in', 'List[Number]'))).toBe(true);
	});

	it('list of dicts expand unpack gather separately', () => {
		const dictType = 'Dict[String, Number]';
		expect(checkEdgeCompat(sp('out', `List[${dictType}]`), ep('in', dictType))).toBe(true);
		expect(checkEdgeCompat(sp('out', dictType), sp('in', dictType))).toBe(true);
		expect(checkEdgeCompat(sp('name', 'String'), gp('in', 'List[String]'))).toBe(true);
		expect(checkEdgeCompat(sp('age', 'Number'), gp('in', 'List[Number]'))).toBe(true);
	});
});

// ── Full pipeline tests using resolveAndValidateTypes ───────────────────────

function makeNode(id: string, nodeType: string, inputs: PortDefinition[], outputs: PortDefinition[]): NodeInstance {
	return {
		id,
		nodeType,
		label: null,
		config: {},
		position: { x: 0, y: 0 },
		inputs,
		outputs,
		features: {},
	};
}

function makeEdge(source: string, sourceHandle: string, target: string, targetHandle: string): Edge {
	return {
		id: `e-${source}-${sourceHandle}-${target}-${targetHandle}`,
		source,
		target,
		sourceHandle,
		targetHandle,
	};
}

function runValidation(nodes: NodeInstance[], edges: Edge[]): WeftParseError[] {
	const errors: WeftParseError[] = [];
	resolveAndValidateTypes(nodes, edges, errors);
	return errors;
}

describe('resolveAndValidateTypes full pipeline', () => {
	it('simple chain valid', () => {
		const errors = runValidation([
			makeNode('a', 'Text', [], [sp('value', 'String')]),
			makeNode('b', 'Debug', [sp('data', 'String')], []),
		], [makeEdge('a', 'value', 'b', 'data')]);
		expect(errors).toHaveLength(0);
	});

	it('type mismatch detected', () => {
		const errors = runValidation([
			makeNode('a', 'Text', [], [sp('value', 'String')]),
			makeNode('b', 'Debug', [sp('data', 'Number')], []),
		], [makeEdge('a', 'value', 'b', 'data')]);
		expect(errors.length).toBeGreaterThan(0);
		expect(errors[0].message).toContain('Type mismatch');
	});

	it('expand-gather chain valid', () => {
		const errors = runValidation([
			makeNode('src', 'List', [], [sp('value', 'List[String]')]),
			makeNode('exp', 'Node', [ep('in', 'String')], [sp('out', 'String')]),
			makeNode('gat', 'Node', [gp('in', 'List[String]')], [sp('out', 'List[String]')]),
		], [
			makeEdge('src', 'value', 'exp', 'in'),
			makeEdge('exp', 'out', 'gat', 'in'),
		]);
		expect(errors).toHaveLength(0);
	});

	it('gather without expand fails (stack depth)', () => {
		const errors = runValidation([
			makeNode('a', 'Node', [], [sp('out', 'String')]),
			makeNode('b', 'Node', [gp('in', 'List[String]')], []),
		], [makeEdge('a', 'out', 'b', 'in')]);
		expect(errors.length).toBeGreaterThan(0);
		expect(errors.some(e => e.message.includes('Gather error'))).toBe(true);
	});

	it('multi-level expand then matching gather is valid', () => {
		// Expand 2 levels, then gather 2 levels: depth goes 0→2→0
		const errors = runValidation([
			makeNode('src', 'Node', [], [sp('out', 'List[List[String]]')]),
			makeNode('worker', 'Node',
				[{ name: 'in', portType: 'String', required: false, laneMode: 'Expand' as LaneMode, laneDepth: 2 }],
				[sp('out', 'String')]),
			makeNode('collector', 'Node',
				[{ name: 'in', portType: 'List[List[String]]', required: false, laneMode: 'Gather' as LaneMode, laneDepth: 2 }],
				[]),
		], [
			makeEdge('src', 'out', 'worker', 'in'),
			makeEdge('worker', 'out', 'collector', 'in'),
		]);
		expect(errors).toHaveLength(0);
	});

	it('gather more levels than expanded fails', () => {
		// Expand 1 level, then try to gather 2 levels: depth goes 0→1, then gather 2 > 1
		const errors = runValidation([
			makeNode('src', 'Node', [], [sp('out', 'List[String]')]),
			makeNode('worker', 'Node',
				[{ name: 'in', portType: 'String', required: false, laneMode: 'Expand' as LaneMode, laneDepth: 1 }],
				[sp('out', 'String')]),
			makeNode('collector', 'Node',
				[{ name: 'in', portType: 'List[List[String]]', required: false, laneMode: 'Gather' as LaneMode, laneDepth: 2 }],
				[]),
		], [
			makeEdge('src', 'out', 'worker', 'in'),
			makeEdge('worker', 'out', 'collector', 'in'),
		]);
		expect(errors.length).toBeGreaterThan(0);
		expect(errors.some(e => e.message.includes('Gather error'))).toBe(true);
	});

	it('three gathers after two expands fails', () => {
		// Expand 2, gather 1, gather 1, gather 1 → third gather at depth 0 fails
		const errors = runValidation([
			makeNode('src', 'Node', [], [sp('out', 'List[List[String]]')]),
			makeNode('exp', 'Node',
				[{ name: 'in', portType: 'String', required: false, laneMode: 'Expand' as LaneMode, laneDepth: 2 }],
				[sp('out', 'String')]),
			makeNode('gat1', 'Node',
				[{ name: 'in', portType: 'List[String]', required: false, laneMode: 'Gather' as LaneMode, laneDepth: 1 }],
				[sp('out', 'List[String]')]),
			makeNode('gat2', 'Node',
				[{ name: 'in', portType: 'List[List[String]]', required: false, laneMode: 'Gather' as LaneMode, laneDepth: 1 }],
				[sp('out', 'List[List[String]]')]),
			makeNode('gat3', 'Node',
				[{ name: 'in', portType: 'List[List[List[String]]]', required: false, laneMode: 'Gather' as LaneMode, laneDepth: 1 }],
				[]),
		], [
			makeEdge('src', 'out', 'exp', 'in'),
			makeEdge('exp', 'out', 'gat1', 'in'),
			makeEdge('gat1', 'out', 'gat2', 'in'),
			makeEdge('gat2', 'out', 'gat3', 'in'),
		]);
		expect(errors.length).toBeGreaterThan(0);
		expect(errors.some(e => e.message.includes('Gather error'))).toBe(true);
	});

	it('expand 5 then gather 5 in one shot is valid', () => {
		const errors = runValidation([
			makeNode('src', 'Node', [], [sp('out', 'List[List[List[List[List[String]]]]]')]),
			makeNode('worker', 'Node',
				[{ name: 'in', portType: 'String', required: false, laneMode: 'Expand' as LaneMode, laneDepth: 5 }],
				[sp('out', 'String')]),
			makeNode('collector', 'Node',
				[{ name: 'in', portType: 'List[List[List[List[List[String]]]]]', required: false, laneMode: 'Gather' as LaneMode, laneDepth: 5 }],
				[]),
		], [
			makeEdge('src', 'out', 'worker', 'in'),
			makeEdge('worker', 'out', 'collector', 'in'),
		]);
		expect(errors).toHaveLength(0);
	});

	it('MustOverride on connected port fails', () => {
		const errors = runValidation([
			makeNode('a', 'Node', [], [sp('out', 'String')]),
			makeNode('b', 'Node', [sp('in', 'MustOverride')], []),
		], [makeEdge('a', 'out', 'b', 'in')]);
		expect(errors.some(e => e.message.includes('requires a type declaration'))).toBe(true);
	});

	it('TypeVar resolution works', () => {
		const nodes = [
			makeNode('a', 'Node', [], [sp('out', 'String')]),
			makeNode('gate', 'Gate', [sp('value', 'T')], [sp('value', 'T')]),
			makeNode('b', 'Node', [sp('in', 'String')], []),
		];
		const errors = runValidation(nodes, [
			makeEdge('a', 'out', 'gate', 'value'),
			makeEdge('gate', 'value', 'b', 'in'),
		]);
		expect(errors).toHaveLength(0);
		// T should have been resolved to String
		expect(nodes[1].inputs[0].portType).toBe('String');
		expect(nodes[1].outputs[0].portType).toBe('String');
	});

	it('TypeVar conflict detected', () => {
		const errors = runValidation([
			makeNode('a', 'Node', [], [sp('out', 'String')]),
			makeNode('gate', 'Gate', [sp('value', 'T')], [sp('value', 'T')]),
			makeNode('b', 'Node', [sp('in', 'Number')], []),
		], [
			makeEdge('a', 'out', 'gate', 'value'),
			makeEdge('gate', 'value', 'b', 'in'),
		]);
		expect(errors.some(e => e.message.includes('conflicting'))).toBe(true);
	});
});

// ── Group passthrough __inner handle tests ──────────────────────────────────

describe('group passthrough type validation', () => {
	it('group gather output: inner String -> gather List[String] valid', () => {
		// Full pipeline: src(List[String]) → expand → worker(String) → gather(List[String])
		// Tests that the inner edge worker → grp.results__inner is type compatible
		const src = makeNode('src', 'Node', [], [sp('out', 'List[String]')]);
		const grp = makeNode('grp', 'Group',
			[ep('items', 'String')],
			[gp('results', 'List[String]')],
		);
		const worker = makeNode('worker', 'Node', [sp('in', 'String')], [sp('out', 'String')]);
		const errors = runValidation(
			[src, grp, worker],
			[
				makeEdge('src', 'out', 'grp', 'items'),
				makeEdge('grp', 'items__inner', 'worker', 'in'),
				makeEdge('worker', 'out', 'grp', 'results__inner'),
			],
		);
		expect(errors).toHaveLength(0);
	});

	it('group gather output: inner Number -> gather List[String] fails', () => {
		// Same pipeline but worker outputs Number instead of String
		// The inner edge Number → String (pre-gather) should fail
		const src = makeNode('src', 'Node', [], [sp('out', 'List[String]')]);
		const grp = makeNode('grp', 'Group',
			[ep('items', 'String')],
			[gp('results', 'List[String]')],
		);
		const worker = makeNode('worker', 'Node', [sp('in', 'String')], [sp('out', 'Number')]);
		const errors = runValidation(
			[src, grp, worker],
			[
				makeEdge('src', 'out', 'grp', 'items'),
				makeEdge('grp', 'items__inner', 'worker', 'in'),
				makeEdge('worker', 'out', 'grp', 'results__inner'),
			],
		);
		expect(errors.some(e => e.message.includes('Type mismatch'))).toBe(true);
	});

	it('group expand output: inner List[String] -> expand String valid', () => {
		// Inner node produces List[String], output port expands to String per lane
		const grp = makeNode('grp', 'Group', [], [ep('results', 'String')]);
		const worker = makeNode('worker', 'Node', [], [sp('out', 'List[String]')]);
		const errors = runValidation(
			[worker, grp],
			[makeEdge('worker', 'out', 'grp', 'results__inner')],
		);
		expect(errors).toHaveLength(0);
	});

	it('group expand output: inner String (non-list) -> expand String fails', () => {
		// Inner node produces String (not a list), can't expand
		const grp = makeNode('grp', 'Group', [], [ep('results', 'String')]);
		const worker = makeNode('worker', 'Node', [], [sp('out', 'String')]);
		const errors = runValidation(
			[worker, grp],
			[makeEdge('worker', 'out', 'grp', 'results__inner')],
		);
		expect(errors.length).toBeGreaterThan(0);
	});

	it('group expand input: in.items -> inner worker valid', () => {
		// Group input expand String, inner worker receives String
		const grp = makeNode('grp', 'Group', [ep('items', 'String')], []);
		const worker = makeNode('worker', 'Node', [sp('in', 'String')], []);
		const errors = runValidation(
			[grp, worker],
			[makeEdge('grp', 'items__inner', 'worker', 'in')],
		);
		expect(errors).toHaveLength(0);
	});

	it('group expand input: in.items -> inner worker wrong type fails', () => {
		// Group input expand String, but inner worker expects Number
		const grp = makeNode('grp', 'Group', [ep('items', 'String')], []);
		const worker = makeNode('worker', 'Node', [sp('in', 'Number')], []);
		const errors = runValidation(
			[grp, worker],
			[makeEdge('grp', 'items__inner', 'worker', 'in')],
		);
		expect(errors.length).toBeGreaterThan(0);
	});

	it('full group expand-gather pipeline valid', () => {
		// src(List[String]) -> grp.items(expand String) -> worker(String->String) -> grp.results(gather List[String]) -> debug(List[String])
		const src = makeNode('src', 'List', [], [sp('value', 'List[String]')]);
		const grp = makeNode('grp', 'Group',
			[ep('items', 'String')],
			[gp('results', 'List[String]')],
		);
		const worker = makeNode('worker', 'Node', [sp('in', 'String')], [sp('out', 'String')]);
		const debug = makeNode('debug', 'Debug', [sp('data', 'List[String]')], []);

		const errors = runValidation(
			[src, grp, worker, debug],
			[
				makeEdge('src', 'value', 'grp', 'items'),         // external -> group expand input
				makeEdge('grp', 'items__inner', 'worker', 'in'),   // group internal -> worker
				makeEdge('worker', 'out', 'grp', 'results__inner'), // worker -> group gather output
				makeEdge('grp', 'results', 'debug', 'data'),       // group external -> debug
			],
		);
		expect(errors).toHaveLength(0);
	});

	it('full group: inner type mismatch caught', () => {
		// Same as above but worker outputs Number instead of String
		const src = makeNode('src', 'List', [], [sp('value', 'List[String]')]);
		const grp = makeNode('grp', 'Group',
			[ep('items', 'String')],
			[gp('results', 'List[String]')],
		);
		const worker = makeNode('worker', 'Node', [sp('in', 'String')], [sp('out', 'Number')]);
		const debug = makeNode('debug', 'Debug', [sp('data', 'List[String]')], []);

		const errors = runValidation(
			[src, grp, worker, debug],
			[
				makeEdge('src', 'value', 'grp', 'items'),
				makeEdge('grp', 'items__inner', 'worker', 'in'),
				makeEdge('worker', 'out', 'grp', 'results__inner'), // Number -> String mismatch
				makeEdge('grp', 'results', 'debug', 'data'),
			],
		);
		expect(errors.length).toBeGreaterThan(0);
		expect(errors.some(e => e.message.includes('Type mismatch'))).toBe(true);
	});

	it('full group: external type mismatch caught', () => {
		// Source sends List[Number] but group expects expand(String) -> wire List[String]
		const src = makeNode('src', 'List', [], [sp('value', 'List[Number]')]);
		const grp = makeNode('grp', 'Group',
			[ep('items', 'String')],
			[gp('results', 'List[String]')],
		);

		const errors = runValidation(
			[src, grp],
			[makeEdge('src', 'value', 'grp', 'items')],
		);
		expect(errors.length).toBeGreaterThan(0);
	});

	it('group with complex types: Dict through expand-gather', () => {
		const dictType = 'Dict[String, Number]';
		const src = makeNode('src', 'List', [], [sp('value', `List[${dictType}]`)]);
		const grp = makeNode('grp', 'Group',
			[ep('items', dictType)],
			[gp('results', `List[${dictType}]`)],
		);
		const worker = makeNode('worker', 'Node',
			[sp('in', dictType)],
			[sp('out', dictType)],
		);

		const errors = runValidation(
			[src, grp, worker],
			[
				makeEdge('src', 'value', 'grp', 'items'),
				makeEdge('grp', 'items__inner', 'worker', 'in'),
				makeEdge('worker', 'out', 'grp', 'results__inner'),
			],
		);
		expect(errors).toHaveLength(0);
	});

	it('group single in/out: type preserved', () => {
		const grp = makeNode('grp', 'Group',
			[sp('data', 'String')],
			[sp('result', 'Number')],
		);
		const worker = makeNode('worker', 'Node', [sp('in', 'String')], [sp('out', 'Number')]);

		const errors = runValidation(
			[grp, worker],
			[
				makeEdge('grp', 'data__inner', 'worker', 'in'),
				makeEdge('worker', 'out', 'grp', 'result__inner'),
			],
		);
		expect(errors).toHaveLength(0);
	});

	it('group single: wrong type from inner caught', () => {
		const grp = makeNode('grp', 'Group',
			[sp('data', 'String')],
			[sp('result', 'Number')],
		);
		// Worker outputs String but group output expects Number
		const worker = makeNode('worker', 'Node', [sp('in', 'String')], [sp('out', 'String')]);

		const errors = runValidation(
			[grp, worker],
			[
				makeEdge('grp', 'data__inner', 'worker', 'in'),
				makeEdge('worker', 'out', 'grp', 'result__inner'),
			],
		);
		expect(errors.length).toBeGreaterThan(0);
	});
});

// ── Type override / narrowing tests ─────────────────────────────────────────

describe('type override compatibility (narrowing)', () => {
	// These test the isWeftTypeCompatible function which is used by both
	// the backend merge_ports and frontend buildNodeInstance for override checks.
	// Rule: weft type must fit INTO catalog type (compatible subset = valid narrowing).

	it('exact same type: compatible', () => {
		expect(isWeftTypeCompatible('String', 'String')).toBe(true);
	});

	it('narrowing union to single: compatible', () => {
		// Catalog: String | Number, AI writes: String. String fits into String|Number.
		expect(isWeftTypeCompatible('String', 'String | Number')).toBe(true);
	});

	it('narrowing union to smaller union: compatible', () => {
		// Catalog: String | Number | Boolean, AI writes: String | Number
		expect(isWeftTypeCompatible('String | Number', 'String | Number | Boolean')).toBe(true);
	});

	it('widening single to union: incompatible', () => {
		// Catalog: String, AI writes: String | Number. Number not in catalog.
		expect(isWeftTypeCompatible('String | Number', 'String')).toBe(false);
	});

	it('partial overlap: incompatible', () => {
		// Catalog: String | Number, AI writes: String | Boolean. Boolean not in catalog.
		expect(isWeftTypeCompatible('String | Boolean', 'String | Number')).toBe(false);
	});

	it('completely different type: incompatible', () => {
		expect(isWeftTypeCompatible('Number', 'String')).toBe(false);
	});

	it('same list type: compatible', () => {
		expect(isWeftTypeCompatible('List[String]', 'List[String]')).toBe(true);
	});

	it('narrowing list inner: compatible (List is covariant)', () => {
		// List[String] fits into List[String | Number] because List checks inner compatibility.
		// A list of strings is a valid list of (strings or numbers).
		expect(isWeftTypeCompatible('List[String]', 'List[String | Number]')).toBe(true);
	});

	it('widening list inner: incompatible', () => {
		// List[String | Number] does NOT fit into List[String], Number elements wouldn't be handled.
		expect(isWeftTypeCompatible('List[String | Number]', 'List[String]')).toBe(false);
	});

	it('different list inner: incompatible', () => {
		expect(isWeftTypeCompatible('List[Number]', 'List[String]')).toBe(false);
	});

	it('same dict type: compatible', () => {
		expect(isWeftTypeCompatible('Dict[String, Number]', 'Dict[String, Number]')).toBe(true);
	});

	it('different dict value: incompatible', () => {
		expect(isWeftTypeCompatible('Dict[String, String]', 'Dict[String, Number]')).toBe(false);
	});

	it('TypeVar always compatible (unresolved)', () => {
		expect(isWeftTypeCompatible('T', 'String')).toBe(true);
		expect(isWeftTypeCompatible('String', 'T')).toBe(true);
	});

	it('MustOverride always compatible (unresolved)', () => {
		expect(isWeftTypeCompatible('MustOverride', 'String')).toBe(true);
		expect(isWeftTypeCompatible('String', 'MustOverride')).toBe(true);
	});
});

describe('deeply nested type compatibility', () => {
	it('List[String] → List[String|Number]: covariant OK', () => {
		expect(isWeftTypeCompatible('List[String]', 'List[String | Number]')).toBe(true);
	});

	it('List[String|Number] → List[String]: fails', () => {
		expect(isWeftTypeCompatible('List[String | Number]', 'List[String]')).toBe(false);
	});

	it('Dict[String, String] → Dict[String, String|Number]: covariant OK', () => {
		expect(isWeftTypeCompatible('Dict[String, String]', 'Dict[String, String | Number]')).toBe(true);
	});

	it('Dict[String, String|Number] → Dict[String, String]: fails', () => {
		expect(isWeftTypeCompatible('Dict[String, String | Number]', 'Dict[String, String]')).toBe(false);
	});

	it('List[List[String]] → List[List[String|Number]]: nested covariant OK', () => {
		expect(isWeftTypeCompatible('List[List[String]]', 'List[List[String | Number]]')).toBe(true);
	});

	it('List[List[String|Number]] → List[List[String]]: nested fails', () => {
		expect(isWeftTypeCompatible('List[List[String | Number]]', 'List[List[String]]')).toBe(false);
	});

	it('List[Dict[String, String|Number]] → List[Dict[String, String|Number|Boolean]]: OK', () => {
		expect(isWeftTypeCompatible(
			'List[Dict[String, String | Number]]',
			'List[Dict[String, String | Number | Boolean]]',
		)).toBe(true);
	});

	it('List[Dict[String, String|Number|Boolean]] → List[Dict[String, String|Number]]: fails', () => {
		expect(isWeftTypeCompatible(
			'List[Dict[String, String | Number | Boolean]]',
			'List[Dict[String, String | Number]]',
		)).toBe(false);
	});

	it('Dict[String, Dict[String, String|Number]] → Dict[String, Dict[String, String|Boolean]]: overlap fails', () => {
		// Inner: String|Number → String|Boolean. Number not in String|Boolean.
		expect(isWeftTypeCompatible(
			'Dict[String, Dict[String, String | Number]]',
			'Dict[String, Dict[String, String | Boolean]]',
		)).toBe(false);
	});

	it('reverse overlap also fails', () => {
		expect(isWeftTypeCompatible(
			'Dict[String, Dict[String, String | Boolean]]',
			'Dict[String, Dict[String, String | Number]]',
		)).toBe(false);
	});

	it('Dict[String, Dict[String, String]] → Dict[String, Dict[String, String|Number|Boolean]]: OK', () => {
		expect(isWeftTypeCompatible(
			'Dict[String, Dict[String, String]]',
			'Dict[String, Dict[String, String | Number | Boolean]]',
		)).toBe(true);
	});

	it('reverse fails', () => {
		expect(isWeftTypeCompatible(
			'Dict[String, Dict[String, String | Number | Boolean]]',
			'Dict[String, Dict[String, String]]',
		)).toBe(false);
	});

	it('List[List[Dict[String, String]]] → List[List[Dict[String, String|Number]]]: triple nested OK', () => {
		expect(isWeftTypeCompatible(
			'List[List[Dict[String, String]]]',
			'List[List[Dict[String, String | Number]]]',
		)).toBe(true);
	});

	it('triple nested reverse fails', () => {
		expect(isWeftTypeCompatible(
			'List[List[Dict[String, String | Number]]]',
			'List[List[Dict[String, String]]]',
		)).toBe(false);
	});
});

describe('type narrowing affects downstream validation', () => {
	it('narrowed type String from String|Number rejects Number downstream', () => {
		// If AI narrows a port from String|Number to String,
		// downstream nodes expecting Number should fail.
		const errors = runValidation([
			makeNode('a', 'Node', [], [sp('out', 'String')]),  // narrowed type
			makeNode('b', 'Node', [sp('in', 'Number')], []),
		], [makeEdge('a', 'out', 'b', 'in')]);
		expect(errors.some(e => e.message.includes('Type mismatch'))).toBe(true);
	});

	it('narrowed type String from String|Number passes String downstream', () => {
		const errors = runValidation([
			makeNode('a', 'Node', [], [sp('out', 'String')]),  // narrowed type
			makeNode('b', 'Node', [sp('in', 'String')], []),
		], [makeEdge('a', 'out', 'b', 'in')]);
		expect(errors).toHaveLength(0);
	});

	it('narrowed type propagates through TypeVar', () => {
		// A outputs String (narrowed from String|Number), gate has T, downstream expects String
		const nodes = [
			makeNode('a', 'Node', [], [sp('out', 'String')]),
			makeNode('gate', 'Gate', [sp('value', 'T')], [sp('value', 'T')]),
			makeNode('b', 'Node', [sp('in', 'String')], []),
		];
		const errors = runValidation(nodes, [
			makeEdge('a', 'out', 'gate', 'value'),
			makeEdge('gate', 'value', 'b', 'in'),
		]);
		expect(errors).toHaveLength(0);
		expect(nodes[1].outputs[0].portType).toBe('String');
	});

	it('narrowed type through TypeVar catches mismatch', () => {
		// A outputs String (narrowed), gate has T, downstream expects Number
		const nodes = [
			makeNode('a', 'Node', [], [sp('out', 'String')]),
			makeNode('gate', 'Gate', [sp('value', 'T')], [sp('value', 'T')]),
			makeNode('b', 'Node', [sp('in', 'Number')], []),
		];
		const errors = runValidation(nodes, [
			makeEdge('a', 'out', 'gate', 'value'),
			makeEdge('gate', 'value', 'b', 'in'),
		]);
		// T resolves to String from A, then String → Number fails
		expect(errors.length).toBeGreaterThan(0);
	});
});
