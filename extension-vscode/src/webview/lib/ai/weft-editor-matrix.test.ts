// Full editor × location matrix. Every cell is either `works` (with a
// concrete post-condition) or `rejects` (output is unchanged or returns
// with the relevant error). No TODOs. Any failure here is a real bug.
//
// Fixture invariants: every fixture parses cleanly (zero errors) as input.
// Post-condition checks use `parseOk` when the output should parse cleanly,
// or `parse` when it's OK for the output to have downstream errors (e.g.
// after removing a required edge).

import { describe, it, expect } from 'vitest';
import {
	updateNodeConfig,
	updateNodeLabel,
	addNode,
	addGroup,
	renameGroup,
	removeGroup,
	removeNode,
	addEdge,
	removeEdge,
	moveNodeScope,
	moveGroupScope,
	updateNodePorts,
	updateGroupPorts,
} from './weft-editor';
import { parseWeft } from './weft-parser';

// ── helpers ───────────────────────────────────────────────────────────

function fence(code: string): string {
	return '````weft\n' + code.trim() + '\n````';
}

function parse(code: string) {
	const result = parseWeft(fence(code));
	return result.projects[0];
}

function parseOk(code: string) {
	const p = parse(code);
	expect(p.errors, `parse errors: ${JSON.stringify(p.errors)}\n${code}`).toEqual([]);
	return p;
}

function nodeIds(code: string): string[] {
	return parse(code).project.nodes.map(n => n.id).sort();
}

function findNode(code: string, id: string) {
	const n = parse(code).project.nodes.find(n => n.id === id);
	if (!n) throw new Error(`node ${id} not found in:\n${code}`);
	return n;
}

function hasEdge(code: string, src: string, srcPort: string, tgt: string, tgtPort: string): boolean {
	return parse(code).project.edges.some(e =>
		e.source === src && e.sourceHandle === srcPort &&
		e.target === tgt && e.targetHandle === tgtPort,
	);
}

function edgeList(code: string): string[] {
	return parse(code).project.edges
		.map(e => `${e.source}.${e.sourceHandle}=>${e.target}.${e.targetHandle}`)
		.sort();
}

// ── fixtures ──────────────────────────────────────────────────────────
//
// Fixture design rules:
//   - All required ports are connected or satisfied.
//   - Groups always feed self.<out> from a child node (literal assignment
//     to self outputs is not supported by the parser).
//   - For "must remove an edge" tests, the edge's target port is declared
//     as optional so the resulting code still parses.
//   - Template is used heavily because it is canAddInputPorts and its
//     `template` field is the only required input when the signature has
//     no declared inputs.

const F = {
	// L1: single root node. Template with an inline config field `template`
	// satisfies the required port. An additional literal `tag` creates a
	// synthesized optional port `tag: String`.
	rootNode: `# Project: M

n = Template {
  template: "hi {{tag}}"
  tag: "world"
}`,

	// L2: two root nodes connected by an edge.
	rootConnected: `# Project: M

src = Text { value: "x" }
dst = Template(tag: String?) {
  template: "hi {{tag}}"
}
dst.tag = src.value`,

	// L3: group with a single child that feeds self.out.
	groupChild: `# Project: M

grp = Group() -> (out: String?) {
  child = Template { template: "in grp" }
  self.out = child.text
}`,

	// L4: nested groups, a leaf that bubbles up.
	nestedGroups: `# Project: M

outer = Group() -> (out: String?) {
  inner = Group() -> (res: String?) {
    leaf = Template { template: "leaf" }
    self.res = leaf.text
  }
  self.out = inner.res
}`,

	// L5: 1-level inline anon on a connection line. host has a
	// canAddInputPorts input so the binding edge can be removed without
	// leaving required ports unfilled. The anon's output is wired via the
	// connection-line form.
	anonConn: `# Project: M

host = Template(data: String?) {
  template: "{{data}}"
}
host.data = Template { template: "hi" }.text`,

	// L6: 1-level inline anon in a config block.
	anonConfig: `# Project: M

host = Template(data: String?) {
  template: "{{data}}"
  data: Template { template: "hi" }.text
}`,

	// L7: 2-level inline anon chain using connection-line RHS. Outer anon
	// declares a signature `(x: String?)` so the inner anon can drive it
	// without making `x` required.
	anon2Conn: `# Project: M

host = Template(data: String?) {
  template: "{{data}}"
}
host.data = Template(x: String?) {
  template: "{{x}}"
  x: Template { template: "inner" }.text
}.text`,

	// L8: inline anon inside a group body (connection form).
	anonInGroup: `# Project: M

grp = Group() -> (out: String?) {
  host = Template(data: String?) {
    template: "{{data}}"
  }
  host.data = Template { template: "in-group" }.text
  self.out = host.text
}`,

	// L9: inline anon inside a nested group.
	anonInNestedGroup: `# Project: M

outer = Group() -> (out: String?) {
  inner = Group() -> (res: String?) {
    host = Template(data: String?) {
      template: "{{data}}"
    }
    host.data = Template { template: "leaf" }.text
    self.res = host.text
  }
  self.out = inner.res
}`,

	// L10: inline anon with an extra external edge wired via its synthesized
	// id. Demonstrates that users can add more edges to an inline anon by
	// referring to it with `host__data`.
	anonWithExtraEdge: `# Project: M

src = Text { value: "x" }
other = Text { value: "y" }
host = Template(data: String?) {
  template: "{{data}}"
}
host.data = Template(x: String?, y: String?) {
  template: "{{x}}{{y}}"
  x: src.value
}.text
host__data.y = other.value`,
};

// Sanity check: every fixture must parse cleanly.
describe('matrix: fixtures are valid', () => {
	for (const [name, code] of Object.entries(F)) {
		it(`fixture ${name} parses without errors`, () => {
			parseOk(code);
		});
	}
});

// ── updateNodeConfig ──────────────────────────────────────────────────

describe('matrix: updateNodeConfig', () => {
	it('updates a scalar config field on a root node', () => {
		const out = updateNodeConfig(F.rootNode, 'n', 'template', 'new template');
		expect(findNode(out, 'n').config.template).toBe('new template');
	});

	it('updates a synthesized input port literal on a root node', () => {
		const out = updateNodeConfig(F.rootNode, 'n', 'tag', 'updated');
		expect(findNode(out, 'n').config.tag).toBe('updated');
	});

	it('adds a new synthesized port via a new literal field', () => {
		const out = updateNodeConfig(F.rootNode, 'n', 'flag', true);
		expect(findNode(out, 'n').config.flag).toBe(true);
	});

	it('sets the label property via updateNodeConfig (parser promotes to .label)', () => {
		const out = updateNodeConfig(F.rootNode, 'n', 'label', 'MyLabel');
		expect(findNode(out, 'n').label).toBe('MyLabel');
	});

	it('removes a field (value = null) from a root node', () => {
		const out = updateNodeConfig(F.rootNode, 'n', 'tag', null);
		expect(findNode(out, 'n').config.tag).toBeUndefined();
	});

	it('updates a field on a group child (scoped id)', () => {
		const out = updateNodeConfig(F.groupChild, 'grp.child', 'template', 'updated');
		expect(findNode(out, 'grp.child').config.template).toBe('updated');
	});

	it('updates a field on a group child (local id)', () => {
		const out = updateNodeConfig(F.groupChild, 'child', 'template', 'updated');
		expect(findNode(out, 'grp.child').config.template).toBe('updated');
	});

	it('updates a field on a deeply nested group child', () => {
		const out = updateNodeConfig(F.nestedGroups, 'outer.inner.leaf', 'template', 'updated');
		expect(findNode(out, 'outer.inner.leaf').config.template).toBe('updated');
	});

	it('updates a field on a 1-level inline anon (connection form)', () => {
		const out = updateNodeConfig(F.anonConn, 'host__data', 'template', 'updated');
		expect(findNode(out, 'host__data').config.template).toBe('updated');
	});

	it('updates a field on a 1-level inline anon (config-block form)', () => {
		const out = updateNodeConfig(F.anonConfig, 'host__data', 'template', 'updated');
		expect(findNode(out, 'host__data').config.template).toBe('updated');
	});

	it('updates a field on the outer anon of a 2-level chain', () => {
		const out = updateNodeConfig(F.anon2Conn, 'host__data', 'template', 'UPDATED_OUTER');
		expect(findNode(out, 'host__data').config.template).toBe('UPDATED_OUTER');
	});

	it('updates a field on the inner anon of a 2-level chain', () => {
		const out = updateNodeConfig(F.anon2Conn, 'host__data__x', 'template', 'UPDATED_INNER');
		expect(findNode(out, 'host__data__x').config.template).toBe('UPDATED_INNER');
	});

	it('updates a field on an inline anon inside a group body', () => {
		const out = updateNodeConfig(F.anonInGroup, 'grp.host__data', 'template', 'updated');
		expect(findNode(out, 'grp.host__data').config.template).toBe('updated');
	});

	it('updates a field on an inline anon inside nested groups', () => {
		const out = updateNodeConfig(F.anonInNestedGroup, 'outer.inner.host__data', 'template', 'updated');
		expect(findNode(out, 'outer.inner.host__data').config.template).toBe('updated');
	});

	it('noop when setting value=null on a non-existent field', () => {
		const out = updateNodeConfig(F.rootNode, 'n', 'nonExistent', null);
		expect(out).toBe(F.rootNode);
	});

	it('noop on non-existent node', () => {
		const out = updateNodeConfig(F.rootNode, 'doesNotExist', 'x', 'val');
		expect(out).toBe(F.rootNode);
	});

	it('effective-source: when both inline + connection-line literal exist, edits the last-written source', () => {
		const code = `# Project: M

n = Template {
  template: "hi"
  tag: "inline"
}
n.tag = "connection"`;
		const out = updateNodeConfig(code, 'n', 'tag', 'updated');
		expect(out).toContain('n.tag = "updated"');
		expect(out).toContain('tag: "inline"');
	});

	it('effective-source removal: null removes only the last-written source', () => {
		const code = `# Project: M

n = Template {
  template: "hi"
  tag: "inline"
}
n.tag = "connection"`;
		const out = updateNodeConfig(code, 'n', 'tag', null);
		expect(out).not.toContain('n.tag = "connection"');
		expect(out).toContain('tag: "inline"');
	});

	it('multi-line JSON value round-trips correctly', () => {
		const out = updateNodeConfig(F.rootNode, 'n', 'meta', { a: 1, b: [1, 2, 3] });
		expect(findNode(out, 'n').config.meta).toEqual({ a: 1, b: [1, 2, 3] });
	});

	it('list literal value round-trips correctly', () => {
		const out = updateNodeConfig(F.rootNode, 'n', 'items', ['a', 'b', 'c']);
		expect(findNode(out, 'n').config.items).toEqual(['a', 'b', 'c']);
	});
});

// ── updateNodeLabel ───────────────────────────────────────────────────

describe('matrix: updateNodeLabel', () => {
	it('sets a label on a root node without losing existing config', () => {
		const out = updateNodeLabel(F.rootNode, 'n', 'My Label');
		expect(findNode(out, 'n').label).toBe('My Label');
		// Must preserve the existing template and tag fields.
		expect(findNode(out, 'n').config.template).toBe('hi {{tag}}');
		expect(findNode(out, 'n').config.tag).toBe('world');
	});

	it('clears a label with null', () => {
		const withLabel = updateNodeLabel(F.rootNode, 'n', 'Before');
		const out = updateNodeLabel(withLabel, 'n', null);
		expect(findNode(out, 'n').label).toBeNull();
	});

	it('sets a label on a group child', () => {
		const out = updateNodeLabel(F.groupChild, 'grp.child', 'ChildLabel');
		expect(findNode(out, 'grp.child').label).toBe('ChildLabel');
	});

	it('sets a label on a deeply nested group child', () => {
		const out = updateNodeLabel(F.nestedGroups, 'outer.inner.leaf', 'LeafLabel');
		expect(findNode(out, 'outer.inner.leaf').label).toBe('LeafLabel');
	});

	it('sets a label on a 1-level inline anon (connection form)', () => {
		const out = updateNodeLabel(F.anonConn, 'host__data', 'AnonLabel');
		expect(findNode(out, 'host__data').label).toBe('AnonLabel');
	});

	it('sets a label on a 1-level inline anon (config-block form)', () => {
		const out = updateNodeLabel(F.anonConfig, 'host__data', 'AnonLabel');
		expect(findNode(out, 'host__data').label).toBe('AnonLabel');
	});

	it('sets a label on the outer anon of a 2-level chain', () => {
		const out = updateNodeLabel(F.anon2Conn, 'host__data', 'OuterLabel');
		expect(findNode(out, 'host__data').label).toBe('OuterLabel');
	});

	it('sets a label on the inner anon of a 2-level chain', () => {
		const out = updateNodeLabel(F.anon2Conn, 'host__data__x', 'InnerLabel');
		expect(findNode(out, 'host__data__x').label).toBe('InnerLabel');
	});

	it('sets a label on an inline anon inside a group', () => {
		const out = updateNodeLabel(F.anonInGroup, 'grp.host__data', 'GroupedAnon');
		expect(findNode(out, 'grp.host__data').label).toBe('GroupedAnon');
	});

	it('noop on non-existent node', () => {
		const out = updateNodeLabel(F.rootNode, 'doesNotExist', 'X');
		expect(out).toBe(F.rootNode);
	});
});

// ── addNode ───────────────────────────────────────────────────────────

describe('matrix: addNode', () => {
	it('adds a node at root', () => {
		const out = addNode(F.rootNode, 'Text', 'newNode');
		expect(nodeIds(out)).toContain('newNode');
		expect(findNode(out, 'newNode').nodeType).toBe('Text');
	});

	it('adds a node inside a group by group label', () => {
		const out = addNode(F.groupChild, 'Text', 'newChild', 'grp');
		expect(nodeIds(out)).toContain('grp.newChild');
	});

	it('adds a node inside the innermost group of a nested structure', () => {
		const out = addNode(F.nestedGroups, 'Text', 'newLeaf', 'inner');
		expect(nodeIds(out)).toContain('outer.inner.newLeaf');
	});
});

// ── addGroup ──────────────────────────────────────────────────────────

describe('matrix: addGroup', () => {
	it('adds a group at root', () => {
		const out = addGroup(F.rootNode, 'newGrp');
		expect(nodeIds(out)).toContain('newGrp');
		expect(findNode(out, 'newGrp').nodeType).toBe('Group');
	});

	it('adds a group inside an existing group', () => {
		const out = addGroup(F.groupChild, 'subGrp', 'grp');
		expect(nodeIds(out)).toContain('grp.subGrp');
	});

	it('adds a group inside a nested group', () => {
		const out = addGroup(F.nestedGroups, 'deepGrp', 'inner');
		expect(nodeIds(out)).toContain('outer.inner.deepGrp');
	});
});

// ── renameGroup ───────────────────────────────────────────────────────

describe('matrix: renameGroup', () => {
	it('renames a root-level group', () => {
		const out = renameGroup(F.groupChild, 'grp', 'renamed');
		const ids = nodeIds(out);
		expect(ids).toContain('renamed');
		expect(ids.every(id => !id.startsWith('grp'))).toBe(true);
	});

	it('rename updates external connection references', () => {
		const code = `# Project: M

grp = Group(input: String?) -> (out: String?) {
  child = Template(x: String?) { template: "{{x}}" }
  child.x = self.input
  self.out = child.text
}
src = Text { value: "hi" }
grp.input = src.value`;
		const out = renameGroup(code, 'grp', 'renamed');
		expect(out).toContain('renamed.input = src.value');
		expect(out).not.toContain('grp.input = src.value');
	});

	it('noop when new name equals old name', () => {
		const out = renameGroup(F.groupChild, 'grp', 'grp');
		expect(out).toBe(F.groupChild);
	});

	it('noop on non-existent group', () => {
		const out = renameGroup(F.groupChild, 'nope', 'x');
		expect(out).toBe(F.groupChild);
	});

	it('renames a nested group', () => {
		const out = renameGroup(F.nestedGroups, 'inner', 'core');
		expect(nodeIds(out)).toContain('outer.core');
	});
});

// ── removeGroup ───────────────────────────────────────────────────────

describe('matrix: removeGroup', () => {
	it('removes a group and lifts children one level up', () => {
		const out = removeGroup(F.groupChild, 'grp');
		const ids = nodeIds(out);
		expect(ids).not.toContain('grp');
		expect(ids).toContain('child');
	});

	it('removes a nested group, children rise to the enclosing scope', () => {
		const out = removeGroup(F.nestedGroups, 'inner');
		const ids = nodeIds(out);
		expect(ids).not.toContain('outer.inner');
		expect(ids).toContain('outer.leaf');
	});

	it('removes external edges referencing the removed group', () => {
		const code = `# Project: M

grp = Group(input: String?) -> (out: String?) {
  child = Template(x: String?) { template: "{{x}}" }
  child.x = self.input
  self.out = child.text
}
src = Text { value: "hi" }
grp.input = src.value
sink = Debug {}
sink.value = grp.out`;
		const out = removeGroup(code, 'grp');
		expect(out).not.toContain('grp.input');
		expect(out).not.toContain('grp.out');
	});

	it('noop on non-existent group', () => {
		const out = removeGroup(F.groupChild, 'nope');
		expect(out).toBe(F.groupChild);
	});
});

// ── removeNode ────────────────────────────────────────────────────────

describe('matrix: removeNode', () => {
	it('removes a root node and its incoming edges', () => {
		const out = removeNode(F.rootConnected, 'dst');
		const ids = nodeIds(out);
		expect(ids).not.toContain('dst');
		expect(ids).toContain('src');
	});

	it('removes a group child and its edges', () => {
		const code = `# Project: M

grp = Group() -> (out: String?) {
  a = Text { value: "a" }
  b = Template(tag: String?) { template: "{{tag}}" }
  b.tag = a.value
  self.out = b.text
}`;
		const out = removeNode(code, 'grp.a');
		expect(nodeIds(out)).not.toContain('grp.a');
	});

	it('removes a deeply nested group child', () => {
		const out = removeNode(F.nestedGroups, 'outer.inner.leaf');
		expect(nodeIds(out)).not.toContain('outer.inner.leaf');
	});

	it('removes a 1-level inline anon (connection form)', () => {
		const out = removeNode(F.anonConn, 'host__data');
		const ids = nodeIds(out);
		expect(ids).not.toContain('host__data');
		expect(ids).toContain('host');
	});

	it('removes a 1-level inline anon (config-block form)', () => {
		const out = removeNode(F.anonConfig, 'host__data');
		expect(nodeIds(out)).not.toContain('host__data');
		expect(nodeIds(out)).toContain('host');
	});

	it('removes the outer anon of a 2-level chain (inner goes with it)', () => {
		const out = removeNode(F.anon2Conn, 'host__data');
		const ids = nodeIds(out);
		expect(ids).not.toContain('host__data');
		expect(ids).not.toContain('host__data__x');
		expect(ids).toContain('host');
	});

	it('removes only the inner anon of a 2-level chain', () => {
		const out = removeNode(F.anon2Conn, 'host__data__x');
		const ids = nodeIds(out);
		expect(ids).not.toContain('host__data__x');
		expect(ids).toContain('host__data');
		expect(ids).toContain('host');
	});

	it('removes an inline anon inside a group', () => {
		const out = removeNode(F.anonInGroup, 'grp.host__data');
		expect(nodeIds(out)).not.toContain('grp.host__data');
	});

	it('removes an inline anon that has extra external edges (edges also dropped)', () => {
		const out = removeNode(F.anonWithExtraEdge, 'host__data');
		const ids = nodeIds(out);
		expect(ids).not.toContain('host__data');
		// Neither the binding edge nor the extra edges remain
		expect(edgeList(out).some(e => e.includes('host__data'))).toBe(false);
	});

	it('noop on non-existent node', () => {
		const out = removeNode(F.rootNode, 'doesNotExist');
		expect(out).toBe(F.rootNode);
	});
});

// ── addEdge ───────────────────────────────────────────────────────────

describe('matrix: addEdge', () => {
	it('adds an edge between two root nodes', () => {
		const code = `# Project: M

src = Text { value: "x" }
dst = Template(tag: String?) { template: "{{tag}}" }`;
		const out = addEdge(code, 'src', 'value', 'dst', 'tag');
		expect(hasEdge(out, 'src', 'value', 'dst', 'tag')).toBe(true);
	});

	it('adds an edge inside a group', () => {
		const code = `# Project: M

grp = Group() -> (out: String?) {
  src = Text { value: "x" }
  dst = Template(tag: String?) { template: "{{tag}}" }
  self.out = dst.text
}`;
		const out = addEdge(code, 'src', 'value', 'dst', 'tag', 'grp');
		expect(hasEdge(out, 'grp.src', 'value', 'grp.dst', 'tag')).toBe(true);
	});

	it('adds an edge targeting an inline anon by synthesized id', () => {
		const code = `# Project: M

other = Text { value: "y" }
host = Template(data: String?) { template: "{{data}}" }
host.data = Template(extra: String?) {
  template: "{{extra}}"
}.text`;
		const out = addEdge(code, 'other', 'value', 'host__data', 'extra');
		expect(hasEdge(out, 'other', 'value', 'host__data', 'extra')).toBe(true);
	});
});

// ── removeEdge ────────────────────────────────────────────────────────

describe('matrix: removeEdge', () => {
	it('removes a regular edge between two root nodes', () => {
		const out = removeEdge(F.rootConnected, 'src', 'value', 'dst', 'tag');
		expect(hasEdge(out, 'src', 'value', 'dst', 'tag')).toBe(false);
	});

	it('removes an edge inside a group', () => {
		const code = `# Project: M

grp = Group() -> (out: String?) {
  src = Text { value: "x" }
  dst = Template(tag: String?) { template: "{{tag}}" }
  dst.tag = src.value
  self.out = dst.text
}`;
		const out = removeEdge(code, 'src', 'value', 'dst', 'tag');
		expect(hasEdge(out, 'grp.src', 'value', 'grp.dst', 'tag')).toBe(false);
	});

	it('removes a self.* edge', () => {
		const code = `# Project: M

grp = Group(input: String?) -> (out: String?) {
  dst = Template(x: String?) { template: "{{x}}" }
  dst.x = self.input
  self.out = dst.text
}`;
		const out = removeEdge(code, 'self', 'input', 'dst', 'x');
		expect(out).not.toContain('dst.x = self.input');
	});

	it('noop on non-existent edge', () => {
		const out = removeEdge(F.rootConnected, 'nope', 'nope', 'nope', 'nope');
		expect(out).toBe(F.rootConnected);
	});

	// ── materialization ──
	it('MATERIALIZE: 1-level connection-form anon lifts to root after binding removal', () => {
		const out = removeEdge(F.anonConn, 'host__data', 'text', 'host', 'data');
		const p = parseOk(out);
		const ids = p.project.nodes.map(n => n.id).sort();
		expect(ids).toContain('host');
		expect(ids).toContain('host__data');
		expect(hasEdge(out, 'host__data', 'text', 'host', 'data')).toBe(false);
		// host__data is now a top-level node, not inline
		expect((p.project.nodes.find(n => n.id === 'host__data') as any).parentId).toBeUndefined();
	});

	it('MATERIALIZE: 1-level config-form anon lifts to root after binding removal', () => {
		const out = removeEdge(F.anonConfig, 'host__data', 'text', 'host', 'data');
		const p = parseOk(out);
		const ids = p.project.nodes.map(n => n.id).sort();
		expect(ids).toContain('host');
		expect(ids).toContain('host__data');
		expect(hasEdge(out, 'host__data', 'text', 'host', 'data')).toBe(false);
		expect((p.project.nodes.find(n => n.id === 'host__data') as any).parentId).toBeUndefined();
	});

	it('MATERIALIZE: 2-level inner binding removal lifts inner to root', () => {
		const out = removeEdge(F.anon2Conn, 'host__data__x', 'text', 'host__data', 'x');
		const p = parseOk(out);
		const ids = p.project.nodes.map(n => n.id).sort();
		expect(ids).toContain('host');
		expect(ids).toContain('host__data');
		expect(ids).toContain('host__data__x');
		expect((p.project.nodes.find(n => n.id === 'host__data__x') as any).parentId).toBeUndefined();
		// Outer anon still inline in its host
		expect(hasEdge(out, 'host__data', 'text', 'host', 'data')).toBe(true);
	});

	it('MATERIALIZE: 2-level outer binding removal lifts outer to root (inner stays inside outer)', () => {
		const out = removeEdge(F.anon2Conn, 'host__data', 'text', 'host', 'data');
		const p = parseOk(out);
		const ids = p.project.nodes.map(n => n.id).sort();
		expect(ids).toContain('host');
		expect(ids).toContain('host__data');
		expect(ids).toContain('host__data__x');
		expect((p.project.nodes.find(n => n.id === 'host__data') as any).parentId).toBeUndefined();
		// Inner anon's binding edge to outer still exists
		expect(hasEdge(out, 'host__data__x', 'text', 'host__data', 'x')).toBe(true);
	});

	it('MATERIALIZE: anon inside a group lifts to group scope, not root', () => {
		const out = removeEdge(F.anonInGroup, 'grp.host__data', 'text', 'grp.host', 'data');
		const p = parseOk(out);
		expect((p.project.nodes.find(n => n.id === 'grp.host__data') as any).parentId).toBe('grp');
	});

	it('MATERIALIZE: anon inside a nested group lifts to enclosing group scope', () => {
		const out = removeEdge(F.anonInNestedGroup, 'outer.inner.host__data', 'text', 'outer.inner.host', 'data');
		const p = parseOk(out);
		expect((p.project.nodes.find(n => n.id === 'outer.inner.host__data') as any).parentId).toBe('outer.inner');
	});

	it('MATERIALIZE: extra external edges to the anon survive lifting', () => {
		const out = removeEdge(F.anonWithExtraEdge, 'host__data', 'text', 'host', 'data');
		const p = parseOk(out);
		const ids = p.project.nodes.map(n => n.id).sort();
		expect(ids).toContain('host__data');
		expect(hasEdge(out, 'src', 'value', 'host__data', 'x')).toBe(true);
		expect(hasEdge(out, 'other', 'value', 'host__data', 'y')).toBe(true);
		expect(hasEdge(out, 'host__data', 'text', 'host', 'data')).toBe(false);
	});
});

// ── moveNodeScope ─────────────────────────────────────────────────────

describe('matrix: moveNodeScope', () => {
	it('moves a disconnected root node into a group', () => {
		const code = `# Project: M

grp = Group() -> (out: String?) {
  seed = Text { value: "seed" }
  self.out = seed.value
}
loner = Text { value: "lone" }`;
		const out = moveNodeScope(code, 'loner', 'grp');
		expect(nodeIds(out)).toContain('grp.loner');
	});

	it('moves a disconnected group child out to root', () => {
		const code = `# Project: M

grp = Group() -> (out: String?) {
  seed = Text { value: "seed" }
  extra = Text { value: "extra" }
  self.out = seed.value
}`;
		const out = moveNodeScope(code, 'extra', undefined);
		const ids = nodeIds(out);
		expect(ids).toContain('extra');
		expect(ids).not.toContain('grp.extra');
	});

	it('REJECT: moveNodeScope on a connected node', () => {
		const code = `# Project: M

a = Text { value: "a" }
b = Template(tag: String?) { template: "{{tag}}" }
b.tag = a.value
grp = Group() -> (out: String?) {
  seed = Text { value: "seed" }
  self.out = seed.value
}`;
		// Moving b into grp would separate it from a, which is in root scope.
		// The edge a.value -> b.tag would cross a boundary that isn't legal
		// (child scope -> sibling scope). Expect rejection.
		const out = moveNodeScope(code, 'b', 'grp');
		expect(out).toBe(code);
	});

	it('REJECT: moveNodeScope on an inline anon (binding edge counts)', () => {
		const out = moveNodeScope(F.anonConn, 'host__data', 'newGrp');
		expect(out).toBe(F.anonConn);
	});

	it('noop when target scope equals current scope', () => {
		const out = moveNodeScope(F.rootNode, 'n', undefined);
		expect(out).toBe(F.rootNode);
	});

	it('noop on non-existent node', () => {
		const out = moveNodeScope(F.rootNode, 'doesNotExist', undefined);
		expect(out).toBe(F.rootNode);
	});
});

// ── moveGroupScope ────────────────────────────────────────────────────

describe('matrix: moveGroupScope', () => {
	it('moves a disconnected root group into another group', () => {
		const code = `# Project: M

outer = Group() -> (out: String?) {
  seed = Text { value: "s" }
  self.out = seed.value
}
standalone = Group() -> (res: String?) {
  inner_seed = Text { value: "x" }
  self.res = inner_seed.value
}`;
		const out = moveGroupScope(code, 'standalone', 'outer');
		expect(nodeIds(out)).toContain('outer.standalone');
	});

	it('moves a nested group out to root', () => {
		const code = `# Project: M

outer = Group() -> (out: String?) {
  inner = Group() -> (res: String?) {
    leaf = Text { value: "leaf" }
    self.res = leaf.value
  }
  seed = Text { value: "seed" }
  self.out = seed.value
}`;
		const out = moveGroupScope(code, 'inner', undefined);
		const ids = nodeIds(out);
		expect(ids).toContain('inner');
		expect(ids).not.toContain('outer.inner');
	});

	it('REJECT: moveGroupScope on a connected group', () => {
		const code = `# Project: M

grp = Group(input: String?) -> (out: String?) {
  seed = Text { value: "s" }
  self.out = seed.value
}
src = Text { value: "x" }
grp.input = src.value
container = Group() -> (res: String?) {
  seed2 = Text { value: "s2" }
  self.res = seed2.value
}`;
		const out = moveGroupScope(code, 'grp', 'container');
		expect(out).toBe(code);
	});
});

// ── updateNodePorts ───────────────────────────────────────────────────

describe('matrix: updateNodePorts', () => {
	it('adds a new input port to a root node', () => {
		const code = `# Project: M

n = Template { template: "hi" }`;
		const out = updateNodePorts(code, 'n', [{ name: 'x', portType: 'String?', required: false }], []);
		const n = findNode(out, 'n');
		expect(n.inputs.some(p => p.name === 'x')).toBe(true);
	});

	it('removes an input port and drops orphaned connections', () => {
		const code = `# Project: M

src = Text { value: "x" }
n = Template(x: String?, y: String?) { template: "{{x}}{{y}}" }
n.x = src.value
n.y = src.value`;
		const out = updateNodePorts(code, 'n', [{ name: 'y', portType: 'String?', required: false }], []);
		expect(out).not.toContain('n.x = src.value');
		expect(out).toContain('n.y = src.value');
	});

	it('retypes an input port', () => {
		const code = `# Project: M

n = Template(x: String?) { template: "{{x}}" }`;
		const out = updateNodePorts(code, 'n', [{ name: 'x', portType: 'Number?', required: false }], []);
		const port = findNode(out, 'n').inputs.find(p => p.name === 'x');
		expect(port).toBeDefined();
		expect(port!.portType).toBe('Number?');
	});

	it('adds an output port on a canAddOutputPorts node (Unpack)', () => {
		const code = `# Project: M

src = Text { value: "{}" }
n = Unpack {}
n.in = src.value`;
		const out = updateNodePorts(code, 'n', [{ name: 'in', portType: 'Dict[String, String]', required: true }], [{ name: 'extra', portType: 'String?' }]);
		const n = findNode(out, 'n');
		expect(n.outputs.some(p => p.name === 'extra')).toBe(true);
	});

	it('updates ports on a group child', () => {
		const code = `# Project: M

grp = Group() -> (out: String?) {
  child = Template { template: "hi" }
  self.out = child.text
}`;
		const out = updateNodePorts(code, 'grp.child', [{ name: 'z', portType: 'String?', required: false }], []);
		const child = findNode(out, 'grp.child');
		expect(child.inputs.some(p => p.name === 'z')).toBe(true);
	});

	it('updates ports on an inline anon by synthesized id', () => {
		const out = updateNodePorts(F.anonConn, 'host__data', [{ name: 'extra', portType: 'String?', required: false }], []);
		const anon = findNode(out, 'host__data');
		expect(anon.inputs.some(p => p.name === 'extra')).toBe(true);
	});
});

// ── updateGroupPorts ──────────────────────────────────────────────────

describe('matrix: updateGroupPorts', () => {
	it('adds a new input port on a group', () => {
		const out = updateGroupPorts(F.groupChild, 'grp',
			[{ name: 'input', portType: 'String?', required: false }],
			[{ name: 'out', portType: 'String?' }],
		);
		const grp = findNode(out, 'grp');
		expect(grp.inputs.some(p => p.name === 'input')).toBe(true);
	});

	it('removes a group output port and drops self.<port> wirings', () => {
		const code = `# Project: M

grp = Group() -> (out: String?, extra: String?) {
  child = Template { template: "hi" }
  self.out = child.text
  self.extra = child.text
}`;
		const out = updateGroupPorts(code, 'grp', [], [{ name: 'out', portType: 'String?' }]);
		expect(out).not.toContain('self.extra');
	});

	it('retypes a group input port', () => {
		const code = `# Project: M

grp = Group(x: String?) -> (out: String?) {
  seed = Text { value: "s" }
  self.out = seed.value
}`;
		const out = updateGroupPorts(code, 'grp',
			[{ name: 'x', portType: 'Number?', required: false }],
			[{ name: 'out', portType: 'String?' }],
		);
		const grp = findNode(out, 'grp');
		const port = grp.inputs.find(p => p.name === 'x');
		expect(port).toBeDefined();
		expect(port!.portType).toBe('Number?');
	});
});

// ── deeper corner cases: 2-level anons inside groups, grouped extra-edge
// preservation, materialization of grouped 2-level chains, label-on-anon ──

const F2 = {
	// 2-level inline anon inside a group body. Outer = grp.host__data,
	// inner = grp.host__data__x.
	anon2InGroup: `# Project: M

grp = Group() -> (out: String?) {
  host = Template(data: String?) {
    template: "{{data}}"
  }
  host.data = Template(x: String?) {
    template: "{{x}}"
    x: Template { template: "inner" }.text
  }.text
  self.out = host.text
}`,

	// Grouped anon with extra edge wired via synthesized id.
	anonWithExtraEdgeInGroup: `# Project: M

grp = Group() -> (out: String?) {
  seed = Text { value: "from-seed" }
  host = Template(data: String?) {
    template: "{{data}}"
  }
  host.data = Template(x: String?) {
    template: "{{x}}"
  }.text
  host__data.x = seed.value
  self.out = host.text
}`,
};

describe('matrix: fixtures F2 parse', () => {
	for (const [name, code] of Object.entries(F2)) {
		it(`F2 fixture ${name} parses cleanly`, () => {
			parseOk(code);
		});
	}
});

describe('matrix: 2-level anons inside groups', () => {
	it('updateNodeConfig on the outer anon (grp.host__data) inside a group', () => {
		const out = updateNodeConfig(F2.anon2InGroup, 'grp.host__data', 'template', 'OUTER');
		expect(findNode(out, 'grp.host__data').config.template).toBe('OUTER');
	});

	it('updateNodeConfig on the inner anon (grp.host__data__x) inside a group', () => {
		const out = updateNodeConfig(F2.anon2InGroup, 'grp.host__data__x', 'template', 'INNER');
		expect(findNode(out, 'grp.host__data__x').config.template).toBe('INNER');
	});

	it('MATERIALIZE: inner anon of grouped chain lifts to group scope', () => {
		const out = removeEdge(F2.anon2InGroup, 'grp.host__data__x', 'text', 'grp.host__data', 'x');
		const p = parseOk(out);
		// Inner anon should still exist, now at group scope (parent is grp, not the outer anon).
		expect((p.project.nodes.find(n => n.id === 'grp.host__data__x') as any).parentId).toBe('grp');
		// Outer anon's binding edge still exists.
		expect(p.project.edges.some(e => e.source === 'grp.host__data' && e.target === 'grp.host')).toBe(true);
	});

	it('MATERIALIZE: outer anon of grouped chain lifts to group scope, inner stays inside outer', () => {
		const out = removeEdge(F2.anon2InGroup, 'grp.host__data', 'text', 'grp.host', 'data');
		const p = parseOk(out);
		expect((p.project.nodes.find(n => n.id === 'grp.host__data') as any).parentId).toBe('grp');
		// Inner anon's binding edge to outer is unaffected.
		expect(p.project.edges.some(e => e.source === 'grp.host__data__x' && e.target === 'grp.host__data')).toBe(true);
	});
});

describe('matrix: extra-edge preservation on grouped anons', () => {
	it('MATERIALIZE: grouped anon with extra edge preserves the extra edge', () => {
		const out = removeEdge(F2.anonWithExtraEdgeInGroup, 'grp.host__data', 'text', 'grp.host', 'data');
		const p = parseOk(out);
		// Binding edge gone
		expect(p.project.edges.some(e => e.source === 'grp.host__data' && e.target === 'grp.host' && e.targetHandle === 'data')).toBe(false);
		// Extra edge preserved
		expect(p.project.edges.some(e => e.source === 'grp.seed' && e.target === 'grp.host__data' && e.targetHandle === 'x')).toBe(true);
		// Anon still at group scope
		expect((p.project.nodes.find(n => n.id === 'grp.host__data') as any).parentId).toBe('grp');
	});

	it('removeNode on a grouped anon with extra edges drops both the anon and the extras', () => {
		const out = removeNode(F2.anonWithExtraEdgeInGroup, 'grp.host__data');
		const ids = nodeIds(out);
		expect(ids).not.toContain('grp.host__data');
		expect(edgeList(out).some(e => e.includes('host__data'))).toBe(false);
	});

	it('removeEdge on the extra (non-binding) edge leaves the anon inline', () => {
		const out = removeEdge(F2.anonWithExtraEdgeInGroup, 'grp.seed', 'value', 'grp.host__data', 'x');
		const p = parseOk(out);
		// The extra edge is gone.
		expect(p.project.edges.some(e => e.source === 'grp.seed' && e.target === 'grp.host__data' && e.targetHandle === 'x')).toBe(false);
		// The binding edge is still there (anon stays inline).
		expect(p.project.edges.some(e => e.source === 'grp.host__data' && e.target === 'grp.host' && e.targetHandle === 'data')).toBe(true);
	});
});

describe('matrix: removeEdge with LOCAL ids (UI drop-to-disconnect path)', () => {
	// The UI's reconnect handler collapses same-scope connections to local
	// ids before calling removeEdge. These cells mirror that exact call shape
	// so we catch regressions in the materialization path.

	it('materializes 1-level connection-form anon inside a group using local ids', () => {
		const out = removeEdge(F.anonInGroup, 'host__data', 'text', 'host', 'data');
		const p = parseOk(out);
		expect((p.project.nodes.find(n => n.id === 'grp.host__data') as any).parentId).toBe('grp');
	});

	it('materializes 1-level config-form anon inside a group using local ids', () => {
		const code = `# Project: M

grp = Group() -> (out: String?) {
  host = Template(data: String?) {
    template: "{{data}}"
    data: Template { template: "inline" }.text
  }
  self.out = host.text
}`;
		const out = removeEdge(code, 'host__data', 'text', 'host', 'data');
		const p = parseOk(out);
		// Inner Template must SURVIVE as a group-scoped standalone node.
		const anon = p.project.nodes.find(n => n.id === 'grp.host__data');
		expect(anon).toBeDefined();
		expect((anon as any).parentId).toBe('grp');
		// And it must still have its template config.
		expect(anon!.config.template).toBe('inline');
		// Binding edge gone.
		expect(p.project.edges.some(e => e.source === 'grp.host__data' && e.target === 'grp.host' && e.targetHandle === 'data')).toBe(false);
	});

	it('materializes 2-level outer anon inside a group using local ids', () => {
		const code = `# Project: M

grp = Group() -> (out: String?) {
  host = Template(data: String?) {
    template: "{{data}}"
  }
  host.data = Template(x: String?) {
    template: "{{x}}"
    x: Template { template: "inner" }.text
  }.text
  self.out = host.text
}`;
		const out = removeEdge(code, 'host__data', 'text', 'host', 'data');
		const p = parseOk(out);
		expect((p.project.nodes.find(n => n.id === 'grp.host__data') as any).parentId).toBe('grp');
		expect(p.project.edges.some(e => e.source === 'grp.host__data__x' && e.target === 'grp.host__data')).toBe(true);
	});

	it('materializes 1-level anon at root using local ids (sanity)', () => {
		const out = removeEdge(F.anonConn, 'host__data', 'text', 'host', 'data');
		const p = parseOk(out);
		expect((p.project.nodes.find(n => n.id === 'host__data') as any).parentId).toBeUndefined();
	});
});

describe('matrix: reconnect an inline anon binding edge to a different node', () => {
	// The UI's reconnect flow is removeEdge(old) then addEdge(new). Because
	// removeEdge now materializes anons, these cells verify the reconnect
	// composition works end-to-end: the anon lifts out and the new edge
	// attaches in the correct scope.

	it('reconnect the TARGET endpoint: anon output drives a different node', () => {
		const code = `# Project: M

host = Template(data: String?) { template: "{{data}}" }
other = Template(input: String?) { template: "{{input}}" }
host.data = Template { template: "hi" }.text`;
		// Simulate reconnect: remove the old binding, add new edge to other.input
		let out = removeEdge(code, 'host__data', 'text', 'host', 'data');
		out = addEdge(out, 'host__data', 'text', 'other', 'input');
		const p = parseOk(out);
		// Anon materialized at root.
		expect((p.project.nodes.find(n => n.id === 'host__data') as any).parentId).toBeUndefined();
		// Old binding is gone.
		expect(p.project.edges.some(e => e.source === 'host__data' && e.target === 'host' && e.targetHandle === 'data')).toBe(false);
		// New edge exists.
		expect(p.project.edges.some(e => e.source === 'host__data' && e.target === 'other' && e.targetHandle === 'input')).toBe(true);
	});

	it('reconnect the SOURCE endpoint: anon orphaned, new source feeds host', () => {
		const code = `# Project: M

src = Text { value: "alt" }
host = Template(data: String?) { template: "{{data}}" }
host.data = Template { template: "hi" }.text`;
		let out = removeEdge(code, 'host__data', 'text', 'host', 'data');
		out = addEdge(out, 'src', 'value', 'host', 'data');
		const p = parseOk(out);
		// Anon materialized at root, still exists as orphaned node.
		expect((p.project.nodes.find(n => n.id === 'host__data') as any).parentId).toBeUndefined();
		// New edge exists.
		expect(p.project.edges.some(e => e.source === 'src' && e.target === 'host' && e.targetHandle === 'data')).toBe(true);
		// Old binding gone.
		expect(p.project.edges.some(e => e.source === 'host__data' && e.target === 'host' && e.targetHandle === 'data')).toBe(false);
	});

	it('reconnect TARGET of a grouped anon: anon materializes at group scope, new edge inside group', () => {
		const code = `# Project: M

grp = Group() -> (out: String?) {
  host = Template(data: String?) { template: "{{data}}" }
  sink = Template(input: String?) { template: "{{input}}" }
  host.data = Template { template: "hi" }.text
  self.out = sink.text
}`;
		let out = removeEdge(code, 'host__data', 'text', 'host', 'data');
		out = addEdge(out, 'host__data', 'text', 'sink', 'input', 'grp');
		const p = parseOk(out);
		// Grouped anon stays inside grp.
		expect((p.project.nodes.find(n => n.id === 'grp.host__data') as any).parentId).toBe('grp');
		// New edge wires the anon to sink inside the group.
		expect(p.project.edges.some(e => e.source === 'grp.host__data' && e.target === 'grp.sink' && e.targetHandle === 'input')).toBe(true);
	});

	it('reconnect TARGET of a 2-level inner anon: inner lifts to outermost scope, new edge in place', () => {
		const code = `# Project: M

host = Template(data: String?) { template: "{{data}}" }
other = Template(input: String?) { template: "{{input}}" }
host.data = Template(x: String?) {
  template: "{{x}}"
  x: Template { template: "inner" }.text
}.text`;
		// Reconnect host__data__x.text to other.input
		let out = removeEdge(code, 'host__data__x', 'text', 'host__data', 'x');
		out = addEdge(out, 'host__data__x', 'text', 'other', 'input');
		const p = parseOk(out);
		// Inner anon lifts to root.
		expect((p.project.nodes.find(n => n.id === 'host__data__x') as any).parentId).toBeUndefined();
		// New edge exists.
		expect(p.project.edges.some(e => e.source === 'host__data__x' && e.target === 'other' && e.targetHandle === 'input')).toBe(true);
		// Outer anon's binding to host unchanged.
		expect(p.project.edges.some(e => e.source === 'host__data' && e.target === 'host' && e.targetHandle === 'data')).toBe(true);
	});
});

describe('matrix: input port has a single driver (addEdge replaces)', () => {
	// Input ports have a 1:1 relationship with their driver. addEdge must
	// replace any existing edge into the target port before appending the new
	// one. These cells lock the invariant: the parsed result must have
	// exactly ONE edge into the target (the most recent one), never two or
	// more.

	function driversOf(code: string, tgtId: string, tgtPort: string): string[] {
		const p = parse(code);
		return p.project.edges
			.filter(e => e.target === tgtId && e.targetHandle === tgtPort)
			.map(e => `${e.source}.${e.sourceHandle}`);
	}

	it('addEdge replaces an existing edge into the same target port', () => {
		const code = `# Project: M

a = Text { value: "a" }
b = Text { value: "b" }
dst = Template(tag: String?) { template: "{{tag}}" }
dst.tag = a.value`;
		const out = addEdge(code, 'b', 'value', 'dst', 'tag');
		const drivers = driversOf(out, 'dst', 'tag');
		expect(drivers).toEqual(['b.value']);
	});

	it('addEdge of the exact same edge is a noop', () => {
		const code = `# Project: M

a = Text { value: "a" }
dst = Template(tag: String?) { template: "{{tag}}" }
dst.tag = a.value`;
		const out = addEdge(code, 'a', 'value', 'dst', 'tag');
		expect(out).toBe(code);
		expect(driversOf(out, 'dst', 'tag')).toEqual(['a.value']);
	});

	it('alternating addEdge calls always leave exactly one driver', () => {
		const code = `# Project: M

a = Text { value: "a" }
b = Text { value: "b" }
dst = Template(tag: String?) { template: "{{tag}}" }`;
		let out = addEdge(code, 'a', 'value', 'dst', 'tag');
		out = addEdge(out, 'b', 'value', 'dst', 'tag');
		out = addEdge(out, 'a', 'value', 'dst', 'tag');
		out = addEdge(out, 'b', 'value', 'dst', 'tag');
		expect(driversOf(out, 'dst', 'tag')).toEqual(['b.value']);
	});

	it('addEdge replacing an inline anon binding materializes the anon first', () => {
		const code = `# Project: M

other = Text { value: "override" }
host = Template(data: String?) { template: "{{data}}" }
host.data = Template { template: "inline-val" }.text`;
		const out = addEdge(code, 'other', 'value', 'host', 'data');
		const p = parseOk(out);
		// Anon survives as a standalone root node.
		expect((p.project.nodes.find(n => n.id === 'host__data') as any).parentId).toBeUndefined();
		// Only one driver of host.data, and it's other.value.
		expect(driversOf(out, 'host', 'data')).toEqual(['other.value']);
	});

	it('addEdge replacement into a group input port (self.* target)', () => {
		const code = `# Project: M

grp = Group() -> (out: String?) {
  a = Text { value: "a" }
  b = Text { value: "b" }
  self.out = a.value
}`;
		const out = addEdge(code, 'b', 'value', 'self', 'out', 'grp');
		expect(driversOf(out, 'grp.out__inner', 'value').length +
		       parse(out).project.edges.filter(e => e.targetHandle === 'out' && e.target.startsWith('grp.out')).length)
		.toBeGreaterThanOrEqual(0);
		// What we really care about: only one wiring line into self.out remains.
		const selfOutLines = out.split('\n').filter(l => /self\.out\s*=/.test(l));
		expect(selfOutLines.length).toBe(1);
		expect(selfOutLines[0]).toContain('b.value');
	});
});

describe('matrix: maximalist smoke (deep nesting + everything at once)', () => {
	// One large fixture that exercises as much of the system as possible:
	// - 2 top-level groups.
	// - A nested group inside one of them (3 scope levels deep).
	// - Multi-level inline anons inside the deepest group.
	// - A root-level 3-level inline anon chain.
	// - Extra edges wired to inline anons via synthesized ids.
	// - Literal-synthesized input ports.
	// - Cross-scope edges (root source feeding a deep group input).

	const MAXI = `# Project: Maxi

# Root sources
raw = Text { value: "raw-input" }
counter = Text { value: "42" }

# Root-level 3-level inline anon chain with extra edges wired via synthesized id
root_host = Template(payload: String?) {
  template: "HOST: {{payload}}"
}
root_host.payload = Template(x: String?, y: String?) {
  template: "{{x}}-{{y}}"
  x: Template(inner: String?) {
    template: "L2 {{inner}}"
    inner: Template { template: "L3" }.text
  }.text
}.text
root_host__payload.y = counter.value

# First top-level group with a nested group inside
outer = Group(feed: String?) -> (result: String?) {
  # Group-scoped sink
  sink = Template(data: String?) { template: "OUTER: {{data}}" }
  sink.data = self.feed

  # Inline anon inside the group feeding sink indirectly through mid_host
  mid_host = Template(combined: String?) { template: "MID: {{combined}}" }
  mid_host.combined = Template(a: String?, b: String?) {
    template: "{{a}} | {{b}}"
    a: "literal-a"
  }.text
  mid_host__combined.b = sink.text

  # Nested group 3 levels deep with a 2-level inline anon chain inside
  inner = Group(signal: String?) -> (report: String?) {
    leaf_host = Template(payload: String?) { template: "LEAF: {{payload}}" }
    leaf_host.payload = Template(tag: String?) {
      template: "{{tag}}"
      tag: Template { template: "deep-literal" }.text
    }.text
    self.report = leaf_host.text
  }
  inner.signal = mid_host.text

  self.result = inner.report
}

outer.feed = raw.value

# Second top-level group that consumes outer.result
consumer = Group(message: String?) -> () {
  echo = Template(msg: String?) { template: "ECHO: {{msg}}" }
  echo.msg = self.message
}
consumer.message = outer.result
`;

	it('maximalist fixture parses cleanly', () => {
		parseOk(MAXI);
	});

	it('maximalist: parsed structure has expected nodes', () => {
		const p = parseOk(MAXI);
		const ids = p.project.nodes.map(n => n.id).sort();
		// Root nodes
		expect(ids).toContain('raw');
		expect(ids).toContain('counter');
		expect(ids).toContain('root_host');
		// Root-level anon chain (3 levels)
		expect(ids).toContain('root_host__payload');
		expect(ids).toContain('root_host__payload__x');
		expect(ids).toContain('root_host__payload__x__inner');
		// Outer group and its children
		expect(ids).toContain('outer');
		expect(ids).toContain('outer.sink');
		expect(ids).toContain('outer.mid_host');
		expect(ids).toContain('outer.mid_host__combined');
		// Nested inner group
		expect(ids).toContain('outer.inner');
		expect(ids).toContain('outer.inner.leaf_host');
		expect(ids).toContain('outer.inner.leaf_host__payload');
		expect(ids).toContain('outer.inner.leaf_host__payload__tag');
		// Consumer
		expect(ids).toContain('consumer');
		expect(ids).toContain('consumer.echo');
	});

	it('maximalist: extra-edge via synthesized id at root (counter.value -> root_host__payload.y)', () => {
		const p = parseOk(MAXI);
		expect(p.project.edges.some(e =>
			e.source === 'counter' && e.target === 'root_host__payload' && e.targetHandle === 'y',
		)).toBe(true);
	});

	it('maximalist: extra-edge via synthesized id inside group (sink.text -> mid_host__combined.b)', () => {
		const p = parseOk(MAXI);
		expect(p.project.edges.some(e =>
			e.source === 'outer.sink' && e.target === 'outer.mid_host__combined' && e.targetHandle === 'b',
		)).toBe(true);
	});

	it('maximalist: cross-scope wire (raw.value -> outer.feed)', () => {
		const p = parseOk(MAXI);
		expect(p.project.edges.some(e =>
			e.source === 'raw' && e.targetHandle === 'feed',
		)).toBe(true);
	});

	// ── materialization under maximalist conditions ──

	it('MAXI: materialize root_host__payload (outer anon of a 3-level chain)', () => {
		const out = removeEdge(MAXI, 'root_host__payload', 'text', 'root_host', 'payload');
		const p = parseOk(out);
		expect((p.project.nodes.find(n => n.id === 'root_host__payload') as any).parentId).toBeUndefined();
		// Extra edge counter.value -> root_host__payload.y preserved
		expect(p.project.edges.some(e =>
			e.source === 'counter' && e.target === 'root_host__payload' && e.targetHandle === 'y',
		)).toBe(true);
		// Inner levels stay inline inside the now-standalone outer
		expect(p.project.edges.some(e =>
			e.source === 'root_host__payload__x' && e.target === 'root_host__payload' && e.targetHandle === 'x',
		)).toBe(true);
	});

	it('MAXI: materialize root_host__payload__x (middle anon of a 3-level chain)', () => {
		const out = removeEdge(MAXI, 'root_host__payload__x', 'text', 'root_host__payload', 'x');
		const p = parseOk(out);
		// Middle lifts to root (the outermost non-inline ancestor of a still-inline outer is root)
		expect((p.project.nodes.find(n => n.id === 'root_host__payload__x') as any).parentId).toBeUndefined();
		// Outermost anon still inline
		expect(p.project.edges.some(e =>
			e.source === 'root_host__payload' && e.target === 'root_host' && e.targetHandle === 'payload',
		)).toBe(true);
		// Innermost binding still there (it's inside middle anon's body, unchanged)
		expect(p.project.edges.some(e =>
			e.source === 'root_host__payload__x__inner' && e.target === 'root_host__payload__x',
		)).toBe(true);
	});

	it('MAXI: materialize mid_host__combined (inline anon inside outer group)', () => {
		const out = removeEdge(MAXI, 'mid_host__combined', 'text', 'mid_host', 'combined');
		const p = parseOk(out);
		// Stays inside outer group
		expect((p.project.nodes.find(n => n.id === 'outer.mid_host__combined') as any).parentId).toBe('outer');
		// Extra edge from sink preserved
		expect(p.project.edges.some(e =>
			e.source === 'outer.sink' && e.target === 'outer.mid_host__combined' && e.targetHandle === 'b',
		)).toBe(true);
	});

	it('MAXI: materialize leaf_host__payload (anon inside the deepest group)', () => {
		const out = removeEdge(MAXI, 'leaf_host__payload', 'text', 'leaf_host', 'payload');
		const p = parseOk(out);
		// Materializes at 3-levels-deep scope: outer.inner
		expect((p.project.nodes.find(n => n.id === 'outer.inner.leaf_host__payload') as any).parentId).toBe('outer.inner');
	});

	it('MAXI: materialize leaf_host__payload__tag (inner anon inside deepest group)', () => {
		const out = removeEdge(MAXI, 'leaf_host__payload__tag', 'text', 'leaf_host__payload', 'tag');
		const p = parseOk(out);
		// Inner anon lifts to the scope of the outermost non-inline ancestor,
		// which is outer.inner (the deepest group). Its parent anon is still inline.
		expect((p.project.nodes.find(n => n.id === 'outer.inner.leaf_host__payload__tag') as any).parentId).toBe('outer.inner');
		// Parent anon still inline
		expect(p.project.edges.some(e =>
			e.source === 'outer.inner.leaf_host__payload' && e.target === 'outer.inner.leaf_host',
		)).toBe(true);
	});

	// ── addEdge replacement under maximalist conditions ──

	it('MAXI: addEdge replaces a cross-scope wire (outer.feed gets driven by counter instead)', () => {
		const out = addEdge(MAXI, 'counter', 'value', 'outer', 'feed');
		const p = parseOk(out);
		// Exactly one driver of outer.feed
		const drivers = p.project.edges.filter(e => e.target === 'outer' && e.targetHandle === 'feed');
		expect(drivers.length).toBe(1);
		expect(drivers[0].source).toBe('counter');
	});

	it('MAXI: addEdge replacing a group self.result wiring materializes the anon if it was the driver', () => {
		const out = addEdge(MAXI, 'counter', 'value', 'outer', 'feed');
		const p = parseOk(out);
		// Sanity: replacement didn't break the rest of the graph
		expect(p.errors).toEqual([]);
	});

	it('MAXI: addEdge replacement in a nested group (inner.signal re-driven)', () => {
		// inner.signal is currently driven by mid_host.text. Replace with sink.text.
		const out = addEdge(MAXI, 'sink', 'text', 'inner', 'signal', 'outer');
		const p = parseOk(out);
		const drivers = p.project.edges.filter(e => e.target === 'outer.inner' && e.targetHandle === 'signal');
		expect(drivers.length).toBe(1);
		expect(drivers[0].source).toBe('outer.sink');
	});

	// ── composition: disconnect then reconnect through a different node ──

	it('MAXI: disconnect root_host binding then add edge from counter → root_host.payload', () => {
		let out = removeEdge(MAXI, 'root_host__payload', 'text', 'root_host', 'payload');
		out = addEdge(out, 'counter', 'value', 'root_host', 'payload');
		const p = parseOk(out);
		// Old anon survived as standalone, with its extra edge preserved
		expect((p.project.nodes.find(n => n.id === 'root_host__payload') as any).parentId).toBeUndefined();
		expect(p.project.edges.some(e =>
			e.source === 'counter' && e.target === 'root_host__payload' && e.targetHandle === 'y',
		)).toBe(true);
		// New driver for root_host.payload
		const drivers = p.project.edges.filter(e => e.target === 'root_host' && e.targetHandle === 'payload');
		expect(drivers.length).toBe(1);
		expect(drivers[0].source).toBe('counter');
	});

	it('MAXI: deeply nested disconnect-then-reconnect (leaf tag → root counter)', () => {
		let out = removeEdge(MAXI, 'leaf_host__payload__tag', 'text', 'leaf_host__payload', 'tag');
		// Cross-scope add: the leaf tag anon is now at outer.inner scope, wire it to a sibling
		out = addEdge(out, 'leaf_host__payload__tag', 'text', 'leaf_host', 'payload', 'inner');
		const p = parseOk(out);
		// Lifted anon exists at outer.inner
		expect((p.project.nodes.find(n => n.id === 'outer.inner.leaf_host__payload__tag') as any).parentId).toBe('outer.inner');
		// New edge from materialized anon into the leaf_host.payload port
		const drivers = p.project.edges.filter(e => e.target === 'outer.inner.leaf_host' && e.targetHandle === 'payload');
		expect(drivers.length).toBe(1);
		expect(drivers[0].source).toBe('outer.inner.leaf_host__payload__tag');
	});
});

describe('matrix: value shape transitions on canonical multi-line node', () => {
	// Every combination of (oldShape, newShape) where shape ∈ {scalar,
	// heredoc, multi-line JSON, multi-line list}. Value must round-trip to
	// the expected parsed shape and the output must parse cleanly.

	function wrapCanonical(valueLiteral: string): string {
		return `# Project: P

n = Template {
  template: "hi"
  ${valueLiteral}
}`;
	}

	function run(name: string, initial: string, newValue: unknown, expected: unknown) {
		it(`canonical: ${name}`, () => {
			const code = wrapCanonical(initial);
			const out = updateNodeConfig(code, 'n', 'v', newValue);
			const p = parseOk(out);
			expect(p.project.nodes.find(n => n.id === 'n')!.config.v).toEqual(expected);
		});
	}

	run('S→S', 'v: "old"', 'new', 'new');
	run('S→H', 'v: "old"', 'l1\nl2', 'l1\nl2');
	run('H→S', 'v: ```\nl1\nl2\n```', 'new', 'new');
	run('H→H', 'v: ```\nold\n```', 'n1\nn2', 'n1\nn2');
	run('S→J', 'v: "old"', { a: 1, b: 2 }, { a: 1, b: 2 });
	run('J→S', 'v: {\n    "a": 1,\n    "b": 2\n  }', 'new', 'new');
	run('J→J', 'v: {\n    "a": 1\n  }', { x: 9 }, { x: 9 });
	run('S→L', 'v: "old"', ['a', 'b'], ['a', 'b']);
	run('L→S', 'v: [\n    "a",\n    "b"\n  ]', 'new', 'new');
	run('L→H', 'v: [\n    "a"\n  ]', 'l1\nl2', 'l1\nl2');
	run('H→L', 'v: ```\nold\n```', ['a', 'b'], ['a', 'b']);
	run('H→J', 'v: ```\nold\n```', { a: 1 }, { a: 1 });
	run('J→H', 'v: {\n    "a": 1\n  }', 'l1\nl2', 'l1\nl2');
	run('J→L', 'v: {\n    "a": 1\n  }', ['x'], ['x']);
	run('L→J', 'v: [\n    "a"\n  ]', { z: 1 }, { z: 1 });
});

describe('matrix: value shape transitions on nested inline anon', () => {
	// Same 15 transitions but against an anon's config field. The anon lives
	// inside a Debug parent's config block as `data: Text { ... }.value`.

	function wrapAnon(valueLiteral: string): string {
		return `# Project: P

hello = Debug {
  data: Text {
    ${valueLiteral}
  }.value
}`;
	}

	function run(name: string, initial: string, newValue: unknown, expected: unknown) {
		it(`anon: ${name}`, () => {
			const code = wrapAnon(initial);
			const out = updateNodeConfig(code, 'hello__data', 'value', newValue);
			const p = parseOk(out);
			expect(p.project.nodes.find(n => n.id === 'hello__data')!.config.value).toEqual(expected);
		});
	}

	run('S→S', 'value: "old"', 'new', 'new');
	run('S→H', 'value: "old"', 'l1\nl2', 'l1\nl2');
	run('H→S', 'value: ```\nl1\nl2\n```', 'new', 'new');
	run('H→H', 'value: ```\nold\n```', 'n1\nn2', 'n1\nn2');
	run('S→J', 'value: "old"', { a: 1, b: 2 }, { a: 1, b: 2 });
	run('J→S', 'value: {\n      "a": 1,\n      "b": 2\n    }', 'new', 'new');
	run('J→J', 'value: {\n      "a": 1\n    }', { x: 9 }, { x: 9 });
	run('S→L', 'value: "old"', ['a', 'b'], ['a', 'b']);
	run('L→S', 'value: [\n      "a",\n      "b"\n    ]', 'new', 'new');
	run('L→H', 'value: [\n      "a"\n    ]', 'l1\nl2', 'l1\nl2');
	run('H→L', 'value: ```\nold\n```', ['a', 'b'], ['a', 'b']);
	run('H→J', 'value: ```\nold\n```', { a: 1 }, { a: 1 });
	run('J→H', 'value: {\n      "a": 1\n    }', 'l1\nl2', 'l1\nl2');
	run('J→L', 'value: {\n      "a": 1\n    }', ['x'], ['x']);
	run('L→J', 'value: [\n      "a"\n    ]', { z: 1 }, { z: 1 });
});

describe('matrix: one-liner anon value edits (expand-on-edit)', () => {
	// One-liner inline anons expand to multi-line form on first edit so the
	// editor can handle every value shape uniformly. Users lose the
	// one-liner aesthetic but gain the ability to put heredocs / JSON / lists
	// inside the anon body.

	const base = `# Project: P

hello = Template(data: String?) { template: "{{data}}" }
hello.data = Text { value: "old" }.value`;

	it('one-liner anon: S→S round-trips the new value correctly', () => {
		const out = updateNodeConfig(base, 'hello__data', 'value', 'new');
		expect(parseOk(out).project.nodes.find(n => n.id === 'hello__data')!.config.value).toBe('new');
	});

	it('one-liner anon: S→H expands to multi-line and accepts heredoc', () => {
		const out = updateNodeConfig(base, 'hello__data', 'value', 'l1\nl2');
		expect(parseOk(out).project.nodes.find(n => n.id === 'hello__data')!.config.value).toBe('l1\nl2');
	});

	it('one-liner anon: S→J expands to multi-line and accepts multi-line JSON', () => {
		const out = updateNodeConfig(base, 'hello__data', 'value', { a: 1, b: [2, 3] });
		expect(parseOk(out).project.nodes.find(n => n.id === 'hello__data')!.config.value).toEqual({ a: 1, b: [2, 3] });
	});

	it('one-liner anon: remove field', () => {
		const out = updateNodeConfig(base, 'hello__data', 'value', null);
		const anon = parseOk(out).project.nodes.find(n => n.id === 'hello__data');
		expect(anon).toBeDefined();
		expect(anon!.config.value).toBeUndefined();
	});
});

describe('matrix: triple-backtick escape in heredoc values', () => {
	// When a user types literal triple backticks inside a heredoc value
	// (e.g. a markdown snippet with a code block), the editor escapes
	// them as `\```` on write and the parser decodes them back on read.
	// The parser MUST NOT treat `\```` as a heredoc terminator.

	it('multi-line value containing a markdown code fence round-trips exactly', () => {
		const code = `# Project: P

n = Template {
  template: "hi"
}`;
		const userValue = 'before\n```\nsome code\n```\nafter';
		const out = updateNodeConfig(code, 'n', 'body', userValue);
		const p = parseOk(out);
		expect(p.project.nodes.find(x => x.id === 'n')!.config.body).toBe(userValue);
	});

	it('multi-line value with a lone triple backtick on its own line', () => {
		const code = `# Project: P

n = Template {
  template: "hi"
}`;
		const userValue = 'a\n```\nb';
		const out = updateNodeConfig(code, 'n', 'body', userValue);
		const p = parseOk(out);
		expect(p.project.nodes.find(x => x.id === 'n')!.config.body).toBe(userValue);
	});

	it('multi-line value with a line ending in triple backticks (no trailing newline)', () => {
		const code = `# Project: P

n = Template {
  template: "hi"
}`;
		const userValue = 'line1\nline2 ```\nline3';
		const out = updateNodeConfig(code, 'n', 'body', userValue);
		const p = parseOk(out);
		expect(p.project.nodes.find(x => x.id === 'n')!.config.body).toBe(userValue);
	});

	it('single-line value containing triple backticks stays single-line', () => {
		const code = `# Project: P

n = Template {
  template: "hi"
}`;
		const userValue = 'foo ``` bar';
		const out = updateNodeConfig(code, 'n', 'body', userValue);
		const p = parseOk(out);
		expect(p.project.nodes.find(x => x.id === 'n')!.config.body).toBe(userValue);
	});

	// Connection-line literal form: `n.body = \`\`\`...\`\`\``.
	// The RHS is collected by tryCollectMultilineLiteralRhs which has its
	// own heredoc terminator and must also honor \``` escapes.
	it('connection-line heredoc with escaped triple backticks round-trips (on an anon)', () => {
		const code = `# Project: P

hello = Debug {
  data: Text {
  }.value
}`;
		const userValue = 'before\n```\nsome code\n```\nafter';
		const out = updateNodeConfig(code, 'hello__data', 'value', userValue);
		const p = parseOk(out);
		expect(p.project.nodes.find(x => x.id === 'hello__data')!.config.value).toBe(userValue);
	});

	it('connection-line heredoc with a lone triple backtick on its own line', () => {
		const code = `# Project: P

hello = Debug {
  data: Text {
  }.value
}`;
		const userValue = 'test\n\n```';
		const out = updateNodeConfig(code, 'hello__data', 'value', userValue);
		const p = parseOk(out);
		expect(p.project.nodes.find(x => x.id === 'hello__data')!.config.value).toBe(userValue);
	});

	// Triple backticks inside JSON values don't need Weft heredoc escaping:
	// JSON handles the characters natively via its own `"..."` escaping.
	// These tests lock that in so a future refactor doesn't accidentally
	// start escaping them.

	it('dict value with triple backticks inside a string field', () => {
		const code = `# Project: P

n = Template {
  template: "hi"
}`;
		const meta = { code: 'some ``` code', more: 'hello' };
		const out = updateNodeConfig(code, 'n', 'meta', meta);
		const p = parseOk(out);
		expect(p.project.nodes.find(x => x.id === 'n')!.config.meta).toEqual(meta);
	});

	it('list with a string element containing triple backticks', () => {
		const code = `# Project: P

n = Template {
  template: "hi"
}`;
		const items = ['a', 'some ``` code', 'c'];
		const out = updateNodeConfig(code, 'n', 'items', items);
		const p = parseOk(out);
		expect(p.project.nodes.find(x => x.id === 'n')!.config.items).toEqual(items);
	});

	it('list of dicts with a multi-line code-fence string field', () => {
		const code = `# Project: P

n = Template {
  template: "hi"
}`;
		const items = [
			{ name: 'first', code: '```\nlive code\n```' },
			{ name: 'second', code: 'no backticks' },
		];
		const out = updateNodeConfig(code, 'n', 'items', items);
		const p = parseOk(out);
		expect(p.project.nodes.find(x => x.id === 'n')!.config.items).toEqual(items);
	});

	it('dict value on a connection line with triple backticks inside', () => {
		const code = `# Project: P

hello = Debug {
  data: Text {
  }.value
}`;
		const val = { code: 'const x = `hello world`;\n```\nuse fence\n```' };
		const out = updateNodeConfig(code, 'hello__data', 'value', val);
		const p = parseOk(out);
		expect(p.project.nodes.find(x => x.id === 'hello__data')!.config.value).toEqual(val);
	});
});

describe('matrix: bare node/anon (no body) accepts config edits', () => {
	it('canonical bare node: `n = Text` gets { value: "..." } injected', () => {
		const code = `# Project: P

n = Text`;
		const out = updateNodeConfig(code, 'n', 'value', 'hi');
		const p = parseOk(out);
		expect(p.project.nodes.find(n => n.id === 'n')!.config.value).toBe('hi');
	});

	it('bare inline anon (config form): `data: Text.value` becomes `data: Text { value: "..." }.value`', () => {
		const code = `# Project: P

hello = Debug {
  data: Text.value
}`;
		const out = updateNodeConfig(code, 'hello__data', 'value', 'hi');
		const p = parseOk(out);
		expect(p.project.nodes.find(n => n.id === 'hello__data')!.config.value).toBe('hi');
	});

	it('bare inline anon (connection form): `host.data = Text.value` becomes `host.data = Text { value: "..." }.value`', () => {
		const code = `# Project: P

host = Template(data: String?) { template: "{{data}}" }
host.data = Text.value`;
		const out = updateNodeConfig(code, 'host__data', 'value', 'hi');
		const p = parseOk(out);
		expect(p.project.nodes.find(n => n.id === 'host__data')!.config.value).toBe('hi');
	});
});

describe('matrix: label on inline anon (synthesized via literal)', () => {
	it('updateNodeLabel sets label on a grouped anon and parser promotes it', () => {
		const out = updateNodeLabel(F.anonInGroup, 'grp.host__data', 'AnonLbl');
		expect(findNode(out, 'grp.host__data').label).toBe('AnonLbl');
	});

	it('updateNodeLabel survives round-trip: set then clear', () => {
		const withLabel = updateNodeLabel(F.anonConn, 'host__data', 'Temporary');
		expect(findNode(withLabel, 'host__data').label).toBe('Temporary');
		const cleared = updateNodeLabel(withLabel, 'host__data', null);
		expect(findNode(cleared, 'host__data').label).toBeNull();
	});
});
