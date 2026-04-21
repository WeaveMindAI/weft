import { describe, it, expect } from 'vitest';
import {
	updateNodeConfig,
	updateNodeLabel,
	addNode,
	addGroup,
	removeNode,
	removeGroup,
	addEdge,
	removeEdge,
	moveNodeScope,
	moveGroupScope,
	renameGroup,
	updateNodePorts,
	updateGroupPorts,
	updateProjectMeta,
} from './weft-editor';

// ── Helpers ─────────────────────────────────────────────────────────────────

/** Normalize whitespace for comparison: trim trailing spaces per line, trim start/end */
function norm(s: string): string {
	return s.split('\n').map(l => l.trimEnd()).join('\n').trim();
}

// ── removeNode ──────────────────────────────────────────────────────────────

describe('removeNode', () => {
	it('removes a top-level node and its connections', () => {
		const code = `# Project: Test

a = Text { value: "hello" }
b = Debug {}

b.data = a.output`;
		const result = removeNode(code, 'a');
		expect(result).not.toContain('a = Text');
		expect(result).not.toContain('b.data = a.output');
		expect(result).toContain('b = Debug');
	});

	it('removes a node inside a group', () => {
		const code = `# Project: Test

grp = Group(data: String) -> (result: String) {
  worker = ExecPython(input: String) -> (output: String) {
    code: "return {}"
  }
  helper = Debug {}

  helper.data = worker.output
  worker.input = self.data
  self.result = worker.output
}`;
		const result = removeNode(code, 'grp.worker');
		expect(result).not.toContain('worker = ExecPython');
		expect(result).not.toContain('helper.data = worker.output');
		expect(result).not.toContain('worker.input = self.data');
		expect(result).not.toContain('self.result = worker.output');
		expect(result).toContain('helper = Debug');
	});

	it('removes a node inside a nested group', () => {
		const code = `# Project: Test

outer = Group(data: String) -> (result: String) {
  inner = Group(x: String) -> (y: String) {
    proc = ExecPython(input: String) -> (output: String) {
      code: "return {}"
    }
    proc.input = self.x
    self.y = proc.output
  }
  inner.x = self.data
  self.result = inner.y
}`;
		const result = removeNode(code, 'outer.inner.proc');
		expect(result).not.toContain('proc = ExecPython');
		expect(result).not.toContain('proc.input');
		expect(result).not.toContain('self.y = proc.output');
		expect(result).toContain('inner = Group');
	});

	it('removes a bare node (no config block)', () => {
		const code = `# Project: Test

a = Text
b = Debug

b.data = a.output`;
		const result = removeNode(code, 'a');
		expect(result).not.toContain('a = Text');
		expect(result).not.toContain('b.data = a.output');
		expect(result).toContain('b = Debug');
	});

	it('removes a one-liner node', () => {
		const code = `# Project: Test

a = Text { value: "hi" }
b = Debug {}`;
		const result = removeNode(code, 'a');
		expect(result).not.toContain('a = Text');
		expect(result).toContain('b = Debug');
	});

	it('removes a node with multiline port signature', () => {
		const code = `# Project: Test

worker = ExecPython(
    data: String,
    context: String?
) -> (
    result: String,
    score: Number?
) {
    code: "return {}"
}

b = Debug {}
b.data = worker.result`;
		const result = removeNode(code, 'worker');
		expect(result).not.toContain('worker');
		expect(result).toContain('b = Debug');
	});

	it('removes a node with heredoc config', () => {
		const code = `# Project: Test

a = ExecPython {
  code: \`\`\`
def run():
    return {"output": "hello"}
\`\`\`
}

b = Debug {}`;
		const result = removeNode(code, 'a');
		expect(result).not.toContain('ExecPython');
		expect(result).not.toContain('def run');
		expect(result).toContain('b = Debug');
	});
});

// ── removeGroup ─────────────────────────────────────────────────────────────

describe('removeGroup', () => {
	it('removes a group and promotes children to parent scope', () => {
		const code = `# Project: Test

grp = Group(data: String) -> (result: String) {
  worker = ExecPython(input: String) -> (output: String) {
    code: "return {}"
  }
  worker.input = self.data
  self.result = worker.output
}

other = Debug {}
other.data = grp.result`;
		const result = removeGroup(code, 'grp');
		expect(result).not.toContain('grp = Group');
		expect(result).not.toContain('other.data = grp.result');
		// Children should be promoted (de-indented)
		expect(result).toContain('worker = ExecPython');
		// self connections should be removed (they reference the group)
		expect(result).not.toContain('self.data');
		expect(result).not.toContain('self.result');
	});

	it('removes an empty group', () => {
		const code = `# Project: Test

grp = Group() -> () {}

other = Debug {}`;
		const result = removeGroup(code, 'grp');
		expect(result).not.toContain('grp');
		expect(result).toContain('other = Debug');
	});

	it('removes a one-liner group', () => {
		const code = `# Project: Test

grp = Group() -> () {}
other = Debug {}`;
		const result = removeGroup(code, 'grp');
		expect(result).not.toContain('grp');
		expect(result).toContain('other = Debug');
	});

	it('removes a nested group and promotes its children to parent group', () => {
		const code = `# Project: Test

outer = Group(data: String) -> (result: String) {
  inner = Group(x: String) -> (y: String) {
    proc = ExecPython(input: String) -> (output: String) {
      code: "return {}"
    }
    proc.input = self.x
    self.y = proc.output
  }
  inner.x = self.data
  self.result = inner.y
}`;
		const result = removeGroup(code, 'inner');
		expect(result).not.toContain('inner = Group');
		// proc should still be inside outer, at outer's inner indent level
		expect(result).toContain('  proc = ExecPython');
		// Connections referencing inner should be removed
		expect(result).not.toContain('inner.x');
		expect(result).not.toContain('inner.y');
	});

	it('removes a group with multiple children and connections between them', () => {
		const code = `# Project: Test

grp = Group(data: String) -> (result: String) {
  a = Text { value: "hello" }
  b = ExecPython(input: String) -> (output: String) {
    code: "return {}"
  }
  c = Debug {}

  b.input = a.output
  c.data = b.output
  b.input = self.data
  self.result = c.data
}`;
		const result = removeGroup(code, 'grp');
		// All children promoted
		expect(result).toContain('a = Text');
		expect(result).toContain('b = ExecPython');
		expect(result).toContain('c = Debug');
		// Internal connections preserved
		expect(result).toContain('b.input = a.output');
		expect(result).toContain('c.data = b.output');
		// self connections removed
		expect(result).not.toContain('self.data');
		expect(result).not.toContain('self.result');
		// External connections to group removed
		expect(result).not.toContain('grp.');
	});

	it('does not leak into nodes after the group', () => {
		const code = `# Project: Test

grp = Group(data: String) -> (result: String) {
  worker = Debug {}
}

after = Text { value: "keep me" }`;
		const result = removeGroup(code, 'grp');
		expect(result).toContain('after = Text');
		expect(result).toContain('keep me');
		expect(result).toContain('worker = Debug');
	});

	it('handles group with heredoc inside a child node', () => {
		const code = `# Project: Test

grp = Group() -> () {
  py = ExecPython {
    code: \`\`\`
def run():
    return {}
\`\`\`
  }
}

after = Debug {}`;
		const result = removeGroup(code, 'grp');
		expect(result).toContain('py = ExecPython');
		expect(result).toContain('def run');
		expect(result).toContain('after = Debug');
	});
});

// ── Precision / no-leak tests ───────────────────────────────────────────────

describe('removeGroup precision', () => {
	it('only removes the group declaration, self-connections, and external edges, nothing else', () => {
		const code = `# Project: Test

before = Text { value: "before" }

grp = Group(data: String) -> (result: String) {
  a = Text { value: "inside_a" }
  b = Debug {}

  b.data = a.output
  a.output = self.data
  self.result = b.data
}

after = Debug { label: "after" }
after.data = grp.result
grp.data = before.output
before_edge = before.output`;

		const result = removeGroup(code, 'grp');

		// Group declaration gone
		expect(result).not.toContain('grp = Group');
		// Self-connections gone
		expect(result).not.toContain('self.data');
		expect(result).not.toContain('self.result');
		// External connections to/from group gone
		expect(result).not.toContain('after.data = grp.result');
		expect(result).not.toContain('grp.data = before.output');

		// Children fully preserved with their config
		expect(result).toContain('a = Text { value: "inside_a" }');
		expect(result).toContain('b = Debug {}');
		// Internal connections between children preserved
		expect(result).toContain('b.data = a.output');

		// Nodes before and after completely untouched
		expect(result).toContain('before = Text { value: "before" }');
		expect(result).toContain('after = Debug { label: "after" }');
		// Unrelated lines that happen to contain "before" are untouched
		expect(result).toContain('before_edge = before.output');
	});

	it('removing one group does not affect a sibling group', () => {
		const code = `# Project: Test

grp1 = Group(a: String) -> (b: String) {
  n1 = Debug {}
  n1.data = self.a
  self.b = n1.data
}

grp2 = Group(c: String) -> (d: String) {
  n2 = Text { value: "keep" }
  n2.input = self.c
  self.d = n2.output
}

grp2.c = grp1.b`;

		const result = removeGroup(code, 'grp1');

		// grp1 gone
		expect(result).not.toContain('grp1 = Group');
		expect(result).not.toContain('grp2.c = grp1.b');
		// grp1 child promoted
		expect(result).toContain('n1 = Debug');

		// grp2 completely untouched, declaration, children, self-connections, all intact
		expect(result).toContain('grp2 = Group(c: String) -> (d: String)');
		expect(result).toContain('n2 = Text { value: "keep" }');
		expect(result).toContain('n2.input = self.c');
		expect(result).toContain('self.d = n2.output');
	});

	it('removing a nested group preserves parent group structure', () => {
		const code = `# Project: Test

outer = Group(x: String) -> (y: String) {
  inner = Group(a: String) -> (b: String) {
    child = Debug {}
    child.data = self.a
    self.b = child.data
  }
  sibling = Text { value: "hi" }

  inner.a = self.x
  sibling.input = inner.b
  self.y = sibling.output
}`;

		const result = removeGroup(code, 'inner');

		// inner group declaration gone
		expect(result).not.toContain('inner = Group');
		// inner self-connections gone
		expect(result).not.toContain('child.data = self.a');
		expect(result).not.toContain('self.b = child.data');
		// Connections referencing inner gone
		expect(result).not.toContain('inner.a');
		expect(result).not.toContain('inner.b');

		// child promoted into outer
		expect(result).toContain('  child = Debug');
		// outer group still intact
		expect(result).toContain('outer = Group(x: String) -> (y: String)');
		// sibling untouched
		expect(result).toContain('  sibling = Text { value: "hi" }');
		// outer's own self-connections still there
		expect(result).toContain('self.y = sibling.output');
	});
});

describe('removeNode precision', () => {
	it('only removes the node and its connections, nothing else', () => {
		const code = `# Project: Test

a = Text { value: "hello" }
b = ExecPython(input: String) -> (output: String) {
  code: "return {}"
  label: "Worker"
}
c = Debug {}

c.data = b.output
b.input = a.output`;

		const result = removeNode(code, 'b');

		// b and all its config gone
		expect(result).not.toContain('b = ExecPython');
		expect(result).not.toContain('code: "return {}"');
		expect(result).not.toContain('label: "Worker"');
		// Connections referencing b gone
		expect(result).not.toContain('c.data = b.output');
		expect(result).not.toContain('b.input = a.output');

		// a and c completely untouched
		expect(result).toContain('a = Text { value: "hello" }');
		expect(result).toContain('c = Debug {}');
		// Project header untouched
		expect(result).toContain('# Project: Test');
	});

	it('removing a node inside a group does not affect the group or siblings', () => {
		const code = `# Project: Test

grp = Group(data: String) -> (result: String) {
  a = Text { value: "keep" }
  target = Debug {}
  b = ExecPython { code: "x" }

  target.data = a.output
  b.input = target.data
  a.output = self.data
  self.result = b.output
}`;

		const result = removeNode(code, 'grp.target');

		// target gone
		expect(result).not.toContain('target = Debug');
		// Connections referencing target gone
		expect(result).not.toContain('target.data');

		// Group still intact
		expect(result).toContain('grp = Group(data: String) -> (result: String)');
		// Siblings untouched
		expect(result).toContain('a = Text { value: "keep" }');
		expect(result).toContain('b = ExecPython { code: "x" }');
		// Connections not involving target preserved
		expect(result).toContain('a.output = self.data');
		expect(result).toContain('self.result = b.output');
	});
});

describe('updateNodePorts precision', () => {
	it('only changes the port signature, config and surrounding code untouched', () => {
		const code = `# Project: Test

before = Text { value: "hi" }

worker = ExecPython(data: String, ctx: String?) -> (result: String) {
  code: "return {}"
  label: "My Worker"
}

after = Debug {}
after.data = worker.result
worker.data = before.output`;

		const result = updateNodePorts(code, 'worker',
			[{ name: 'input', portType: 'Number', required: true }],
			[{ name: 'output', portType: 'String' }],
		);

		// New ports present
		expect(result).toContain('input: Number');
		expect(result).toContain('output: String');
		// Old ports gone from signature
		expect(result).not.toContain('data: String');
		expect(result).not.toContain('ctx: String');
		// Config preserved exactly
		expect(result).toContain('code: "return {}"');
		expect(result).toContain('label: "My Worker"');
		// Surrounding nodes untouched
		expect(result).toContain('before = Text { value: "hi" }');
		expect(result).toContain('after = Debug {}');
		// Orphaned connections removed (result port renamed)
		expect(result).not.toContain('worker.result');
		expect(result).not.toContain('worker.data');
	});
});

// ── addNode ─────────────────────────────────────────────────────────────────

describe('addNode', () => {
	it('adds a node at top level', () => {
		const code = `# Project: Test

a = Debug {}`;
		const result = addNode(code, 'Text', 'b');
		expect(result).toContain('b = Text {}');
	});

	it('adds a node inside a group', () => {
		const code = `# Project: Test

grp = Group() -> () {
  a = Debug {}
}`;
		const result = addNode(code, 'Text', 'b', 'grp');
		expect(result).toContain('  b = Text {}');
	});
});

// ── addGroup ────────────────────────────────────────────────────────────────

describe('addGroup', () => {
	it('adds a group at top level', () => {
		const code = `# Project: Test

a = Debug {}`;
		const result = addGroup(code, 'myGroup');
		expect(result).toContain('myGroup = Group() -> () {}');
	});

	it('adds a group inside another group', () => {
		const code = `# Project: Test

outer = Group() -> () {
  a = Debug {}
}`;
		const result = addGroup(code, 'inner', 'outer');
		expect(result).toContain('  inner = Group() -> () {}');
	});
});

// ── addEdge / removeEdge ────────────────────────────────────────────────────

describe('addEdge', () => {
	it('adds an edge at top level', () => {
		const code = `# Project: Test

a = Text {}
b = Debug {}`;
		const result = addEdge(code, 'a', 'output', 'b', 'data');
		expect(result).toContain('b.data = a.output');
	});

	it('adds an edge inside a group', () => {
		const code = `# Project: Test

grp = Group() -> () {
  a = Text {}
  b = Debug {}
}`;
		const result = addEdge(code, 'a', 'output', 'b', 'data', 'grp');
		expect(result).toContain('  b.data = a.output');
	});
});

describe('removeEdge', () => {
	it('removes an edge', () => {
		const code = `# Project: Test

a = Text {}
b = Debug {}

b.data = a.output`;
		const result = removeEdge(code, 'a', 'output', 'b', 'data');
		expect(result).not.toContain('b.data = a.output');
		expect(result).toContain('a = Text');
		expect(result).toContain('b = Debug');
	});
});

// ── updateNodeConfig ────────────────────────────────────────────────────────

describe('updateNodeConfig', () => {
	it('adds a config field to a node', () => {
		const code = `a = Text {}`;
		const result = updateNodeConfig(code, 'a', 'value', 'hello');
		expect(result).toContain('value: "hello"');
	});

	it('updates an existing config field', () => {
		const code = `a = Text {
  value: "old"
}`;
		const result = updateNodeConfig(code, 'a', 'value', 'new');
		expect(result).toContain('value: "new"');
		expect(result).not.toContain('old');
	});

	it('removes a config field with undefined', () => {
		const code = `a = Text {
  value: "hello"
  label: "test"
}`;
		const result = updateNodeConfig(code, 'a', 'value', undefined);
		expect(result).not.toContain('value:');
		expect(result).toContain('label: "test"');
	});

	it('converts one-liner to multi-line when adding config', () => {
		const code = `a = Text { value: "hi" }`;
		const result = updateNodeConfig(code, 'a', 'label', 'test');
		// Should not still be a one-liner
		expect(result).toContain('label: "test"');
	});

	it('handles heredoc values', () => {
		const code = `a = ExecPython {}`;
		const result = updateNodeConfig(code, 'a', 'code', 'line1\nline2');
		expect(result).toContain('```');
		expect(result).toContain('line1');
		expect(result).toContain('line2');
	});

	it('updates heredoc values without duplication', () => {
		const code = `a = ExecPython {
  code: \`\`\`
line1
line2
\`\`\`
}`;
		const result = updateNodeConfig(code, 'a', 'code', 'updated\ncontent');
		const backtickCount = (result.match(/```/g) || []).length;
		expect(backtickCount).toBe(2);
		expect(result).toContain('updated');
		expect(result).not.toContain('line1');
	});

	it('handles config field in node inside a group', () => {
		const code = `grp = Group() -> () {
  a = Text {
    value: "old"
  }
}`;
		const result = updateNodeConfig(code, 'grp.a', 'value', 'new');
		expect(result).toContain('value: "new"');
		expect(result).not.toContain('old');
	});
});

// ── updateNodeLabel ─────────────────────────────────────────────────────────

describe('updateNodeLabel', () => {
	it('adds a label to a node', () => {
		const code = `a = Text {
  value: "hello"
}`;
		const result = updateNodeLabel(code, 'a', 'My Text');
		expect(result).toContain('label: "My Text"');
	});

	it('removes a label', () => {
		const code = `a = Text {
  label: "Old"
  value: "hello"
}`;
		const result = updateNodeLabel(code, 'a', null);
		expect(result).not.toContain('label:');
		expect(result).toContain('value: "hello"');
	});
});

// ── renameGroup ─────────────────────────────────────────────────────────────

describe('renameGroup', () => {
	it('renames a group and updates connection references', () => {
		const code = `# Project: Test

grp = Group(data: String) -> (result: String) {
  worker = Debug {}
  worker.data = self.data
  self.result = worker.data
}

out = Debug {}
out.data = grp.result`;
		const result = renameGroup(code, 'grp', 'processing');
		expect(result).toContain('processing = Group');
		expect(result).not.toContain('grp = Group');
		expect(result).toContain('out.data = processing.result');
		expect(result).not.toContain('grp.result');
	});
});

// ── moveNodeScope ───────────────────────────────────────────────────────────

describe('moveNodeScope', () => {
	it('moves a node into a group', () => {
		const code = `# Project: Test

a = Text { value: "hello" }

grp = Group() -> () {
  b = Debug {}
}`;
		const result = moveNodeScope(code, 'a', 'grp');
		// a should now be inside grp (indented)
		expect(result).toContain('  a = Text');
		// a should not be at top level anymore
		const lines = result.split('\n');
		const aLine = lines.find(l => l.includes('a = Text'));
		expect(aLine).toBeDefined();
		expect(aLine!.startsWith('  ')).toBe(true);
	});

	it('moves a node out of a group to top level', () => {
		const code = `# Project: Test

grp = Group() -> () {
  a = Text { value: "hello" }
  b = Debug {}
}`;
		const result = moveNodeScope(code, 'grp.a', undefined);
		// a should be at top level (no indent)
		const lines = result.split('\n');
		const aLine = lines.find(l => l.includes('a = Text'));
		expect(aLine).toBeDefined();
		expect(aLine!.startsWith('a = Text')).toBe(true);
	});
});

// ── moveGroupScope ──────────────────────────────────────────────────────────

describe('moveGroupScope', () => {
	it('moves a group into another group', () => {
		const code = `# Project: Test

inner = Group() -> () {
  a = Debug {}
}

outer = Group() -> () {
  b = Debug {}
}`;
		const result = moveGroupScope(code, 'inner', 'outer');
		expect(result).toContain('  inner = Group');
	});
});

// ── updateNodePorts ─────────────────────────────────────────────────────────

describe('updateNodePorts', () => {
	it('adds ports to a bare node', () => {
		const code = `node = Debug`;
		const result = updateNodePorts(code, 'node',
			[{ name: 'data', portType: 'String', required: true }],
			[],
		);
		expect(result).toContain('data: String');
	});

	it('updates ports on a node with existing ports', () => {
		const code = `worker = ExecPython(data: String) -> (result: String) {
  code: "return {}"
}`;
		const result = updateNodePorts(code, 'worker',
			[{ name: 'input', portType: 'Number', required: true }],
			[{ name: 'output', portType: 'String' }],
		);
		expect(result).toContain('input: Number');
		expect(result).toContain('output: String');
		expect(result).not.toContain('data: String');
		expect(result).not.toContain('result: String');
	});

	it('removes orphaned connections when ports are removed', () => {
		const code = `# Project: Test

worker = ExecPython(data: String) -> (result: String) {}

out = Debug {}
out.data = worker.result`;
		const result = updateNodePorts(code, 'worker',
			[{ name: 'data', portType: 'String', required: true }],
			[{ name: 'newOut', portType: 'String' }],
		);
		// result port was removed, connection should be gone
		expect(result).not.toContain('worker.result');
	});

	it('handles multiline port signatures', () => {
		const code = `worker = ExecPython(
    data: String,
    context: String?
) -> (
    result: String,
    score: Number?
) {
    code: "return {}"
}`;
		const result = updateNodePorts(code, 'worker',
			[{ name: 'input', portType: 'String', required: true }],
			[{ name: 'output', portType: 'String' }],
		);
		expect(result).toContain('input: String');
		expect(result).toContain('output: String');
		expect(result).not.toContain('data: String');
		expect(result).toContain('code: "return {}"');
	});

	it('preserves config block when updating ports', () => {
		const code = `worker = ExecPython(data: String) -> (result: String) {
  code: "return {}"
  label: "Worker"
}`;
		const result = updateNodePorts(code, 'worker',
			[{ name: 'input', portType: 'Number', required: true }],
			[{ name: 'output', portType: 'String' }],
		);
		expect(result).toContain('code: "return {}"');
		expect(result).toContain('label: "Worker"');
	});
});

// ── updateGroupPorts ────────────────────────────────────────────────────────

describe('updateGroupPorts', () => {
	it('updates ports on a group', () => {
		const code = `grp = Group(data: String) -> (result: String) {
  worker = Debug {}
}`;
		const result = updateGroupPorts(code, 'grp',
			[{ name: 'input', portType: 'Number', required: true }],
			[{ name: 'output', portType: 'String' }],
		);
		expect(result).toContain('input: Number');
		expect(result).toContain('output: String');
		expect(result).not.toContain('data: String');
	});

	it('removes orphaned self connections inside the group', () => {
		const code = `grp = Group(data: String) -> (result: String) {
  worker = Debug {}
  worker.input = self.data
  self.result = worker.output
}`;
		const result = updateGroupPorts(code, 'grp',
			[{ name: 'newIn', portType: 'String', required: true }],
			[{ name: 'newOut', portType: 'String' }],
		);
		// Old self connections should be removed
		expect(result).not.toContain('self.data');
		expect(result).not.toContain('self.result');
	});

	it('removes orphaned external connections to the group', () => {
		const code = `# Project: Test

grp = Group(data: String) -> (result: String) {
  worker = Debug {}
}

out = Debug {}
out.data = grp.result
grp.data = out.data`;
		const result = updateGroupPorts(code, 'grp',
			[{ name: 'newIn', portType: 'String', required: true }],
			[{ name: 'newOut', portType: 'String' }],
		);
		expect(result).not.toContain('grp.result');
		expect(result).not.toContain('grp.data');
	});
});

// ── Edge removal precision ──────────────────────────────────────────────────

describe('removeEdge precision', () => {
	it('removes only the targeted edge, not other edges between same nodes', () => {
		const code = `# Project: Test

a = ExecPython(x: String) -> (out1: String, out2: String) {}
b = Debug {}

b.data = a.out1
b.extra = a.out2`;
		const result = removeEdge(code, 'a', 'out1', 'b', 'data');
		expect(result).not.toContain('b.data = a.out1');
		expect(result).toContain('b.extra = a.out2');
	});

	it('removes edge inside a group without affecting edges outside', () => {
		const code = `# Project: Test

grp = Group(data: String) -> (result: String) {
  a = Text {}
  b = Debug {}
  b.data = a.output
  b.extra = self.data
}

outside = Debug {}
outside.data = grp.result`;
		const result = removeEdge(code, 'a', 'output', 'b', 'data');
		expect(result).not.toContain('b.data = a.output');
		// Other edges untouched
		expect(result).toContain('b.extra = self.data');
		expect(result).toContain('outside.data = grp.result');
	});

	it('removes self-connection edge inside a group', () => {
		const code = `grp = Group(data: String) -> (result: String) {
  worker = Debug {}
  worker.input = self.data
  self.result = worker.output
}`;
		const result = removeEdge(code, 'self', 'data', 'worker', 'input');
		expect(result).not.toContain('worker.input = self.data');
		expect(result).toContain('self.result = worker.output');
	});
});

// ── Port update with complex types ─────────────────────────────────────────

describe('updateNodePorts complex types', () => {
	it('handles union types', () => {
		const code = `node = ExecPython(data: String) -> (result: String) {}`;
		const result = updateNodePorts(code, 'node',
			[{ name: 'data', portType: 'String | Number', required: true }],
			[{ name: 'result', portType: 'String | Null' }],
		);
		expect(result).toContain('data: String | Number');
		expect(result).toContain('result: String | Null');
	});

	it('handles optional ports', () => {
		const code = `node = ExecPython(data: String) -> (result: String) {}`;
		const result = updateNodePorts(code, 'node',
			[
				{ name: 'required_in', portType: 'String', required: true },
				{ name: 'optional_in', portType: 'String', required: false },
			],
			[{ name: 'result', portType: 'String' }],
		);
		expect(result).toContain('required_in: String');
		expect(result).toContain('optional_in: String?');
	});

	it('handles List and Dict types', () => {
		const code = `node = ExecPython {}`;
		const result = updateNodePorts(code, 'node',
			[{ name: 'items', portType: 'List[String]', required: true }],
			[{ name: 'map', portType: 'Dict[String, Number]' }],
		);
		expect(result).toContain('items: List[String]');
		expect(result).toContain('map: Dict[String, Number]');
	});

	it('handles nested List types', () => {
		const code = `node = ExecPython {}`;
		const result = updateNodePorts(code, 'node',
			[{ name: 'nested', portType: 'List[List[String]]', required: true }],
			[],
		);
		expect(result).toContain('nested: List[List[String]]');
	});
});

// ── Post-config output ports ────────────────────────────────────────────────

describe('post-config output ports', () => {
	it('updateNodePorts handles node with post-config outputs', () => {
		const code = `# Project: Test

node = LlmInference {
  parseJson: true
} -> (
  summary: String,
  score: Number?
)

out = Debug {}
out.data = node.summary`;
		const result = updateNodePorts(code, 'node',
			[],
			[{ name: 'response', portType: 'String' }],
		);
		// New port should be present
		expect(result).toContain('response: String');
		// Old post-config ports should be gone
		expect(result).not.toContain('summary: String');
		expect(result).not.toContain('score: Number');
		// Config preserved
		expect(result).toContain('parseJson: true');
		// Orphaned connection removed
		expect(result).not.toContain('node.summary');
	});

	it('updateNodePorts handles post-config outputs on same line as closing brace', () => {
		const code = `node = LlmInference {
  model: "gpt-4"
} -> (result: String)

b = Debug {}
b.data = node.result`;
		const result = updateNodePorts(code, 'node',
			[],
			[{ name: 'output', portType: 'String' }],
		);
		expect(result).toContain('output: String');
		expect(result).not.toContain('result: String');
		expect(result).toContain('model: "gpt-4"');
	});

	it('removeNode handles node with post-config outputs', () => {
		const code = `# Project: Test

before = Text {}

node = LlmInference {
  parseJson: true
} -> (
  summary: String
)

after = Debug {}
after.data = node.summary`;
		const result = removeNode(code, 'node');
		expect(result).not.toContain('LlmInference');
		expect(result).not.toContain('parseJson');
		expect(result).not.toContain('summary');
		expect(result).not.toContain('node.summary');
		expect(result).toContain('before = Text');
		expect(result).toContain('after = Debug');
	});

	it('updateNodePorts with both pre and post-config outputs', () => {
		const code = `node = ExecPython(data: String) -> (result: String) {
  code: "return {}"
} -> (extra: Number)

out = Debug {}
out.data = node.result
out.num = node.extra`;
		const result = updateNodePorts(code, 'node',
			[{ name: 'input', portType: 'String', required: true }],
			[{ name: 'output', portType: 'String' }],
		);
		expect(result).toContain('input: String');
		expect(result).toContain('output: String');
		// Both old pre and post outputs gone
		expect(result).not.toContain('result: String');
		expect(result).not.toContain('extra: Number');
		// Config preserved
		expect(result).toContain('code: "return {}"');
		// Orphaned connections removed
		expect(result).not.toContain('node.result');
		expect(result).not.toContain('node.extra');
	});
});

// ── Group port operations ───────────────────────────────────────────────────

describe('updateGroupPorts precision', () => {
	it('preserves group body when updating ports', () => {
		const code = `grp = Group(data: String) -> (result: String) {
  a = Text { value: "hello" }
  b = ExecPython(input: String) -> (output: String) {
    code: "return {}"
  }

  b.input = a.output
  b.input = self.data
  self.result = b.output
}`;
		const result = updateGroupPorts(code, 'grp',
			[{ name: 'input', portType: 'Number', required: true }],
			[{ name: 'output', portType: 'String' }],
		);
		// New ports
		expect(result).toContain('input: Number');
		expect(result).toContain('output: String');
		// Old ports gone from signature
		expect(result).not.toMatch(/Group\(data: String\)/);
		// Children fully preserved
		expect(result).toContain('a = Text { value: "hello" }');
		expect(result).toContain('code: "return {}"');
		// Internal non-self connections preserved
		expect(result).toContain('b.input = a.output');
		// Old self connections referencing removed ports gone
		expect(result).not.toContain('self.data');
		expect(result).not.toContain('self.result');
	});

	it('handles group with multiline port signature', () => {
		const code = `grp = Group(
    data: String,
    context: String?
) -> (
    result: String,
    score: Number?
) {
  worker = Debug {}
}`;
		const result = updateGroupPorts(code, 'grp',
			[{ name: 'input', portType: 'String', required: true }],
			[{ name: 'output', portType: 'String' }],
		);
		expect(result).toContain('input: String');
		expect(result).toContain('output: String');
		expect(result).not.toContain('data: String');
		expect(result).not.toContain('context: String');
		expect(result).not.toContain('score: Number');
		expect(result).toContain('worker = Debug');
	});

	it('updates ports with union types on a group', () => {
		const code = `grp = Group(data: String) -> (result: String) {
  worker = Debug {}
}`;
		const result = updateGroupPorts(code, 'grp',
			[{ name: 'data', portType: 'String | Number', required: true }],
			[{ name: 'result', portType: 'String | Null' }],
		);
		expect(result).toContain('data: String | Number');
		expect(result).toContain('result: String | Null');
	});

	it('renaming group ports removes old self-connections but keeps new ones valid', () => {
		const code = `grp = Group(old_in: String) -> (old_out: String) {
  worker = Debug {}
  worker.data = self.old_in
  self.old_out = worker.output
}

ext = Debug {}
ext.data = grp.old_out
grp.old_in = ext.data`;
		const result = updateGroupPorts(code, 'grp',
			[{ name: 'new_in', portType: 'String', required: true }],
			[{ name: 'new_out', portType: 'String' }],
		);
		// Old port names gone everywhere
		expect(result).not.toContain('old_in');
		expect(result).not.toContain('old_out');
		// New ports in signature
		expect(result).toContain('new_in: String');
		expect(result).toContain('new_out: String');
	});
});

// ── renameGroup precision ───────────────────────────────────────────────────

describe('renameGroup precision', () => {
	it('does not rename identifiers that happen to contain the group name', () => {
		const code = `# Project: Test

grp = Group(data: String) -> (result: String) {
  worker = Debug {}
}

grp_extra = Text { value: "not a reference" }
out = Debug {}
out.data = grp.result`;
		const result = renameGroup(code, 'grp', 'processing');
		expect(result).toContain('processing = Group');
		expect(result).toContain('out.data = processing.result');
		// grp_extra should NOT be renamed, it's a different identifier
		expect(result).toContain('grp_extra = Text');
	});

	it('renames all connection references in multiple scopes', () => {
		const code = `# Project: Test

grp = Group(x: String) -> (y: String) {
  a = Debug {}
  a.data = self.x
  self.y = a.output
}

b = Debug {}
c = Debug {}
b.data = grp.y
grp.x = c.output`;
		const result = renameGroup(code, 'grp', 'pipeline');
		expect(result).toContain('pipeline = Group');
		expect(result).toContain('b.data = pipeline.y');
		expect(result).toContain('pipeline.x = c.output');
		// self connections inside are unchanged (they use self, not group name)
		expect(result).toContain('a.data = self.x');
		expect(result).toContain('self.y = a.output');
	});
});

// ── Edge operations with complex topologies ─────────────────────────────────

describe('edge operations in complex topologies', () => {
	it('addEdge inside nested group', () => {
		const code = `outer = Group() -> () {
  inner = Group() -> () {
    a = Text {}
    b = Debug {}
  }
}`;
		const result = addEdge(code, 'a', 'output', 'b', 'data', 'inner');
		expect(result).toContain('b.data = a.output');
	});

	it('removeNode cleans up all edges to/from the node in a complex graph', () => {
		const code = `# Project: Test

a = Text {}
b = ExecPython(x: String, y: String) -> (out: String) {}
c = Debug {}
d = Debug {}

b.x = a.output
b.y = a.output
c.data = b.out
d.data = b.out`;
		const result = removeNode(code, 'b');
		expect(result).not.toContain('b.x');
		expect(result).not.toContain('b.y');
		expect(result).not.toContain('c.data = b.out');
		expect(result).not.toContain('d.data = b.out');
		expect(result).toContain('a = Text');
		expect(result).toContain('c = Debug');
		expect(result).toContain('d = Debug');
	});
});

// ── updateProjectMeta ───────────────────────────────────────────────────────

describe('updateProjectMeta', () => {
	it('updates project name', () => {
		const code = `# Project: Old
# Description: A project`;
		const result = updateProjectMeta(code, 'New');
		expect(result).toContain('# Project: New');
	});

	it('updates project description', () => {
		const code = `# Project: Test
# Description: Old desc`;
		const result = updateProjectMeta(code, undefined, 'New desc');
		expect(result).toContain('# Description: New desc');
	});

	it('adds description if missing', () => {
		const code = `# Project: Test`;
		const result = updateProjectMeta(code, undefined, 'Added desc');
		expect(result).toContain('# Description: Added desc');
	});
});

describe('updateNodeConfig with connection-line literals', () => {
	it('rewrites the external connection-line literal when it is the source-last write', () => {
		const code = `n = Template { label: "g" }
n.template = "original"`;
		const out = updateNodeConfig(code, 'n', 'template', 'updated');
		// The external literal should be rewritten, not the inline block
		expect(out).toContain('n.template = "updated"');
		// The inline block should NOT have been given a new template field
		expect(out).not.toMatch(/Template\s*\{[^}]*template:/);
	});

	it('rewrites the inline field when the inline field is later than the external literal', () => {
		const code = `n.template = "original"
n = Template { template: "inline" }`;
		const out = updateNodeConfig(code, 'n', 'template', 'updated');
		// Inline field is later in source, so it should be rewritten
		expect(out).toMatch(/Template\s*\{\s*template:\s*"updated"\s*\}/);
		// The earlier connection-line literal is untouched (its value is overridden at runtime anyway)
		expect(out).toContain('n.template = "original"');
	});

	it('rewrites the inline field when only the inline field exists', () => {
		const code = `n = Template {
  template: "original"
}`;
		const out = updateNodeConfig(code, 'n', 'template', 'updated');
		expect(out).toMatch(/template:\s*"updated"/);
		expect(out).not.toContain('n.template = ');
	});

	it('rewrites the connection-line literal when only it exists', () => {
		const code = `n = Template { label: "g" }
n.template = "original"`;
		const out = updateNodeConfig(code, 'n', 'template', 'new value');
		expect(out).toContain('n.template = "new value"');
	});

	it('removes only the effective source (external literal) when value is null', () => {
		const code = `n = Template { template: "fallback" }
n.template = "active"`;
		const out = updateNodeConfig(code, 'n', 'template', null);
		// External literal line should be gone
		expect(out).not.toContain('n.template = "active"');
		// Fallback inline field stays
		expect(out).toContain('template: "fallback"');
	});
});

	it('rewrites a multi-line triple-backtick connection-line literal in place', () => {
		const code = `n = ExecPython { label: "run" }
n.code = \`\`\`
return 1
\`\`\``;
		const out = updateNodeConfig(code, 'n', 'code', 'return 2\nreturn 3');
		// The multi-line block should be replaced.
		expect(out).toContain('return 2');
		expect(out).toContain('return 3');
		expect(out).not.toContain('return 1');
		// There should still be exactly one `n.code = ` line.
		const matches = out.split('\n').filter(l => l.trim().startsWith('n.code ='));
		expect(matches.length).toBe(1);
	});

	it('rewrites a multi-line JSON connection-line literal in place', () => {
		const code = `n = Template { template: "hi" }
n.data = {
  "a": 1,
  "b": "two"
}`;
		const out = updateNodeConfig(code, 'n', 'data', { a: 99 });
		expect(out).toMatch(/n\.data = .*99/s);
		expect(out).not.toContain('"two"');
	});

	it('removes a multi-line triple-backtick connection-line literal when value is null', () => {
		const code = `n = ExecPython { label: "run" }
n.code = \`\`\`
return 1
\`\`\``;
		const out = updateNodeConfig(code, 'n', 'code', null);
		expect(out).not.toContain('n.code = ');
		expect(out).not.toContain('return 1');
		expect(out).not.toContain('\`\`\`');
	});
